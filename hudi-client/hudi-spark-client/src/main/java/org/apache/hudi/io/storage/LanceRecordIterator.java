/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.storage;

import com.lancedb.lance.file.LanceFileReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.LanceArrowColumnVector;

import java.io.IOException;
import java.util.Iterator;

/**
 * Shared iterator implementation for reading Lance files and converting Arrow batches to Spark rows.
 * This iterator is used by both Hudi's internal Lance reader and Spark datasource integration.
 *
 * <p>The iterator manages the lifecycle of:
 * <ul>
 *   <li>BufferAllocator - Arrow memory management</li>
 *   <li>LanceFileReader - Lance file handle</li>
 *   <li>ArrowReader - Arrow batch reader</li>
 *   <li>ColumnarBatch - Current batch being iterated</li>
 * </ul>
 *
 * <p>Records are converted to {@link UnsafeRow} using {@link UnsafeProjection} for efficient
 * serialization and memory management.
 */
public class LanceRecordIterator implements ClosableIterator<UnsafeRow> {
  private final BufferAllocator allocator;
  private final LanceFileReader lanceReader;
  private final ArrowReader arrowReader;
  private final UnsafeProjection projection;
  private final String path;

  private ColumnarBatch currentBatch;
  private Iterator<InternalRow> rowIterator;
  private ColumnVector[] columnVectors;
  private boolean closed = false;

  /**
   * Creates a new Lance record iterator.
   *
   * @param allocator Arrow buffer allocator for memory management
   * @param lanceReader Lance file reader
   * @param arrowReader Arrow reader for batch reading
   * @param schema Spark schema for the records
   * @param path File path (for error messages)
   */
  public LanceRecordIterator(BufferAllocator allocator,
                             LanceFileReader lanceReader,
                             ArrowReader arrowReader,
                             StructType schema,
                             String path) {
    this.allocator = allocator;
    this.lanceReader = lanceReader;
    this.arrowReader = arrowReader;
    this.projection = UnsafeProjection.create(schema);
    this.path = path;
  }

  @Override
  public boolean hasNext() {
    // If we have records in current batch, return true
    if (rowIterator != null && rowIterator.hasNext()) {
      return true;
    }

    // Close previous batch before loading next
    if (currentBatch != null) {
      currentBatch.close();
      currentBatch = null;
    }

    // Try to load next batch
    try {
      if (arrowReader.loadNextBatch()) {
        VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();

        // Wrap each Arrow FieldVector in LanceArrowColumnVector for type-safe access
        // Cache the column wrappers on first batch and reuse for all subsequent batches
        if (columnVectors == null) {
          columnVectors = root.getFieldVectors().stream()
                  .map(LanceArrowColumnVector::new)
                  .toArray(ColumnVector[]::new);
        }

        // Create ColumnarBatch and keep it alive while iterating
        currentBatch = new ColumnarBatch(columnVectors, root.getRowCount());
        rowIterator = currentBatch.rowIterator();
        return rowIterator.hasNext();
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to read next batch from Lance file: " + path, e);
    }

    return false;
  }

  @Override
  public UnsafeRow next() {
    if (!hasNext()) {
      throw new IllegalStateException("No more records available");
    }
    InternalRow row = rowIterator.next();
    // Convert to UnsafeRow immediately while batch is still open
    return projection.apply(row).copy();
  }

  @Override
  public void close() {
    // Make close() idempotent - safe to call multiple times
    if (closed) {
      return;
    }
    closed = true;

    IOException arrowException = null;
    Exception lanceException = null;

    // Close current batch if exists
    if (currentBatch != null) {
      currentBatch.close();
      currentBatch = null;
    }

    // Close Arrow reader
    if (arrowReader != null) {
      try {
        arrowReader.close();
      } catch (IOException e) {
        arrowException = e;
      }
    }

    // Close Lance reader
    if (lanceReader != null) {
      try {
        lanceReader.close();
      } catch (Exception e) {
        lanceException = e;
      }
    }

    // Always close allocator
    if (allocator != null) {
      allocator.close();
    }

    // Throw any exceptions that occurred
    if (arrowException != null) {
      throw new HoodieIOException("Failed to close Arrow reader", arrowException);
    }
    if (lanceException != null) {
      throw new HoodieException("Failed to close Lance reader", lanceException);
    }
  }
}
