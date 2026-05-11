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

import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.lance.file.LanceFileReader;
import org.lance.spark.vectorized.LanceArrowColumnVector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Iterator for reading Lance files and converting Arrow batches to Spark {@link UnsafeRow}s.
 * Used by both Hudi's internal Lance reader and Spark datasource integration.
 *
 * <p>The iterator manages the lifecycle of:
 * <ul>
 *   <li>BufferAllocator - Arrow memory management</li>
 *   <li>LanceFileReader - Lance file handle</li>
 *   <li>ArrowReader - Arrow batch reader</li>
 *   <li>ColumnarBatch - Current batch being iterated</li>
 * </ul>
 *
 * <p>An optional {@link BlobDescriptorTransform} can be composed in to rewrite BLOB columns
 * in DESCRIPTOR mode.
 */
public final class LanceRecordIterator implements ClosableIterator<UnsafeRow> {
  private final BufferAllocator allocator;
  private final LanceFileReader lanceReader;
  private final ArrowReader arrowReader;
  private final StructType sparkSchema;
  private final UnsafeProjection projection;
  private final String path;
  private final BlobDescriptorTransform blobTransform;

  private ColumnarBatch currentBatch;
  private Iterator<InternalRow> rowIterator;
  private ColumnVector[] columnVectors;
  /** Row index within the current batch; only used by {@link BlobDescriptorTransform}. */
  private int rowIdInBatch;
  private boolean closed = false;

  /**
   * Creates a new Lance record iterator without blob transform (CONTENT mode or non-blob reads).
   */
  public LanceRecordIterator(BufferAllocator allocator,
                             LanceFileReader lanceReader,
                             ArrowReader arrowReader,
                             StructType schema,
                             String path) {
    this(allocator, lanceReader, arrowReader, schema, path, null);
  }

  /**
   * Creates a new Lance record iterator.
   *
   * @param allocator      Arrow buffer allocator for memory management
   * @param lanceReader    Lance file reader
   * @param arrowReader    Arrow reader for batch reading
   * @param schema         Spark schema for the records
   * @param path           File path (for error messages)
   * @param blobTransform  optional blob descriptor transform for DESCRIPTOR-mode reads; null for
   *                       CONTENT mode or non-blob reads
   */
  public LanceRecordIterator(BufferAllocator allocator,
                             LanceFileReader lanceReader,
                             ArrowReader arrowReader,
                             StructType schema,
                             String path,
                             BlobDescriptorTransform blobTransform) {
    this.allocator = allocator;
    this.lanceReader = lanceReader;
    this.arrowReader = arrowReader;
    this.sparkSchema = schema;
    this.projection = UnsafeProjection.create(schema);
    this.path = path;
    this.blobTransform = blobTransform;
  }

  @Override
  public boolean hasNext() {
    if (rowIterator != null && rowIterator.hasNext()) {
      return true;
    }

    if (currentBatch != null) {
      currentBatch.close();
      currentBatch = null;
    }

    // Try to load next batch. Loop so zero-row batches (legitimately returned e.g. after
    // filter pushdown) don't silently terminate iteration and drop subsequent non-empty batches.
    try {
      while (arrowReader.loadNextBatch()) {
        VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();

        // Build ColumnVector[] in Spark-schema order by looking each field up by name;
        // lance-spark 0.4.0's VectorSchemaRoot may return the file's on-disk order, which
        // would misalign the UnsafeProjection. Cached on the first batch and reused thereafter.
        if (columnVectors == null) {
          buildColumnVectors(root);
        }

        currentBatch = new ColumnarBatch(columnVectors, root.getRowCount());
        rowIterator = currentBatch.rowIterator();
        rowIdInBatch = 0;
        if (rowIterator.hasNext()) {
          return true;
        }
        currentBatch.close();
        currentBatch = null;
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
    if (blobTransform != null) {
      return blobTransform.transformRow(row, rowIdInBatch++, columnVectors, projection);
    }
    rowIdInBatch++;
    return projection.apply(row).copy();
  }

  private void buildColumnVectors(VectorSchemaRoot root) {
    List<FieldVector> fieldVectors = root.getFieldVectors();
    Map<String, FieldVector> byName = new HashMap<>(fieldVectors.size() * 2);
    for (FieldVector fv : fieldVectors) {
      byName.put(fv.getName(), fv);
    }
    StructField[] sparkFields = sparkSchema.fields();
    if (sparkFields.length != fieldVectors.size()) {
      throw new HoodieException("Lance batch column count " + fieldVectors.size()
          + " does not match expected Spark schema size " + sparkFields.length
          + " for file: " + path);
    }
    columnVectors = new ColumnVector[sparkFields.length];
    for (int i = 0; i < sparkFields.length; i++) {
      String name = sparkFields[i].name();
      FieldVector fv = byName.get(name);
      if (fv == null) {
        throw new HoodieException("Lance batch missing expected column '" + name
            + "' for file: " + path + "; available columns: " + byName.keySet());
      }
      columnVectors[i] = new LanceArrowColumnVector(fv);
    }
    if (blobTransform != null) {
      blobTransform.init(columnVectors, sparkSchema);
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    ColumnarBatch batch = currentBatch;
    currentBatch = null;
    LanceResourceCloser.closeAll(batch, arrowReader, lanceReader, allocator);
  }
}
