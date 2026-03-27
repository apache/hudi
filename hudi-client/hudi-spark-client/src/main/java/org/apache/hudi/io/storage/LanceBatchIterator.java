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

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.LanceArrowColumnVector;
import org.lance.file.LanceFileReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator that returns {@link ColumnarBatch} directly from Lance files without
 * decomposing to individual rows. Used for vectorized/columnar batch reading
 * in Spark's COW base-file-only read path.
 *
 * <p>Unlike {@link LanceRecordIterator} which extracts rows one by one,
 * this iterator preserves the columnar format for zero-copy batch processing.
 *
 * <p>Manages the lifecycle of:
 * <ul>
 *   <li>BufferAllocator - Arrow memory management</li>
 *   <li>LanceFileReader - Lance file handle</li>
 *   <li>ArrowReader - Arrow batch reader</li>
 * </ul>
 */
public class LanceBatchIterator implements Iterator<ColumnarBatch>, Closeable {
  private final BufferAllocator allocator;
  private final LanceFileReader lanceReader;
  private final ArrowReader arrowReader;
  private final String path;

  private ColumnVector[] columnVectors;
  private ColumnarBatch currentBatch;
  private boolean nextBatchLoaded = false;
  private boolean finished = false;
  private boolean closed = false;

  /**
   * Creates a new Lance batch iterator.
   *
   * @param allocator   Arrow buffer allocator for memory management
   * @param lanceReader Lance file reader
   * @param arrowReader Arrow reader for batch reading
   * @param path        File path (for error messages)
   */
  public LanceBatchIterator(BufferAllocator allocator,
                            LanceFileReader lanceReader,
                            ArrowReader arrowReader,
                            String path) {
    this.allocator = allocator;
    this.lanceReader = lanceReader;
    this.arrowReader = arrowReader;
    this.path = path;
  }

  @Override
  public boolean hasNext() {
    if (finished) {
      return false;
    }
    if (nextBatchLoaded) {
      return true;
    }

    try {
      if (arrowReader.loadNextBatch()) {
        VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();

        // Create column vector wrappers once and reuse across batches
        // (ArrowReader reuses the same VectorSchemaRoot)
        if (columnVectors == null) {
          columnVectors = root.getFieldVectors().stream()
              .map(LanceArrowColumnVector::new)
              .toArray(ColumnVector[]::new);
        }

        currentBatch = new ColumnarBatch(columnVectors, root.getRowCount());
        nextBatchLoaded = true;
        return true;
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to read next batch from Lance file: " + path, e);
    }

    finished = true;
    return false;
  }

  @Override
  public ColumnarBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more batches available");
    }
    nextBatchLoaded = false;
    return currentBatch;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;

    IOException arrowException = null;
    Exception lanceException = null;

    if (currentBatch != null) {
      currentBatch.close();
      currentBatch = null;
    }

    if (arrowReader != null) {
      try {
        arrowReader.close();
      } catch (IOException e) {
        arrowException = e;
      }
    }

    if (lanceReader != null) {
      try {
        lanceReader.close();
      } catch (Exception e) {
        lanceException = e;
      }
    }

    if (allocator != null) {
      allocator.close();
    }

    if (arrowException != null) {
      throw new HoodieIOException("Failed to close Arrow reader", arrowException);
    }
    if (lanceException != null) {
      throw new HoodieException("Failed to close Lance reader", lanceException);
    }
  }
}
