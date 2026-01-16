/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io.lance;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.memory.HoodieArrowAllocator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.file.LanceFileWriter;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;

/**
 * Base class for Hudi Lance file writers supporting different record types.
 *
 * This class handles common Lance file operations including:
 * - LanceFileWriter lifecycle management
 * - BufferAllocator management
 * - Record buffering and batch flushing
 * - File size checks
 *
 * Subclasses must implement type-specific conversion to Arrow format.
 *
 * @param <R> The record type (e.g., GenericRecord, InternalRow)
 */
@NotThreadSafe
public abstract class HoodieBaseLanceWriter<R> implements Closeable {
  /** Memory size for data write operations: 120MB */
  private static final long LANCE_DATA_ALLOCATOR_SIZE = 120 * 1024 * 1024;

  protected static final int DEFAULT_BATCH_SIZE = 1000;
  protected final HoodieStorage storage;
  protected final StoragePath path;
  protected final BufferAllocator allocator;
  protected final int batchSize;
  protected long writtenRecordCount = 0;
  protected int currentBatchSize = 0;
  protected VectorSchemaRoot root;
  protected ArrowWriter<R> arrowWriter;

  private LanceFileWriter writer;

  /**
   * Constructor for base Lance writer.
   *
   * @param storage HoodieStorage instance
   * @param path Path where Lance file will be written
   * @param batchSize Number of records to buffer before flushing to Lance
   */
  protected HoodieBaseLanceWriter(HoodieStorage storage, StoragePath path, int batchSize) {
    this.storage = storage;
    this.path = path;
    this.allocator = HoodieArrowAllocator.newChildAllocator(
        getClass().getSimpleName() + "-data-" + path.getName(), LANCE_DATA_ALLOCATOR_SIZE);
    this.batchSize = batchSize;
  }

  /**
   * Create and initialize the arrow writer for writing records to VectorSchemaRoot.
   * Called once during lazy initialization when the first record is written.
   *
   * @param root The VectorSchemaRoot to write into
   * @return An arrow writer implementation that writes records of type R to the root
   */
  protected abstract ArrowWriter<R> createArrowWriter(VectorSchemaRoot root);

  /**
   * Get the Arrow schema for this writer.
   * Subclasses must provide the Arrow schema corresponding to their record type.
   *
   * @return Arrow schema
   */
  protected abstract Schema getArrowSchema();

  /**
   * Write a single record. Records are buffered and flushed in batches.
   *
   * @param record Record to write
   * @throws IOException if write fails
   */
  public void write(R record) throws IOException {
    // Lazy initialization on first write
    if (writer == null) {
      initializeWriter();
    }
    if (root == null) {
      root = VectorSchemaRoot.create(getArrowSchema(), allocator);
    }
    if (arrowWriter == null) {
      arrowWriter = createArrowWriter(root);
    }

    // Reset arrow writer at the start of each new batch
    if (currentBatchSize == 0) {
      arrowWriter.reset();
    }

    arrowWriter.write(record);
    currentBatchSize++;
    writtenRecordCount++;

    // Flush when batch is full
    if (currentBatchSize >= batchSize) {
      flushBatch();
    }
  }

  /**
   * Get the total number of records written so far.
   *
   * @return Number of records written
   */
  public long getWrittenRecordCount() {
    return writtenRecordCount;
  }

  /**
   * Close the writer, flushing any remaining buffered records.
   *
   * @throws IOException if close fails
   */
  @Override
  public void close() throws IOException {
    Exception primaryException = null;

    // 1. Flush remaining records
    try {
      // Flush any remaining records in current batch
      if (currentBatchSize > 0) {
        flushBatch();
      }

      // Ensure writer is initialized even if no data was written
      // This creates an empty Lance file with just schema metadata
      if (writer == null && root == null) {
        initializeWriter();
        root = VectorSchemaRoot.create(getArrowSchema(), allocator);
        root.setRowCount(0);
        writer.write(root);
      }
    } catch (Exception e) {
      primaryException = e;
    }

    // Close Lance writer
    if (writer != null) {
      try {
        writer.close();
      } catch (Exception e) {
        if (primaryException == null) {
          primaryException = e;
        } else {
          primaryException.addSuppressed(e);
        }
      }
    }

    // Close VectorSchemaRoot
    if (root != null) {
      try {
        root.close();
      } catch (Exception e) {
        if (primaryException == null) {
          primaryException = e;
        } else {
          primaryException.addSuppressed(e);
        }
      }
    }

    // Always close allocator
    try {
      allocator.close();
    } catch (Exception e) {
      if (primaryException == null) {
        primaryException = e;
      } else {
        primaryException.addSuppressed(e);
      }
    }

    if (primaryException != null) {
      throw new HoodieException("Failed to close Lance writer: " + path, primaryException);
    }
  }

  /**
   * Flush buffered records to Lance file.
   */
  private void flushBatch() throws IOException {
    if (currentBatchSize == 0) {
      return;  // Nothing to flush
    }

    // Finalize the arrow writer (sets row count on VectorSchemaRoot)
    arrowWriter.finish();

    // Write VectorSchemaRoot to Lance file
    writer.write(root);

    // Reset batch counter for next batch
    currentBatchSize = 0;
  }

  /**
   * Initialize LanceFileWriter (lazy initialization).
   */
  private void initializeWriter() throws IOException {
    writer = LanceFileWriter.open(path.toString(), allocator, null);
  }

  protected interface ArrowWriter<T> {
    void write(T row) throws IOException;

    void finish() throws IOException;

    void reset();
  }
}
