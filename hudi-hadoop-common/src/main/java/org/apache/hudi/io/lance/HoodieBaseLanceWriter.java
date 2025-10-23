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

import com.lancedb.lance.file.LanceFileWriter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
  protected static final int DEFAULT_BATCH_SIZE = 1000;
  protected final HoodieStorage storage;
  protected final StoragePath path;
  protected final BufferAllocator allocator;
  protected final List<R> bufferedRecords;
  protected final int batchSize;
  protected final long maxFileSize;
  protected long writtenRecordCount = 0;
  protected VectorSchemaRoot root;

  private LanceFileWriter writer;

  /**
   * Constructor for base Lance writer.
   *
   * @param storage HoodieStorage instance
   * @param path Path where Lance file will be written
   * @param batchSize Number of records to buffer before flushing to Lance
   * @param maxFileSize Maximum file size in bytes before rolling over to new file
   */
  protected HoodieBaseLanceWriter(HoodieStorage storage, StoragePath path, int batchSize, long maxFileSize) {
    this.storage = storage;
    this.path = path;
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    this.bufferedRecords = new ArrayList<>(batchSize);
    this.batchSize = batchSize;
    this.maxFileSize = maxFileSize;
  }

  /**
   * Populate the VectorSchemaRoot with buffered records.
   * Subclasses must implement type-specific conversion logic.
   * The VectorSchemaRoot field is reused across batches and managed by this base class.
   *
   * @param records List of records to convert
   */
  protected abstract void populateVectorSchemaRoot(List<R> records);

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
    bufferedRecords.add(record);
    writtenRecordCount++;

    if (bufferedRecords.size() >= batchSize) {
      flushBatch();
    }
  }

  /**
   * Check if writer can accept more records based on file size.
   * Uses filesystem-based size checking (similar to ORC/HFile approach).
   *
   * @return true if writer can accept more records, false if file size limit reached
   */
  public boolean canWrite() {
    //TODO will need to implement proper way to compute this
    return true;
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
    try {
      // Flush any remaining buffered records
      if (!bufferedRecords.isEmpty()) {
        flushBatch();
      }

      // Close Lance writer
      if (writer != null) {
        writer.close();
      }

      // Close VectorSchemaRoot
      if (root != null) {
        root.close();
      }
    } catch (Exception e) {
      throw new HoodieException("Failed to close Lance writer: " + path, e);
    } finally {
      // Always close allocator
      allocator.close();
    }
  }

  /**
   * Flush buffered records to Lance file.
   */
  private void flushBatch() throws IOException {
    if (bufferedRecords.isEmpty()) {
      return;
    }

    // Lazy initialization of writer and root
    if (writer == null) {
      initializeWriter();
    }
    if (root == null) {
      root = VectorSchemaRoot.create(getArrowSchema(), allocator);
    }

    // Reset root state for new batch
    root.setRowCount(0);

    // Populate root with records and write to Lance
    populateVectorSchemaRoot(bufferedRecords);
    writer.write(root);

    // Clear buffer
    bufferedRecords.clear();
  }

  /**
   * Initialize LanceFileWriter (lazy initialization).
   */
  private void initializeWriter() throws IOException {
    writer = LanceFileWriter.open(path.toString(), allocator, null);
  }
}
