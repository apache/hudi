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

package org.apache.hudi.io.vortex;

import org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.memory.HoodieArrowAllocator;
import org.apache.hudi.storage.StoragePath;

import dev.vortex.api.Session;
import dev.vortex.api.VortexWriter;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;

/**
 * Base class for Hudi Vortex file writers supporting different record types.
 *
 * This class handles common Vortex file operations including:
 * - {@link VortexWriter} lifecycle management (vortex-jni {@code dev.vortex.api})
 * - BufferAllocator management
 * - Record buffering and batch flushing. Each flushed batch is exported through the Arrow C Data
 *   Interface and handed to {@link VortexWriter#writeBatch(long, long)}; because vortex copies the
 *   batch without invoking the C release callback, {@link ArrowArray#release()} is called after each
 *   write so the exported buffers are freed.
 * - File size checks
 *
 * Subclasses must implement type-specific conversion to Arrow format and provide the Arrow schema.
 *
 * <p>NOTE: vortex-jni 0.76.0 exposes no file-level custom-metadata API, so Hudi's bloom filter and
 * min/max record-key footers cannot be persisted into the Vortex file yet. Record keys are still
 * tracked via {@link #bloomFilterWriteSupportOpt} but the finalized metadata is not written.
 * TODO(#18623): persist bloom/min-max footers once Vortex supports file metadata.
 *
 * @param <R> The record type (e.g., GenericRecord, InternalRow)
 */
@Slf4j
@NotThreadSafe
public abstract class HoodieBaseVortexWriter<R, K extends Comparable<K>> implements Closeable {
  protected static final int DEFAULT_BATCH_SIZE = 1000;
  private final StoragePath path;
  private final BufferAllocator allocator;
  private final int batchSize;
  private final long flushByteWatermark;
  @Getter(value = AccessLevel.PROTECTED)
  private long writtenRecordCount = 0;
  private long totalFlushedDataSize = 0;
  private int currentBatchSize = 0;
  private VectorSchemaRoot root;
  private ArrowWriter<R> arrowWriter;
  protected final Option<HoodieBloomFilterWriteSupport<K>> bloomFilterWriteSupportOpt;

  // Held for the writer's lifetime. session/nativeAllocator back the native writer; nativeAllocator
  // is a dedicated allocator isolated from the strict data allocator (see initializeWriter).
  private Session session;
  private VortexWriter writer;
  private BufferAllocator nativeAllocator;
  private ArrowSchema exportedSchema;
  private long exportedSchemaAddr;

  /**
   * Constructor for base Vortex writer.
   *
   * @param path Path where Vortex file will be written
   * @param batchSize Row-count threshold; the current batch is flushed when this many records have been buffered
   * @param allocatorSize Maximum bytes the per-writer Arrow child allocator may hold at once
   * @param flushByteWatermark Byte-size threshold; the current batch is flushed when the sum of in-flight
   *                           FieldVector buffer sizes reaches this value
   * @param bloomFilterWriteSupportOpt Optional bloom filter write support for record key tracking
   */
  protected HoodieBaseVortexWriter(StoragePath path, int batchSize, long allocatorSize, long flushByteWatermark,
                                   Option<HoodieBloomFilterWriteSupport<K>> bloomFilterWriteSupportOpt) {
    this.path = path;
    this.allocator = HoodieArrowAllocator.newChildAllocator(
        getClass().getSimpleName() + "-data-" + path.getName(), allocatorSize);
    this.batchSize = batchSize;
    this.flushByteWatermark = flushByteWatermark;
    this.bloomFilterWriteSupportOpt = bloomFilterWriteSupportOpt;
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
   * Get the Arrow schema for this writer. It is passed directly to {@link VortexWriter} and each
   * written batch must conform to it.
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

    // Flush when row-count batch is full OR in-flight Arrow buffers cross the byte watermark.
    if (currentBatchSize >= batchSize || currentBufferBytes() >= flushByteWatermark) {
      flushBatch();
    }
  }

  /**
   * Bytes currently held by the writer's Arrow child allocator.
   */
  private long currentBufferBytes() {
    return allocator.getAllocatedMemory();
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
      if (currentBatchSize > 0) {
        flushBatch();
      }

      // Ensure the file is created even if no data was written, by emitting a single empty batch.
      if (writer == null) {
        initializeWriter();
        try (VectorSchemaRoot emptyRoot = VectorSchemaRoot.create(getArrowSchema(), allocator)) {
          emptyRoot.setRowCount(0);
          writeBatchFfi(emptyRoot);
        }
      }
    } catch (Exception e) {
      primaryException = e;
    }

    // Close Vortex writer (finalizes the file)
    if (writer != null) {
      try {
        writer.close();
      } catch (Exception e) {
        primaryException = addSuppressed(primaryException, e);
      }
    }

    // Close the exported schema handle.
    if (exportedSchema != null) {
      try {
        exportedSchema.release();
        exportedSchema.close();
      } catch (Exception e) {
        primaryException = addSuppressed(primaryException, e);
      }
    }

    // Close VectorSchemaRoot
    if (root != null) {
      try {
        root.close();
      } catch (Exception e) {
        primaryException = addSuppressed(primaryException, e);
      }
    }

    // Close the strict data allocator (per-batch exports are released, so this should be clean).
    try {
      allocator.close();
    } catch (Exception e) {
      primaryException = addSuppressed(primaryException, e);
    }

    // Close the native allocator last. vortex-jni 0.76.0's VortexWriter.create exports the file
    // schema into this allocator and relies on GC/Cleaner-based cleanup rather than freeing it on
    // close(), so a small one-time amount can still be outstanding here; tolerate it (it is freed
    // when the native writer is reclaimed) rather than failing the whole write.
    // TODO(#18623): drop this once vortex-jni frees the create() schema export on writer close.
    if (nativeAllocator != null) {
      try {
        nativeAllocator.close();
      } catch (IllegalStateException e) {
        log.warn("Vortex native allocator retained memory at close for {} (vortex-jni create() "
            + "schema export, reclaimed on GC): {}", path, e.getMessage());
      }
    }

    if (primaryException != null) {
      throw new HoodieException("Failed to close Vortex writer: " + path, primaryException);
    }
  }

  private static Exception addSuppressed(Exception primary, Exception e) {
    if (primary == null) {
      return e;
    }
    primary.addSuppressed(e);
    return primary;
  }

  /**
   * Returns the estimated data size in bytes, including both flushed batches and the current
   * in-progress batch.
   */
  protected long getDataSize() {
    long currentBufferSize = currentBatchSize > 0 ? allocator.getAllocatedMemory() : 0;
    return totalFlushedDataSize + currentBufferSize;
  }

  /**
   * Flush buffered records to Vortex file.
   */
  private void flushBatch() throws IOException {
    if (currentBatchSize == 0) {
      return;
    }

    arrowWriter.finishBatch();

    for (FieldVector vector : root.getFieldVectors()) {
      totalFlushedDataSize += vector.getBufferSize();
    }

    writeBatchFfi(root);

    // Release Arrow buffers so capacity does not accumulate across batches.
    root.close();
    root = null;
    arrowWriter = null;

    currentBatchSize = 0;
  }

  /**
   * Export a VectorSchemaRoot's array through the Arrow C Data Interface and write it to the Vortex
   * file. The array is exported from the strict data allocator and released after the write (vortex
   * copies the batch but does not invoke the C release callback), so no data-allocator memory leaks.
   */
  private void writeBatchFfi(VectorSchemaRoot batch) throws IOException {
    try (ArrowArray cArray = ArrowArray.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, batch, null, cArray);
      writer.writeBatch(cArray.memoryAddress(), exportedSchemaAddr);
      cArray.release();
    }
  }

  /**
   * Initialize the {@link VortexWriter} (lazy initialization). The writer and its exported schema
   * use a dedicated {@link #nativeAllocator} so the native side's non-deterministic cleanup does
   * not pollute the strict per-batch data allocator.
   */
  private void initializeWriter() throws IOException {
    session = Session.create();
    nativeAllocator = new RootAllocator();
    Schema arrowSchema = getArrowSchema();
    writer = VortexWriter.create(session, toVortexUri(path), arrowSchema, Collections.emptyMap(), nativeAllocator);
    exportedSchema = ArrowSchema.allocateNew(nativeAllocator);
    Data.exportSchema(nativeAllocator, arrowSchema, null, exportedSchema);
    exportedSchemaAddr = exportedSchema.memoryAddress();
  }

  /**
   * Vortex requires a well-formed URL. Hudi {@link StoragePath}s for remote stores already carry a
   * scheme (s3://, hdfs://, ...); local paths may not, so qualify them as {@code file://} URIs.
   */
  private static String toVortexUri(StoragePath path) {
    URI uri = path.toUri();
    if (uri.getScheme() == null) {
      return new File(uri.getPath()).toURI().toString();
    }
    return uri.toString();
  }

  /**
   * Arrow writer interface for writing records of type T to a VectorSchemaRoot.
   * @param <T> the record type
   */
  protected interface ArrowWriter<T> {
    void write(T row) throws IOException;

    void finishBatch() throws IOException;

    void reset();
  }
}
