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

package org.apache.hudi.io.arrow;

import org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.memory.HoodieArrowAllocator;
import org.apache.hudi.storage.StoragePath;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;

/**
 * Base class for Hudi file writers of Arrow-native formats (e.g. Lance, Vortex) supporting
 * different record types.
 *
 * This class handles the format-independent parts of writing an Arrow-batched file:
 * - BufferAllocator management
 * - Record buffering into a {@link VectorSchemaRoot} and batch flushing
 * - File size checks
 * - Record key tracking for bloom filter / min-max metadata
 * - Empty-file handling and close ordering
 *
 * Format subclasses (Lance, Vortex, ...) implement the hooks that create the native format
 * writer, hand each flushed batch to it, and close it; engine subclasses (Spark, Flink, ...)
 * implement the type-specific conversion to Arrow format and provide the Arrow schema.
 *
 * @param <R> The record type (e.g., GenericRecord, InternalRow)
 * @param <K> The record key type used for bloom filter tracking
 */
@NotThreadSafe
public abstract class HoodieBaseArrowWriter<R, K extends Comparable<K>> implements Closeable {
  protected static final int DEFAULT_BATCH_SIZE = 1000;
  @Getter(value = AccessLevel.PROTECTED)
  private final StoragePath path;
  @Getter(value = AccessLevel.PROTECTED)
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

  /**
   * Constructor for base Arrow-format writer.
   *
   * @param path Path where the file will be written
   * @param batchSize Row-count threshold; the current batch is flushed when this many records have been buffered
   * @param allocatorSize Maximum bytes the per-writer Arrow child allocator may hold at once; sized so Arrow's
   *                      power-of-2 buffer doubling never requests a chunk above this cap
   * @param flushByteWatermark Byte-size threshold; the current batch is flushed when the sum of in-flight
   *                           FieldVector buffer sizes reaches this value. Must be small enough that the next
   *                           doubling step stays below {@code allocatorSize}.
   * @param bloomFilterWriteSupportOpt Optional bloom filter write support for record key tracking
   */
  protected HoodieBaseArrowWriter(StoragePath path, int batchSize, long allocatorSize, long flushByteWatermark,
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
   * Get the Arrow schema for this writer.
   * Subclasses must provide the Arrow schema corresponding to their record type; each written
   * batch must conform to it.
   *
   * @return Arrow schema
   */
  protected abstract Schema getArrowSchema();

  /**
   * The format name (e.g. "Lance", "Vortex") used in error messages.
   */
  protected abstract String getFormatName();

  /**
   * Whether the native format writer has been initialized. Used as the lazy-initialization
   * sentinel: {@link #initializeFormatWriter()} is invoked on the first write (or on close for
   * an empty file) only while this returns false.
   */
  protected abstract boolean isFormatWriterInitialized();

  /**
   * Create the native format writer (lazy initialization).
   */
  protected abstract void initializeFormatWriter() throws IOException;

  /**
   * Write one finished batch to the native format writer.
   *
   * @param batch The VectorSchemaRoot holding the batch; row count is already set
   */
  protected abstract void writeBatch(VectorSchemaRoot batch) throws IOException;

  /**
   * Close the native format writer, finalizing the file. Called unconditionally during
   * {@link #close()}; implementations must no-op when the writer was never initialized.
   */
  protected abstract void closeFormatWriter() throws Exception;

  /**
   * Subclass hook invoked once during {@link #close()}, after remaining records are flushed and
   * empty-file handling, before the format writer closes. Formats that support file-level
   * key-value metadata persist footer entries (bloom filter, min/max keys, ...) here.
   * Default implementation does nothing.
   */
  protected void finalizeFooterMetadata() throws IOException {
  }

  /**
   * Subclass hook for closing format-specific resources that must be released after the format
   * writer closes but before the Arrow data buffers are freed. Called unconditionally during
   * {@link #close()} in its own try/catch, so failures here are recorded alongside (not instead
   * of) other close failures. Default implementation does nothing.
   */
  protected void closeFormatResources() throws Exception {
  }

  /**
   * Subclass hook for cleanup that must run after the data allocator has closed (e.g. a
   * dedicated native-side allocator). Called last during {@link #close()}; exceptions thrown
   * here propagate directly, so implementations should catch what they intend to tolerate.
   * Default implementation does nothing.
   */
  protected void closeAfterAllocator() {
  }

  /**
   * Write a single record. Records are buffered and flushed in batches.
   *
   * @param record Record to write
   * @throws IOException if write fails
   */
  public void write(R record) throws IOException {
    // Lazy initialization on first write
    if (!isFormatWriterInitialized()) {
      initializeFormatWriter();
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
    // The byte-based check bounds in-flight memory so Arrow's power-of-2 vector reallocation
    // can't escalate to a chunk size above the allocator cap regardless of per-row payload.
    if (currentBatchSize >= batchSize || currentBufferBytes() >= flushByteWatermark) {
      flushBatch();
    }
  }

  /**
   * Bytes currently held by the writer's Arrow child allocator. Used to drive the byte-aware
   * flush.
   *
   * <p>Note: we deliberately do <em>not</em> use {@code FieldVector.getBufferSize()} here.
   * For variable-width vectors (e.g. {@code BaseLargeVariableWidthVector} backing BLOB columns)
   * that method short-circuits to 0 when {@code valueCount == 0}, and {@code valueCount} is only
   * set during {@link ArrowWriter#finishBatch()} — i.e. at flush time. Mid-batch it always
   * reports zero, so a watermark driven by it never fires. {@link BufferAllocator#getAllocatedMemory()}
   * tracks the underlying ArrowBuf capacities directly and is exactly the quantity the allocator
   * cap is enforced against.
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
      // Flush any remaining records in current batch
      if (currentBatchSize > 0) {
        flushBatch();
      }

      // Ensure writer is initialized even if no data was written
      // This creates an empty file with just schema metadata
      if (!isFormatWriterInitialized() && root == null) {
        initializeFormatWriter();
        root = VectorSchemaRoot.create(getArrowSchema(), allocator);
        root.setRowCount(0);
        writeBatch(root);
      }

      // Persist footer key-value metadata (bloom filter, min/max keys, ...) for formats
      // that support it, before the format writer closes.
      finalizeFooterMetadata();
    } catch (Exception e) {
      primaryException = e;
    }

    // Close the format writer (finalizes the file)
    try {
      closeFormatWriter();
    } catch (Exception e) {
      primaryException = addSuppressed(primaryException, e);
    }

    // Close format-specific resources that must be released before the Arrow buffers
    try {
      closeFormatResources();
    } catch (Exception e) {
      primaryException = addSuppressed(primaryException, e);
    }

    // Close VectorSchemaRoot
    if (root != null) {
      try {
        root.close();
      } catch (Exception e) {
        primaryException = addSuppressed(primaryException, e);
      }
    }

    // Always close allocator
    try {
      allocator.close();
    } catch (Exception e) {
      primaryException = addSuppressed(primaryException, e);
    }

    // Format-specific cleanup that must run after the data allocator closes
    closeAfterAllocator();

    if (primaryException != null) {
      throw new HoodieException("Failed to close " + getFormatName() + " writer: " + path, primaryException);
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
   * in-progress batch. The in-progress portion uses {@link BufferAllocator#getAllocatedMemory()}
   * — see {@link #currentBufferBytes()} for why per-vector {@code getBufferSize()} is unreliable
   * mid-batch. This may slightly overestimate due to Arrow's pre-allocation overhead.
   */
  protected long getDataSize() {
    long currentBufferSize = currentBatchSize > 0 ? allocator.getAllocatedMemory() : 0;
    return totalFlushedDataSize + currentBufferSize;
  }

  /**
   * Flush buffered records to the file.
   */
  private void flushBatch() throws IOException {
    if (currentBatchSize == 0) {
      return;  // Nothing to flush
    }

    // Finalize the arrow writer (sets row count on VectorSchemaRoot)
    arrowWriter.finishBatch();

    // Accumulate the uncompressed Arrow buffer sizes for this batch
    for (FieldVector vector : root.getFieldVectors()) {
      totalFlushedDataSize += vector.getBufferSize();
    }

    // Write VectorSchemaRoot to the file
    writeBatch(root);

    // Release Arrow buffers so capacity does not accumulate across batches.
    // Arrow's BaseVariableWidthVector grows by doubling and never shrinks on its own;
    // without releasing, a vector that doubled to 128MB on one batch would attempt
    // to double to 256MB on the next, with the old 128MB still held — exceeding the
    // allocator cap. Closing root here ensures each batch starts from the small
    // initial capacity, so the watermark-bounded growth within a batch is the only
    // memory the cap has to accommodate.
    root.close();
    root = null;
    arrowWriter = null;

    // Reset batch counter for next batch
    currentBatchSize = 0;
  }

  /**
   * Arrow writer interface for writing records of type T to a VectorSchemaRoot.
   * Each engine can provide its own implementation for converting records from the engine specific format to Arrow format.
   * @param <T> the record type
   */
  protected interface ArrowWriter<T> {
    /**
     * Write a single record to the VectorSchemaRoot.
     * @param row Record to write
     * @throws IOException if write fails
     */
    void write(T row) throws IOException;

    /**
     * Finalize the current batch including setting row count on VectorSchemaRoot.
     * @throws IOException if finalization fails
     */
    void finishBatch() throws IOException;

    /**
     * Reset the writer state for a new batch.
     */
    void reset();
  }
}
