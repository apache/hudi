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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.memory.HoodieArrowAllocator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import dev.vortex.api.Session;
import dev.vortex.api.VortexWriter;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base class for Hudi Vortex file writers supporting different record types.
 *
 * This class handles common Vortex file operations including:
 * - {@link VortexWriter} lifecycle management (vortex-jni {@code dev.vortex.api})
 * - BufferAllocator management
 * - Record buffering and batch flushing. Each flushed batch goes to
 *   {@link VortexWriter#writeBatch(VectorSchemaRoot, BufferAllocator)}, which gives its buffers to
 *   the native writer without a copy.
 * - File size checks
 * - Footer metadata. When the writer closes, the bloom filter and the min/max record keys from
 *   {@link #bloomFilterWriteSupportOpt}, and the entries from {@link #addFooterMetadata}, go into
 *   the Vortex file metadata segments, with the same keys as the Parquet footer. Each value is the
 *   UTF-8 bytes of the footer string.
 * - Output. If the caller gives a {@link HoodieStorage}, the file is written through it, thus to
 *   every storage that Hudi supports. Otherwise the native writer writes to the URI of the path.
 *
 * Subclasses must implement type-specific conversion to Arrow format and provide the Arrow schema.
 *
 * @param <R> The record type (e.g., GenericRecord, InternalRow)
 */
@Slf4j
@NotThreadSafe
public abstract class HoodieBaseVortexWriter<R, K extends Comparable<K>> implements Closeable {

  static {
    // Set the Arrow properties before this class uses Arrow buffers and vectors.
    VortexArrowProperties.ensureConfigured();
  }
  // The row block size of the Vortex writer. A batch of this size goes to the compressor without
  // a concatenation with other batches.
  protected static final int DEFAULT_BATCH_SIZE = 8192;

  // The writer checks the byte size of the current batch after this number of rows. A sum over
  // all vectors for each row costs more than the write of the row.
  private static final int BYTE_CHECK_INTERVAL = 256;

  private final StoragePath path;
  private final HoodieStorage storage;
  private final BufferAllocator allocator;
  private final int batchSize;
  private final long flushByteWatermark;

  // Vortex file format edition for this writer. Set it explicitly: a newer vortex-jni can change
  // its default edition, and older Vortex readers cannot read files from a newer edition.
  private final String edition;

  // If true, the writer prefers a smaller file over read speed.
  private final boolean compact;

  // The minimum number of native worker threads. With workers, the writer compresses data while
  // the Java thread makes the next batch.
  private final int workerThreads;

  // Footer entries from the caller, for example the log block header of a native log file.
  private final Map<String, String> extraFooterMetadata = new LinkedHashMap<>();
  @Getter(value = AccessLevel.PROTECTED)
  private long writtenRecordCount = 0;
  private int currentBatchSize = 0;
  private VectorSchemaRoot root;
  private ArrowWriter<R> arrowWriter;
  protected final Option<HoodieBloomFilterWriteSupport<K>> bloomFilterWriteSupportOpt;

  // Held for the writer's lifetime. session/nativeAllocator back the native writer; nativeAllocator
  // is a dedicated allocator isolated from the strict data allocator (see initializeWriter).
  private Session session;
  private VortexWriter writer;
  private BufferAllocator nativeAllocator;

  // The output when the file is written through the storage. The native writer does not close it.
  private HoodieVortexWritable writable;

  /**
   * Constructor for base Vortex writer.
   *
   * @param path Path where Vortex file will be written
   * @param batchSize Row-count threshold; the current batch is flushed when this many records have been buffered
   * @param allocatorSize Maximum bytes the per-writer Arrow child allocator may hold at once
   * @param flushByteWatermark Byte-size threshold; the current batch is flushed when the sum of its
   *                           FieldVector buffer sizes reaches this value
   * @param edition Vortex file format edition to write, for example {@code core2026.08.3}
   * @param compact If true, the writer prefers a smaller file over read speed
   * @param workerThreads Minimum number of native Vortex worker threads in the JVM
   * @param storage Storage to write the file through, or null to write to the URI of the path
   * @param bloomFilterWriteSupportOpt Optional bloom filter write support for record key tracking
   */
  protected HoodieBaseVortexWriter(StoragePath path, int batchSize, long allocatorSize, long flushByteWatermark,
                                   String edition, boolean compact, int workerThreads, HoodieStorage storage,
                                   Option<HoodieBloomFilterWriteSupport<K>> bloomFilterWriteSupportOpt) {
    this.path = path;
    this.storage = storage;
    this.allocator = HoodieArrowAllocator.newChildAllocator(
        getClass().getSimpleName() + "-data-" + path.getName(), allocatorSize);
    this.batchSize = batchSize;
    this.flushByteWatermark = flushByteWatermark;
    this.edition = edition;
    this.compact = compact;
    this.workerThreads = workerThreads;
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

      // Allocate the vectors for a full batch. The Arrow default is 3970 values, which is too
      // large for a small batch and too small for a large one. The native writer keeps each batch
      // until it compresses it, thus memory that a batch does not use is held too.
      for (FieldVector vector : root.getFieldVectors()) {
        vector.setInitialCapacity(batchSize);
      }
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

    // Flush when the batch has the maximum number of rows or bytes. Count only the bytes of this
    // batch: the allocator also has the earlier batches, which the native writer keeps until it
    // compresses them. With those bytes, the writer would flush after each row.
    if (currentBatchSize >= batchSize
        || (currentBatchSize % BYTE_CHECK_INTERVAL == 0 && currentBatchBytes() >= flushByteWatermark)) {
      flushBatch();
    }
  }

  /**
   * Adds entries to the footer of the file. The writer writes them when it closes.
   */
  public void addFooterMetadata(Map<String, String> footerMetadata) {
    if (footerMetadata != null) {
      extraFooterMetadata.putAll(footerMetadata);
    }
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
          writeBatch(emptyRoot);
        }
      }
    } catch (Exception e) {
      primaryException = e;
    }

    // Finalize the file. Write the footer metadata only if all records were flushed, because
    // the footer must agree with the data in the file.
    if (writer != null) {
      try {
        if (primaryException == null) {
          writer.finish(footerMetadata());
        } else {
          writer.close();
        }
      } catch (Exception e) {
        primaryException = addSuppressed(primaryException, e);
      }
    }

    // The native writer flushed the output, but did not close it.
    if (writable != null) {
      try {
        writable.close();
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

    // Close the native allocator last. VortexWriter.Builder#build releases the schema that it
    // exports into this allocator, so the allocator closes clean. Tolerate retained memory
    // nonetheless, so that a delay in native cleanup does not fail the whole write.
    if (nativeAllocator != null) {
      try {
        nativeAllocator.close();
      } catch (IllegalStateException e) {
        log.warn("Vortex native allocator retained memory at close for {} (native writer "
            + "cleanup, reclaimed on GC): {}", path, e.getMessage());
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
   * Returns the footer metadata segments for the Vortex file: the entries from
   * {@link #addFooterMetadata}, and the finalized bloom filter and min/max record keys, encoded as
   * UTF-8 bytes.
   */
  private Map<String, byte[]> footerMetadata() {
    Map<String, byte[]> metadata = new HashMap<>();
    extraFooterMetadata.forEach((key, value) -> metadata.put(key, StringUtils.getUTF8Bytes(value)));
    if (bloomFilterWriteSupportOpt.isPresent()) {
      bloomFilterWriteSupportOpt.get().finalizeMetadata()
          .forEach((key, value) -> metadata.put(key, StringUtils.getUTF8Bytes(value)));
    }
    return metadata;
  }

  /**
   * Returns the estimated file size in bytes. The Vortex writer gives an estimate of the
   * compressed size of the data that it has. The records in the current batch are not in the
   * Vortex writer yet, so their Arrow size is multiplied by the compression ratio of the writer.
   *
   * <p>The Vortex writer uses a compression ratio of 1.0 until it compresses its first block of
   * rows. Thus, before that block, this estimate is the uncompressed size.
   */
  protected long getDataSize() {
    long currentBatchBytes = currentBatchBytes();
    if (writer == null) {
      return currentBatchBytes;
    }

    return writer.estimatedSize() + (long) (currentBatchBytes * writer.compressionRatio());
  }

  /**
   * Bytes that the vectors of the current batch use. Do not use the allocator memory here: the
   * native writer can keep the buffers of earlier batches until it compresses them, and these
   * buffers are already in the estimate of the Vortex writer.
   *
   * <p>The Arrow writer sets the value count of the vectors only when the batch is complete. Thus
   * ask for the size of the rows written so far, which the vectors find from their offsets.
   */
  private long currentBatchBytes() {
    if (currentBatchSize == 0 || root == null) {
      return 0;
    }

    long bytes = 0;
    for (FieldVector vector : root.getFieldVectors()) {
      bytes += vector.getBufferSizeFor(currentBatchSize);
    }
    return bytes;
  }

  /**
   * Flush buffered records to Vortex file.
   */
  private void flushBatch() throws IOException {
    if (currentBatchSize == 0) {
      return;
    }

    arrowWriter.finishBatch();
    writeBatch(root);

    // Release Arrow buffers so capacity does not accumulate across batches.
    root.close();
    root = null;
    arrowWriter = null;

    currentBatchSize = 0;
  }

  /**
   * Writes a batch to the Vortex file without a copy of its data. The native writer keeps each
   * buffer until it compresses the batch, and releases each buffer separately.
   *
   * <p>Do not export the batch with {@code Data.exportVectorSchemaRoot} and
   * {@link VortexWriter#writeBatch(long)}. With that path, the native writer releases the batch on
   * its worker threads through the release callback of the Arrow C Data library. The callback
   * attaches the thread to the JVM and detaches it again, thus each batch makes a new JVM thread.
   * With this method, vortex-jni releases the buffers from threads that stay attached.
   */
  private void writeBatch(VectorSchemaRoot batch) throws IOException {
    writer.writeBatch(batch, allocator);
  }


  /**
   * Initialize the {@link VortexWriter} (lazy initialization). The writer uses a dedicated
   * {@link #nativeAllocator} so the native side's non-deterministic cleanup does not pollute the
   * strict per-batch data allocator.
   */
  private void initializeWriter() throws IOException {
    VortexWorkerThreads.ensureAtLeast(workerThreads);
    session = Session.create();
    session.enableEdition(edition);

    nativeAllocator = new RootAllocator();
    Schema arrowSchema = getArrowSchema();
    if (storage == null) {
      writer = VortexWriter.builder(session, toVortexUri(path), arrowSchema, nativeAllocator)
          .compact(compact)
          .build();
      return;
    }

    HoodieVortexWritable output = new HoodieVortexWritable(storage.create(path, true));
    try {
      writer = VortexWriter.builder(session, output, arrowSchema, nativeAllocator)
          .compact(compact)
          .build();
    } catch (IOException | RuntimeException e) {
      try {
        output.close();
      } catch (IOException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
    writable = output;
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
