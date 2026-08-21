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
import org.apache.hudi.io.arrow.HoodieBaseArrowWriter;
import org.apache.hudi.storage.StoragePath;

import dev.vortex.api.Session;
import dev.vortex.api.VortexWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;

/**
 * Base class for Hudi Vortex file writers supporting different record types.
 *
 * This class handles the Vortex-specific file operations:
 * - {@link VortexWriter} lifecycle management (vortex-jni {@code dev.vortex.api})
 * - Writing flushed Arrow batches: each batch is exported through the Arrow C Data Interface and
 *   handed to {@link VortexWriter#writeBatch(long, long)}; because vortex copies the batch without
 *   invoking the C release callback, {@link ArrowArray#release()} is called after each write so
 *   the exported buffers are freed.
 *
 * Record buffering, batch flushing, allocator management, and close ordering are inherited from
 * {@link HoodieBaseArrowWriter}. Subclasses must implement type-specific conversion to Arrow
 * format and provide the Arrow schema.
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
public abstract class HoodieBaseVortexWriter<R, K extends Comparable<K>> extends HoodieBaseArrowWriter<R, K> {
  // Held for the writer's lifetime. session/nativeAllocator back the native writer; nativeAllocator
  // is a dedicated allocator isolated from the strict data allocator (see initializeFormatWriter).
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
    super(path, batchSize, allocatorSize, flushByteWatermark, bloomFilterWriteSupportOpt);
  }

  @Override
  protected String getFormatName() {
    return "Vortex";
  }

  @Override
  protected boolean isFormatWriterInitialized() {
    return writer != null;
  }

  /**
   * Export a VectorSchemaRoot's array through the Arrow C Data Interface and write it to the Vortex
   * file. The array is exported from the strict data allocator and released after the write (vortex
   * copies the batch but does not invoke the C release callback), so no data-allocator memory leaks.
   */
  @Override
  protected void writeBatch(VectorSchemaRoot batch) throws IOException {
    BufferAllocator allocator = getAllocator();
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
  @Override
  protected void initializeFormatWriter() throws IOException {
    session = Session.create();
    nativeAllocator = new RootAllocator();
    Schema arrowSchema = getArrowSchema();
    writer = VortexWriter.create(session, toVortexUri(getPath()), arrowSchema, Collections.emptyMap(), nativeAllocator);
    exportedSchema = ArrowSchema.allocateNew(nativeAllocator);
    Data.exportSchema(nativeAllocator, arrowSchema, null, exportedSchema);
    exportedSchemaAddr = exportedSchema.memoryAddress();
  }

  /**
   * Close the Vortex writer (finalizes the file).
   */
  @Override
  protected void closeFormatWriter() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }

  /**
   * Close the exported schema handle.
   */
  @Override
  protected void closeFormatResources() throws Exception {
    if (exportedSchema != null) {
      exportedSchema.release();
      exportedSchema.close();
    }
  }

  /**
   * Close the native allocator last. vortex-jni 0.76.0's VortexWriter.create exports the file
   * schema into this allocator and relies on GC/Cleaner-based cleanup rather than freeing it on
   * close(), so a small one-time amount can still be outstanding here; tolerate it (it is freed
   * when the native writer is reclaimed) rather than failing the whole write.
   * TODO(#18623): drop this once vortex-jni frees the create() schema export on writer close.
   */
  @Override
  protected void closeAfterAllocator() {
    if (nativeAllocator != null) {
      try {
        nativeAllocator.close();
      } catch (IllegalStateException e) {
        log.warn("Vortex native allocator retained memory at close for {} (vortex-jni create() "
            + "schema export, reclaimed on GC): {}", getPath(), e.getMessage());
      }
    }
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
}
