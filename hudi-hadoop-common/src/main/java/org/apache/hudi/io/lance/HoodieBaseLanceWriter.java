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

import org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.arrow.HoodieBaseArrowWriter;
import org.apache.hudi.storage.StoragePath;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.lance.file.LanceFileWriter;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base class for Hudi Lance file writers supporting different record types.
 *
 * This class handles the Lance-specific file operations:
 * - {@link LanceFileWriter} lifecycle management
 * - Writing flushed Arrow batches through the Lance native writer API
 * - Bloom filter and footer metadata writing
 *
 * Record buffering, batch flushing, allocator management, and close ordering are inherited from
 * {@link HoodieBaseArrowWriter}. Subclasses must implement type-specific conversion to Arrow
 * format.
 *
 * @param <R> The record type (e.g., GenericRecord, InternalRow)
 */
@NotThreadSafe
public abstract class HoodieBaseLanceWriter<R, K extends Comparable<K>> extends HoodieBaseArrowWriter<R, K> {
  private final Map<String, String> footerMetadata = new LinkedHashMap<>();

  private LanceFileWriter writer;

  /**
   * Constructor for base Lance writer.
   *
   * @param path Path where Lance file will be written
   * @param batchSize Row-count threshold; the current batch is flushed when this many records have been buffered
   * @param allocatorSize Maximum bytes the per-writer Arrow child allocator may hold at once; sized so Arrow's
   *                      power-of-2 buffer doubling never requests a chunk above this cap
   * @param flushByteWatermark Byte-size threshold; the current batch is flushed when the sum of in-flight
   *                           FieldVector buffer sizes reaches this value. Must be small enough that the next
   *                           doubling step stays below {@code allocatorSize}.
   * @param bloomFilterWriteSupportOpt Optional bloom filter write support for record key tracking
   */
  protected HoodieBaseLanceWriter(StoragePath path, int batchSize, long allocatorSize, long flushByteWatermark,
                                  Option<HoodieBloomFilterWriteSupport<K>> bloomFilterWriteSupportOpt) {
    super(path, batchSize, allocatorSize, flushByteWatermark, bloomFilterWriteSupportOpt);
  }

  /**
   * Subclass hook for emitting additional Lance file-footer key-value metadata
   * alongside any bloom-filter entries. Called once during {@link #close()}.
   *
   * <p>Default implementation returns an empty map. Overriders should return a
   * fresh map; the caller does not retain a reference. Colliding keys are
   * overwritten per {@code LanceFileWriter.addSchemaMetadata} semantics.
   *
   * @return map of footer metadata key-value pairs, or empty map for none
   */
  protected Map<String, String> additionalSchemaMetadata() {
    return Collections.emptyMap();
  }

  /**
   * Add Hudi file footer metadata to the Lance schema custom metadata.
   *
   * <p>Lance exposes file-level key-value metadata through the Arrow schema custom metadata
   * returned by {@code LanceFileReader.schema()}. The metadata is flushed to the file footer
   * when the underlying {@link LanceFileWriter} is closed.
   */
  public void addFooterMetadata(Map<String, String> footerMetadata) {
    if (footerMetadata != null && !footerMetadata.isEmpty()) {
      this.footerMetadata.putAll(footerMetadata);
    }
  }

  @Override
  protected String getFormatName() {
    return "Lance";
  }

  @Override
  protected boolean isFormatWriterInitialized() {
    return writer != null;
  }

  /**
   * Initialize LanceFileWriter (lazy initialization).
   */
  @Override
  protected void initializeFormatWriter() throws IOException {
    writer = LanceFileWriter.open(getPath().toString(), getAllocator(), null);
  }

  @Override
  protected void writeBatch(VectorSchemaRoot batch) throws IOException {
    writer.write(batch);
  }

  /**
   * Finalize and write bloom filter and footer metadata before the Lance writer closes.
   */
  @Override
  protected void finalizeFooterMetadata() throws IOException {
    if (writer == null) {
      return;
    }

    // Finalize and write bloom filter metadata
    if (bloomFilterWriteSupportOpt.isPresent()) {
      Map<String, String> metadata = bloomFilterWriteSupportOpt.get().finalizeMetadata();
      if (!metadata.isEmpty()) {
        writer.addSchemaMetadata(metadata);
      }
    }

    // Allow subclasses to contribute additional footer key-value metadata
    // (e.g. Spark writer emits `hoodie.vector.columns` for forward-compat read).
    // Called unconditionally; returns an empty map when no VECTOR columns are present.
    Map<String, String> extra = additionalSchemaMetadata();
    if (extra != null && !extra.isEmpty()) {
      writer.addSchemaMetadata(extra);
    }

    if (!footerMetadata.isEmpty()) {
      writer.addSchemaMetadata(new HashMap<>(footerMetadata));
    }
  }

  @Override
  protected void closeFormatWriter() throws Exception {
    if (writer != null) {
      writer.close();
    }
  }
}
