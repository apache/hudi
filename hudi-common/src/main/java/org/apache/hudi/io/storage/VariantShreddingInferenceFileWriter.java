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

package org.apache.hudi.io.storage;

import org.apache.hudi.avro.VariantShreddingSchemaInferrer;
import org.apache.hudi.avro.VariantShreddingSchemaInferrer.VariantSample;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.exception.HoodieIOException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A {@link HoodieFileWriter} decorator that infers a per-file variant shredding schema from the
 * first records before opening the real parquet writer.
 *
 * <p>Records are buffered (and their variant binaries sampled) until a threshold is reached or
 * the writer closes, the sampled binaries are fed to a {@link VariantShreddingSchemaInferrer},
 * the real writer is created against the schema with the inferred typed_value spliced in, and
 * the buffer is replayed in arrival order. Replay reproduces each call exactly (write vs
 * writeWithMetadata), so commit seqnos, bloom filters and min/max record keys come out
 * identical to a non-buffered write. Buffering thresholds mirror Spark's
 * {@code ParquetOutputWriterWithVariantShredding} (4096 rows / 64MB).
 *
 * <p>Buffered records are {@link HoodieRecord#copy() copied} because Spark iterators reuse row
 * instances. For record types where copy() is identity (Avro), replay additionally relies on
 * writer-level records being freshly allocated per record, which holds today; variant samples
 * are extracted eagerly into immutable byte arrays so inference itself never depends on it.
 *
 * <p>Inference failures never fail the write: the file falls back to unshredded variants. This
 * deliberately diverges from Spark (which propagates inference failures) because a throwing
 * inference would fail compaction. Writer-creation or replay failures, however, are latched and
 * rethrown from every subsequent call including {@link #close()}, so a task cannot silently
 * drop buffered records that the handle already counted as written.
 *
 * <p>Single-threaded by contract, same as the writers it wraps.
 *
 * <p>See https://github.com/apache/hudi/issues/18937.</p>
 */
@Slf4j
public class VariantShreddingInferenceFileWriter implements HoodieFileWriter {

  /** Buffer caps mirroring Spark's ParquetOutputWriterWithVariantShredding. */
  public static final int MAX_BUFFERED_RECORDS = 4096;
  public static final long MAX_BUFFERED_BYTES = 64L * 1024 * 1024;
  private static final int SIZE_ESTIMATE_INTERVAL = 100;

  /**
   * Extracts the variant binaries of the inferable columns from a record. Bound to the writer
   * schema and column set by the creating factory; must defensively copy the bytes.
   */
  @FunctionalInterface
  public interface VariantSampleExtractor {
    VariantSample[] extract(HoodieRecord record, HoodieSchema schema, Properties props) throws IOException;
  }

  /** Creates the real file writer once the inferred typed_value schemas are known. */
  @FunctionalInterface
  public interface InferredWriterFactory {
    HoodieFileWriter create(Map<String, HoodieSchema> inferredTypedValues) throws IOException;
  }

  private final List<String> variantColumns;
  private final VariantSampleExtractor extractor;
  private final VariantShreddingSchemaInferrer inferrer;
  private final InferredWriterFactory writerFactory;
  private final long maxBufferedBytes;
  private final SizeEstimator<HoodieRecord> sizeEstimator = new DefaultSizeEstimator<>();

  private final List<BufferedWrite> buffer = new ArrayList<>();
  private final List<VariantSample[]> samples = new ArrayList<>();
  private long estimatedRecordSize = 0;
  private long bufferedBytes = 0;
  private HoodieFileWriter delegate;
  private IOException fatalFailure;
  private boolean closed = false;

  public VariantShreddingInferenceFileWriter(List<String> variantColumns,
                                             VariantSampleExtractor extractor,
                                             VariantShreddingSchemaInferrer inferrer,
                                             InferredWriterFactory writerFactory,
                                             long maxFileSize) {
    this.variantColumns = variantColumns;
    this.extractor = extractor;
    this.inferrer = inferrer;
    this.writerFactory = writerFactory;
    this.maxBufferedBytes = Math.min(MAX_BUFFERED_BYTES, Math.max(1, maxFileSize));
  }

  @Override
  public boolean canWrite() {
    // Nothing has been physically written while buffering, so size-based rollover cannot apply yet.
    return delegate == null || delegate.canWrite();
  }

  @Override
  public void writeWithMetadata(HoodieKey key, HoodieRecord record, HoodieSchema schema, Properties props) throws IOException {
    rethrowIfFailed();
    if (delegate != null) {
      delegate.writeWithMetadata(key, record, schema, props);
    } else {
      buffer(true, key, null, record, schema, props);
    }
  }

  @Override
  public void write(String recordKey, HoodieRecord record, HoodieSchema schema, Properties props) throws IOException {
    rethrowIfFailed();
    if (delegate != null) {
      delegate.write(recordKey, record, schema, props);
    } else {
      buffer(false, null, recordKey, record, schema, props);
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    try {
      rethrowIfFailed();
      // Materialize even with an empty buffer: handles expect the file to exist at close.
      materialize();
      delegate.close();
    } catch (IOException | RuntimeException e) {
      if (delegate != null) {
        try {
          delegate.close();
        } catch (Exception suppressed) {
          // Best-effort cleanup; surface the original failure.
        }
      }
      throw e;
    }
  }

  @Override
  public Object getFileFormatMetadata() {
    try {
      rethrowIfFailed();
      materialize();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to materialize the parquet writer for format metadata", e);
    }
    return delegate.getFileFormatMetadata();
  }

  private void buffer(boolean withMetadata, HoodieKey key, String recordKey, HoodieRecord record,
                      HoodieSchema schema, Properties props) throws IOException {
    rethrowIfFailed();
    HoodieRecord copied = record.copy();
    // Eager extraction: immutable byte copies decouple inference from buffered-record identity,
    // and per-record extraction failures (corrupt binaries) surface exactly like an eager write.
    samples.add(extractor.extract(copied, schema, props));
    buffer.add(new BufferedWrite(withMetadata, key, recordKey, copied, schema, props));
    // Re-estimate periodically so a small first record cannot defeat the byte cap
    // (same moving-average idiom as ExternalSpillableMap).
    if (estimatedRecordSize == 0 || buffer.size() % SIZE_ESTIMATE_INTERVAL == 0) {
      long sampled = Math.max(1, sizeEstimator.sizeEstimate(copied));
      estimatedRecordSize = estimatedRecordSize == 0
          ? sampled : (long) (estimatedRecordSize * 0.9 + sampled * 0.1);
    }
    bufferedBytes += estimatedRecordSize;
    if (buffer.size() >= MAX_BUFFERED_RECORDS || bufferedBytes >= maxBufferedBytes) {
      materialize();
    }
  }

  private void materialize() throws IOException {
    if (delegate != null) {
      return;
    }
    try {
      delegate = writerFactory.create(inferTypedValues());
      for (BufferedWrite write : buffer) {
        if (write.withMetadata) {
          delegate.writeWithMetadata(write.key, write.record, write.schema, write.props);
        } else {
          delegate.write(write.recordKey, write.record, write.schema, write.props);
        }
      }
      buffer.clear();
      samples.clear();
    } catch (IOException e) {
      fatalFailure = e;
      throw e;
    } catch (RuntimeException e) {
      fatalFailure = new IOException("Deferred parquet writer materialization failed", e);
      throw e;
    }
  }

  private Map<String, HoodieSchema> inferTypedValues() {
    if (samples.isEmpty()) {
      return Collections.emptyMap();
    }
    try {
      Map<String, HoodieSchema> inferred = inferrer.inferTypedValueSchemas(variantColumns, samples);
      return inferred == null ? Collections.emptyMap() : inferred;
    } catch (Exception e) {
      log.warn("Variant shredding schema inference failed for columns {}; writing unshredded variants.",
          variantColumns, e);
      return Collections.emptyMap();
    }
  }

  private void rethrowIfFailed() throws IOException {
    if (fatalFailure != null) {
      throw fatalFailure;
    }
  }

  private static final class BufferedWrite {
    private final boolean withMetadata;
    private final HoodieKey key;
    private final String recordKey;
    private final HoodieRecord record;
    private final HoodieSchema schema;
    private final Properties props;

    BufferedWrite(boolean withMetadata, HoodieKey key, String recordKey, HoodieRecord record,
                  HoodieSchema schema, Properties props) {
      this.withMetadata = withMetadata;
      this.key = key;
      this.recordKey = recordKey;
      this.record = record;
      this.schema = schema;
      this.props = props;
    }
  }
}
