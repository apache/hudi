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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.VariantShreddingSchemaInferrer;
import org.apache.hudi.avro.VariantShreddingSchemaInferrer.VariantSample;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.io.storage.VariantShreddingInferenceFileWriter;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@link HoodieInternalRowFileWriter} decorator that infers a per-file variant shredding
 * schema from the first rows before opening the real parquet writer; the row-writer-path
 * sibling of {@link VariantShreddingInferenceFileWriter}, sharing its buffering thresholds and
 * failure semantics.
 *
 * <p>Meta columns including the commit seqno are composed into the row by the handle BEFORE
 * {@code writeRow}, so ordered replay is value-exact here by construction. Rows and keys are
 * copied because Spark iterators reuse their instances.</p>
 */
@Slf4j
public class VariantShreddingInferenceInternalRowFileWriter implements HoodieInternalRowFileWriter {

  /** Creates the real row file writer once the inferred typed_value schemas are known. */
  @FunctionalInterface
  public interface InferredRowWriterFactory {
    HoodieInternalRowFileWriter create(Map<String, HoodieSchema> inferredTypedValues) throws IOException;
  }

  private final List<String> variantColumns;
  private final int[] ordinals;
  private final VariantShreddingSchemaInferrer inferrer;
  private final InferredRowWriterFactory writerFactory;
  private final long maxBufferedBytes;
  private final DefaultSizeEstimator<InternalRow> sizeEstimator = new DefaultSizeEstimator<>();

  private static final int SIZE_ESTIMATE_INTERVAL = 100;

  private final List<BufferedRow> buffer = new ArrayList<>();
  private final List<VariantSample[]> samples = new ArrayList<>();
  private long bufferedBytes = 0;
  private long estimatedRowSize = 0;
  private HoodieInternalRowFileWriter delegate;
  private IOException fatalFailure;
  private boolean closed = false;

  public VariantShreddingInferenceInternalRowFileWriter(List<String> variantColumns,
                                                        int[] ordinals,
                                                        VariantShreddingSchemaInferrer inferrer,
                                                        InferredRowWriterFactory writerFactory,
                                                        long maxFileSize) {
    this.variantColumns = variantColumns;
    this.ordinals = ordinals;
    this.inferrer = inferrer;
    this.writerFactory = writerFactory;
    this.maxBufferedBytes = Math.min(VariantShreddingInferenceFileWriter.MAX_BUFFERED_BYTES, Math.max(1, maxFileSize));
  }

  /** Resolves the buffer ordinal of each variant column in {@code structType}; -1 when absent. */
  public static int[] resolveOrdinals(StructType structType, List<String> columnNames) {
    int[] ordinals = new int[columnNames.size()];
    StructField[] fields = structType.fields();
    for (int i = 0; i < columnNames.size(); i++) {
      ordinals[i] = -1;
      for (int j = 0; j < fields.length; j++) {
        if (fields[j].name().equals(columnNames.get(i))) {
          ordinals[i] = j;
          break;
        }
      }
    }
    return ordinals;
  }

  @Override
  public boolean canWrite() {
    // Nothing has been physically written while buffering, so size-based rollover cannot apply yet.
    return delegate == null || delegate.canWrite();
  }

  @Override
  public void writeRow(UTF8String key, InternalRow row) throws IOException {
    rethrowIfFailed();
    if (delegate != null) {
      delegate.writeRow(key, row);
    } else {
      // copy(): Spark iterators reuse key instances.
      buffer(key == null ? null : key.copy(), true, row);
    }
  }

  @Override
  public void writeRow(InternalRow row) throws IOException {
    rethrowIfFailed();
    if (delegate != null) {
      delegate.writeRow(row);
    } else {
      buffer(null, false, row);
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

  private void buffer(UTF8String key, boolean withKey, InternalRow row) throws IOException {
    rethrowIfFailed();
    InternalRow copied = row.copy();
    samples.add(extractSamples(copied));
    buffer.add(new BufferedRow(key, withKey, copied));
    bufferedBytes += estimateSize(copied);
    if (buffer.size() >= VariantShreddingInferenceFileWriter.MAX_BUFFERED_RECORDS || bufferedBytes >= maxBufferedBytes) {
      materialize();
    }
  }

  private VariantSample[] extractSamples(InternalRow row) {
    VariantSample[] out = new VariantSample[ordinals.length];
    for (int i = 0; i < ordinals.length; i++) {
      if (ordinals[i] >= 0) {
        out[i] = SparkAdapterSupport$.MODULE$.sparkAdapter().extractVariantBinary(row, ordinals[i]);
      }
    }
    return out;
  }

  private long estimateSize(InternalRow row) {
    if (row instanceof UnsafeRow) {
      return ((UnsafeRow) row).getSizeInBytes();
    }
    // Re-estimate periodically so a small first row cannot defeat the byte cap
    // (same moving-average idiom as ExternalSpillableMap).
    if (estimatedRowSize == 0 || buffer.size() % SIZE_ESTIMATE_INTERVAL == 0) {
      long sampled = Math.max(1, sizeEstimator.sizeEstimate(row));
      estimatedRowSize = estimatedRowSize == 0
          ? sampled : (long) (estimatedRowSize * 0.9 + sampled * 0.1);
    }
    return estimatedRowSize;
  }

  private void materialize() throws IOException {
    if (delegate != null) {
      return;
    }
    try {
      delegate = writerFactory.create(inferTypedValues());
      for (BufferedRow buffered : buffer) {
        if (buffered.withKey) {
          delegate.writeRow(buffered.key, buffered.row);
        } else {
          delegate.writeRow(buffered.row);
        }
      }
      buffer.clear();
      samples.clear();
    } catch (IOException e) {
      fatalFailure = e;
      throw e;
    } catch (RuntimeException e) {
      fatalFailure = new IOException("Deferred parquet row writer materialization failed", e);
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

  private static final class BufferedRow {
    private final UTF8String key;
    private final boolean withKey;
    private final InternalRow row;

    BufferedRow(UTF8String key, boolean withKey, InternalRow row) {
      this.key = key;
      this.withKey = withKey;
      this.row = row;
    }
  }
}
