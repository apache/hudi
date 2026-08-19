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

import org.apache.hudi.common.avro.VariantShreddingSchemaInferrer;
import org.apache.hudi.common.avro.VariantShreddingSchemaInferrer.VariantSample;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.core.io.storage.VariantShreddingInferenceFileWriter;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The row-writer sibling of {@code TestVariantShreddingInferenceFileWriter}. The decorator is
 * driven with ordinals of -1 (the column absent from the row's StructType), which is the
 * sampling branch that needs no Spark adapter on the classpath; sample extraction itself goes
 * through {@code SparkAdapter.extractVariantBinary} and is covered by the functional tests.
 */
public class TestVariantShreddingInferenceInternalRowFileWriter {

  private static final int[] ABSENT_COLUMN = {-1};

  private static InternalRow row(long id) {
    return new GenericInternalRow(new Object[] {id});
  }

  /** A 16-byte UnsafeRow (8-byte null bitset + one long), so byte-cap arithmetic is exact. */
  private static UnsafeRow unsafeRow(long id) {
    UnsafeRow row = new UnsafeRow(1);
    byte[] buffer = new byte[16];
    row.pointTo(buffer, 16);
    row.setLong(0, id);
    return row;
  }

  /** Records every call so replay order and call kinds can be asserted. */
  private static class RecordingRowWriter implements HoodieInternalRowFileWriter {
    private final List<String> calls = new ArrayList<>();
    private final List<InternalRow> rows = new ArrayList<>();
    private int closeCount = 0;
    private IOException failWriteWith;

    @Override
    public boolean canWrite() {
      return true;
    }

    @Override
    public void writeRow(UTF8String key, InternalRow row) throws IOException {
      failIfConfigured();
      calls.add("keyed:" + key);
      rows.add(row);
    }

    @Override
    public void writeRow(InternalRow row) throws IOException {
      failIfConfigured();
      calls.add("plain:" + row.getLong(0));
      rows.add(row);
    }

    @Override
    public void close() {
      closeCount++;
    }

    private void failIfConfigured() throws IOException {
      if (failWriteWith != null) {
        throw failWriteWith;
      }
    }
  }

  private static VariantShreddingInferenceInternalRowFileWriter writer(
      int[] ordinals,
      VariantShreddingInferenceInternalRowFileWriter.InferredRowWriterFactory factory,
      long maxFileSize) {
    return writer(ordinals, (columns, samples) -> Collections.emptyMap(), factory, maxFileSize);
  }

  private static VariantShreddingInferenceInternalRowFileWriter writer(
      int[] ordinals,
      VariantShreddingSchemaInferrer inferrer,
      VariantShreddingInferenceInternalRowFileWriter.InferredRowWriterFactory factory,
      long maxFileSize) {
    return new VariantShreddingInferenceInternalRowFileWriter(singletonList("v"), ordinals, inferrer, factory, maxFileSize);
  }

  @Test
  public void testResolveOrdinals() {
    StructType structType = new StructType()
        .add("id", DataTypes.LongType)
        .add("v", DataTypes.BinaryType)
        .add("w", DataTypes.BinaryType);
    // Present columns map to their ordinal; an absent one maps to -1 and is sampled as null.
    assertArrayEquals(new int[] {2, 1, -1},
        VariantShreddingInferenceInternalRowFileWriter.resolveOrdinals(structType, Arrays.asList("w", "v", "missing")));
    assertArrayEquals(new int[0],
        VariantShreddingInferenceInternalRowFileWriter.resolveOrdinals(structType, Collections.emptyList()));
  }

  @Test
  public void testReplayPreservesOrderAndCallKinds() throws IOException {
    Map<String, HoodieSchema> inferred = new HashMap<>();
    inferred.put("v", HoodieSchema.create(HoodieSchemaType.LONG));
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    List<List<VariantSample[]>> seenSamples = new ArrayList<>();
    RecordingRowWriter delegate = new RecordingRowWriter();
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN,
        (columns, samples) -> {
          seenSamples.add(new ArrayList<>(samples));
          return inferred;
        },
        map -> {
          factoryCalls.add(map);
          return delegate;
        }, Long.MAX_VALUE);

    assertTrue(writer.canWrite());
    writer.writeRow(UTF8String.fromString("k1"), row(1));
    writer.writeRow(row(2));
    writer.writeRow(UTF8String.fromString("k3"), row(3));
    assertTrue(delegate.calls.isEmpty(), "rows are buffered until materialization");

    writer.close();
    assertEquals(1, factoryCalls.size());
    assertSame(inferred, factoryCalls.get(0));
    assertEquals(Arrays.asList("keyed:k1", "plain:2", "keyed:k3"), delegate.calls);
    // One sample slot per row, null for the absent column.
    assertEquals(3, seenSamples.get(0).size());
    assertEquals(1, seenSamples.get(0).get(0).length);
    assertNull(seenSamples.get(0).get(0)[0]);
    assertEquals(1, delegate.closeCount);

    // Idempotent close.
    writer.close();
    assertEquals(1, factoryCalls.size());
    assertEquals(1, delegate.closeCount);
  }

  @Test
  public void testBufferedRowsAreCopies() throws IOException {
    // Spark iterators reuse row instances: the buffered row must be a copy taken at write time.
    RecordingRowWriter delegate = new RecordingRowWriter();
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> delegate, Long.MAX_VALUE);
    GenericInternalRow reused = new GenericInternalRow(new Object[] {1L});
    writer.writeRow(reused);
    reused.update(0, 2L);
    writer.writeRow(reused);
    writer.close();
    assertEquals(Arrays.asList("plain:1", "plain:2"), delegate.calls);
    assertFalse(delegate.rows.get(0) == reused);
  }

  @Test
  public void testBufferedKeysAreCopies() throws IOException {
    // Keys are reused by Spark iterators too: a key over a mutable buffer must be copied at write
    // time, or the replay would carry whatever the buffer holds by then.
    RecordingRowWriter delegate = new RecordingRowWriter();
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> delegate, Long.MAX_VALUE);
    byte[] keyBytes = "k1".getBytes(StandardCharsets.UTF_8);
    UTF8String reusedKey = UTF8String.fromBytes(keyBytes);
    writer.writeRow(reusedKey, row(1));
    keyBytes[1] = '2';
    writer.writeRow(reusedKey, row(2));
    writer.close();
    assertEquals(Arrays.asList("keyed:k1", "keyed:k2"), delegate.calls);
  }

  @Test
  public void testByteCapAccumulatesThroughTheEstimator() throws IOException {
    // Non-UnsafeRow rows of one shape estimate the same size, so a cap of 150 rows' worth
    // materializes on exactly the 150th write, after the periodic re-estimation at row 100 (the
    // small slack absorbs the moving average's floating-point rounding).
    long perRow = new DefaultSizeEstimator<InternalRow>().sizeEstimate(row(0));
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> {
      factoryCalls.add(map);
      return new RecordingRowWriter();
    }, 150 * perRow - 100);

    for (int i = 0; i < 149; i++) {
      writer.writeRow(row(i));
    }
    assertTrue(factoryCalls.isEmpty(), "149 rows stay under a 150-row cap");
    writer.writeRow(row(149));
    assertEquals(1, factoryCalls.size(), "the 150th row meets the cap");
    writer.close();
  }

  @Test
  public void testRecordCountThresholdTriggersMaterialization() throws IOException {
    RecordingRowWriter delegate = new RecordingRowWriter();
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> {
      factoryCalls.add(map);
      return delegate;
    }, Long.MAX_VALUE);

    for (int i = 0; i < VariantShreddingInferenceFileWriter.MAX_BUFFERED_RECORDS; i++) {
      writer.writeRow(row(i));
    }
    // Threshold reached: delegate created and buffer replayed before close.
    assertEquals(1, factoryCalls.size());
    assertEquals(VariantShreddingInferenceFileWriter.MAX_BUFFERED_RECORDS, delegate.calls.size());
    // Subsequent rows stream straight through.
    writer.writeRow(row(-1));
    assertEquals(VariantShreddingInferenceFileWriter.MAX_BUFFERED_RECORDS + 1, delegate.calls.size());
    writer.close();
    assertEquals(1, factoryCalls.size());
  }

  @Test
  public void testUnsafeRowByteCapTriggersMaterialization() throws IOException {
    // UnsafeRow sizes are exact (no estimator): two 16-byte rows meet a 32-byte cap.
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> {
      factoryCalls.add(map);
      return new RecordingRowWriter();
    }, 32L);

    writer.writeRow(unsafeRow(1));
    assertTrue(factoryCalls.isEmpty(), "16 of 32 bytes buffered");
    writer.writeRow(unsafeRow(2));
    assertEquals(1, factoryCalls.size(), "32 of 32 bytes buffered: materialize");
    writer.close();
  }

  @Test
  public void testEstimatedByteCapTriggersEarlyMaterialization() throws IOException {
    // Non-UnsafeRow rows go through the size estimator; a 1-byte cap is exceeded by any row.
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> {
      factoryCalls.add(map);
      return new RecordingRowWriter();
    }, 1L);
    writer.writeRow(row(1));
    assertEquals(1, factoryCalls.size());
    writer.close();
  }

  @Test
  public void testInferrerFailureDeclinesAndWritesUnshredded() throws IOException {
    RecordingRowWriter delegate = new RecordingRowWriter();
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN,
        (columns, samples) -> {
          throw new IllegalStateException("malformed variant");
        },
        map -> {
          factoryCalls.add(map);
          return delegate;
        }, Long.MAX_VALUE);

    writer.writeRow(row(1));
    writer.close();
    assertEquals(1, factoryCalls.size());
    assertTrue(factoryCalls.get(0).isEmpty());
    assertEquals(singletonList("plain:1"), delegate.calls);
  }

  @Test
  public void testZeroRowCloseStillCreatesDelegate() throws IOException {
    RecordingRowWriter delegate = new RecordingRowWriter();
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN,
        (columns, samples) -> {
          throw new AssertionError("inferrer must not be called with an empty buffer");
        },
        map -> {
          factoryCalls.add(map);
          return delegate;
        }, Long.MAX_VALUE);

    writer.close();
    assertEquals(1, factoryCalls.size());
    assertTrue(factoryCalls.get(0).isEmpty());
    assertEquals(1, delegate.closeCount);
  }

  @Test
  public void testWriterCreationFailureIsLatchedAndRethrown() throws IOException {
    IOException boom = new IOException("create failed");
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> {
      throw boom;
    }, Long.MAX_VALUE);

    writer.writeRow(row(1));
    assertSame(boom, assertThrows(IOException.class, writer::close));
    // Every subsequent call keeps failing: buffered rows were never written.
    assertSame(boom, assertThrows(IOException.class, () -> writer.writeRow(row(2))));
    assertSame(boom, assertThrows(IOException.class, () -> writer.writeRow(UTF8String.fromString("k"), row(2))));
  }

  @Test
  public void testReplayFailureIsLatchedAndDelegateClosedOnce() throws IOException {
    RecordingRowWriter delegate = new RecordingRowWriter();
    IOException boom = new IOException("replay failed");
    delegate.failWriteWith = boom;
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> delegate, Long.MAX_VALUE);

    writer.writeRow(row(1));
    assertSame(boom, assertThrows(IOException.class, writer::close));
    // Created but never closed by the try path, so the catch path closes it exactly once.
    assertEquals(1, delegate.closeCount);
    assertSame(boom, assertThrows(IOException.class, () -> writer.writeRow(row(2))));
  }

  @Test
  public void testCanWriteDelegatesAfterMaterialization() throws IOException {
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> new RecordingRowWriter() {
      @Override
      public boolean canWrite() {
        return false;
      }
    }, 1L);

    assertTrue(writer.canWrite());
    writer.writeRow(row(1));
    assertFalse(writer.canWrite());
    writer.close();
  }
}
