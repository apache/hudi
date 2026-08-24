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

  /** A row of about a kilobyte, so byte-cap arithmetic on the estimator is not dominated by rounding. */
  private static InternalRow wideRow(long id) {
    return new GenericInternalRow(new Object[] {id, UTF8String.fromString(new String(new char[1024]).replace('\0', 'x'))});
  }

  /** A row of a few megabytes, to dwarf {@link #row(long)} in the byte-cap re-estimation test. */
  private static InternalRow bigRow(long id) {
    return new GenericInternalRow(new Object[] {id, UTF8String.fromString(new String(new char[1 << 22]).replace('\0', 'x'))});
  }

  /** A 16-byte UnsafeRow (8-byte null bitset + one long), so byte-cap arithmetic is exact. */
  private static UnsafeRow unsafeRow(long id) {
    return unsafeRow(id, 16);
  }

  /** An UnsafeRow of exactly {@code sizeInBytes} bytes, holding one long. */
  private static UnsafeRow unsafeRow(long id, int sizeInBytes) {
    UnsafeRow row = new UnsafeRow(1);
    byte[] buffer = new byte[sizeInBytes];
    row.pointTo(buffer, sizeInBytes);
    row.setLong(0, id);
    return row;
  }

  /** Records every call so replay order and call kinds can be asserted. */
  private static class RecordingRowWriter implements HoodieInternalRowFileWriter {
    private final List<String> calls = new ArrayList<>();
    private final List<InternalRow> rows = new ArrayList<>();
    private int closeCount = 0;
    /** An IOException or an Error; anything else is a misuse of the stub. */
    private Throwable failWriteWith;
    private IOException failCloseWith;

    @Override
    public boolean canWrite() {
      return true;
    }

    @Override
    public void writeRow(UTF8String key, InternalRow row) throws IOException {
      failIfConfigured(failWriteWith);
      calls.add("keyed:" + key);
      rows.add(row);
    }

    @Override
    public void writeRow(InternalRow row) throws IOException {
      failIfConfigured(failWriteWith);
      calls.add("plain:" + row.getLong(0));
      rows.add(row);
    }

    @Override
    public void close() throws IOException {
      closeCount++;
      failIfConfigured(failCloseWith);
    }

    private static void failIfConfigured(Throwable failure) throws IOException {
      if (failure instanceof Error) {
        throw (Error) failure;
      } else if (failure != null) {
        throw (IOException) failure;
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
  public void testByteCapRechargesTheBufferWhenTheEstimateGrows() throws IOException {
    // 99 small estimated rows, then a big 100th that lands on the periodic re-estimation. The
    // moving average grows to about 0.9 * small + 0.1 * big, and the re-estimation recharges the
    // 99 earlier rows at that too, so the buffer (about 90 small + 10 big) meets a cap of one big
    // row right there. Without the recharge the 99 would stay charged at small, and about 100
    // small plus a tenth of big would leave the cap untripped: the precondition keeps that gap.
    DefaultSizeEstimator<InternalRow> estimator = new DefaultSizeEstimator<>();
    long small = estimator.sizeEstimate(row(0));
    long big = estimator.sizeEstimate(bigRow(99));
    assertTrue(big > 200 * small, "expected the big row to dwarf the small ones: " + small + " vs " + big);
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> {
      factoryCalls.add(map);
      return new RecordingRowWriter();
    }, big);

    for (int i = 0; i < 99; i++) {
      writer.writeRow(row(i));
    }
    assertTrue(factoryCalls.isEmpty(), "99 small rows stay under a one-big-row cap");
    writer.writeRow(bigRow(99));
    assertEquals(1, factoryCalls.size(), "the re-estimation on the 100th row recharges the buffer past the cap");
    writer.close();
  }

  @Test
  public void testUnsafeRowChargesSurviveReEstimation() throws IOException {
    // UnsafeRows are charged exactly and other rows through the estimator, in one byte count. The
    // re-estimation on the 100th estimated row recharges the estimated rows only; assigning the
    // estimated total instead (what the record writer does, having no exact charges) would drop
    // the UnsafeRow's bytes. So an UnsafeRow among 100 same-shaped estimated rows meets a cap of
    // its size plus 100 rows' worth, less a slack that the estimated rows alone cannot cover.
    long perRow = new DefaultSizeEstimator<InternalRow>().sizeEstimate(wideRow(0));
    assertTrue(perRow > 1000, "expected a kilobyte-sized row, got " + perRow);
    int unsafeRowSize = 4096;
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> {
      factoryCalls.add(map);
      return new RecordingRowWriter();
    }, unsafeRowSize + 100 * perRow - 500);

    for (int i = 0; i < 50; i++) {
      writer.writeRow(wideRow(i));
    }
    writer.writeRow(unsafeRow(50, unsafeRowSize));
    for (int i = 51; i < 100; i++) {
      writer.writeRow(wideRow(i));
    }
    assertTrue(factoryCalls.isEmpty(), "the UnsafeRow plus 99 estimated rows stay under the cap");
    writer.writeRow(wideRow(100));
    assertEquals(1, factoryCalls.size(), "the 100th estimated row's re-estimation keeps the UnsafeRow's bytes and meets the cap");
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
  public void testReplayErrorIsLatchedTooAndCloseDoesNotFinishTheFile() throws IOException {
    // An Error mid-replay latches like an exception does: inference already treats a LinkageError
    // as reachable (a writer linked against another Spark than the runtime's), and an unlatched
    // one would let close() finish the file without the rows left in the buffer.
    RecordingRowWriter delegate = new RecordingRowWriter();
    NoClassDefFoundError boom = new NoClassDefFoundError("replay failed");
    delegate.failWriteWith = boom;
    // A 1-byte cap materializes on the first row, so the Error surfaces from writeRow(), not close().
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> delegate, 1L);

    assertSame(boom, assertThrows(NoClassDefFoundError.class, () -> writer.writeRow(row(1))));
    assertSame(boom, assertThrows(IOException.class, writer::close).getCause());
    assertEquals(1, delegate.closeCount);
  }

  @Test
  public void testMaterializeErrorInsideCloseStillClosesTheDelegate() throws IOException {
    // With the caps never tripped, the first materialization happens inside close(): the Error
    // must still close the delegate created just above, or the file handle leaks.
    RecordingRowWriter delegate = new RecordingRowWriter();
    NoClassDefFoundError boom = new NoClassDefFoundError("replay failed");
    delegate.failWriteWith = boom;
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> delegate, Long.MAX_VALUE);

    writer.writeRow(row(1));
    assertSame(boom, assertThrows(NoClassDefFoundError.class, writer::close));
    assertEquals(1, delegate.closeCount);
  }

  @Test
  public void testThrowingDelegateCloseSurfacesAndIsNotRetried() throws IOException {
    RecordingRowWriter delegate = new RecordingRowWriter();
    IOException boom = new IOException("close failed");
    delegate.failCloseWith = boom;
    VariantShreddingInferenceInternalRowFileWriter writer = writer(ABSENT_COLUMN, map -> delegate, Long.MAX_VALUE);

    writer.writeRow(row(1));
    assertSame(boom, assertThrows(IOException.class, writer::close));
    assertEquals(1, delegate.closeCount, "a throwing delegate.close() must surface, not be retried");
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
