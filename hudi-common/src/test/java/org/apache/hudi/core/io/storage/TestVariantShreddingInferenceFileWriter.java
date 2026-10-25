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

package org.apache.hudi.core.io.storage;

import org.apache.hudi.common.avro.VariantShreddingSchemaInferrer;
import org.apache.hudi.common.avro.VariantShreddingSchemaInferrer.VariantSample;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestVariantShreddingInferenceFileWriter {

  private static final HoodieSchema RECORD_SCHEMA = HoodieSchema.createRecord("rec", null, null,
      singletonList(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING))));
  private static final Properties PROPS = new Properties();

  private final VariantShreddingInferenceFileWriter.VariantSampleExtractor noopExtractor =
      (record, schema, props) -> new VariantSample[1];

  /** A decorator over {@link #noopExtractor} for the column {@code v}. */
  private VariantShreddingInferenceFileWriter<Object> writer(
      VariantShreddingSchemaInferrer inferrer,
      VariantShreddingInferenceFileWriter.InferredWriterFactory<Object> factory,
      long maxFileSize) {
    return new VariantShreddingInferenceFileWriter<>(singletonList("v"), noopExtractor, inferrer, factory, maxFileSize);
  }

  private static HoodieRecord newRecord(String id) {
    GenericRecord data = new GenericData.Record(RECORD_SCHEMA.toAvroSchema());
    data.put("id", id);
    return new HoodieAvroIndexedRecord(new HoodieKey(id, "p"), data);
  }

  /** Records every call so replay order and call kinds can be asserted. */
  private static class RecordingWriter implements HoodieFileWriter<Object> {
    private final List<String> calls = new ArrayList<>();
    private final List<HoodieRecord> writtenRecords = new ArrayList<>();
    private final Map<String, String> footerMetadata = new LinkedHashMap<>();
    private final Object fileFormatMetadata = new Object();
    private int closeCount = 0;
    /** An IOException or an Error; anything else is a misuse of the stub. */
    private Throwable failWriteWith;
    private IOException failCloseWith;

    @Override
    public boolean canWrite() {
      return true;
    }

    @Override
    public void writeWithMetadata(HoodieKey key, HoodieRecord record, HoodieSchema schema, Properties props) throws IOException {
      failIfConfigured(failWriteWith);
      calls.add("meta:" + key.getRecordKey());
      writtenRecords.add(record);
    }

    @Override
    public void write(String recordKey, HoodieRecord record, HoodieSchema schema, Properties props) throws IOException {
      failIfConfigured(failWriteWith);
      calls.add("plain:" + recordKey);
      writtenRecords.add(record);
    }

    @Override
    public void writeRow(String recordKey, Object record) {
      calls.add("row:" + recordKey);
    }

    @Override
    public void addFooterMetadata(Map<String, String> footerMetadata) {
      this.footerMetadata.putAll(footerMetadata);
    }

    @Override
    public Object getFileFormatMetadata() {
      return fileFormatMetadata;
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

  @Test
  public void testReplayPreservesOrderAndCallKinds() throws IOException {
    Map<String, HoodieSchema> inferred = new HashMap<>();
    inferred.put("v", HoodieSchema.create(HoodieSchemaType.LONG));
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    RecordingWriter delegate = new RecordingWriter();

    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> inferred,
        map -> {
          factoryCalls.add(map);
          return delegate;
        }, Long.MAX_VALUE);

    assertTrue(writer.canWrite());
    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    writer.writeWithMetadata(new HoodieKey("r2", "p"), newRecord("r2"), RECORD_SCHEMA, PROPS);
    writer.write("r3", newRecord("r3"), RECORD_SCHEMA, PROPS);
    assertTrue(delegate.calls.isEmpty());

    writer.close();
    assertEquals(1, factoryCalls.size());
    assertSame(inferred, factoryCalls.get(0));
    assertEquals(Arrays.asList("plain:r1", "meta:r2", "plain:r3"), delegate.calls);
    assertEquals(1, delegate.closeCount, "the delegate must be closed exactly once");

    // Idempotent close
    writer.close();
    assertEquals(1, factoryCalls.size());
    assertEquals(1, delegate.closeCount);
  }

  @Test
  public void testRecordCountThresholdTriggersMaterialization() throws IOException {
    RecordingWriter delegate = new RecordingWriter();
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceFileWriter<Object> writer = writer(
        (columns, samples) -> {
          assertEquals(VariantShreddingInferenceFileWriter.MAX_BUFFERED_RECORDS, samples.size());
          return Collections.emptyMap();
        },
        map -> {
          factoryCalls.add(map);
          return delegate;
        }, Long.MAX_VALUE);

    for (int i = 0; i < VariantShreddingInferenceFileWriter.MAX_BUFFERED_RECORDS; i++) {
      writer.write("r" + i, newRecord("r" + i), RECORD_SCHEMA, PROPS);
    }
    // Threshold reached: delegate created and buffer replayed before close.
    assertEquals(1, factoryCalls.size());
    assertEquals(VariantShreddingInferenceFileWriter.MAX_BUFFERED_RECORDS, delegate.calls.size());

    // Subsequent writes stream straight through.
    writer.write("tail", newRecord("tail"), RECORD_SCHEMA, PROPS);
    assertEquals(VariantShreddingInferenceFileWriter.MAX_BUFFERED_RECORDS + 1, delegate.calls.size());
    writer.close();
    assertEquals(1, factoryCalls.size());
  }

  @Test
  public void testByteCapTriggersEarlyMaterialization() throws IOException {
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> Collections.emptyMap(),
        map -> {
          factoryCalls.add(map);
          return new RecordingWriter();
        }, 1L);

    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    // A 1-byte cap is exceeded by any record.
    assertEquals(1, factoryCalls.size());
    writer.close();
  }

  @Test
  public void testByteCapIsBoundedByTheSharedMaximum() throws IOException {
    // The cap is min(MAX_BUFFERED_BYTES, maxFileSize): a file size limit above 64MB must not lift
    // the buffer above 64MB. One record estimating at least the maximum meets it on its own.
    byte[] payload = new byte[(int) VariantShreddingInferenceFileWriter.MAX_BUFFERED_BYTES];
    Arrays.fill(payload, (byte) 'x');
    HoodieRecord record = newRecord(new String(payload, StandardCharsets.ISO_8859_1));
    assertTrue(new DefaultSizeEstimator<HoodieRecord>().sizeEstimate(record) >= VariantShreddingInferenceFileWriter.MAX_BUFFERED_BYTES);
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> Collections.emptyMap(),
        map -> {
          factoryCalls.add(map);
          return new RecordingWriter();
        }, Long.MAX_VALUE);

    writer.write("r1", record, RECORD_SCHEMA, PROPS);
    assertEquals(1, factoryCalls.size(), "a record at the shared maximum materializes whatever the file size limit");
    writer.close();
  }

  @Test
  public void testInferrerFailureDeclinesAndWritesUnshredded() throws IOException {
    RecordingWriter delegate = new RecordingWriter();
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceFileWriter<Object> writer = writer(
        (columns, samples) -> {
          throw new IllegalStateException("malformed variant");
        },
        map -> {
          factoryCalls.add(map);
          return delegate;
        }, Long.MAX_VALUE);

    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    writer.close();

    assertEquals(1, factoryCalls.size());
    assertTrue(factoryCalls.get(0).isEmpty());
    assertEquals(singletonList("plain:r1"), delegate.calls);
    assertEquals(1, delegate.closeCount);
  }

  @Test
  public void testZeroRecordCloseStillCreatesDelegate() throws IOException {
    RecordingWriter delegate = new RecordingWriter();
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceFileWriter<Object> writer = writer(
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
    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> Collections.emptyMap(),
        map -> {
          throw boom;
        }, Long.MAX_VALUE);

    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    IOException fromClose = assertThrows(IOException.class, writer::close);
    assertSame(boom, fromClose);
    // Every subsequent call keeps failing: buffered records were never written.
    IOException fromWrite = assertThrows(IOException.class,
        () -> writer.write("r2", newRecord("r2"), RECORD_SCHEMA, PROPS));
    assertSame(boom, fromWrite);
  }

  @Test
  public void testSamplesAlignWithBufferedRecords() throws IOException {
    // Snapshot: the decorator's internal list is cleared after replay.
    List<List<VariantSample[]>> seenSamples = new ArrayList<>();
    VariantShreddingInferenceFileWriter.VariantSampleExtractor extractor = (record, schema, props) -> {
      VariantSample[] samples = new VariantSample[1];
      samples[0] = new VariantSample(new byte[] {1}, new byte[] {2});
      return samples;
    };
    VariantShreddingInferenceFileWriter<Object> writer = new VariantShreddingInferenceFileWriter<>(
        singletonList("v"), extractor, (columns, samples) -> {
          seenSamples.add(new ArrayList<>(samples));
          assertEquals(singletonList("v"), columns);
          return Collections.emptyMap();
        },
        map -> new RecordingWriter(), Long.MAX_VALUE);

    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    writer.write("r2", newRecord("r2"), RECORD_SCHEMA, PROPS);
    writer.close();

    assertEquals(1, seenSamples.size());
    assertEquals(2, seenSamples.get(0).size());
    assertNotNull(seenSamples.get(0).get(0)[0]);
    assertEquals(1, seenSamples.get(0).get(0)[0].getValue()[0]);
  }

  @Test
  public void testCanWriteDelegatesAfterMaterialization() throws IOException {
    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> Collections.emptyMap(),
        map -> new RecordingWriter() {
          @Override
          public boolean canWrite() {
            return false;
          }
        }, 1L);

    assertTrue(writer.canWrite());
    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    assertFalse(writer.canWrite());
    writer.close();
  }

  @Test
  public void testNullInferredMapTreatedAsDecline() throws IOException {
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> null,
        map -> {
          factoryCalls.add(map);
          return new RecordingWriter();
        }, Long.MAX_VALUE);

    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    writer.close();
    assertEquals(1, factoryCalls.size());
    assertNotNull(factoryCalls.get(0));
    assertTrue(factoryCalls.get(0).isEmpty());
  }

  @Test
  public void testWriteRowMaterializesAndPassesThrough() throws IOException {
    RecordingWriter delegate = new RecordingWriter();
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> Collections.emptyMap(),
        map -> {
          factoryCalls.add(map);
          return delegate;
        }, Long.MAX_VALUE);

    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    assertTrue(factoryCalls.isEmpty());
    // A raw row has nothing to sample from: the buffered records are replayed first, then the
    // row goes straight through, preserving arrival order.
    writer.writeRow("r2", new Object());
    assertEquals(1, factoryCalls.size());
    assertEquals(Arrays.asList("plain:r1", "row:r2"), delegate.calls);
    writer.close();
    assertEquals(1, factoryCalls.size());
  }

  @Test
  public void testFooterMetadataQueuedUntilMaterialization() throws IOException {
    RecordingWriter delegate = new RecordingWriter();
    // A 1-byte cap materializes on the first write, so the forwarded leg below runs on an open writer.
    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> Collections.emptyMap(),
        map -> delegate, 1L);

    writer.addFooterMetadata(Collections.singletonMap("k1", "v1"));
    assertTrue(delegate.footerMetadata.isEmpty(), "queued until the real writer exists");
    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    assertEquals("v1", delegate.footerMetadata.get("k1"), "handed over at materialization");

    // After materialization the call is forwarded directly.
    writer.addFooterMetadata(Collections.singletonMap("k2", "v2"));
    assertEquals("v2", delegate.footerMetadata.get("k2"));
    writer.close();
  }

  @Test
  public void testGetFileFormatMetadataMaterializesAndDelegates() throws IOException {
    RecordingWriter delegate = new RecordingWriter();
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> Collections.emptyMap(),
        map -> {
          factoryCalls.add(map);
          return delegate;
        }, Long.MAX_VALUE);

    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    assertTrue(factoryCalls.isEmpty());
    // Footer metadata lives in the real writer, so asking for it creates that writer first.
    assertSame(delegate.fileFormatMetadata, writer.getFileFormatMetadata());
    assertEquals(1, factoryCalls.size());
    assertEquals(singletonList("plain:r1"), delegate.calls);

    // The native log-format writer asks after close() (column stats): still the delegate's answer.
    writer.close();
    assertSame(delegate.fileFormatMetadata, writer.getFileFormatMetadata());
    assertEquals(1, factoryCalls.size());
  }

  @Test
  public void testReplayFailureIsLatchedAndRethrown() throws IOException {
    RecordingWriter delegate = new RecordingWriter();
    IOException boom = new IOException("replay failed");
    delegate.failWriteWith = boom;
    IOException closeBoom = new IOException("close failed");
    delegate.failCloseWith = closeBoom;
    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> Collections.emptyMap(),
        map -> delegate, Long.MAX_VALUE);

    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    IOException fromClose = assertThrows(IOException.class, writer::close);
    assertSame(boom, fromClose);
    // The delegate was created but never closed by the try path, so the catch path closes it once,
    // and a failure of that close rides along as suppressed rather than replacing or hiding boom.
    assertEquals(1, delegate.closeCount);
    assertSame(closeBoom, fromClose.getSuppressed()[0]);
    // Latched: the buffered record was never written, so every later call keeps failing.
    assertSame(boom, assertThrows(IOException.class, () -> writer.write("r2", newRecord("r2"), RECORD_SCHEMA, PROPS)));
    assertSame(boom, assertThrows(HoodieIOException.class, writer::getFileFormatMetadata).getCause());
  }

  @Test
  public void testReplayErrorIsLatchedTooAndCloseDoesNotFinishTheFile() throws IOException {
    // An Error mid-replay latches like an exception does: inference already treats a LinkageError
    // as reachable (a writer linked against another Spark than the runtime's), and an unlatched
    // one would let close() finish the file without the records left in the buffer.
    RecordingWriter delegate = new RecordingWriter();
    NoClassDefFoundError boom = new NoClassDefFoundError("replay failed");
    delegate.failWriteWith = boom;
    // A 1-byte cap materializes on the first write, so the Error surfaces from write(), not close().
    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> Collections.emptyMap(),
        map -> delegate, 1L);

    assertSame(boom, assertThrows(NoClassDefFoundError.class,
        () -> writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS)));
    assertSame(boom, assertThrows(IOException.class, writer::close).getCause());
    assertEquals(1, delegate.closeCount);
  }

  @Test
  public void testMaterializeErrorInsideCloseStillClosesTheDelegate() throws IOException {
    // With the caps never tripped, the first materialization happens inside close(): the Error
    // must still close the delegate created just above, or the file handle leaks.
    RecordingWriter delegate = new RecordingWriter();
    NoClassDefFoundError boom = new NoClassDefFoundError("replay failed");
    delegate.failWriteWith = boom;
    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> Collections.emptyMap(),
        map -> delegate, Long.MAX_VALUE);

    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    assertSame(boom, assertThrows(NoClassDefFoundError.class, writer::close));
    assertEquals(1, delegate.closeCount);
  }

  @Test
  public void testThrowingDelegateCloseSurfacesAndIsNotRetried() throws IOException {
    RecordingWriter delegate = new RecordingWriter();
    IOException boom = new IOException("close failed");
    delegate.failCloseWith = boom;
    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> Collections.emptyMap(),
        map -> delegate, Long.MAX_VALUE);

    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    assertSame(boom, assertThrows(IOException.class, writer::close));
    assertEquals(1, delegate.closeCount, "a throwing delegate.close() must surface, not be retried");
  }

  @Test
  public void testPreparedRecordIsSampledAndReplayed() throws IOException {
    // An extractor that materializes the record (the Avro one) hands the materialized form back
    // via prepare(); the decorator samples that form and replays it, so the writer never redoes
    // the materialization.
    HoodieRecord prepared = newRecord("prepared");
    List<HoodieRecord> sampled = new ArrayList<>();
    VariantShreddingInferenceFileWriter.VariantSampleExtractor extractor =
        new VariantShreddingInferenceFileWriter.VariantSampleExtractor() {
          @Override
          public VariantSample[] extract(HoodieRecord record, HoodieSchema schema, Properties props) {
            sampled.add(record);
            return new VariantSample[1];
          }

          @Override
          public HoodieRecord prepare(HoodieRecord record, HoodieSchema schema, Properties props) {
            return prepared;
          }
        };
    RecordingWriter delegate = new RecordingWriter();
    VariantShreddingInferenceFileWriter<Object> writer = new VariantShreddingInferenceFileWriter<>(
        singletonList("v"), extractor, (columns, samples) -> Collections.emptyMap(),
        map -> delegate, Long.MAX_VALUE);

    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    writer.close();

    assertEquals(singletonList(prepared), sampled);
    assertEquals(singletonList(prepared), delegate.writtenRecords);
  }

  @Test
  public void testByteCapRechargesTheBufferWhenTheEstimateGrows() throws IOException {
    // 99 small records, then a big 100th that lands on the periodic re-estimation. The moving
    // average grows to about 0.9 * small + 0.1 * big, and the re-estimation charges the 99 earlier
    // records at that too, so the buffer (about 90 small + 10 big) meets a cap of one big record
    // right there. Without the rescale the 99 would stay charged at small, and about 100 small
    // plus a tenth of big would leave the cap untripped: the precondition below keeps that gap.
    String padding = new String(new char[1 << 22]).replace('\0', 'x');
    DefaultSizeEstimator<HoodieRecord> estimator = new DefaultSizeEstimator<>();
    long small = estimator.sizeEstimate(newRecord("r000"));
    long big = estimator.sizeEstimate(newRecord("r099" + padding));
    assertTrue(big > 200 * small, "expected the big record to dwarf the small ones: " + small + " vs " + big);
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceFileWriter<Object> writer = writer((columns, samples) -> Collections.emptyMap(),
        map -> {
          factoryCalls.add(map);
          return new RecordingWriter();
        }, big);

    for (int i = 0; i < 99; i++) {
      writer.write("r" + i, newRecord(String.format("r%03d", i)), RECORD_SCHEMA, PROPS);
    }
    assertTrue(factoryCalls.isEmpty(), "99 small records stay under a one-big-record cap");
    writer.write("r99", newRecord("r099" + padding), RECORD_SCHEMA, PROPS);
    assertEquals(1, factoryCalls.size(), "the re-estimation on the 100th record recharges the buffer past the cap");
    writer.close();
  }

  @Test
  public void testSharedSizeIsNotChargedPerRecord() throws IOException {
    // An extractor reporting shared state (the schema graph every record references) has that
    // much subtracted from each record's estimate: here all but 10 bytes, so a 25-byte cap holds
    // two records and trips on the third instead of the first.
    long perRecord = new DefaultSizeEstimator<HoodieRecord>().sizeEstimate(newRecord("r0"));
    VariantShreddingInferenceFileWriter.VariantSampleExtractor sharing =
        new VariantShreddingInferenceFileWriter.VariantSampleExtractor() {
          @Override
          public VariantSample[] extract(HoodieRecord record, HoodieSchema schema, Properties props) {
            return new VariantSample[1];
          }

          @Override
          public long sharedSizeEstimate(HoodieSchema schema) {
            return perRecord - 10;
          }
        };
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceFileWriter<Object> writer = new VariantShreddingInferenceFileWriter<>(
        singletonList("v"), sharing, (columns, samples) -> Collections.emptyMap(),
        map -> {
          factoryCalls.add(map);
          return new RecordingWriter();
        }, 25L);

    writer.write("r0", newRecord("r0"), RECORD_SCHEMA, PROPS);
    writer.write("r1", newRecord("r1"), RECORD_SCHEMA, PROPS);
    assertTrue(factoryCalls.isEmpty(), "20 of 25 bytes: two records buffered");
    writer.write("r2", newRecord("r2"), RECORD_SCHEMA, PROPS);
    assertEquals(1, factoryCalls.size(), "30 of 25 bytes: materialize on the third");
    writer.close();
  }
}
