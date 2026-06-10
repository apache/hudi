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

import org.apache.hudi.avro.VariantShreddingSchemaInferrer.VariantSample;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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

  private static HoodieRecord newRecord(String id) {
    GenericRecord data = new GenericData.Record(RECORD_SCHEMA.toAvroSchema());
    data.put("id", id);
    return new HoodieAvroIndexedRecord(new HoodieKey(id, "p"), data);
  }

  /** Records every call so replay order and call kinds can be asserted. */
  private static class RecordingWriter implements HoodieFileWriter {
    private final List<String> calls = new ArrayList<>();
    private boolean closed = false;

    @Override
    public boolean canWrite() {
      return true;
    }

    @Override
    public void writeWithMetadata(HoodieKey key, HoodieRecord record, HoodieSchema schema, Properties props) {
      calls.add("meta:" + key.getRecordKey());
    }

    @Override
    public void write(String recordKey, HoodieRecord record, HoodieSchema schema, Properties props) {
      calls.add("plain:" + recordKey);
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  @Test
  public void testReplayPreservesOrderAndCallKinds() throws IOException {
    Map<String, HoodieSchema> inferred = new HashMap<>();
    inferred.put("v", HoodieSchema.create(HoodieSchemaType.LONG));
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    RecordingWriter delegate = new RecordingWriter();

    VariantShreddingInferenceFileWriter writer = new VariantShreddingInferenceFileWriter(
        singletonList("v"), noopExtractor, (columns, samples) -> inferred,
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
    assertTrue(delegate.closed);

    // Idempotent close
    writer.close();
    assertEquals(1, factoryCalls.size());
  }

  @Test
  public void testRecordCountThresholdTriggersMaterialization() throws IOException {
    RecordingWriter delegate = new RecordingWriter();
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceFileWriter writer = new VariantShreddingInferenceFileWriter(
        singletonList("v"), noopExtractor, (columns, samples) -> {
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
    VariantShreddingInferenceFileWriter writer = new VariantShreddingInferenceFileWriter(
        singletonList("v"), noopExtractor, (columns, samples) -> Collections.emptyMap(),
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
  public void testInferrerFailureDeclinesAndWritesUnshredded() throws IOException {
    RecordingWriter delegate = new RecordingWriter();
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceFileWriter writer = new VariantShreddingInferenceFileWriter(
        singletonList("v"), noopExtractor, (columns, samples) -> {
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
    assertTrue(delegate.closed);
  }

  @Test
  public void testZeroRecordCloseStillCreatesDelegate() throws IOException {
    RecordingWriter delegate = new RecordingWriter();
    List<Map<String, HoodieSchema>> factoryCalls = new ArrayList<>();
    VariantShreddingInferenceFileWriter writer = new VariantShreddingInferenceFileWriter(
        singletonList("v"), noopExtractor,
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
    assertTrue(delegate.closed);
  }

  @Test
  public void testWriterCreationFailureIsLatchedAndRethrown() throws IOException {
    IOException boom = new IOException("create failed");
    VariantShreddingInferenceFileWriter writer = new VariantShreddingInferenceFileWriter(
        singletonList("v"), noopExtractor, (columns, samples) -> Collections.emptyMap(),
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
    VariantShreddingInferenceFileWriter writer = new VariantShreddingInferenceFileWriter(
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
    VariantShreddingInferenceFileWriter writer = new VariantShreddingInferenceFileWriter(
        singletonList("v"), noopExtractor, (columns, samples) -> Collections.emptyMap(),
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
    VariantShreddingInferenceFileWriter writer = new VariantShreddingInferenceFileWriter(
        singletonList("v"), noopExtractor, (columns, samples) -> null,
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
}
