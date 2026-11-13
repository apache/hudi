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

package org.apache.hudi.common.model;

import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link BaseAvroPayload#getRecord(Schema)}, exercised through the concrete
 * {@link OverwriteWithLatestAvroPayload} subclass.
 *
 * <p>A payload built in memory resolves a requested schema by name from its writer schema, for
 * example a data-only schema for a record that declares the {@code _hoodie_} meta fields.
 */
class TestBaseAvroPayload {

  // Java serialization of OverwriteWithLatestAvroPayload with ordering value 42 and record bytes of
  // {id: "key", ts: 42, value: "updated"} (or none), written by BaseAvroPayload as of commit
  // 8e884960820a081e34cdce8e78505e9ed54dff28. Golden bytes catch serialVersionUID and format changes.
  private static final String GOLDEN_JAVA_TOMBSTONE = "rO0ABXNyADtvcmcuYXBhY2hlLmh1ZGkuY29tbW9uLm1vZGVsLk92ZXJ3cml0ZVdpdGhMYXRlc3RBdnJvUGF5bG9hZGa/j5KgaG9h"
      + "AgAAeHIALG9yZy5hcGFjaGUuaHVkaS5jb21tb24ubW9kZWwuQmFzZUF2cm9QYXlsb2FkOJGhgVrvqjUCAANaAA9pc0RlbGV0ZWRS"
      + "ZWNvcmRMAAtvcmRlcmluZ1ZhbHQAFkxqYXZhL2xhbmcvQ29tcGFyYWJsZTtbAAtyZWNvcmRCeXRlc3QAAltCeHABc3IADmphdmEu"
      + "bGFuZy5Mb25nO4vkkMyPI98CAAFKAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAAAAAAqdXIAAltC"
      + "rPMX+AYIVOACAAB4cAAAAAA=";
  private static final String GOLDEN_JAVA_PAYLOAD = "rO0ABXNyADtvcmcuYXBhY2hlLmh1ZGkuY29tbW9uLm1vZGVsLk92ZXJ3cml0ZVdpdGhMYXRlc3RBdnJvUGF5bG9hZGa/j5KgaG9h"
      + "AgAAeHIALG9yZy5hcGFjaGUuaHVkaS5jb21tb24ubW9kZWwuQmFzZUF2cm9QYXlsb2FkOJGhgVrvqjUCAANaAA9pc0RlbGV0ZWRS"
      + "ZWNvcmRMAAtvcmRlcmluZ1ZhbHQAFkxqYXZhL2xhbmcvQ29tcGFyYWJsZTtbAAtyZWNvcmRCeXRlc3QAAltCeHAAc3IADmphdmEu"
      + "bGFuZy5Mb25nO4vkkMyPI98CAAFKAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAAAAAAqdXIAAltC"
      + "rPMX+AYIVOACAAB4cAAAAA0Ga2V5VA51cGRhdGVk";

  // The five _hoodie_ meta fields ahead of the data fields.
  private final Schema fullSchema = SchemaBuilder.record("full")
      .fields()
      .optionalString(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
      .optionalString(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD)
      .optionalString(HoodieRecord.RECORD_KEY_METADATA_FIELD)
      .optionalString(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
      .optionalString(HoodieRecord.FILENAME_METADATA_FIELD)
      .requiredString("id")
      .requiredLong("ts")
      .requiredString("name")
      .requiredString("price")
      .optionalBoolean(HoodieRecord.HOODIE_IS_DELETED_FIELD)
      .endRecord();

  // The same data fields without the meta fields.
  private final Schema dataOnlySchema = SchemaBuilder.record("dataOnly")
      .fields()
      .requiredString("id")
      .requiredLong("ts")
      .requiredString("name")
      .requiredString("price")
      .optionalBoolean(HoodieRecord.HOODIE_IS_DELETED_FIELD)
      .endRecord();

  // The full schema plus a required field the writer never had.
  private final Schema readerSchemaWithUnresolvableField = SchemaBuilder.record("fullPlusUnresolvable")
      .fields()
      .optionalString(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
      .optionalString(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD)
      .optionalString(HoodieRecord.RECORD_KEY_METADATA_FIELD)
      .optionalString(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
      .optionalString(HoodieRecord.FILENAME_METADATA_FIELD)
      .requiredString("id")
      .requiredLong("ts")
      .requiredString("name")
      .requiredString("price")
      .optionalBoolean(HoodieRecord.HOODIE_IS_DELETED_FIELD)
      .requiredString("unresolvable_field_with_no_default")
      .endRecord();

  private GenericRecord newFullRecord() {
    return new GenericRecordBuilder(fullSchema)
        .set(HoodieRecord.COMMIT_TIME_METADATA_FIELD, "20260807120000000")
        .set(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, "20260807120000000_0_0")
        .set(HoodieRecord.RECORD_KEY_METADATA_FIELD, "id:1")
        .set(HoodieRecord.PARTITION_PATH_METADATA_FIELD, "partition0")
        .set(HoodieRecord.FILENAME_METADATA_FIELD, "file1.parquet")
        .set("id", "1")
        .set("ts", 100L)
        .set("name", "alice")
        .set("price", "9.99")
        .set(HoodieRecord.HOODIE_IS_DELETED_FIELD, false)
        .build();
  }

  @Test
  void getInsertValueProjectsByNameWhenRequestedSchemaOmitsMetaFields() throws IOException {
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L);

    // The requested schema omits the meta fields the record declares.
    IndexedRecord projected = payload.getInsertValue(dataOnlySchema).get();

    assertEquals("1", String.valueOf(projected.get(dataOnlySchema.getField("id").pos())));
    assertEquals(100L, projected.get(dataOnlySchema.getField("ts").pos()));
    assertEquals("alice", String.valueOf(projected.get(dataOnlySchema.getField("name").pos())));
    assertEquals("9.99", String.valueOf(projected.get(dataOnlySchema.getField("price").pos())));
    assertEquals(false, projected.get(dataOnlySchema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD).pos()));
  }

  @Test
  void getRecordStaysCorrectAcrossMultipleDifferentSchemaRequestsNarrowThenFull() throws IOException {
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L);

    IndexedRecord narrow = payload.getInsertValue(dataOnlySchema).get();
    assertEquals("1", String.valueOf(narrow.get(dataOnlySchema.getField("id").pos())));

    // A later request with an equal schema of another instance decodes the original bytes.
    Schema fullSchemaCopy = new Schema.Parser().parse(fullSchema.toString());
    IndexedRecord full = payload.getInsertValue(fullSchemaCopy).get();

    assertEquals("20260807120000000",
        String.valueOf(full.get(fullSchemaCopy.getField(HoodieRecord.COMMIT_TIME_METADATA_FIELD).pos())));
    assertEquals("1", String.valueOf(full.get(fullSchemaCopy.getField("id").pos())));
    assertEquals(100L, full.get(fullSchemaCopy.getField("ts").pos()));
    assertEquals("alice", String.valueOf(full.get(fullSchemaCopy.getField("name").pos())));
    assertEquals("9.99", String.valueOf(full.get(fullSchemaCopy.getField("price").pos())));
  }

  @Test
  void getRecordStaysCorrectAcrossMultipleDifferentSchemaRequestsFullThenNarrow() throws IOException {
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L);

    // An equal schema of another instance first, then a projection.
    Schema fullSchemaCopy = new Schema.Parser().parse(fullSchema.toString());
    IndexedRecord full = payload.getInsertValue(fullSchemaCopy).get();
    assertEquals("1", String.valueOf(full.get(fullSchemaCopy.getField("id").pos())));

    IndexedRecord narrow = payload.getInsertValue(dataOnlySchema).get();

    assertEquals("1", String.valueOf(narrow.get(dataOnlySchema.getField("id").pos())));
    assertEquals(100L, narrow.get(dataOnlySchema.getField("ts").pos()));
    assertEquals("alice", String.valueOf(narrow.get(dataOnlySchema.getField("name").pos())));
    assertEquals("9.99", String.valueOf(narrow.get(dataOnlySchema.getField("price").pos())));
    assertEquals(false, narrow.get(dataOnlySchema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD).pos()));
  }

  @Test
  void defaultHoodieRecordPayloadIsDeletedThenGetInsertValueStaysCorrect() throws IOException {
    DefaultHoodieRecordPayload payload = new DefaultHoodieRecordPayload(newFullRecord(), 100L);

    // isDeleted decodes with the data-only schema first, as the merge paths do.
    assertFalse(payload.isDeleted(dataOnlySchema, new Properties()));

    Schema fullSchemaCopy = new Schema.Parser().parse(fullSchema.toString());
    IndexedRecord result = payload.getInsertValue(fullSchemaCopy, new Properties()).get();

    assertEquals("1", String.valueOf(result.get(fullSchemaCopy.getField("id").pos())));
    assertEquals("alice", String.valueOf(result.get(fullSchemaCopy.getField("name").pos())));
    assertEquals(100L, result.get(fullSchemaCopy.getField("ts").pos()));
  }

  @Test
  void getRecordThrowsWhenReaderSchemaHasNonDefaultedFieldMissingFromWriterSchema() {
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L);

    // A reader field with no writer field and no default fails instead of reading another value.
    assertThrows(AvroTypeException.class, () -> payload.getInsertValue(readerSchemaWithUnresolvableField));
  }

  @Test
  void renamedFieldWithDefaultKeepsValueAtSamePosition() throws IOException {
    // A rename at the same position keeps the value, as Hive and Presto realtime readers expect.
    Schema writerSchema = SchemaBuilder.record("renamedWriter")
        .fields()
        .optionalString("col1")
        .optionalString("col2")
        .endRecord();
    Schema readerSchema = SchemaBuilder.record("renamedReader")
        .fields()
        .optionalString("col1")
        .optionalString("col2_new")
        .endRecord();
    GenericRecord record = new GenericRecordBuilder(writerSchema)
        .set("col1", "1.1")
        .set("col2", "text2")
        .build();

    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(record, 100L);
    IndexedRecord decoded = payload.getInsertValue(readerSchema).get();

    assertEquals("1.1", String.valueOf(decoded.get(readerSchema.getField("col1").pos())));
    assertEquals("text2", String.valueOf(decoded.get(readerSchema.getField("col2_new").pos())));
  }

  @Test
  void incompatibleReplacementUsesDefault() throws IOException {
    Schema writer = SchemaBuilder.record("replacement").fields()
        .requiredString("id").requiredString("removed").endRecord();
    Schema reader = SchemaBuilder.record("replacement").fields()
        .requiredString("id").optionalLong("added").endRecord();
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(
        new GenericRecordBuilder(writer).set("id", "key").set("removed", "not-a-number").build(), 1L);
    IndexedRecord result = payload.getInsertValue(reader).get();
    assertEquals("key", result.get(0).toString());
    assertNull(result.get(1));
    assertEquals("not-a-number", payload.getInsertValue(writer).get().get(1).toString());
  }

  @Test
  void renamedRequiredFieldKeepsValueAtSamePosition() throws IOException {
    Schema writer = SchemaBuilder.record("requiredRename").fields()
        .requiredString("id").requiredString("old_name").endRecord();
    Schema reader = SchemaBuilder.record("requiredRename").fields()
        .requiredString("id").requiredString("new_name").endRecord();
    GenericRecord original = new GenericRecordBuilder(writer).set("id", "key").set("old_name", "value").build();
    IndexedRecord result = new OverwriteWithLatestAvroPayload(original, 1L).getInsertValue(reader).get();
    assertEquals("key", result.get(0).toString());
    assertEquals("value", result.get(1).toString());
  }

  @Test
  void positionalFallbackDoesNotReuseAliasedField() throws Exception {
    Schema writer = SchemaBuilder.record("aliasReplacement").fields()
        .requiredString("removed").requiredString("kept").endRecord();
    Schema reader = SchemaBuilder.record("aliasReplacement").fields()
        .optionalString("added")
        .name("renamed").aliases("removed").type().stringType().noDefault().endRecord();
    GenericRecord original = new GenericRecordBuilder(writer).set("removed", "value").set("kept", "other").build();
    IndexedRecord result = new OverwriteWithLatestAvroPayload(original, 1L).getInsertValue(reader).get();
    assertNull(result.get(0));
    assertEquals("value", result.get(1).toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void deserializedPayloadDecodesEachRequestPositionally(boolean serializeAfterPrefix) throws Exception {
    Schema writer = SchemaBuilder.record("prefixed").fields()
        .requiredString("id").optionalString("trailing").endRecord();
    Schema prefix = SchemaBuilder.record("prefixed").fields().requiredString("id").endRecord();
    OverwriteWithLatestAvroPayload payload = kryoRoundTrip(new OverwriteWithLatestAvroPayload(
        new GenericRecordBuilder(writer).set("id", "key").set("trailing", "retained").build(), 1L));
    assertEquals("key", payload.getInsertValue(prefix).get().get(0).toString());
    if (serializeAfterPrefix) {
      payload = kryoRoundTrip(payload);
    }
    // A prefix request must not become the schema of the bytes.
    IndexedRecord full = payload.getInsertValue(writer).get();
    assertEquals("key", full.get(0).toString());
    assertEquals("retained", full.get(1).toString());
  }

  @Test
  void compatiblePositionalRenameAllowsNumericPromotion() throws IOException {
    Schema writer = SchemaBuilder.record("promotion").fields().requiredInt("old_value").endRecord();
    Schema reader = SchemaBuilder.record("promotion").fields().optionalLong("new_value").endRecord();
    GenericRecord original = new GenericRecordBuilder(writer).set("old_value", 42).build();
    assertEquals(42L, new OverwriteWithLatestAvroPayload(original, 1L).getInsertValue(reader).get().get(0));
  }

  @Test
  void serializedBytesOfEncodedPayloadMatchEarlierBytes() throws IOException {
    Schema writer = SchemaBuilder.record("payload").fields()
        .requiredString("id").requiredLong("ts").requiredString("value").endRecord();
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(
        new GenericRecordBuilder(writer).set("id", "key").set("ts", 42L).set("value", "updated").build(), 42L);
    byte[] recordBytes = payload.getRecordBytes();
    byte[] compactKryo;
    try (Output output = new Output(256, -1)) {
      output.writeInt(recordBytes.length);
      output.writeBytes(recordBytes);
      new Kryo().writeClassAndObject(output, 42L);
      output.writeBoolean(false);
      compactKryo = output.toBytes();
    }
    assertArrayEquals(compactKryo, kryoBytes(payload));
    assertEquals(GOLDEN_JAVA_PAYLOAD, Base64.getEncoder().encodeToString(javaBytes(payload)));
    // A projection keeps the writer schema in memory only.
    payload.getInsertValue(SchemaBuilder.record("payload").fields().requiredString("value").endRecord());
    assertArrayEquals(compactKryo, kryoBytes(payload));
    assertEquals(GOLDEN_JAVA_PAYLOAD, Base64.getEncoder().encodeToString(javaBytes(payload)));

    OverwriteWithLatestAvroPayload tombstone = new OverwriteWithLatestAvroPayload(null, 42L);
    tombstone.getRecordBytes();
    assertEquals(GOLDEN_JAVA_TOMBSTONE, Base64.getEncoder().encodeToString(javaBytes(tombstone)));
  }

  @Test
  void javaSerializationKeepsRecordBuiltInMemory() throws Exception {
    OverwriteWithLatestAvroPayload restored = javaRoundTrip(new OverwriteWithLatestAvroPayload(newFullRecord(), 100L));
    assertEquals("alice", restored.getInsertValue(fullSchema).get().get(fullSchema.getField("name").pos()).toString());
  }

  @ParameterizedTest
  @CsvSource({"BITCASK,false", "BITCASK,true", "ROCKS_DB,false"})
  void spilledPayloadDecodesWithSuppliedSchema(ExternalSpillableMap.DiskMapType diskType,
                                                  boolean compression, @TempDir Path directory) throws IOException {
    try (ExternalSpillableMap<String, OverwriteWithLatestAvroPayload> records = new ExternalSpillableMap<>(
        1L, directory.toString(), key -> 64L, value -> 1024L, diskType, new DefaultSerializer<>(), compression, "payload-test")) {
      OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L);
      payload.getInsertValue(dataOnlySchema);
      records.put("full", payload);
      assertEquals(1, records.getDiskBasedMapNumEntries());
      IndexedRecord restored = records.get("full").getInsertValue(fullSchema).get();
      assertEquals("20260807120000000", restored.get(0).toString());
      assertEquals("alice", restored.get(fullSchema.getField("name").pos()).toString());
    }
  }

  @Test
  void kryoRoundTripPreservesTombstone() throws IOException {
    OverwriteWithLatestAvroPayload restored = kryoRoundTrip(new OverwriteWithLatestAvroPayload(null, 100L));
    assertFalse(restored.getInsertValue(fullSchema).isPresent());
    assertEquals(100L, restored.getOrderingVal());
    assertTrue(restored.isDeleted(fullSchema, new Properties()));
  }

  private static byte[] kryoBytes(OverwriteWithLatestAvroPayload payload) {
    try (Output output = new Output(256, -1)) {
      payload.write(new Kryo(), output);
      return output.toBytes();
    }
  }

  private static OverwriteWithLatestAvroPayload kryoRoundTrip(OverwriteWithLatestAvroPayload payload) {
    OverwriteWithLatestAvroPayload restored = new OverwriteWithLatestAvroPayload(Option.empty());
    try (Input input = new Input(kryoBytes(payload))) {
      restored.read(new Kryo(), input);
    }
    return restored;
  }

  private static byte[] javaBytes(OverwriteWithLatestAvroPayload payload) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(payload);
    }
    return bytes.toByteArray();
  }

  private static OverwriteWithLatestAvroPayload javaRoundTrip(OverwriteWithLatestAvroPayload payload) throws Exception {
    try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(javaBytes(payload)))) {
      return (OverwriteWithLatestAvroPayload) input.readObject();
    }
  }

  @Test
  void resolvesOnlyMatchingNamedUnionBranchesDuringEvolution() throws Exception {
    Schema writer = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"unionRoot\",\"fields\":["
        + "{\"name\":\"item\",\"type\":[{\"type\":\"record\",\"name\":\"left\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"string\"}]},{\"type\":\"record\",\"name\":\"right\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"}]}]}]}");
    Schema reader = new Schema.Parser().parse(writer.toString().replace(
        "\"name\":\"id\",\"type\":\"string\"", "\"name\":\"id\",\"type\":\"string\"},"
            + "{\"name\":\"added\",\"type\":[\"null\",\"string\"],\"default\":null"));
    for (int branch = 0; branch < 2; branch++) {
      Schema branchSchema = writer.getField("item").schema().getTypes().get(branch);
      GenericRecord item = new GenericRecordBuilder(branchSchema).set(branch == 0 ? "id" : "name", "value").build();
      GenericRecord original = new GenericRecordBuilder(writer).set("item", item).build();
      IndexedRecord decoded = new OverwriteWithLatestAvroPayload(original, 1L).getInsertValue(reader).get();
      IndexedRecord actual = (IndexedRecord) decoded.get(0);
      assertEquals("value", actual.get(0).toString());
      if (branch == 0) {
        assertNull(actual.get(1));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "true,false", "false,true", "true,true"})
  void unionBranchesResolveRenamesIndependently(boolean reversedReaderBranches, boolean readerAliases) throws Exception {
    Schema writer = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"branches\",\"fields\":["
        + "{\"name\":\"item\",\"type\":[" + branch("A", "oldA", null) + "," + branch("B", "oldB", null) + "]}]}");
    String readerA = branch("A", "value", readerAliases ? "oldA" : null);
    String readerB = branch("B", "value", readerAliases ? "oldB" : null);
    Schema reader = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"branches\",\"fields\":["
        + "{\"name\":\"item\",\"type\":[" + (reversedReaderBranches ? readerB + "," + readerA : readerA + "," + readerB) + "]}]}");
    for (Schema branch : writer.getField("item").schema().getTypes()) {
      String value = "kept-" + branch.getName();
      GenericRecord item = new GenericRecordBuilder(branch).set(branch.getFields().get(0).name(), value).build();
      GenericRecord original = new GenericRecordBuilder(writer).set("item", item).build();
      IndexedRecord decoded = (IndexedRecord) new OverwriteWithLatestAvroPayload(original, 1L).getInsertValue(reader).get().get(0);
      assertEquals(branch.getName(), decoded.getSchema().getName());
      assertEquals(value, decoded.get(0).toString());
    }
  }

  private static String branch(String name, String field, String alias) {
    return "{\"type\":\"record\",\"name\":\"" + name + "\",\"fields\":[{\"name\":\"" + field + "\","
        + (alias == null ? "" : "\"aliases\":[\"" + alias + "\"],") + "\"type\":[\"null\",\"string\"],\"default\":null}]}";
  }

  @Test
  void recursiveRenameKeepsNestedValues() throws Exception {
    String node = "{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"old\",\"type\":[\"null\",\"string\"],\"default\":null},"
        + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"],\"default\":null},"
        + "{\"name\":\"children\",\"type\":{\"type\":\"array\",\"items\":\"Node\"},\"default\":[]}]}";
    Schema writer = new Schema.Parser().parse(node);
    Schema reader = new Schema.Parser().parse(node.replace("\"name\":\"old\"", "\"name\":\"new\""));
    GenericRecord deep = new GenericRecordBuilder(writer).set("old", "deep").build();
    GenericRecord next = new GenericRecordBuilder(writer).set("old", "next").set("next", deep).build();
    GenericRecord child = new GenericRecordBuilder(writer).set("old", "child").build();
    GenericRecord root = new GenericRecordBuilder(writer).set("old", "root").set("next", next)
        .set("children", Collections.singletonList(child)).build();

    GenericRecord decoded = (GenericRecord) new OverwriteWithLatestAvroPayload(root, 1L).getInsertValue(reader).get();
    assertEquals("root", decoded.get("new").toString());
    GenericRecord decodedNext = (GenericRecord) decoded.get("next");
    assertEquals("next", decodedNext.get("new").toString());
    assertEquals("deep", ((GenericRecord) decodedNext.get("next")).get("new").toString());
    assertEquals("child", ((GenericRecord) ((List<?>) decoded.get("children")).get(0)).get("new").toString());
    assertSame(reader, decoded.getSchema());
    assertSame(reader, decodedNext.getSchema());
  }

  @Test
  void mapValueRenameKeepsValues() throws Exception {
    String schema = "{\"type\":\"record\",\"name\":\"holder\",\"fields\":[{\"name\":\"entries\",\"type\":{\"type\":\"map\","
        + "\"values\":{\"type\":\"record\",\"name\":\"entry\",\"fields\":[{\"name\":\"old\",\"type\":[\"null\",\"string\"],\"default\":null}]}}}]}";
    Schema writer = new Schema.Parser().parse(schema);
    Schema reader = new Schema.Parser().parse(schema.replace("\"name\":\"old\"", "\"name\":\"new\""));
    Schema entry = writer.getField("entries").schema().getValueType();
    Map<String, GenericRecord> entries = new HashMap<>();
    entries.put("a", new GenericRecordBuilder(entry).set("old", "first").build());
    entries.put("b", new GenericRecordBuilder(entry).set("old", "second").build());
    GenericRecord original = new GenericRecordBuilder(writer).set("entries", entries).build();
    Map<?, ?> decoded = (Map<?, ?>) new OverwriteWithLatestAvroPayload(original, 1L).getInsertValue(reader).get().get(0);
    Map<String, String> values = new HashMap<>();
    decoded.forEach((key, value) -> values.put(key.toString(), ((GenericRecord) value).get("new").toString()));
    Map<String, String> expected = new HashMap<>();
    expected.put("a", "first");
    expected.put("b", "second");
    assertEquals(expected, values);
  }

  @Test
  void nullSchemaDecodesPositionallyAsBefore() throws IOException {
    Option<IndexedRecord> decoded = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L).getInsertValue(null);
    assertTrue(decoded.get() instanceof SerializableIndexedRecord);
  }

  @Test
  void projectionDoesNotChangeSerializedBytes() throws Exception {
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L);
    payload.getInsertValue(dataOnlySchema);
    for (OverwriteWithLatestAvroPayload restored : new OverwriteWithLatestAvroPayload[] {javaRoundTrip(payload), kryoRoundTrip(payload)}) {
      IndexedRecord full = restored.getInsertValue(fullSchema).get();
      assertEquals("20260807120000000", full.get(0).toString());
      assertEquals("alice", full.get(fullSchema.getField("name").pos()).toString());
    }
  }

  @Test
  void readsCompactKryoPayloadWithSuppliedSchema() throws Exception {
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L);
    Kryo kryo = new Kryo();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (Output output = new Output(bytes)) {
      byte[] recordBytes = payload.getRecordBytes();
      output.writeInt(recordBytes.length);
      output.writeBytes(recordBytes);
      kryo.writeClassAndObject(output, 100L);
      output.writeBoolean(false);
    }
    OverwriteWithLatestAvroPayload restored = new OverwriteWithLatestAvroPayload(Option.empty());
    try (Input input = new Input(bytes.toByteArray())) {
      restored.read(kryo, input);
    }
    assertEquals("alice", restored.getInsertValue(fullSchema).get().get(fullSchema.getField("name").pos()).toString());
    assertEquals(100L, restored.getOrderingValue());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void javaSerializationReadsGoldenPayloads(boolean deleted) throws Exception {
    String bytes = deleted ? GOLDEN_JAVA_TOMBSTONE : GOLDEN_JAVA_PAYLOAD;
    OverwriteWithLatestAvroPayload restored;
    try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(bytes)))) {
      restored = (OverwriteWithLatestAvroPayload) input.readObject();
    }
    assertEquals(42L, restored.getOrderingVal());
    Schema writerSchema = SchemaBuilder.record("payload").fields()
        .requiredString("id").requiredLong("ts").requiredString("value").endRecord();
    if (deleted) {
      assertFalse(restored.getInsertValue(writerSchema).isPresent());
    } else {
      Schema prefix = SchemaBuilder.record("payload").fields().requiredString("id").endRecord();
      assertEquals("key", restored.getInsertValue(prefix).get().get(0).toString());
      IndexedRecord record = restored.getInsertValue(writerSchema).get();
      assertEquals("key", record.get(0).toString());
      assertEquals(42L, record.get(1));
      assertEquals("updated", record.get(2).toString());
    }
  }

  @Test
  void getRecordHandlesProjectionSerializationAndEvolutionWithoutDataLoss() {
    assertAll(
        this::assertReaderFieldAliasIsResolved,
        this::assertNestedRenameCompatibilityIsPreserved,
        this::assertDefaultIsAppliedForAddedReaderField,
        this::assertReorderedFieldsAreProjectedByName);
  }

  private void assertReaderFieldAliasIsResolved() throws IOException {
    Schema writerSchema = new Schema.Parser().parse("{"
        + "\"type\":\"record\",\"name\":\"aliasedRecord\",\"fields\":["
        + "{\"name\":\"old_name\",\"type\":\"string\"}]}");
    Schema readerSchema = new Schema.Parser().parse("{"
        + "\"type\":\"record\",\"name\":\"aliasedRecord\",\"fields\":["
        + "{\"name\":\"new_name\",\"aliases\":[\"old_name\"],\"type\":\"string\"}]}");
    GenericRecord record = new GenericRecordBuilder(writerSchema)
        .set("old_name", "preserved-through-alias")
        .build();

    IndexedRecord decoded = new OverwriteWithLatestAvroPayload(record, 100L)
        .getInsertValue(readerSchema)
        .get();

    assertEquals("preserved-through-alias",
        String.valueOf(decoded.get(readerSchema.getField("new_name").pos())),
        "Avro reader-field aliases must participate in name-based schema resolution");
  }

  private void assertNestedRenameCompatibilityIsPreserved() throws IOException {
    Schema writerSchema = new Schema.Parser().parse("{"
        + "\"type\":\"record\",\"name\":\"outerRecord\",\"namespace\":\"org.apache.hudi\",\"fields\":["
        + "{\"name\":\"nested\",\"type\":{\"type\":\"record\",\"name\":\"nestedRecord\",\"fields\":["
        + "{\"name\":\"old_name\",\"type\":[\"null\",\"string\"],\"default\":null}]}}]}");
    Schema readerSchema = new Schema.Parser().parse("{"
        + "\"type\":\"record\",\"name\":\"outerRecord\",\"namespace\":\"org.apache.hudi\",\"fields\":["
        + "{\"name\":\"nested\",\"type\":{\"type\":\"record\",\"name\":\"nestedRecord\",\"fields\":["
        + "{\"name\":\"new_name\",\"type\":[\"null\",\"string\"],\"default\":null}]}}]}");
    Schema writerNestedSchema = writerSchema.getField("nested").schema();
    GenericRecord nestedRecord = new GenericRecordBuilder(writerNestedSchema)
        .set("old_name", "nested-value")
        .build();
    GenericRecord record = new GenericRecordBuilder(writerSchema)
        .set("nested", nestedRecord)
        .build();

    IndexedRecord decoded = new OverwriteWithLatestAvroPayload(record, 100L)
        .getInsertValue(readerSchema)
        .get();
    GenericRecord decodedNested = (GenericRecord) decoded.get(readerSchema.getField("nested").pos());

    assertEquals("nested-value",
        String.valueOf(decodedNested.get(readerSchema.getField("nested").schema().getField("new_name").pos())),
        "a nested rename keeps its value");
  }

  private void assertDefaultIsAppliedForAddedReaderField() throws IOException {
    Schema writerSchema = SchemaBuilder.record("addedFieldRecord")
        .fields()
        .requiredString("id")
        .endRecord();
    Schema readerSchema = SchemaBuilder.record("addedFieldRecord")
        .fields()
        .requiredString("id")
        .optionalString("new_field")
        .endRecord();
    GenericRecord record = new GenericRecordBuilder(writerSchema)
        .set("id", "1")
        .build();

    IndexedRecord decoded = new OverwriteWithLatestAvroPayload(record, 100L)
        .getInsertValue(readerSchema)
        .get();

    assertEquals("1", String.valueOf(decoded.get(readerSchema.getField("id").pos())));
    assertNull(decoded.get(readerSchema.getField("new_field").pos()),
        "an added reader field must receive its Avro default instead of being decoded past the writer bytes");
  }

  private void assertReorderedFieldsAreProjectedByName() throws IOException {
    Schema reorderedSchema = SchemaBuilder.record("reorderedProjection")
        .fields()
        .requiredString("price")
        .requiredString("id")
        .requiredString("name")
        .requiredLong("ts")
        .endRecord();

    IndexedRecord decoded = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L)
        .getInsertValue(reorderedSchema)
        .get();

    assertEquals("9.99", String.valueOf(decoded.get(reorderedSchema.getField("price").pos())));
    assertEquals("1", String.valueOf(decoded.get(reorderedSchema.getField("id").pos())));
    assertEquals("alice", String.valueOf(decoded.get(reorderedSchema.getField("name").pos())));
    assertEquals(100L, decoded.get(reorderedSchema.getField("ts").pos()));
  }
}
