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

import org.apache.hudi.common.util.Option;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link BaseAvroPayload#getRecord(Schema)}, exercised through the concrete
 * {@link OverwriteWithLatestAvroPayload} subclass.
 *
 * <p>Regression coverage for a positional-misdecode bug: when a payload's true (writer) schema
 * differs from the schema requested by the caller -- e.g. the caller asks for a data-fields-only
 * projection of a schema that also declares the five {@code _hoodie_} meta fields -- decoding must
 * resolve fields by name, not by position.
 */
class TestBaseAvroPayload {

  // Mirrors what withPopulateMetaFields(true) produces: the five _hoodie_ meta fields ahead of the
  // data fields.
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

  // Same data fields, without the meta fields -- e.g. a merge path's data-only projection schema.
  private final Schema dataOnlySchema = SchemaBuilder.record("dataOnly")
      .fields()
      .requiredString("id")
      .requiredLong("ts")
      .requiredString("name")
      .requiredString("price")
      .optionalBoolean(HoodieRecord.HOODIE_IS_DELETED_FIELD)
      .endRecord();

  // Same fields as fullSchema, plus one required field with no default that the writer never had --
  // used to lock in the intentional loud-failure contract (AvroTypeException), not silent corruption.
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

    // The record's true schema (10 fields, meta fields first) is a distinct instance from the
    // requested schema (5 fields, no meta fields) even though the shared fields have identical
    // names/types -- this bypasses the reference-equality fast path and forces a re-decode, which is
    // exactly where the positional-misdecode defect corrupted data fields to null.
    IndexedRecord projected = payload.getInsertValue(dataOnlySchema).get();

    // Binary-decoded Avro strings come back as org.apache.avro.util.Utf8, not java.lang.String -- compare
    // via toString() rather than equals() so this test only asserts on the actual data-corruption bug
    // (fields nulled/misaligned) and not on that unrelated representational detail.
    assertEquals("1", String.valueOf(projected.get(dataOnlySchema.getField("id").pos())));
    assertEquals(100L, projected.get(dataOnlySchema.getField("ts").pos()));
    assertEquals("alice", String.valueOf(projected.get(dataOnlySchema.getField("name").pos())));
    assertEquals("9.99", String.valueOf(projected.get(dataOnlySchema.getField("price").pos())));
    assertEquals(false, projected.get(dataOnlySchema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD).pos()));
  }

  @Test
  void getRecordStaysCorrectAcrossMultipleDifferentSchemaRequestsNarrowThenFull() throws IOException {
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L);

    // First call: project down to the narrow, data-only schema -- this reassigns the payload's internal
    // cached record to the narrow-schema decode result.
    IndexedRecord narrow = payload.getInsertValue(dataOnlySchema).get();
    assertEquals("1", String.valueOf(narrow.get(dataOnlySchema.getField("id").pos())));

    // Second call: request the FULL schema again, via a schema instance that is NOT reference-equal to the
    // original `fullSchema` (a fresh parse of the same definition) -- this still misses the
    // reference-equality fast path and forces a real re-decode. Before the writer-schema-caching fix, this
    // call derived the "writer schema" from the now-narrow cached record left behind by the FIRST call
    // above, instead of the payload's true (full) writer schema -- decoding the full-width bytes as if
    // they were narrow-width. Depending on the exact bytes, this pre-fix bug surfaced as either silent,
    // positionally-misaligned garbage or a hard decoding exception (e.g. "Malformed data. Length is
    // negative") -- this test covers that direction of the two-call sequence; the reverse direction
    // (full-schema call first) is covered separately below.
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

    // First call: request the FULL schema via a schema instance that is NOT reference-equal to the
    // original `fullSchema` (a fresh parse of the same definition) -- this misses the reference-equality
    // fast path and forces a real re-decode, reassigning the payload's internal cached record to this
    // (still full-width) decode result.
    Schema fullSchemaCopy = new Schema.Parser().parse(fullSchema.toString());
    IndexedRecord full = payload.getInsertValue(fullSchemaCopy).get();
    assertEquals("1", String.valueOf(full.get(fullSchemaCopy.getField("id").pos())));

    // Second call: project down to the narrow, data-only schema. This is the OTHER call order a second,
    // independent verifier found the pre-fix bug in: because the two orders decode against structurally
    // different byte layouts, this direction is not guaranteed to fail the same way as narrow-then-full
    // above (that one produced silent garbage; this one produced a hard AvroRuntimeException) -- both
    // orders must be covered, not just one.
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

    // isDeleted() decodes against the narrow (data-only) schema first, exercising getRecord(schema)
    // exactly like the CoW/MoR merge paths reached during a real write.
    assertFalse(payload.isDeleted(dataOnlySchema, new Properties()));

    // ...then getInsertValue() is asked for a DIFFERENT (full) schema instance: this is the
    // isDeleted -> getInsertValue sequence that reaches the writer-schema-caching bug via
    // DefaultHoodieRecordPayload#isDeleted (getRecord(schema)) followed by prependMetaFields /
    // rewriteRecordWithNewSchema calling getInsertValue(recordSchema) with a distinct Schema instance.
    Schema fullSchemaCopy = new Schema.Parser().parse(fullSchema.toString());
    IndexedRecord result = payload.getInsertValue(fullSchemaCopy, new Properties()).get();

    assertEquals("1", String.valueOf(result.get(fullSchemaCopy.getField("id").pos())));
    assertEquals("alice", String.valueOf(result.get(fullSchemaCopy.getField("name").pos())));
    assertEquals(100L, result.get(fullSchemaCopy.getField("ts").pos()));
  }

  @Test
  void getRecordThrowsWhenReaderSchemaHasNonDefaultedFieldMissingFromWriterSchema() {
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L);

    // A reader-schema field with no default that the writer schema never had must fail loudly
    // (AvroTypeException) rather than silently decode a garbage/positionally-misaligned value -- this is
    // the documented, intentional contract of resolving-by-name decoding (see class javadoc).
    assertThrows(AvroTypeException.class, () -> payload.getInsertValue(readerSchemaWithUnresolvableField),
        "a reader schema field with no default that the writer schema lacks must fail loudly, not decode garbage");
  }

  @Test
  void getRecordFallsBackToPositionalDecodeForRenamedFieldWithDefault() throws IOException {
    // Schema-evolution shape seen on the Hive/Presto MoR realtime read path: a column was renamed after
    // the log record was written, so the requested schema carries the NEW name (a nullable field with a
    // null default) while the payload's writer schema still has the OLD name at the same position. The
    // merge path relies on the legacy positional decode to carry the value across the rename -- resolving
    // by name instead would silently null the field out (see TestHiveTableSchemaEvolution).
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

  @Test
  void javaSerializationRetainsOriginalSchemaAfterProjection() throws Exception {
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L);
    payload.getInsertValue(dataOnlySchema);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(payload);
    }
    try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      OverwriteWithLatestAvroPayload restored = (OverwriteWithLatestAvroPayload) input.readObject();
      IndexedRecord full = restored.getInsertValue(fullSchema).get();
      assertEquals("alice", full.get(fullSchema.getField("name").pos()).toString());
      assertEquals("1", restored.getInsertValue(dataOnlySchema).get().get(dataOnlySchema.getField("id").pos()).toString());
    }
  }

  @Test
  void readsLegacyKryoPayloadWithSuppliedWriterSchema() throws Exception {
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
    assertEquals("1", restored.getInsertValue(dataOnlySchema).get().get(dataOnlySchema.getField("id").pos()).toString());
    assertEquals(100L, restored.getOrderingValue());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void javaSerializationReadsPreMorPayloads(boolean deleted) throws Exception {
    // Generated with BaseAvroPayload from pre-MOR commit 8e884960820a081e34cdce8e78505e9ed54dff28.
    // Materialize record bytes before using that version's default Java serializer. Golden bytes
    // catch serialVersionUID changes that a same-version round trip cannot detect.
    String bytes = deleted
        ? "rO0ABXNyADtvcmcuYXBhY2hlLmh1ZGkuY29tbW9uLm1vZGVsLk92ZXJ3cml0ZVdpdGhMYXRlc3RBdnJvUGF5bG9hZGa/j5KgaG9h"
        + "AgAAeHIALG9yZy5hcGFjaGUuaHVkaS5jb21tb24ubW9kZWwuQmFzZUF2cm9QYXlsb2FkOJGhgVrvqjUCAANaAA9pc0RlbGV0ZWRS"
        + "ZWNvcmRMAAtvcmRlcmluZ1ZhbHQAFkxqYXZhL2xhbmcvQ29tcGFyYWJsZTtbAAtyZWNvcmRCeXRlc3QAAltCeHABc3IADmphdmEu"
        + "bGFuZy5Mb25nO4vkkMyPI98CAAFKAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAAAAAAqdXIAAltC"
        + "rPMX+AYIVOACAAB4cAAAAAA="
        : "rO0ABXNyADtvcmcuYXBhY2hlLmh1ZGkuY29tbW9uLm1vZGVsLk92ZXJ3cml0ZVdpdGhMYXRlc3RBdnJvUGF5bG9hZGa/j5KgaG9h"
        + "AgAAeHIALG9yZy5hcGFjaGUuaHVkaS5jb21tb24ubW9kZWwuQmFzZUF2cm9QYXlsb2FkOJGhgVrvqjUCAANaAA9pc0RlbGV0ZWRS"
        + "ZWNvcmRMAAtvcmRlcmluZ1ZhbHQAFkxqYXZhL2xhbmcvQ29tcGFyYWJsZTtbAAtyZWNvcmRCeXRlc3QAAltCeHAAc3IADmphdmEu"
        + "bGFuZy5Mb25nO4vkkMyPI98CAAFKAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAAAAAAqdXIAAltC"
        + "rPMX+AYIVOACAAB4cAAAAA0Ga2V5VA51cGRhdGVk";
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
      assertEquals("key", restored.getInsertValue(writerSchema).get().get(0).toString());
      Schema projection = SchemaBuilder.record("payload").fields().requiredString("value").requiredString("id").endRecord();
      IndexedRecord record = restored.getInsertValue(projection).get();
      assertEquals("updated", record.get(0).toString());
      assertEquals("key", record.get(1).toString());
      assertEquals(42L, restored.getInsertValue(writerSchema).get().get(1));
    }
  }

  @Test
  void legacyFormatDoesNotAffectIndependentSelfContainedPayloads() throws IOException {
    Kryo legacy = new Kryo();
    BaseAvroPayload.useLegacyKryoFormat(legacy);
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L);
    try (Output output = new Output(256, -1)) {
      payload.write(legacy, output);
      try (Input input = new Input(output.toBytes())) {
        assertEquals(payload.getRecordBytes().length, input.readInt());
      }
    }
    assertProjectionByNameAfterKryoRoundTrip();
  }

  @Test
  void getRecordHandlesProjectionSerializationAndEvolutionWithoutDataLoss() {
    assertAll(
        this::assertProjectionByNameAfterKryoRoundTrip,
        this::assertReaderFieldAliasIsResolved,
        this::assertNestedRenameCompatibilityIsPreserved,
        this::assertDefaultIsAppliedForAddedReaderField,
        this::assertReorderedFieldsAreProjectedByName);
  }

  private void assertProjectionByNameAfterKryoRoundTrip() throws IOException {
    OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(newFullRecord(), 100L);

    // Self-contained payloads must support projection without the original record or Kryo instance.
    Kryo kryo = new Kryo();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (Output output = new Output(bytes)) {
      payload.write(kryo, output);
    }
    OverwriteWithLatestAvroPayload restored = new OverwriteWithLatestAvroPayload(Option.empty());
    try (Input input = new Input(new ByteArrayInputStream(bytes.toByteArray()))) {
      restored.read(new Kryo(), input);
    }
    IndexedRecord projected = restored.getInsertValue(dataOnlySchema).get();

    assertEquals("1", String.valueOf(projected.get(dataOnlySchema.getField("id").pos())));
    assertEquals(100L, projected.get(dataOnlySchema.getField("ts").pos()));
    assertEquals("alice", String.valueOf(projected.get(dataOnlySchema.getField("name").pos())));
    assertEquals("9.99", String.valueOf(projected.get(dataOnlySchema.getField("price").pos())));
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
        "nested evolution must not be misclassified as a pure top-level projection and silently null data");
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
