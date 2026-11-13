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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.core.io.storage.HoodieAvroBootstrapFileReader;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.variant.Spark4VariantShreddingProvider;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.parquet.avro.HoodieAvroParquetSchemaConverter.getAvroSchemaConverter;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip coverage for the successful {@code create} -> {@code reconstruct} path of
 * {@link HoodieVariantReconstruction}: the AVRO read-path orchestration (isTarget alignment,
 * shredded/unshredded sub-schema indexing, and per-record field mapping).
 *
 * <p>Runs in hudi-spark4-common so the real {@link Spark4VariantShreddingProvider} is auto-detected
 * on the classpath; the package is {@code org.apache.hudi.io.storage.hadoop} to reach the
 * package-private class. Spark compaction never reaches this path (it reads base files via the
 * InternalRow reader), so this is the only place the alignment is exercised end to end.
 * The null/throw guards of {@code create} are covered by {@code TestHoodieVariantReconstruction}
 * (hudi-hadoop-common); the provider's shred/rebuild in isolation by {@code TestSpark4VariantShreddingProvider}.
 * Files with typed_value encodings Hudi's writer never produces are read through {@link HoodieAvroParquetReader},
 * which hands the reconstruction the footer's parquet type.
 */
class TestHoodieVariantReconstructionRoundTrip {

  @Test
  void createThenReconstructRebuildsVariantAndPassesThroughNonVariant(@TempDir Path tmp) throws Exception {
    // A shredded variant column "v" alongside a non-variant column "id", to exercise field alignment.
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.STRING));
    shreddedFields.put("b", HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema.Variant shreddedVariant = HoodieSchema.createVariantShreddedObject(null, null, null, shreddedFields);
    HoodieSchema.Variant unshreddedVariant = HoodieSchema.createVariant();

    HoodieSchema fileSchema = HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("v", shreddedVariant)));
    HoodieSchema requestedSchema = HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("v", unshreddedVariant)));

    HoodieStorage storage = HoodieTestUtils.getStorage(tmp.toString()); // allow.reading.shredded defaults true
    HoodieVariantReconstruction reconstruction =
        HoodieVariantReconstruction.create(fileSchema,
            getAvroSchemaConverter(storage.getConf().unwrapAs(Configuration.class)).convert(fileSchema),
            requestedSchema, storage);
    assertNotNull(reconstruction, "create should build a reconstruction for a shredded variant column");

    // Build the input the parquet-avro reader would produce: a record at the intermediate (shredded)
    // schema, i.e. {id, v=<shredded {metadata, value, typed_value}>}.
    Spark4VariantShreddingProvider provider = new Spark4VariantShreddingProvider();
    Variant original = VariantBuilder.parseJson("{\"a\":\"x\",\"b\":5}", false);
    GenericRecord unshreddedV = new GenericData.Record(unshreddedVariant.getAvroSchema());
    unshreddedV.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(original.getMetadata()));
    unshreddedV.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, ByteBuffer.wrap(original.getValue()));
    GenericRecord shreddedV =
        provider.shredVariantRecord(unshreddedV, shreddedVariant.getAvroSchema(), shreddedVariant);

    GenericRecord input = new GenericData.Record(reconstruction.intermediateSchema().getAvroSchema());
    input.put("id", 7L);
    input.put("v", shreddedV);

    IndexedRecord out = reconstruction.reconstruct(input);

    // Non-variant column passes through unchanged at its position; the variant column is rebuilt unshredded.
    assertEquals(7L, out.get(0));
    GenericRecord rebuiltV = (GenericRecord) out.get(1);
    Variant rebuilt = new Variant(
        toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD)),
        toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD)));
    assertEquals(original.toJson(ZoneOffset.UTC), rebuilt.toJson(ZoneOffset.UTC));
  }

  @Test
  void createThenReconstructRebuildsAValueLessShreddedGroup(@TempDir Path tmp) throws Exception {
    // The shredding spec lets a writer omit `value` when nothing is left over. TestHoodieVariantReconstruction
    // pins that Hudi detects the resulting two-field group, but its stub provider ignores the parquet
    // group, so this is the only place the real provider sees the shape: buildReadVariantSchema assigns
    // variantIdx = -1 there, which shifts every other ordinal and is what ShreddingUtils.rebuild reads to
    // decide there is no residual. Get the mapping wrong and the rebuild reads the wrong Avro field.
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.STRING));
    shreddedFields.put("b", HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema.Variant shreddedVariant = HoodieSchema.createVariantShreddedObject(null, null, null, shreddedFields);
    HoodieSchema.Variant unshreddedVariant = HoodieSchema.createVariant();

    // Shred with the real provider first, then drop the (null) top-level residual, which yields exactly
    // what a writer that omits `value` would have put on disk.
    Spark4VariantShreddingProvider provider = new Spark4VariantShreddingProvider();
    Variant original = VariantBuilder.parseJson("{\"a\":\"x\",\"b\":5}", false);
    GenericRecord unshreddedV = new GenericData.Record(unshreddedVariant.getAvroSchema());
    unshreddedV.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(original.getMetadata()));
    unshreddedV.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, ByteBuffer.wrap(original.getValue()));
    GenericRecord shreddedV =
        provider.shredVariantRecord(unshreddedV, shreddedVariant.getAvroSchema(), shreddedVariant);
    assertNull(shreddedV.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD),
        "both fields matched the shredding schema, so there is no residual and dropping `value` loses nothing");

    // The file side is a plain {metadata, typed_value} record: a real footer conversion loses the variant
    // logical type, so this is also what shape detection has to recognize.
    HoodieSchema valueLessShredded = HoodieSchema.createRecord("v", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of(HoodieSchema.Variant.VARIANT_METADATA_FIELD, HoodieSchema.create(HoodieSchemaType.BYTES)),
        HoodieSchemaField.of(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD,
            shreddedVariant.getTypedValueField().get())));
    HoodieSchema fileSchema = HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("v", valueLessShredded)));
    HoodieSchema requestedSchema = HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("v", unshreddedVariant)));

    HoodieStorage storage = HoodieTestUtils.getStorage(tmp.toString()); // allow.reading.shredded defaults true
    HoodieVariantReconstruction reconstruction =
        HoodieVariantReconstruction.create(fileSchema,
            getAvroSchemaConverter(storage.getConf().unwrapAs(Configuration.class)).convert(fileSchema),
            requestedSchema, storage);
    assertNotNull(reconstruction, "a shredded group with no value column must still engage");

    GenericRecord valueLessV = new GenericData.Record(
        reconstruction.intermediateSchema().getAvroSchema().getField("v").schema());
    valueLessV.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD,
        shreddedV.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD));
    valueLessV.put(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD,
        shreddedV.get(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD));

    GenericRecord input = new GenericData.Record(reconstruction.intermediateSchema().getAvroSchema());
    input.put("id", 7L);
    input.put("v", valueLessV);

    IndexedRecord out = reconstruction.reconstruct(input);

    assertEquals(7L, out.get(0));
    GenericRecord rebuiltV = (GenericRecord) out.get(1);
    Variant rebuilt = new Variant(
        toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD)),
        toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD)));
    assertEquals(original.toJson(ZoneOffset.UTC), rebuilt.toJson(ZoneOffset.UTC));
  }

  @Test
  void requiredShreddedColumnReadsThroughANullableRequest(@TempDir Path tmp) throws Exception {
    // Spark 4.1 writes a non-null variant column REQUIRED (a DataFrame insert of a non-null
    // parse_json, or a vanilla Spark file brought in by bootstrap) while the table schema the merge
    // requests has it nullable (#20349). parquet-avro must read the REQUIRED group through the
    // nullable shredded read schema, and every row must rebuild into the requested schema.
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.STRING));
    shreddedFields.put("b", HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema.Variant shreddedVariant = HoodieSchema.createVariantShreddedObject(null, null, null, shreddedFields);
    HoodieSchema.Variant unshreddedVariant = HoodieSchema.createVariant();
    HoodieSchema fileSchema = HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("v", shreddedVariant)));

    // Fully typed, partly residual (b missing, c extra), a scalar and a variant null.
    Spark4VariantShreddingProvider provider = new Spark4VariantShreddingProvider();
    String[] jsons = {"{\"a\":\"x\",\"b\":5}", "{\"a\":\"y\",\"c\":true}", "42", "null"};
    Path file = tmp.resolve("required.parquet");
    try (AvroParquetWriter<GenericRecord> writer =
             new AvroParquetWriter<>(new org.apache.hadoop.fs.Path(file.toString()), fileSchema.getAvroSchema())) {
      for (int i = 0; i < jsons.length; i++) {
        Variant variant = VariantBuilder.parseJson(jsons[i], false);
        GenericRecord unshreddedV = new GenericData.Record(unshreddedVariant.getAvroSchema());
        unshreddedV.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(variant.getMetadata()));
        unshreddedV.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, ByteBuffer.wrap(variant.getValue()));
        GenericRecord record = new GenericData.Record(fileSchema.getAvroSchema());
        record.put("id", (long) i);
        record.put("v", provider.shredVariantRecord(unshreddedV, shreddedVariant.getAvroSchema(), shreddedVariant));
        writer.write(record);
      }
    }

    HoodieStorage storage = HoodieTestUtils.getStorage(tmp.toString());
    HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, new StoragePath(file.toUri().toString()));
    assertFalse(reader.getSchema().getField("v").get().schema().isNullable(), "the file must hold v REQUIRED");
    HoodieSchema requestedSchema = HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("v", HoodieSchema.createNullable(unshreddedVariant), null, HoodieSchema.NULL_VALUE)));
    try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(requestedSchema, requestedSchema)) {
      for (int i = 0; i < jsons.length; i++) {
        assertTrue(iterator.hasNext());
        IndexedRecord out = iterator.next();
        assertEquals(requestedSchema.getAvroSchema(), out.getSchema());
        assertEquals((long) i, out.get(0));
        GenericRecord rebuiltV = (GenericRecord) out.get(1);
        Variant rebuilt = new Variant(
            toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD)),
            toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD)));
        assertEquals(VariantBuilder.parseJson(jsons[i], false).toJson(ZoneOffset.UTC), rebuilt.toJson(ZoneOffset.UTC));
      }
      assertFalse(iterator.hasNext());
    }
    reader.close();
  }

  private static final String VARIANT_TEST_NAMESPACE = "org.apache.hudi.test";

  private static Variant scalar(Consumer<VariantBuilder> append) {
    VariantBuilder builder = new VariantBuilder(false);
    append.accept(builder);
    return builder.result();
  }

  /** A required shredded variant group {@code column} with the given optional typed_value declaration. */
  private static String shreddedGroup(String column, String typedValue) {
    return " required group " + column + " { required binary metadata; optional binary value; optional "
        + typedValue + "; }";
  }

  private static StoragePath writeParquet(Path tmp, MessageType schema, Group row) throws IOException {
    Path file = tmp.resolve("shredded.parquet");
    try (ParquetWriter<Group> writer =
             ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(file.toString())).withType(schema).build()) {
      writer.write(row);
    }
    return new StoragePath(file.toUri().toString());
  }

  @Test
  void readerRebuildsTypedValueEncodingsTheFooterConversionLoses(@TempDir Path tmp) throws Exception {
    // A file from another writer whose typed_value leaves the footer's Avro conversion reads as a plain
    // int, long, fixed and bytes. The reader decodes each leaf by its parquet type, as Spark 4 does.
    String json = "{\"a\":[1,2]}";
    byte[] bytes = {1, 2, 3, (byte) 0xFF};
    long millis = 1_700_000_000_123L;
    MessageType fileSchema = MessageTypeParser.parseMessageType("message r {"
        + shreddedGroup("v_dec", "int32 typed_value (DECIMAL(9,2))")
        + shreddedGroup("v_ts", "int64 typed_value (TIMESTAMP(MILLIS,true))")
        + shreddedGroup("v_fixed", "fixed_len_byte_array(4) typed_value")
        + shreddedGroup("v_u64", "int64 typed_value (INTEGER(64,false))")
        + shreddedGroup("v_json", "binary typed_value (JSON)") + " }");
    Binary metadata = Binary.fromConstantByteArray(scalar(VariantBuilder::appendNull).getMetadata());
    Group row = new SimpleGroupFactory(fileSchema).newGroup();
    row.addGroup("v_dec").append(HoodieSchema.Variant.VARIANT_METADATA_FIELD, metadata)
        .append(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD, 150);
    row.addGroup("v_ts").append(HoodieSchema.Variant.VARIANT_METADATA_FIELD, metadata)
        .append(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD, millis);
    row.addGroup("v_fixed").append(HoodieSchema.Variant.VARIANT_METADATA_FIELD, metadata)
        .append(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD, Binary.fromConstantByteArray(bytes));
    row.addGroup("v_u64").append(HoodieSchema.Variant.VARIANT_METADATA_FIELD, metadata)
        .append(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD, -1L);
    row.addGroup("v_json").append(HoodieSchema.Variant.VARIANT_METADATA_FIELD, metadata)
        .append(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD, json);

    Map<String, Variant> expected = new LinkedHashMap<>();
    expected.put("v_dec", scalar(b -> b.appendDecimal(new BigDecimal("1.50"))));
    expected.put("v_ts", scalar(b -> b.appendTimestamp(millis * 1000)));
    expected.put("v_fixed", scalar(b -> b.appendBinary(bytes)));
    expected.put("v_u64", scalar(b -> b.appendDecimal(new BigDecimal("18446744073709551615"))));
    expected.put("v_json", scalar(b -> b.appendString(json)));
    HoodieSchema requestedSchema = HoodieSchema.createRecord("r", VARIANT_TEST_NAMESPACE, null,
        expected.keySet().stream()
            .map(name -> HoodieSchemaField.of(name, HoodieSchema.createVariant(name, VARIANT_TEST_NAMESPACE, null)))
            .collect(Collectors.toList()));

    HoodieStorage storage = HoodieTestUtils.getStorage(tmp.toString());
    try (HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, writeParquet(tmp, fileSchema, row));
         ClosableIterator<IndexedRecord> records = reader.getIndexedRecordIterator(requestedSchema, requestedSchema)) {
      GenericRecord out = (GenericRecord) records.next();
      for (Map.Entry<String, Variant> column : expected.entrySet()) {
        GenericRecord rebuiltV = (GenericRecord) out.get(column.getKey());
        Variant rebuilt = new Variant(
            toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD)),
            toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD)));
        assertEquals(column.getValue().toJson(ZoneOffset.UTC), rebuilt.toJson(ZoneOffset.UTC), column.getKey());
        // The binary also pins the variant type (e.g. decimal vs long), which the json can hide.
        assertArrayEquals(column.getValue().getValue(), rebuilt.getValue(), column.getKey());
      }
    }
  }

  @Test
  void readerFailsOnOpenForTypedValueEncodingSparkRejects(@TempDir Path tmp) throws Exception {
    // Spark 4 rejects a TIMESTAMP(NANOS) typed_value when it opens the file, whatever the rows hold. So
    // does the reader, here with every typed_value null and the variant kept in the residual value.
    MessageType fileSchema = MessageTypeParser.parseMessageType("message r {"
        + shreddedGroup("v", "int64 typed_value (TIMESTAMP(NANOS,true))") + " }");
    Variant variant = scalar(b -> b.appendLong(7));
    Group row = new SimpleGroupFactory(fileSchema).newGroup();
    row.addGroup("v")
        .append(HoodieSchema.Variant.VARIANT_METADATA_FIELD, Binary.fromConstantByteArray(variant.getMetadata()))
        .append(HoodieSchema.Variant.VARIANT_VALUE_FIELD, Binary.fromConstantByteArray(variant.getValue()));
    HoodieSchema requestedSchema = HoodieSchema.createRecord("r", VARIANT_TEST_NAMESPACE, null,
        Collections.singletonList(HoodieSchemaField.of("v", HoodieSchema.createVariant())));

    HoodieStorage storage = HoodieTestUtils.getStorage(tmp.toString());
    try (HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, writeParquet(tmp, fileSchema, row))) {
      HoodieException e = assertThrows(HoodieException.class,
          () -> reader.getIndexedRecordIterator(requestedSchema, requestedSchema));
      assertTrue(e.getMessage().contains("column 'v'") && e.getMessage().contains("TIMESTAMP(NANOS,true)"),
          e.getMessage());
    }
  }

  private static byte[] toBytes(Object byteBuffer) {
    ByteBuffer buf = ((ByteBuffer) byteBuffer).duplicate();
    byte[] out = new byte[buf.remaining()];
    buf.get(out);
    return out;
  }

  /**
   * What every bootstrap leg expects of the joined row read at {@code requestedSchema}: the
   * variant column survives the join, rebuilds to {@code original}, and the data column below it
   * keeps its value rather than shifting.
   */
  private static void assertBootstrapRoundTrip(IndexedRecord out, HoodieSchema requestedSchema, Variant original) {
    GenericRecord rebuiltV = (GenericRecord) out.get(requestedSchema.getAvroSchema().getField("v").pos());
    assertNotNull(rebuiltV, "the variant column must survive the bootstrap join");
    Variant rebuilt = new Variant(
        toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_VALUE_FIELD)),
        toBytes(rebuiltV.get(HoodieSchema.Variant.VARIANT_METADATA_FIELD)));
    assertEquals(original.toJson(ZoneOffset.UTC), rebuilt.toJson(ZoneOffset.UTC),
        "the shredded payload must be reconstructed, not dropped");
    assertEquals(7L, out.get(requestedSchema.getAvroSchema().getField("id").pos()),
        "the data column must keep its value");
  }

  @Test
  void bootstrapReaderReconstructsShreddedDataFileUsingTableSchema(@TempDir java.nio.file.Path tmp) throws Exception {
    // HoodieMergeHelper's bootstrap branch uses the one-argument getRecordIterator overload,
    // which used to hand the DATA file its own footer schema: for a shredded variant that is a
    // plain {metadata, value, typed_value} record with the logical type lost, so reconstruction
    // never engaged and the later rewrite to the writer schema silently dropped typed_value.
    // The overload now requests the caller's schema (minus meta fields), which anchors
    // HoodieVariantReconstruction just like the two-argument form.
    //
    // AVRO bootstrap reader only, deliberately: the changed line lives on the shared base
    // class, but the SPARK twin reads through HoodieSparkParquetReader (whose getSchema returns
    // a nullable union, not a record) and would need a Spark session plus InternalRow
    // assertions that do not fit this unit test. The non-variant SPARK bootstrap path is
    // covered end-to-end by TestBootstrap.
    Map<String, HoodieSchema> shreddedFields = new LinkedHashMap<>();
    shreddedFields.put("a", HoodieSchema.create(HoodieSchemaType.STRING));
    shreddedFields.put("b", HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema.Variant shreddedVariant = HoodieSchema.createVariantShreddedObject(null, null, null, shreddedFields);
    HoodieSchema.Variant unshreddedVariant = HoodieSchema.createVariant();
    // Data fields are nullable with null defaults, as Hudi table schemas declare them; the
    // skeleton read requests the full schema and fills the data columns with the defaults.
    HoodieSchema dataFileSchema = HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.createNullable(HoodieSchemaType.LONG), null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("v", HoodieSchema.createNullable(shreddedVariant), null, HoodieSchema.NULL_VALUE)));

    // The shredded data file, as an external bootstrap source would carry it.
    Spark4VariantShreddingProvider provider = new Spark4VariantShreddingProvider();
    Variant original = VariantBuilder.parseJson("{\"a\":\"x\",\"b\":5}", false);
    GenericRecord unshreddedV = new GenericData.Record(unshreddedVariant.getAvroSchema());
    unshreddedV.put(HoodieSchema.Variant.VARIANT_METADATA_FIELD, ByteBuffer.wrap(original.getMetadata()));
    unshreddedV.put(HoodieSchema.Variant.VARIANT_VALUE_FIELD, ByteBuffer.wrap(original.getValue()));
    GenericRecord shreddedV =
        provider.shredVariantRecord(unshreddedV, shreddedVariant.getAvroSchema(), shreddedVariant);
    java.nio.file.Path dataFile = tmp.resolve("data.parquet");
    try (AvroParquetWriter<GenericRecord> writer =
             new AvroParquetWriter<>(new org.apache.hadoop.fs.Path(dataFile.toString()), dataFileSchema.getAvroSchema())) {
      GenericRecord record = new GenericData.Record(dataFileSchema.getAvroSchema());
      record.put("id", 7L);
      record.put("v", shreddedV);
      writer.write(record);
    }

    // The skeleton file: just the meta columns for the row.
    HoodieSchema skeletonSchema = HoodieSchemaUtils.addMetadataFields(
        HoodieSchema.createRecord("r", "org.apache.hudi.test", null, java.util.Collections.emptyList()));
    java.nio.file.Path skeletonFile = tmp.resolve("skeleton.parquet");
    try (AvroParquetWriter<GenericRecord> writer =
             new AvroParquetWriter<>(new org.apache.hadoop.fs.Path(skeletonFile.toString()), skeletonSchema.getAvroSchema())) {
      GenericRecord meta = new GenericData.Record(skeletonSchema.getAvroSchema());
      meta.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, "001");
      meta.put(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, "001_0_1");
      meta.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, "key-7");
      meta.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, "");
      meta.put(HoodieRecord.FILENAME_METADATA_FIELD, "data.parquet");
      writer.write(meta);
    }

    HoodieStorage storage = HoodieTestUtils.getStorage(tmp.toString());
    HoodieAvroFileReaderFactory readerFactory = new HoodieAvroFileReaderFactory(storage);
    HoodieFileReader skeletonReader =
        readerFactory.getFileReader(new HoodieConfig(), new StoragePath(skeletonFile.toUri().toString()));
    HoodieFileReader dataReader =
        readerFactory.getFileReader(new HoodieConfig(), new StoragePath(dataFile.toUri().toString()));
    HoodieAvroBootstrapFileReader bootstrapReader = (HoodieAvroBootstrapFileReader)
        readerFactory.newBootstrapFileReader(skeletonReader, dataReader, Option.empty(), new Object[0]);

    // The merge helper requests the writer schema with meta fields: an UNSHREDDED variant.
    HoodieSchema tableSchema = HoodieSchemaUtils.addMetadataFields(
        HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.createNullable(HoodieSchemaType.LONG), null, HoodieSchema.NULL_VALUE),
            HoodieSchemaField.of("v", HoodieSchema.createNullable(unshreddedVariant), null, HoodieSchema.NULL_VALUE))));
    try (ClosableIterator<HoodieRecord<IndexedRecord>> iterator =
             (ClosableIterator) bootstrapReader.getRecordIterator(tableSchema)) {
      assertTrue(iterator.hasNext(), "the joined bootstrap row must come back");
      IndexedRecord out = (IndexedRecord) iterator.next().getData();
      assertBootstrapRoundTrip(out, tableSchema, original);
    }

    // The alignment only bites when the requested schema holds a column the external data file
    // does not: a partitioned bootstrap table's partition column (the source is read with
    // basePath, so the table schema gains it while the file lacks it) or a column added after
    // bootstrap. Reading the data file at its own footer schema also mis-sizes the meta-field
    // prefix in HoodieAvroIndexedRecord.prependMetaFields, which shifts every data column.
    // HUDI-5392 (#7461) pinned the same "request the caller's schema" alignment for arrays, which
    // were read at the wrong LIST level when the request came from Hudi instead of the file.
    HoodieSchema partitionedTableSchema = HoodieSchemaUtils.addMetadataFields(
        HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.createNullable(HoodieSchemaType.LONG), null, HoodieSchema.NULL_VALUE),
            HoodieSchemaField.of("v", HoodieSchema.createNullable(unshreddedVariant), null, HoodieSchema.NULL_VALUE),
            HoodieSchemaField.of("part", HoodieSchema.createNullable(HoodieSchemaType.STRING), null, HoodieSchema.NULL_VALUE))));
    // Partition values reach the reader through its constructor - HoodieBootstrapRecordIterator
    // reads them from there and never parses them out of the file path - so `part` comes back as
    // the value handed in here, not as null and not from a part=p1 directory.
    HoodieAvroBootstrapFileReader partitionedReader = (HoodieAvroBootstrapFileReader)
        readerFactory.newBootstrapFileReader(
            readerFactory.getFileReader(new HoodieConfig(), new StoragePath(skeletonFile.toUri().toString())),
            readerFactory.getFileReader(new HoodieConfig(), new StoragePath(dataFile.toUri().toString())),
            Option.of(new String[] {"part"}), new Object[] {"p1"});
    try (ClosableIterator<HoodieRecord<IndexedRecord>> iterator =
             (ClosableIterator) partitionedReader.getRecordIterator(partitionedTableSchema)) {
      assertTrue(iterator.hasNext(), "the joined bootstrap row must come back");
      IndexedRecord out = (IndexedRecord) iterator.next().getData();
      assertEquals(partitionedTableSchema.getAvroSchema(), out.getSchema(),
          "the record must carry exactly the requested schema's fields");
      assertBootstrapRoundTrip(out, partitionedTableSchema, original);
      assertEquals("p1", String.valueOf(out.get(partitionedTableSchema.getAvroSchema().getField("part").pos())),
          "the partition column, absent from the data file, is filled from the partition values");
    }

    // A column the data file lacks that is NON-nullable and has no default has nothing to fill it
    // with, so the read must fail rather than serve a row. Pre-PR the data file was read at its own
    // footer schema, so the same request silently shifted the columns instead.
    HoodieSchema extraColumnSchema = HoodieSchemaUtils.addMetadataFields(
        HoodieSchema.createRecord("r", "org.apache.hudi.test", null, Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.createNullable(HoodieSchemaType.LONG), null, HoodieSchema.NULL_VALUE),
            HoodieSchemaField.of("v", HoodieSchema.createNullable(unshreddedVariant), null, HoodieSchema.NULL_VALUE),
            HoodieSchemaField.of("extra", HoodieSchema.create(HoodieSchemaType.STRING)))));
    HoodieAvroBootstrapFileReader extraColumnReader = (HoodieAvroBootstrapFileReader)
        readerFactory.newBootstrapFileReader(
            readerFactory.getFileReader(new HoodieConfig(), new StoragePath(skeletonFile.toUri().toString())),
            readerFactory.getFileReader(new HoodieConfig(), new StoragePath(dataFile.toUri().toString())),
            Option.empty(), new Object[0]);
    Exception failure = assertThrows(Exception.class, () -> {
      try (ClosableIterator<HoodieRecord<IndexedRecord>> iterator =
               (ClosableIterator) extraColumnReader.getRecordIterator(extraColumnSchema)) {
        while (iterator.hasNext()) {
          iterator.next();
        }
      }
    });
    StringBuilder causes = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      causes.append(cause.getMessage()).append('\n');
    }
    assertTrue(causes.toString().contains("extra"),
        "the failure must name the column that cannot be filled, got:\n" + causes);
  }
}
