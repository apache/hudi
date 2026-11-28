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

package org.apache.hudi.common.table.log.block;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compares {@link AvroProjectionReader} with Avro's resolving reader, field by field, on projections of a schema with
 * every Avro type.
 */
class TestAvroProjectionReader {

  private static final Schema WRITER_SCHEMA = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"row\", \"fields\": ["
      + "{\"name\": \"key\", \"type\": \"string\"},"
      + "{\"name\": \"c_boolean\", \"type\": \"boolean\"},"
      + "{\"name\": \"c_int\", \"type\": \"int\"},"
      + "{\"name\": \"c_long\", \"type\": [\"null\", \"long\"]},"
      + "{\"name\": \"c_long_null_last\", \"type\": [\"long\", \"null\"]},"
      + "{\"name\": \"c_float\", \"type\": \"float\"},"
      + "{\"name\": \"c_double\", \"type\": [\"null\", \"double\"]},"
      + "{\"name\": \"c_string\", \"type\": [\"null\", \"string\"]},"
      + "{\"name\": \"c_java_string\", \"type\": {\"type\": \"string\", \"avro.java.string\": \"String\"}},"
      + "{\"name\": \"c_java_string_union\", \"type\": [{\"type\": \"string\", \"avro.java.string\": \"String\"}, \"null\"]},"
      + "{\"name\": \"c_utf8_string\", \"type\": {\"type\": \"string\", \"avro.java.string\": \"Utf8\"}},"
      + "{\"name\": \"c_bytes\", \"type\": [\"null\", \"bytes\"]},"
      + "{\"name\": \"c_fixed\", \"type\": {\"type\": \"fixed\", \"name\": \"f4\", \"size\": 4}},"
      + "{\"name\": \"c_enum\", \"type\": {\"type\": \"enum\", \"name\": \"color\", \"symbols\": [\"RED\", \"GREEN\", \"BLUE\"]}},"
      + "{\"name\": \"c_enum_union\", \"type\": [\"null\", \"color\"]},"
      + "{\"name\": \"c_date\", \"type\": [\"null\", {\"type\": \"int\", \"logicalType\": \"date\"}]},"
      + "{\"name\": \"c_timestamp\", \"type\": [\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}]},"
      + "{\"name\": \"c_timestamp_millis\", \"type\": [{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}, \"null\"]},"
      + "{\"name\": \"c_uuid\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}},"
      + "{\"name\": \"c_decimal_fixed\", \"type\": [\"null\", {\"type\": \"fixed\", \"name\": \"d72\", \"size\": 4, \"logicalType\": \"decimal\", \"precision\": 7, \"scale\": 2}]},"
      + "{\"name\": \"c_decimal_bytes\", \"type\": [\"null\", {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 20, \"scale\": 4}]},"
      + "{\"name\": \"c_record\", \"type\": [\"null\", {\"type\": \"record\", \"name\": \"nested\", \"fields\": ["
      + "  {\"name\": \"n_string\", \"type\": [\"null\", \"string\"]}, {\"name\": \"n_longs\", \"type\": {\"type\": \"array\", \"items\": \"long\"}}]}]},"
      + "{\"name\": \"c_array\", \"type\": {\"type\": \"array\", \"items\": [\"null\", \"string\"]}},"
      + "{\"name\": \"c_map\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"nested\"}]},"
      + "{\"name\": \"c_last\", \"type\": \"long\"}"
      + "]}");

  private static final Schema TREE_SCHEMA = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"row\", \"fields\": ["
      + "{\"name\": \"key\", \"type\": \"string\"},"
      + "{\"name\": \"tree\", \"type\": {\"type\": \"record\", \"name\": \"node\", \"fields\": ["
      + "  {\"name\": \"value\", \"type\": \"long\"}, {\"name\": \"next\", \"type\": [\"null\", \"node\"]}]}},"
      + "{\"name\": \"c_last\", \"type\": \"long\"}"
      + "]}");

  @Test
  void testProjectionsReadTheRecordsOfAvrosResolvingReader() throws IOException {
    List<GenericRecord> records = randomRecords(200);
    List<byte[]> encoded = encode(WRITER_SCHEMA, records, false);
    // Arrays and maps written in blocks prefixed with their byte size, which skipArray and skipMap jump over
    List<byte[]> blockEncoded = encode(WRITER_SCHEMA, records, true);
    assertFalse(Arrays.equals(encoded.get(29), blockEncoded.get(29)), "the blocking encoder writes byte-sized blocks");

    GenericData withConversions = new GenericData();
    withConversions.addLogicalTypeConversion(new Conversions.DecimalConversion());
    withConversions.addLogicalTypeConversion(new Conversions.UUIDConversion());
    withConversions.addLogicalTypeConversion(new TimeConversions.DateConversion());
    withConversions.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
    withConversions.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());

    List<Schema> projections = Arrays.asList(
        // Drops every nested, array and map field
        project(WRITER_SCHEMA, "row", "key", "c_long", "c_last"),
        project(WRITER_SCHEMA, "row", "c_string", "c_java_string", "c_java_string_union", "c_utf8_string", "c_bytes", "c_fixed", "c_enum",
            "c_enum_union"),
        project(WRITER_SCHEMA, "row", "c_date", "c_timestamp", "c_timestamp_millis", "c_uuid", "c_decimal_fixed", "c_decimal_bytes",
            "c_boolean", "c_float", "c_double", "c_int", "c_long_null_last"),
        // Keeps nested, array and map fields, read through Avro's own reader
        project(WRITER_SCHEMA, "row", "c_last", "c_record", "c_array", "c_map"),
        // Reader order differs from writer order, under another record name
        project(WRITER_SCHEMA, "renamed_row", "c_last", "c_double", "key"));
    for (Schema readerSchema : projections) {
      for (GenericData data : Arrays.asList(GenericData.get(), withConversions)) {
        assertReadsLikeAvro(WRITER_SCHEMA, readerSchema, data, encoded);
        assertReadsLikeAvro(WRITER_SCHEMA, readerSchema, data, blockEncoded);
      }
    }
  }

  @Test
  void testKeptRecursiveTypeReadsTheRecordsOfAvrosResolvingReader() throws IOException {
    Schema nodeSchema = TREE_SCHEMA.getField("tree").schema();
    List<GenericRecord> records = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericData.Record tree = null;
      for (int depth = 0; depth <= i; depth++) {
        GenericData.Record node = new GenericData.Record(nodeSchema);
        node.put("value", (long) depth);
        node.put("next", tree);
        tree = node;
      }
      GenericData.Record record = new GenericData.Record(TREE_SCHEMA);
      record.put("key", "key" + i);
      record.put("tree", tree);
      record.put("c_last", (long) i);
      records.add(record);
    }
    assertReadsLikeAvro(TREE_SCHEMA, project(TREE_SCHEMA, "row", "tree", "c_last"), GenericData.get(), encode(TREE_SCHEMA, records, false));
  }

  @Test
  void testSchemaResolutionKeepsTheResolvingReader() {
    // Every field kept: nothing to skip
    assertFalse(AvroProjectionReader.create(WRITER_SCHEMA, WRITER_SCHEMA).isPresent());
    // A reader field the writer lacks takes its default
    assertFalse(AvroProjectionReader.create(WRITER_SCHEMA, parseRow("{\"name\": \"key\", \"type\": \"string\"}, "
        + "{\"name\": \"added\", \"type\": [\"null\", \"long\"], \"default\": null}")).isPresent());
    // A promoted field: int read as long
    assertFalse(AvroProjectionReader.create(WRITER_SCHEMA, parseRow("{\"name\": \"c_int\", \"type\": \"long\"}")).isPresent());
    // A field renamed through an alias, and a kept field whose alias names another writer field
    assertFalse(AvroProjectionReader.create(WRITER_SCHEMA, parseRow("{\"name\": \"renamed\", \"aliases\": [\"c_int\"], \"type\": \"int\"}")).isPresent());
    assertFalse(AvroProjectionReader.create(WRITER_SCHEMA, parseRow("{\"name\": \"c_int\", \"aliases\": [\"c_boolean\"], \"type\": \"int\"}")).isPresent());
    // A union with its branches in another order
    assertFalse(AvroProjectionReader.create(WRITER_SCHEMA, parseRow("{\"name\": \"c_long\", \"type\": [\"long\", \"null\"]}")).isPresent());
    // Another string type
    assertFalse(AvroProjectionReader.create(WRITER_SCHEMA,
        parseRow("{\"name\": \"c_string\", \"type\": [\"null\", {\"type\": \"string\", \"avro.java.string\": \"String\"}]}")).isPresent());
    // A logical type the reader drops
    assertFalse(AvroProjectionReader.create(WRITER_SCHEMA, parseRow("{\"name\": \"c_date\", \"type\": [\"null\", \"int\"]}")).isPresent());
    // A dropped recursive type
    assertFalse(AvroProjectionReader.create(TREE_SCHEMA, project(TREE_SCHEMA, "row", "key", "c_last")).isPresent());
    assertTrue(AvroProjectionReader.create(WRITER_SCHEMA, project(WRITER_SCHEMA, "row", "key")).isPresent());
  }

  @Test
  void testDroppedEnumOutOfRangeFails() {
    Schema writerSchema = parseRow("{\"name\": \"e\", \"type\": {\"type\": \"enum\", \"name\": \"e2\", \"symbols\": [\"A\", \"B\"]}}, "
        + "{\"name\": \"b\", \"type\": \"long\"}");
    AvroProjectionReader projectionReader = AvroProjectionReader.create(writerSchema, project(writerSchema, "row", "b")).get();
    // Enum ordinal 5 (zig-zag 0x0a), then b = 3
    assertThrows(AvroTypeException.class, () -> projectionReader.read(DecoderFactory.get().binaryDecoder(new byte[] {0x0a, 0x06}, null)));
  }

  private static void assertReadsLikeAvro(Schema writerSchema, Schema readerSchema, GenericData data, List<byte[]> encoded) throws IOException {
    AvroProjectionReader projectionReader = AvroProjectionReader.create(writerSchema, readerSchema, data).get();
    GenericDatumReader<IndexedRecord> stockReader = new GenericDatumReader<>(writerSchema, readerSchema, data);
    for (byte[] bytes : encoded) {
      IndexedRecord expected = stockReader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
      IndexedRecord actual = projectionReader.read(decoder);
      assertTrue(decoder.isEnd(), "the whole record is consumed");
      assertEquals(readerSchema, actual.getSchema());
      for (Schema.Field field : readerSchema.getFields()) {
        Object expectedValue = expected.get(field.pos());
        Object actualValue = actual.get(field.pos());
        assertEquals(expectedValue, actualValue, field.name());
        assertEquals(expectedValue == null ? null : expectedValue.getClass(), actualValue == null ? null : actualValue.getClass(), field.name());
      }
    }
  }

  private static Schema parseRow(String fields) {
    return new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"row\", \"fields\": [" + fields + "]}");
  }

  private static Schema project(Schema writerSchema, String recordName, String... fieldNames) {
    List<Schema.Field> fields = Arrays.stream(fieldNames)
        .map(name -> {
          Schema.Field field = writerSchema.getField(name);
          return new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal());
        })
        .collect(Collectors.toList());
    // Parsed again, so the reader's field schemas are equal to the writer's but not the same instances
    return new Schema.Parser().parse(Schema.createRecord(recordName, null, null, false, fields).toString());
  }

  private static List<GenericRecord> randomRecords(int count) {
    Random random = new Random(7);
    Schema colorSchema = WRITER_SCHEMA.getField("c_enum").schema();
    Schema decimalFixed = WRITER_SCHEMA.getField("c_decimal_fixed").schema().getTypes().get(1);
    Schema nestedSchema = WRITER_SCHEMA.getField("c_record").schema().getTypes().get(1);
    List<GenericRecord> records = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      boolean nulls = i % 5 == 0;
      GenericData.Record record = new GenericData.Record(WRITER_SCHEMA);
      record.put("key", "key" + i);
      record.put("c_boolean", random.nextBoolean());
      record.put("c_int", random.nextInt());
      record.put("c_long", nulls ? null : random.nextLong());
      record.put("c_long_null_last", nulls ? null : random.nextLong());
      record.put("c_float", random.nextFloat());
      record.put("c_double", nulls ? null : random.nextDouble());
      record.put("c_string", nulls ? null : "string-" + random.nextInt(1000));
      record.put("c_java_string", "java-" + i);
      record.put("c_java_string_union", nulls ? null : "java-union-" + i);
      record.put("c_utf8_string", "utf8-" + i);
      record.put("c_bytes", nulls ? null : ByteBuffer.wrap(new byte[] {(byte) i, 1, 2}));
      record.put("c_fixed", new GenericData.Fixed(WRITER_SCHEMA.getField("c_fixed").schema(), new byte[] {(byte) i, 0, 0, 7}));
      record.put("c_enum", new GenericData.EnumSymbol(colorSchema, "GREEN"));
      record.put("c_enum_union", nulls ? null : new GenericData.EnumSymbol(colorSchema, colorSchema.getEnumSymbols().get(i % 3)));
      record.put("c_date", nulls ? null : 18000 + i);
      record.put("c_timestamp", nulls ? null : 1_600_000_000_000_000L + i);
      record.put("c_timestamp_millis", nulls ? null : 1_600_000_000_000L + i);
      record.put("c_uuid", new UUID(random.nextLong(), random.nextLong()).toString());
      record.put("c_decimal_fixed", nulls ? null : new GenericData.Fixed(decimalFixed, new byte[] {0, 0, (byte) i, 9}));
      record.put("c_decimal_bytes", nulls ? null : ByteBuffer.wrap(new byte[] {(byte) -i, 3}));
      GenericData.Record nested = new GenericData.Record(nestedSchema);
      nested.put("n_string", nulls ? null : "nested-" + i);
      nested.put("n_longs", Collections.nCopies(i % 40, (long) i));
      record.put("c_record", nulls ? null : nested);
      List<String> array = new ArrayList<>();
      for (int j = 0; j < i % 30; j++) {
        array.add(j % 4 == 0 ? null : "a" + j);
      }
      record.put("c_array", array);
      Map<String, Object> map = new HashMap<>();
      for (int j = 0; j < i % 6; j++) {
        map.put("m" + j, nested);
      }
      record.put("c_map", nulls ? null : map);
      record.put("c_last", (long) i);
      records.add(record);
    }
    return records;
  }

  private static List<byte[]> encode(Schema schema, List<GenericRecord> records, boolean blocking) throws IOException {
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    EncoderFactory blockingFactory = new EncoderFactory().configureBlockSize(64);
    List<byte[]> encoded = new ArrayList<>();
    for (GenericRecord record : records) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Encoder encoder = blocking ? blockingFactory.blockingBinaryEncoder(out, null) : EncoderFactory.get().binaryEncoder(out, null);
      writer.write(record, encoder);
      encoder.flush();
      encoded.add(out.toByteArray());
    }
    return encoded;
  }
}
