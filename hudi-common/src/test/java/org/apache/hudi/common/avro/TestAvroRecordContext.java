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

package org.apache.hudi.common.avro;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ArrayComparable;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.avro.AvroRecordContext.getFieldValueFromIndexedRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestAvroRecordContext {

  private static final Schema RECORD_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"top\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"int\"},"
          + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"address\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"address\",\"fields\":["
          + "{\"name\":\"city\",\"type\":\"string\"},"
          + "{\"name\":\"zip\",\"type\":[\"null\",\"int\"],\"default\":null}]}],\"default\":null},"
          + "{\"name\":\"multi\",\"type\":[\"null\",\"string\",\"int\"],\"default\":null}]}");

  private static final Schema MAP_AND_ARRAY_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"complex\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"int\"},"
          + "{\"name\":\"str_map\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}],\"default\":null},"
          + "{\"name\":\"int_array\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"int\"}],\"default\":null},"
          + "{\"name\":\"rec_map\",\"type\":[\"null\",{\"type\":\"map\",\"values\":{\"type\":\"record\","
          + "\"name\":\"inner\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}}],\"default\":null}]}");

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testOrderingValuesMatchIncomingRecord(boolean consistentTimestamp) {
    Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ordering\",\"fields\":["
        + "{\"name\":\"ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
        + "{\"name\":\"date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
        + "{\"name\":\"decimal\",\"type\":{\"type\":\"fixed\",\"name\":\"fixed_decimal\",\"size\":5,"
        + "\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}},"
        + "{\"name\":\"nested\",\"type\":{\"type\":\"record\",\"name\":\"inner_date\",\"fields\":["
        + "{\"name\":\"date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}]}}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("ts", 1790812800000000L);
    record.put("date", 20727);
    Schema decimalSchema = schema.getField("decimal").schema();
    record.put("decimal", new Conversions.DecimalConversion().toFixed(new BigDecimal("1.50"), decimalSchema, LogicalTypes.decimal(10, 2)));
    GenericRecord nested = new GenericData.Record(schema.getField("nested").schema());
    nested.put("date", 20727);
    record.put("nested", nested);
    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(schema);
    TypedProperties props = new TypedProperties();
    props.setProperty(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), String.valueOf(consistentTimestamp));
    AvroRecordContext context = new AvroRecordContext(new HoodieTableConfig(), null, props);

    for (String field : Arrays.asList("ts", "date", "decimal", "nested.date")) {
      Comparable incoming = new HoodieAvroIndexedRecord(record).getOrderingValue(hoodieSchema, props, new String[] {field});
      assertEquals(incoming, context.getOrderingValue(record, hoodieSchema, new String[] {field}), field);
      assertEquals(incoming, context.getOrderingValue(record, hoodieSchema, Arrays.asList(field)), field);
    }
    assertEquals(OrderingValues.getDefault(), context.getOrderingValue(record, hoodieSchema, new String[] {"missing"}));
    assertNull(new HoodieAvroIndexedRecord(record).getOrderingValue(hoodieSchema, props, new String[] {"missing"}));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDeleteOrderingValuesMatchData(boolean consistentTimestamp) throws IOException, ClassNotFoundException {
    Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"delete_ordering\",\"fields\":["
        + "{\"name\":\"date\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},"
        + "{\"name\":\"ts_millis\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
        + "{\"name\":\"ts_micros\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
        + "{\"name\":\"decimal_fixed\",\"type\":{\"type\":\"fixed\",\"name\":\"decimal\",\"size\":5,"
        + "\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}},"
        + "{\"name\":\"decimal_bytes\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}},"
        + "{\"name\":\"plain_int\",\"type\":\"int\"},{\"name\":\"plain_long\",\"type\":\"long\"},"
        + "{\"name\":\"text\",\"type\":\"string\"},"
        + "{\"name\":\"nested\",\"type\":{\"type\":\"record\",\"name\":\"nested_ordering\",\"fields\":["
        + "{\"name\":\"date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}]}}]}");
    Map<String, Comparable> deleteValues = new LinkedHashMap<>();
    deleteValues.put("date", 20727);
    deleteValues.put("ts_millis", 1790812800123L);
    deleteValues.put("ts_micros", 1790812800123456L);
    deleteValues.put("decimal_fixed", new BigDecimal("1.50"));
    deleteValues.put("decimal_bytes", new BigDecimal("2.50"));
    deleteValues.put("plain_int", 20727);
    deleteValues.put("plain_long", 1790812800123456L);
    deleteValues.put("text", "value");
    GenericRecord record = new GenericData.Record(schema);
    deleteValues.forEach(record::put);
    record.put("text", new Utf8("value"));
    Schema fixedSchema = schema.getField("decimal_fixed").schema();
    Schema bytesSchema = schema.getField("decimal_bytes").schema();
    record.put("decimal_fixed", new Conversions.DecimalConversion().toFixed(
        (BigDecimal) deleteValues.get("decimal_fixed"), fixedSchema, fixedSchema.getLogicalType()));
    record.put("decimal_bytes", new Conversions.DecimalConversion().toBytes(
        (BigDecimal) deleteValues.get("decimal_bytes"), bytesSchema, bytesSchema.getLogicalType()));
    GenericRecord nested = new GenericData.Record(schema.getField("nested").schema());
    nested.put("date", 20727);
    record.put("nested", nested);
    deleteValues.put("nested.date", 20727);
    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(schema);
    TypedProperties props = new TypedProperties();
    props.setProperty(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), String.valueOf(consistentTimestamp));
    AvroRecordContext context = new AvroRecordContext(new HoodieTableConfig(), null, props);

    for (Map.Entry<String, Comparable> entry : deleteValues.entrySet()) {
      List<String> fields = Collections.singletonList(entry.getKey());
      context.initOrderingValueConverter(hoodieSchema, fields);
      Comparable dataOrdering = context.getOrderingValue(record, hoodieSchema, fields);
      Comparable deleteOrdering = context.getOrderingValue(roundTripDelete(entry.getValue()));
      assertEquals(0, dataOrdering.compareTo(deleteOrdering), entry.getKey());
      assertEquals(deleteOrdering, context.convertOrderingValueToEngineType(deleteOrdering), entry.getKey());
    }

    // SQL date/timestamp wrappers decode to different Java representations than primitive wrappers.
    context.initOrderingValueConverter(hoodieSchema, Collections.singletonList("date"));
    LocalDate date = LocalDate.ofEpochDay(20727);
    assertEquals(date, context.getOrderingValue(roundTripDelete(Date.valueOf(date))));
    assertEquals(date, context.convertOrderingValueToEngineType(date));
    for (String field : Arrays.asList("ts_millis", "ts_micros")) {
      List<String> fields = Collections.singletonList(field);
      context.initOrderingValueConverter(hoodieSchema, fields);
      long rawValue = (long) deleteValues.get(field);
      Instant instant = field.equals("ts_millis") ? Instant.ofEpochMilli(rawValue)
          : Instant.ofEpochSecond(rawValue / 1_000_000, (rawValue % 1_000_000) * 1_000);
      Comparable dataOrdering = context.getOrderingValue(record, hoodieSchema, fields);
      assertEquals(0, dataOrdering.compareTo(context.getOrderingValue(roundTripDelete(Timestamp.from(instant)))));
      assertEquals(0, dataOrdering.compareTo(context.convertOrderingValueToEngineType(instant)));
    }

    for (String field : Arrays.asList("decimal_fixed", "decimal_bytes")) {
      context.initOrderingValueConverter(hoodieSchema, Collections.singletonList(field));
      assertEquals(deleteValues.get(field), context.convertOrderingValueToEngineType((Comparable) record.get(field)));
    }

    List<String> fields = new ArrayList<>(deleteValues.keySet());
    context.initOrderingValueConverter(hoodieSchema, fields);
    Comparable multipleValues = OrderingValues.create(deleteValues.values().toArray(new Comparable[0]));
    Comparable decodedValues = roundTripDelete(multipleValues).getOrderingValue();
    Comparable dataOrdering = context.getOrderingValue(record, hoodieSchema, fields);
    assertEquals(0, dataOrdering.compareTo(context.convertOrderingValueToEngineType(decodedValues)));
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(AvroOrderingValueConverter.create(hoodieSchema, fields, consistentTimestamp));
    }
    try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      AvroOrderingValueConverter converter = (AvroOrderingValueConverter) input.readObject();
      assertEquals(0, dataOrdering.compareTo(converter.convert(decodedValues)));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"local-timestamp-millis", "local-timestamp-micros"})
  void testLocalTimestampDeleteOrdering(String logicalType) throws IOException {
    Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"local_timestamp\",\"fields\":["
        + "{\"name\":\"ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"" + logicalType + "\"}}]}");
    HoodieSchema hoodieSchema = HoodieSchema.fromAvroSchema(schema);
    Instant instant = Instant.parse("2026-10-01T00:00:00.123456Z");
    LocalDateTime localTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    long rawValue = logicalType.equals("local-timestamp-millis") ? instant.toEpochMilli()
        : instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
    GenericRecord record = new GenericData.Record(schema);
    record.put("ts", rawValue);
    TypedProperties props = new TypedProperties();
    props.setProperty(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), "true");
    AvroRecordContext context = new AvroRecordContext(new HoodieTableConfig(), null, props);
    List<String> fields = Collections.singletonList("ts");
    context.initOrderingValueConverter(hoodieSchema, fields);
    Comparable expected = context.getOrderingValue(record, hoodieSchema, fields);
    assertEquals(expected, context.getOrderingValue(roundTripDelete(rawValue)));
    assertEquals(expected, context.convertOrderingValueToEngineType(localTime));
    assertEquals(expected, context.getOrderingValue(roundTripDelete(Timestamp.valueOf(localTime))));
  }

  @Test
  void testDeleteOrderingPreservesSentinelsAndUnknownFields() {
    HoodieSchema schema = HoodieSchema.fromAvroSchema(new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"sentinels\",\"fields\":["
            + "{\"name\":\"date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
            + "{\"name\":\"ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}}]}"));
    AvroRecordContext context = new AvroRecordContext();
    context.initOrderingValueConverter(schema, Arrays.asList("date", "ts"));
    assertEquals(OrderingValues.getDefault(), context.getOrderingValue(DeleteRecord.create("key", "", null)));
    assertEquals(OrderingValues.getDefault(), context.getOrderingValue(DeleteRecord.create("key", "", OrderingValues.getDefault())));
    ArrayComparable values = new ArrayComparable(new Comparable[] {OrderingValues.getDefault(), null});
    assertEquals(values, context.getOrderingValue(DeleteRecord.create("key", "", values)));

    context.initOrderingValueConverter(schema, Collections.singletonList("missing"));
    assertEquals(20727, context.getOrderingValue(DeleteRecord.create("key", "", 20727)));
    assertEquals(new Utf8("value"), context.getOrderingValue(DeleteRecord.create("key", "", "value")));
    context.initOrderingValueConverter(schema, Collections.emptyList());
    assertEquals(new Utf8("value"), context.getOrderingValue(DeleteRecord.create("key", "", "value")));
  }

  private static DeleteRecord roundTripDelete(Comparable value) throws IOException {
    HoodieDeleteBlock written = new HoodieDeleteBlock(
        Collections.singletonList(Pair.of(DeleteRecord.create("key", "", value), -1L)), Collections.emptyMap());
    HoodieDeleteBlock read = new HoodieDeleteBlock(Option.of(written.getContentBytes(null).toByteArray()),
        null, false, Option.empty(), Collections.emptyMap(), Collections.emptyMap());
    return read.getRecordsToDelete()[0];
  }

  private static Stream<Arguments> testConvertValueToEngineType() {
    return Stream.of(
        Arguments.of(1L, 1L),
        Arguments.of("test", new Utf8("test")),
        Arguments.of(new Utf8("utf8_string"), new Utf8("utf8_string")),
        Arguments.of(1.23, 1.23));
  }

  @ParameterizedTest
  @MethodSource
  void testConvertValueToEngineType(Comparable input, Comparable expected) {
    Comparable actual = AvroRecordContext.getFieldAccessorInstance().convertValueToEngineType(input);
    assertEquals(expected, actual);
  }

  private static GenericRecord buildRecord() {
    GenericRecord address = new GenericData.Record(RECORD_SCHEMA.getField("address").schema().getTypes().get(1));
    address.put("city", new Utf8("sf"));
    address.put("zip", 94105);
    GenericRecord record = new GenericData.Record(RECORD_SCHEMA);
    record.put("id", 1);
    record.put("name", new Utf8("alice"));
    record.put("address", address);
    return record;
  }

  @ParameterizedTest
  @MethodSource("virtualRecordKeyParams")
  void testVirtualRecordKey(String recordKeyFields, String partitionFields, String expectedKey) {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.META_FIELDS_MODE, MetaFieldsMode.NONE.name());
    tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, recordKeyFields);
    if (partitionFields != null) {
      tableConfig.setValue(HoodieTableConfig.PARTITION_FIELDS, partitionFields);
    }
    AvroRecordContext context = new AvroRecordContext(tableConfig, null);
    assertEquals(expectedKey, context.getRecordKey(buildRecord(), HoodieSchema.fromAvroSchema(RECORD_SCHEMA)));
  }

  private static Stream<Arguments> virtualRecordKeyParams() {
    return Stream.of(
        Arguments.of("id", null, "1"),
        Arguments.of("id", "", "1"),
        Arguments.of("id", "name", "1"),
        Arguments.of("id", "name,address.city", "id:1"),
        Arguments.of("id,name", null, "id:1,name:alice"),
        Arguments.of("id,name", "name", "id:1,name:alice"),
        Arguments.of("id,name", "name,address.city", "id:1,name:alice"));
  }

  @Test
  void testGetFieldValueTopLevel() {
    GenericRecord record = buildRecord();
    assertEquals(1, getFieldValueFromIndexedRecord(record, "id"));
    assertEquals(new Utf8("alice"), getFieldValueFromIndexedRecord(record, "name"));
    assertNull(getFieldValueFromIndexedRecord(record, "multi"));
    assertNull(getFieldValueFromIndexedRecord(record, "missing"));
  }

  @Test
  void testGetFieldValueNested() {
    GenericRecord record = buildRecord();
    // intermediate segment unwraps the [null, record] union
    assertEquals(new Utf8("sf"), getFieldValueFromIndexedRecord(record, "address.city"));
    assertEquals(94105, getFieldValueFromIndexedRecord(record, "address.zip"));
    assertNull(getFieldValueFromIndexedRecord(record, "address.missing"));
    assertNull(getFieldValueFromIndexedRecord(record, "missing.nested"));
  }

  @Test
  void testGetFieldValueErrorCases() {
    GenericRecord record = buildRecord();
    // a union that is not [null, T] cannot be navigated into; instead of throwing, the value
    // navigator returns null so callers (e.g. column-stats collection) degrade gracefully,
    // matching HoodieAvroUtils.getNestedFieldVal
    assertNull(getFieldValueFromIndexedRecord(record, "multi.sub"));
    // an empty field name is still rejected up front
    assertThrows(IllegalArgumentException.class, () -> getFieldValueFromIndexedRecord(record, ""));
  }

  @Test
  void testGetFieldValueMapAndArrayLeavesReturnNull() {
    GenericRecord record = new GenericData.Record(MAP_AND_ARRAY_SCHEMA);
    record.put("id", 7);
    Map<Utf8, Utf8> strMap = new HashMap<>();
    strMap.put(new Utf8("a"), new Utf8("v1"));
    record.put("str_map", strMap);
    record.put("int_array", Arrays.asList(3, 1, 2));
    // rec_map left null

    // top-level scalar still resolves normally
    assertEquals(7, getFieldValueFromIndexedRecord(record, "id"));

    // Parquet-style synthetic accessors that traverse a MAP (".key_value.key/value") or an
    // ARRAY (".list.element") cannot be resolved to a single value and must return null instead
    // of throwing. Regression: these previously threw
    // IllegalStateException "Cannot get field from schema type: MAP" during MOR log-append
    // column-stats collection. Such nested leaves still get statistics from the base-file path.
    assertNull(getFieldValueFromIndexedRecord(record, "str_map.key_value.key"));
    assertNull(getFieldValueFromIndexedRecord(record, "str_map.key_value.value"));
    assertNull(getFieldValueFromIndexedRecord(record, "int_array.list.element"));
    // a deep path descending through a MAP into a record field also degrades to null
    assertNull(getFieldValueFromIndexedRecord(record, "rec_map.key_value.value.x"));

    // and null when the complex field itself is absent/null
    GenericRecord empty = new GenericData.Record(MAP_AND_ARRAY_SCHEMA);
    empty.put("id", 0);
    assertNull(getFieldValueFromIndexedRecord(empty, "str_map.key_value.value"));
    assertNull(getFieldValueFromIndexedRecord(empty, "int_array.list.element"));
  }

  @Test
  void testGetFieldValueAcrossEqualSchemaInstances() {
    // records from different files carry equal but distinct schema instances; both must intern to
    // the same canonical wrapper and resolve identically
    Schema schemaCopy = new Schema.Parser().parse(RECORD_SCHEMA.toString());
    GenericRecord record = buildRecord();
    GenericRecord recordWithCopy = new GenericData.Record(schemaCopy);
    for (Schema.Field field : RECORD_SCHEMA.getFields()) {
      recordWithCopy.put(field.pos(), record.get(field.pos()));
    }
    assertEquals(getFieldValueFromIndexedRecord(record, "id"), getFieldValueFromIndexedRecord(recordWithCopy, "id"));
    assertEquals(getFieldValueFromIndexedRecord(record, "address.city"), getFieldValueFromIndexedRecord(recordWithCopy, "address.city"));
  }
}
