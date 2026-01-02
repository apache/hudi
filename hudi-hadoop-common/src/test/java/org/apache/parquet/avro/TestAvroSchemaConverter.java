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

package org.apache.parquet.avro;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCompatibility;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.parquet.avro.HoodieAvroParquetSchemaConverter.getAvroSchemaConverter;
import static org.apache.parquet.schema.OriginalType.DATE;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MICROS;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;
import static org.apache.parquet.schema.OriginalType.TIME_MICROS;
import static org.apache.parquet.schema.OriginalType.TIME_MILLIS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestAvroSchemaConverter {

  private static final Configuration NEW_BEHAVIOR = new Configuration(false);

  @BeforeAll
  public static void setupConf() {
    NEW_BEHAVIOR.setBoolean("parquet.avro.add-list-element-records", false);
    NEW_BEHAVIOR.setBoolean("parquet.avro.write-old-list-structure", false);
  }

  public static final String ALL_PARQUET_SCHEMA = "message org.apache.parquet.avro.myrecord {\n"
      + "  required boolean myboolean;\n"
      + "  required int32 myint;\n"
      + "  required int64 mylong;\n"
      + "  required float myfloat;\n"
      + "  required double mydouble;\n"
      + "  required binary mybytes;\n"
      + "  required binary mystring (UTF8);\n"
      + "  required group mynestedrecord {\n"
      + "    required int32 mynestedint;\n"
      + "  }\n"
      + "  required binary myenum (ENUM);\n"
      + "  required group myarray (LIST) {\n"
      + "    repeated int32 array;\n"
      + "  }\n"
      + "  optional group myoptionalarray (LIST) {\n"
      + "    repeated int32 array;\n"
      + "  }\n"
      + "  required group myarrayofoptional (LIST) {\n"
      + "    repeated group list {\n"
      + "      optional int32 element;\n"
      + "    }\n"
      + "  }\n"
      + "  required group myrecordarray (LIST) {\n"
      + "    repeated group array {\n"
      + "      required int32 a;\n"
      + "      required int32 b;\n"
      + "    }\n"
      + "  }\n"
      + "  required group mymap (MAP) {\n"
      + "    repeated group map (MAP_KEY_VALUE) {\n"
      + "      required binary key (UTF8);\n"
      + "      required int32 value;\n"
      + "    }\n"
      + "  }\n"
      + "  required fixed_len_byte_array(1) myfixed;\n"
      + "}\n";

  private void testAvroToParquetConversion(HoodieSchema schema, String schemaString) throws Exception {
    testAvroToParquetConversion(new Configuration(false), schema, schemaString);
  }

  private void testAvroToParquetConversion(Configuration conf, HoodieSchema schema, String schemaString)
      throws Exception {
    HoodieAvroParquetSchemaConverter avroSchemaConverter = getAvroSchemaConverter(conf);
    MessageType messageType = avroSchemaConverter.convert(schema);
    MessageType expectedMT = MessageTypeParser.parseMessageType(schemaString);
    assertEquals(expectedMT.toString(), messageType.toString());
  }

  private void testParquetToAvroConversion(HoodieSchema schema, String schemaString) throws Exception {
    testParquetToAvroConversion(new Configuration(false), schema, schemaString);
  }

  private void testParquetToAvroConversion(Configuration conf, HoodieSchema schema, String schemaString)
      throws Exception {
    HoodieAvroParquetSchemaConverter avroSchemaConverter = getAvroSchemaConverter(conf);
    HoodieSchema convertedSchema = avroSchemaConverter.convert(MessageTypeParser.parseMessageType(schemaString));
    assertEquals(schema.toString(), convertedSchema.toString());
  }

  private void testRoundTripConversion(HoodieSchema schema, String schemaString) throws Exception {
    testRoundTripConversion(new Configuration(), schema, schemaString);
  }

  private void testRoundTripConversion(Configuration conf, HoodieSchema schema, String schemaString) throws Exception {
    HoodieAvroParquetSchemaConverter avroSchemaConverter = getAvroSchemaConverter(conf);
    MessageType messageType = avroSchemaConverter.convert(schema);
    MessageType expectedMT = MessageTypeParser.parseMessageType(schemaString);
    assertEquals(expectedMT.toString(), messageType.toString());
    HoodieSchema convertedSchema = avroSchemaConverter.convert(expectedMT);
    assertEquals(schema.toString(), convertedSchema.toString());
  }

  @Test()
  public void testTopLevelMustBeARecord() {
    assertThrows("expected to throw", IllegalArgumentException.class, () -> getAvroSchemaConverter(new Configuration()).convert(HoodieSchema.create(HoodieSchemaType.INT)));
  }

  @Test
  public void testAllTypes() throws Exception {
    HoodieSchema schema = getSchemaFromResource(TestAvroSchemaConverter.class, "/parquet-java/all.avsc");
    testAvroToParquetConversion(
        NEW_BEHAVIOR,
        schema,
        "message org.apache.parquet.avro.myrecord {\n"
            // Avro nulls are not encoded, unless they are null unions
            + "  required boolean myboolean;\n"
            + "  required int32 myint;\n"
            + "  required int64 mylong;\n"
            + "  required float myfloat;\n"
            + "  required double mydouble;\n"
            + "  required binary mybytes;\n"
            + "  required binary mystring (UTF8);\n"
            + "  required group mynestedrecord {\n"
            + "    required int32 mynestedint;\n"
            + "  }\n"
            + "  required binary myenum (ENUM);\n"
            + "  required group myarray (LIST) {\n"
            + "    repeated group list {\n"
            + "      required int32 element;\n"
            + "    }\n"
            + "  }\n"
            + "  required group myemptyarray (LIST) {\n"
            + "    repeated group list {\n"
            + "      required int32 element;\n"
            + "    }\n"
            + "  }\n"
            + "  optional group myoptionalarray (LIST) {\n"
            + "    repeated group list {\n"
            + "      required int32 element;\n"
            + "    }\n"
            + "  }\n"
            + "  required group myarrayofoptional (LIST) {\n"
            + "    repeated group list {\n"
            + "      optional int32 element;\n"
            + "    }\n"
            + "  }\n"
            + "  required group mymap (MAP) {\n"
            + "    repeated group key_value (MAP_KEY_VALUE) {\n"
            + "      required binary key (UTF8);\n"
            + "      required int32 value;\n"
            + "    }\n"
            + "  }\n"
            + "  required group myemptymap (MAP) {\n"
            + "    repeated group key_value (MAP_KEY_VALUE) {\n"
            + "      required binary key (UTF8);\n"
            + "      required int32 value;\n"
            + "    }\n"
            + "  }\n"
            + "  required fixed_len_byte_array(1) myfixed;\n"
            + "}\n");
  }

  @Test
  public void testAllTypesOldListBehavior() throws Exception {
    HoodieSchema schema = getSchemaFromResource(TestAvroSchemaConverter.class, "/parquet-java/all.avsc");
    testAvroToParquetConversion(
        schema,
        "message org.apache.parquet.avro.myrecord {\n"
            // Avro nulls are not encoded, unless they are null unions
            + "  required boolean myboolean;\n"
            + "  required int32 myint;\n"
            + "  required int64 mylong;\n"
            + "  required float myfloat;\n"
            + "  required double mydouble;\n"
            + "  required binary mybytes;\n"
            + "  required binary mystring (UTF8);\n"
            + "  required group mynestedrecord {\n"
            + "    required int32 mynestedint;\n"
            + "  }\n"
            + "  required binary myenum (ENUM);\n"
            + "  required group myarray (LIST) {\n"
            + "    repeated int32 array;\n"
            + "  }\n"
            + "  required group myemptyarray (LIST) {\n"
            + "    repeated int32 array;\n"
            + "  }\n"
            + "  optional group myoptionalarray (LIST) {\n"
            + "    repeated int32 array;\n"
            + "  }\n"
            + "  required group myarrayofoptional (LIST) {\n"
            + "    repeated int32 array;\n"
            + "  }\n"
            + "  required group mymap (MAP) {\n"
            + "    repeated group key_value (MAP_KEY_VALUE) {\n"
            + "      required binary key (UTF8);\n"
            + "      required int32 value;\n"
            + "    }\n"
            + "  }\n"
            + "  required group myemptymap (MAP) {\n"
            + "    repeated group key_value (MAP_KEY_VALUE) {\n"
            + "      required binary key (UTF8);\n"
            + "      required int32 value;\n"
            + "    }\n"
            + "  }\n"
            + "  required fixed_len_byte_array(1) myfixed;\n"
            + "}\n");
  }

  @Test
  public void testAllTypesParquetToAvro() throws Exception {
    HoodieSchema schema = getSchemaFromResource(TestAvroSchemaConverter.class, "/parquet-java/allFromParquetNewBehavior.avsc");
    // Cannot use round-trip assertion because enum is lost
    testParquetToAvroConversion(NEW_BEHAVIOR, schema, ALL_PARQUET_SCHEMA);
  }

  @Test
  public void testAllTypesParquetToAvroOldBehavior() throws Exception {
    HoodieSchema schema = getSchemaFromResource(TestAvroSchemaConverter.class, "/parquet-java/allFromParquetOldBehavior.avsc");
    // Cannot use round-trip assertion because enum is lost
    testParquetToAvroConversion(schema, ALL_PARQUET_SCHEMA);
  }

  @Test
  public void testParquetMapWithNonStringKeyFails() throws Exception {
    MessageType parquetSchema =
        MessageTypeParser.parseMessageType("message myrecord {\n" + "  required group mymap (MAP) {\n"
            + "    repeated group map (MAP_KEY_VALUE) {\n"
            + "      required int32 key;\n"
            + "      required int32 value;\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
    assertThrows("expected to throw", IllegalArgumentException.class, () -> getAvroSchemaConverter(new Configuration()).convert(parquetSchema));
  }

  @Test
  public void testOptionalFields() throws Exception {
    HoodieSchema optionalInt = HoodieSchema.createNullable(HoodieSchemaType.INT);
    HoodieSchema schema = HoodieSchema.createRecord("record1", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("myint", optionalInt, null, HoodieSchema.NULL_VALUE)));
    testRoundTripConversion(schema, "message record1 {\n" + "  optional int32 myint;\n" + "}\n");
  }

  @Test
  public void testOptionalMapValue() throws Exception {
    HoodieSchema optionalIntMap = HoodieSchema.createMap(HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT)));
    HoodieSchema schema = HoodieSchema.createRecord("record1", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("myintmap", optionalIntMap, null, null)));
    testRoundTripConversion(
        schema,
        "message record1 {\n" + "  required group myintmap (MAP) {\n"
            + "    repeated group key_value (MAP_KEY_VALUE) {\n"
            + "      required binary key (UTF8);\n"
            + "      optional int32 value;\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testOptionalArrayElement() throws Exception {
    HoodieSchema optionalIntArray = HoodieSchema.createArray(HoodieSchema.createNullable(HoodieSchemaType.INT));
    HoodieSchema schema = HoodieSchema.createRecord("record1", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("myintarray", optionalIntArray, null, null)));
    testRoundTripConversion(
        NEW_BEHAVIOR,
        schema,
        "message record1 {\n" + "  required group myintarray (LIST) {\n"
            + "    repeated group list {\n"
            + "      optional int32 element;\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testUnionOfTwoTypes() throws Exception {
    HoodieSchema multipleTypes = HoodieSchema.createUnion(
        Arrays.asList(HoodieSchema.create(HoodieSchemaType.NULL),
            HoodieSchema.create(HoodieSchemaType.INT),
            HoodieSchema.create(HoodieSchemaType.FLOAT)));
    HoodieSchema schema = HoodieSchema.createRecord("record2", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("myunion", multipleTypes, null, HoodieSchema.NULL_VALUE)));

    // Avro union is modelled using optional data members of the different
    // types. This does not translate back into an Avro union
    testAvroToParquetConversion(
        schema,
        "message record2 {\n" + "  optional group myunion {\n"
            + "    optional int32 member0;\n"
            + "    optional float member1;\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testArrayOfOptionalRecords() throws Exception {
    HoodieSchema optionalString = HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING));
    List<HoodieSchemaField> innerRecordFields = Arrays.asList(
        HoodieSchemaField.of("s1", optionalString, null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("s2", optionalString, null, HoodieSchema.NULL_VALUE));
    HoodieSchema innerRecord = HoodieSchema.createRecord("element", null, null, false, innerRecordFields);
    HoodieSchema schema = HoodieSchema.createRecord("HasArray", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("myarray", HoodieSchema.createArray(HoodieSchema.createNullable(innerRecord)), null, null)));

    testRoundTripConversion(
        NEW_BEHAVIOR,
        schema,
        "message HasArray {\n" + "  required group myarray (LIST) {\n"
            + "    repeated group list {\n"
            + "      optional group element {\n"
            + "        optional binary s1 (UTF8);\n"
            + "        optional binary s2 (UTF8);\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testArrayOfOptionalRecordsOldBehavior() throws Exception {
    HoodieSchema optionalString = HoodieSchema.createNullable(HoodieSchemaType.STRING);
    List<HoodieSchemaField> innerRecordFields = Arrays.asList(
        HoodieSchemaField.of("s1", optionalString, null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("s2", optionalString, null, HoodieSchema.NULL_VALUE));
    HoodieSchema innerRecord = HoodieSchema.createRecord("InnerRecord", null, null, false, innerRecordFields);
    HoodieSchema schema = HoodieSchema.createRecord("HasArray", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("myarray", HoodieSchema.createArray(HoodieSchema.createNullable(innerRecord)), null, null)));

    // Cannot use round-trip assertion because InnerRecord optional is removed
    testAvroToParquetConversion(
        schema,
        "message HasArray {\n" + "  required group myarray (LIST) {\n"
            + "    repeated group array {\n"
            + "      optional binary s1 (UTF8);\n"
            + "      optional binary s2 (UTF8);\n"
            + "    }\n"
            + "  }\n"
            + "}\n");
  }

  @Test
  public void testOldAvroListOfLists() throws Exception {
    HoodieSchema listOfLists = HoodieSchema.createNullable(HoodieSchema.createArray(HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT))));
    HoodieSchema schema = HoodieSchema.createRecord("AvroCompatListInList", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("listOfLists", listOfLists, null, HoodieSchema.NULL_VALUE)));

    testRoundTripConversion(
        schema,
        "message AvroCompatListInList {\n" + "  optional group listOfLists (LIST) {\n"
            + "    repeated group array (LIST) {\n"
            + "      repeated int32 array;\n"
            + "    }\n"
            + "  }\n"
            + "}");
    // Cannot use round-trip assertion because 3-level representation is used
    testParquetToAvroConversion(
        NEW_BEHAVIOR,
        schema,
        "message AvroCompatListInList {\n" + "  optional group listOfLists (LIST) {\n"
            + "    repeated group array (LIST) {\n"
            + "      repeated int32 array;\n"
            + "    }\n"
            + "  }\n"
            + "}");
  }

  @Test
  public void testOldThriftListOfLists() throws Exception {
    HoodieSchema listOfLists = HoodieSchema.createNullable(HoodieSchema.createArray(HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT))));
    HoodieSchema schema = HoodieSchema.createRecord("ThriftCompatListInList", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("listOfLists", listOfLists, null, HoodieSchema.NULL_VALUE)));

    // Cannot use round-trip assertion because repeated group names differ
    testParquetToAvroConversion(
        schema,
        "message ThriftCompatListInList {\n" + "  optional group listOfLists (LIST) {\n"
            + "    repeated group listOfLists_tuple (LIST) {\n"
            + "      repeated int32 listOfLists_tuple_tuple;\n"
            + "    }\n"
            + "  }\n"
            + "}");
    // Cannot use round-trip assertion because 3-level representation is used
    testParquetToAvroConversion(
        NEW_BEHAVIOR,
        schema,
        "message ThriftCompatListInList {\n" + "  optional group listOfLists (LIST) {\n"
            + "    repeated group listOfLists_tuple (LIST) {\n"
            + "      repeated int32 listOfLists_tuple_tuple;\n"
            + "    }\n"
            + "  }\n"
            + "}");
  }

  @Test
  public void testUnknownTwoLevelListOfLists() throws Exception {
    // This tests the case where we don't detect a 2-level list by the repeated
    // group's name, but it must be 2-level because the repeated group doesn't
    // contain an optional or repeated element as required for 3-level lists
    HoodieSchema listOfLists = HoodieSchema.createNullable(HoodieSchema.createArray(HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT))));
    HoodieSchema schema = HoodieSchema.createRecord("UnknownTwoLevelListInList", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("listOfLists", listOfLists, null, HoodieSchema.NULL_VALUE)));

    // Cannot use round-trip assertion because repeated group names differ
    testParquetToAvroConversion(
        schema,
        "message UnknownTwoLevelListInList {\n" + "  optional group listOfLists (LIST) {\n"
            + "    repeated group mylist (LIST) {\n"
            + "      repeated int32 innerlist;\n"
            + "    }\n"
            + "  }\n"
            + "}");
    // Cannot use round-trip assertion because 3-level representation is used
    testParquetToAvroConversion(
        NEW_BEHAVIOR,
        schema,
        "message UnknownTwoLevelListInList {\n" + "  optional group listOfLists (LIST) {\n"
            + "    repeated group mylist (LIST) {\n"
            + "      repeated int32 innerlist;\n"
            + "    }\n"
            + "  }\n"
            + "}");
  }

  @Test
  public void testParquetMapWithoutMapKeyValueAnnotation() throws Exception {
    HoodieSchema map = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.INT));
    HoodieSchema schema = HoodieSchema.createRecord("myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("mymap", map, null, null)));
    String parquetSchema = "message myrecord {\n" + "  required group mymap (MAP) {\n"
        + "    repeated group map {\n"
        + "      required binary key (UTF8);\n"
        + "      required int32 value;\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    testParquetToAvroConversion(schema, parquetSchema);
    testParquetToAvroConversion(NEW_BEHAVIOR, schema, parquetSchema);
  }

  @Test
  public void testDecimalBytesType() throws Exception {
    HoodieSchema decimal = HoodieSchema.createDecimal(9, 2);
    HoodieSchema schema = HoodieSchema.createRecord("myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("dec", decimal, null, null)));

    testRoundTripConversion(schema, "message myrecord {\n" + "  required binary dec (DECIMAL(9,2));\n" + "}\n");
  }

  @Test
  public void testDecimalFixedType() throws Exception {
    HoodieSchema decimal = HoodieSchema.createDecimal("dec", null, null, 9, 2, 8);
    HoodieSchema schema = HoodieSchema.createRecord("myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("dec", decimal, null, null)));

    testRoundTripConversion(
        schema, "message myrecord {\n" + "  required fixed_len_byte_array(8) dec (DECIMAL(9,2));\n" + "}\n");
  }

  @Test
  public void testDecimalIntegerType() throws Exception {
    HoodieSchema expected = HoodieSchema.createRecord("myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("dec", HoodieSchema.create(HoodieSchemaType.INT), null, null)));

    // the decimal portion is lost because it isn't valid in Avro
    testParquetToAvroConversion(
        expected, "message myrecord {\n" + "  required int32 dec (DECIMAL(9,2));\n" + "}\n");
  }

  @Test
  public void testDecimalLongType() throws Exception {
    HoodieSchema expected = HoodieSchema.createRecord(
        "myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("dec", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));

    // the decimal portion is lost because it isn't valid in Avro
    testParquetToAvroConversion(
        expected, "message myrecord {\n" + "  required int64 dec (DECIMAL(9,2));\n" + "}\n");
  }

  @Test
  public void testParquetInt96AsFixed12AvroType() throws Exception {
    Configuration enableInt96ReadingConfig = new Configuration();
    enableInt96ReadingConfig.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);

    HoodieSchema int96schema = HoodieSchema.createFixed("INT96", "INT96 represented as byte[12]", null, 12);
    HoodieSchema schema = HoodieSchema.createRecord("myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("int96_field", int96schema, null, null)));

    testParquetToAvroConversion(enableInt96ReadingConfig, schema, "message myrecord {\n"
        + "  required int96 int96_field;\n"
        + "}\n");
  }

  @Test
  public void testParquetInt96DefaultFail() throws Exception {
    MessageType parquetSchemaWithInt96 =
        MessageTypeParser.parseMessageType("message myrecord {\n  required int96 int96_field;\n}\n");

    assertThrows(
        "INT96 is deprecated. As interim enable READ_INT96_AS_FIXED  flag to read as byte array.",
        IllegalArgumentException.class,
        () -> getAvroSchemaConverter(new Configuration()).convert(parquetSchemaWithInt96));
  }

  @Test
  public void testDateType() throws Exception {
    HoodieSchema date = HoodieSchema.createDate();
    HoodieSchema expected = HoodieSchema.createRecord(
        "myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("date", date, null, null)));

    testRoundTripConversion(expected, "message myrecord {\n" + "  required int32 date (DATE);\n" + "}\n");

    for (PrimitiveTypeName primitive :
        new PrimitiveTypeName[] {INT64, INT96, FLOAT, DOUBLE, BOOLEAN, BINARY, FIXED_LEN_BYTE_ARRAY}) {
      final PrimitiveType type;
      if (primitive == FIXED_LEN_BYTE_ARRAY) {
        type = new PrimitiveType(REQUIRED, primitive, 12, "test", DATE);
      } else {
        type = new PrimitiveType(REQUIRED, primitive, "test", DATE);
      }

      assertThrows(
          "Should not allow TIME_MICROS with " + primitive,
          IllegalArgumentException.class,
          () -> getAvroSchemaConverter(new Configuration()).convert(message(type)));
    }
  }

  @Test
  public void testTimeMillisType() throws Exception {
    HoodieSchema timeMillis = HoodieSchema.createTimeMillis();
    HoodieSchema expected = HoodieSchema.createRecord(
        "myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("time", timeMillis, null, null)));

    testRoundTripConversion(
        expected, "message myrecord {\n" + "  required int32 time (TIME(MILLIS,true));\n" + "}\n");

    for (PrimitiveTypeName primitive :
        new PrimitiveTypeName[] {INT64, INT96, FLOAT, DOUBLE, BOOLEAN, BINARY, FIXED_LEN_BYTE_ARRAY}) {
      final PrimitiveType type;
      if (primitive == FIXED_LEN_BYTE_ARRAY) {
        type = new PrimitiveType(REQUIRED, primitive, 12, "test", TIME_MILLIS);
      } else {
        type = new PrimitiveType(REQUIRED, primitive, "test", TIME_MILLIS);
      }

      assertThrows(
          "Should not allow TIME_MICROS with " + primitive,
          IllegalArgumentException.class,
          () -> getAvroSchemaConverter(new Configuration()).convert(message(type)));
    }
  }

  @Test
  public void testTimeMicrosType() throws Exception {
    HoodieSchema timeMicros = HoodieSchema.createTimeMicros();
    HoodieSchema expected = HoodieSchema.createRecord(
        "myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("time", timeMicros, null, null)));

    testRoundTripConversion(
        expected, "message myrecord {\n" + "  required int64 time (TIME(MICROS,true));\n" + "}\n");

    for (PrimitiveTypeName primitive :
        new PrimitiveTypeName[] {INT32, INT96, FLOAT, DOUBLE, BOOLEAN, BINARY, FIXED_LEN_BYTE_ARRAY}) {
      final PrimitiveType type;
      if (primitive == FIXED_LEN_BYTE_ARRAY) {
        type = new PrimitiveType(REQUIRED, primitive, 12, "test", TIME_MICROS);
      } else {
        type = new PrimitiveType(REQUIRED, primitive, "test", TIME_MICROS);
      }

      assertThrows(
          "Should not allow TIME_MICROS with " + primitive,
          IllegalArgumentException.class,
          () -> getAvroSchemaConverter(new Configuration()).convert(message(type)));
    }
  }

  @Test
  public void testTimestampMillisType() throws Exception {
    HoodieSchema timestampMillis = HoodieSchema.createTimestampMillis();
    HoodieSchema expected = HoodieSchema.createRecord(
        "myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("timestamp", timestampMillis, null, null)));

    testRoundTripConversion(
        expected, "message myrecord {\n" + "  required int64 timestamp (TIMESTAMP(MILLIS,true));\n" + "}\n");

    final HoodieSchema converted = getAvroSchemaConverter(new Configuration())
        .convert(Types.buildMessage()
            .addField(Types.primitive(INT64, Type.Repetition.REQUIRED)
                .as(LogicalTypeAnnotation.timestampType(
                    false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .length(1)
                .named("timestamp_type"))
            .named("TestAvro"));
    assertEquals(
        "local-timestamp-millis",
        converted
            .getField("timestamp_type")
            .get()
            .schema()
            .getName());

    for (PrimitiveTypeName primitive :
        new PrimitiveTypeName[] {INT32, INT96, FLOAT, DOUBLE, BOOLEAN, BINARY, FIXED_LEN_BYTE_ARRAY}) {
      final PrimitiveType type;
      if (primitive == FIXED_LEN_BYTE_ARRAY) {
        type = new PrimitiveType(REQUIRED, primitive, 12, "test", TIMESTAMP_MILLIS);
      } else {
        type = new PrimitiveType(REQUIRED, primitive, "test", TIMESTAMP_MILLIS);
      }

      assertThrows(
          "Should not allow TIMESTAMP_MILLIS with " + primitive,
          IllegalArgumentException.class,
          () -> getAvroSchemaConverter(new Configuration()).convert(message(type)));
    }
  }

  @Test
  public void testLocalTimestampMillisType() throws Exception {
    HoodieSchema localTimestampMillis = HoodieSchema.createLocalTimestampMillis();
    HoodieSchema expected = HoodieSchema.createRecord(
        "myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("timestamp", localTimestampMillis, null, null)));

    testRoundTripConversion(
        expected, "message myrecord {\n" + "  required int64 timestamp (TIMESTAMP(MILLIS,false));\n" + "}\n");

    for (PrimitiveTypeName primitive :
        new PrimitiveTypeName[] {INT32, INT96, FLOAT, DOUBLE, BOOLEAN, BINARY, FIXED_LEN_BYTE_ARRAY}) {
      final PrimitiveType type;
      if (primitive == FIXED_LEN_BYTE_ARRAY) {
        type = new PrimitiveType(REQUIRED, primitive, 12, "test", TIMESTAMP_MILLIS);
      } else {
        type = new PrimitiveType(REQUIRED, primitive, "test", TIMESTAMP_MILLIS);
      }

      assertThrows(
          "Should not allow TIMESTAMP_MILLIS with " + primitive,
          IllegalArgumentException.class,
          () -> getAvroSchemaConverter(new Configuration()).convert(message(type)));
    }
  }

  @Test
  public void testTimestampMicrosType() throws Exception {
    HoodieSchema timestampMicros = HoodieSchema.createTimestampMicros();
    HoodieSchema expected = HoodieSchema.createRecord(
        "myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("timestamp", timestampMicros, null, null)));

    testRoundTripConversion(
        expected, "message myrecord {\n" + "  required int64 timestamp (TIMESTAMP(MICROS,true));\n" + "}\n");

    for (PrimitiveTypeName primitive :
        new PrimitiveTypeName[] {INT32, INT96, FLOAT, DOUBLE, BOOLEAN, BINARY, FIXED_LEN_BYTE_ARRAY}) {
      final PrimitiveType type;
      if (primitive == FIXED_LEN_BYTE_ARRAY) {
        type = new PrimitiveType(REQUIRED, primitive, 12, "test", TIMESTAMP_MICROS);
      } else {
        type = new PrimitiveType(REQUIRED, primitive, "test", TIMESTAMP_MICROS);
      }

      assertThrows(
          "Should not allow TIMESTAMP_MICROS with " + primitive,
          IllegalArgumentException.class,
          () -> getAvroSchemaConverter(new Configuration()).convert(message(type)));
    }

    final HoodieSchema converted = getAvroSchemaConverter(new Configuration())
        .convert(Types.buildMessage()
            .addField(Types.primitive(INT64, Type.Repetition.REQUIRED)
                .as(LogicalTypeAnnotation.timestampType(
                    false, LogicalTypeAnnotation.TimeUnit.MICROS))
                .length(1)
                .named("timestamp_type"))
            .named("TestAvro"));

    assertEquals(
        "local-timestamp-micros",
        converted
            .getField("timestamp_type")
            .get()
            .schema()
            .getName());
  }

  @Test
  public void testLocalTimestampMicrosType() throws Exception {
    HoodieSchema localTimestampMicros = HoodieSchema.createLocalTimestampMicros();
    HoodieSchema expected = HoodieSchema.createRecord(
        "myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("timestamp", localTimestampMicros, null, null)));

    testRoundTripConversion(
        expected, "message myrecord {\n" + "  required int64 timestamp (TIMESTAMP(MICROS,false));\n" + "}\n");

    for (PrimitiveTypeName primitive :
        new PrimitiveTypeName[] {INT32, INT96, FLOAT, DOUBLE, BOOLEAN, BINARY, FIXED_LEN_BYTE_ARRAY}) {
      final PrimitiveType type;
      if (primitive == FIXED_LEN_BYTE_ARRAY) {
        type = new PrimitiveType(REQUIRED, primitive, 12, "test", TIMESTAMP_MICROS);
      } else {
        type = new PrimitiveType(REQUIRED, primitive, "test", TIMESTAMP_MICROS);
      }

      assertThrows(
          "Should not allow TIMESTAMP_MICROS with " + primitive,
          IllegalArgumentException.class,
          () -> getAvroSchemaConverter(new Configuration()).convert(message(type)));
    }
  }

  @Test
  public void testReuseNameInNestedStructure() throws Exception {
    HoodieSchema innerA1 = HoodieSchema.createRecord("a1", null, "a12", false,
        Collections.singletonList(HoodieSchemaField.of("a4", HoodieSchema.create(HoodieSchemaType.FLOAT))));
    HoodieSchema outerA1 = HoodieSchema.createRecord("a1", null,null, false,
        Arrays.asList(
            HoodieSchemaField.of("a2", HoodieSchema.create(HoodieSchemaType.FLOAT)),
            HoodieSchemaField.of("a1", HoodieSchema.createNullable(innerA1), null, HoodieSchema.NULL_VALUE)));
    HoodieSchema schema = HoodieSchema.createRecord("Message", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("a1", HoodieSchema.createNullable(outerA1), null, HoodieSchema.NULL_VALUE)));

    String parquetSchema = "message Message {\n"
        + "  optional group a1 {\n"
        + "    required float a2;\n"
        + "    optional group a1 {\n"
        + "      required float a4;\n"
        + "     }\n"
        + "  }\n"
        + "}\n";

    testParquetToAvroConversion(schema, parquetSchema);
    testParquetToAvroConversion(NEW_BEHAVIOR, schema, parquetSchema);
  }

  @Test
  public void testReuseNameInNestedStructureAtSameLevel() throws Exception {
    HoodieSchema a2 = HoodieSchema.createRecord(
        "a2", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("a4", HoodieSchema.create(HoodieSchemaType.FLOAT))));
    HoodieSchema a22 = HoodieSchema.createRecord(
        "a2", "a22", null, Arrays.asList(
            HoodieSchemaField.of("a4", HoodieSchema.create(HoodieSchemaType.FLOAT)),
            HoodieSchemaField.of("a5", HoodieSchema.create(HoodieSchemaType.FLOAT))));

    HoodieSchema a1 = HoodieSchema.createRecord("a1", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("a2", HoodieSchema.createNullable(a2), null, HoodieSchema.NULL_VALUE)));
    HoodieSchema a3 = HoodieSchema.createRecord("a3", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("a2", HoodieSchema.createNullable(a22), null, HoodieSchema.NULL_VALUE)));

    HoodieSchema schema = HoodieSchema.createRecord("Message", null, null, false,
        Arrays.asList(
            HoodieSchemaField.of("a1", HoodieSchema.createNullable(a1), null, HoodieSchema.NULL_VALUE),
            HoodieSchemaField.of("a3", HoodieSchema.createNullable(a3), null, HoodieSchema.NULL_VALUE)));


    String parquetSchema = "message Message {\n"
        + "  optional group a1 {\n"
        + "    optional group a2 {\n"
        + "      required float a4;\n"
        + "     }\n"
        + "  }\n"
        + "  optional group a3 {\n"
        + "    optional group a2 {\n"
        + "      required float a4;\n"
        + "      required float a5;\n"
        + "     }\n"
        + "  }\n"
        + "}\n";

    testParquetToAvroConversion(schema, parquetSchema);
    testParquetToAvroConversion(NEW_BEHAVIOR, schema, parquetSchema);
  }

  @Test
  public void testUUIDType() throws Exception {
    HoodieSchema fromSchema = HoodieSchema.createRecord(
        "myrecord",
        null,
        null,
        false,
        Collections.singletonList(
            HoodieSchemaField.of("uuid", HoodieSchema.createUUID(), null, null)));
    String parquet = "message myrecord {\n" + "  required binary uuid (STRING);\n" + "}\n";
    HoodieSchema toSchema = HoodieSchema.createRecord(
        "myrecord",
        null,
        null,
        false,
        Collections.singletonList(HoodieSchemaField.of("uuid", HoodieSchema.create(HoodieSchemaType.STRING), null, null)));

    testAvroToParquetConversion(fromSchema, parquet);
    testParquetToAvroConversion(toSchema, parquet);

    assertTrue(HoodieSchemaCompatibility.areSchemasCompatible(fromSchema, toSchema));
  }

  @Test
  public void testUUIDTypeWithParquetUUID() throws Exception {
    HoodieSchema uuid = HoodieSchema.createUUID();
    HoodieSchema expected = HoodieSchema.createRecord(
        "myrecord", null, null, false,
        Collections.singletonList(HoodieSchemaField.of("uuid", uuid, null, null)));

    testRoundTripConversion(
        conf(AvroWriteSupport.WRITE_PARQUET_UUID, true),
        expected,
        "message myrecord {\n" + "  required fixed_len_byte_array(16) uuid (UUID);\n" + "}\n");
  }

  @Test
  public void testAvroFixed12AsParquetInt96Type() throws Exception {
    HoodieSchema schema = getSchemaFromResource(TestAvroSchemaConverter.class, "/parquet-java/fixedToInt96.avsc");

    Configuration conf = new Configuration();
    conf.setStrings(
        "parquet.avro.writeFixedAsInt96",
        "int96",
        "mynestedrecord.int96inrecord",
        "mynestedrecord.myarrayofoptional",
        "mynestedrecord.mymap");
    testAvroToParquetConversion(
        conf,
        schema,
        "message org.apache.parquet.avro.fixedToInt96 {\n"
            + "  required int96 int96;\n"
            + "  required fixed_len_byte_array(12) notanint96;\n"
            + "  required group mynestedrecord {\n"
            + "    required int96 int96inrecord;\n"
            + "    required group myarrayofoptional (LIST) {\n"
            + "      repeated int96 array;\n"
            + "    }\n"
            + "    required group mymap (MAP) {\n"
            + "      repeated group key_value (MAP_KEY_VALUE) {\n"
            + "        required binary key (STRING);\n"
            + "        required int96 value;\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "  required fixed_len_byte_array(1) onebytefixed;\n"
            + "}");

    conf.setStrings("parquet.avro.writeFixedAsInt96", "onebytefixed");
    assertThrows(
        "Exception should be thrown for fixed types to be converted to INT96 where the size is not 12 bytes",
        IllegalArgumentException.class,
        () -> getAvroSchemaConverter(conf).convert(schema));
  }

  public static MessageType message(PrimitiveType primitive) {
    return Types.buildMessage().addField(primitive).named("myrecord");
  }

  /**
   * A convenience method to avoid a large number of @Test(expected=...) tests
   *
   * @param message  A String message to describe this assertion
   * @param expected An Exception class that the Runnable should throw
   * @param runnable A Runnable that is expected to throw the exception
   */
  public static void assertThrows(String message, Class<? extends Exception> expected, Runnable runnable) {
    try {
      runnable.run();
      fail("No exception was thrown (" + message + "), expected: " + expected.getName());
    } catch (Exception actual) {
      try {
        assertEquals(expected, actual.getClass(), message);
      } catch (AssertionError e) {
        e.addSuppressed(actual);
        throw e;
      }
    }
  }

  public static Configuration conf(String name, boolean value) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(name, value);
    return conf;
  }

}