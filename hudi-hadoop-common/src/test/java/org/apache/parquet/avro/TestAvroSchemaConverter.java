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

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
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

import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
import static org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility;
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

  private void testAvroToParquetConversion(Schema avroSchema, String schemaString) throws Exception {
    testAvroToParquetConversion(new Configuration(false), avroSchema, schemaString);
  }

  private void testAvroToParquetConversion(Configuration conf, Schema avroSchema, String schemaString)
      throws Exception {
    HoodieAvroParquetSchemaConverter avroSchemaConverter = getAvroSchemaConverter(conf);
    MessageType schema = avroSchemaConverter.convert(avroSchema);
    MessageType expectedMT = MessageTypeParser.parseMessageType(schemaString);
    assertEquals(expectedMT.toString(), schema.toString());
  }

  private void testParquetToAvroConversion(Schema avroSchema, String schemaString) throws Exception {
    testParquetToAvroConversion(new Configuration(false), avroSchema, schemaString);
  }

  private void testParquetToAvroConversion(Configuration conf, Schema avroSchema, String schemaString)
      throws Exception {
    HoodieAvroParquetSchemaConverter avroSchemaConverter = getAvroSchemaConverter(conf);
    Schema schema = avroSchemaConverter.convert(MessageTypeParser.parseMessageType(schemaString));
    assertEquals(avroSchema.toString(), schema.toString());
  }

  private void testRoundTripConversion(Schema avroSchema, String schemaString) throws Exception {
    testRoundTripConversion(new Configuration(), avroSchema, schemaString);
  }

  private void testRoundTripConversion(Configuration conf, Schema avroSchema, String schemaString) throws Exception {
    HoodieAvroParquetSchemaConverter avroSchemaConverter = getAvroSchemaConverter(conf);
    MessageType schema = avroSchemaConverter.convert(avroSchema);
    MessageType expectedMT = MessageTypeParser.parseMessageType(schemaString);
    assertEquals(expectedMT.toString(), schema.toString());
    Schema convertedAvroSchema = avroSchemaConverter.convert(expectedMT);
    assertEquals(avroSchema.toString(), convertedAvroSchema.toString());
  }

  @Test()
  public void testTopLevelMustBeARecord() {
    assertThrows("expected to throw", IllegalArgumentException.class, () -> getAvroSchemaConverter(new Configuration()).convert(Schema.create(INT)));
  }

  @Test
  public void testAllTypes() throws Exception {
    HoodieSchema schema = getSchemaFromResource(TestAvroSchemaConverter.class, "/parquet-java/all.avsc");
    testAvroToParquetConversion(
        NEW_BEHAVIOR,
        schema.toAvroSchema(),
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
        schema.toAvroSchema(),
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
    testParquetToAvroConversion(NEW_BEHAVIOR, schema.toAvroSchema(), ALL_PARQUET_SCHEMA);
  }

  @Test
  public void testAllTypesParquetToAvroOldBehavior() throws Exception {
    HoodieSchema schema = getSchemaFromResource(TestAvroSchemaConverter.class, "/parquet-java/allFromParquetOldBehavior.avsc");
    // Cannot use round-trip assertion because enum is lost
    testParquetToAvroConversion(schema.toAvroSchema(), ALL_PARQUET_SCHEMA);
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
    Schema schema = Schema.createRecord("record1", null, null, false);
    Schema optionalInt = optional(Schema.create(INT));
    schema.setFields(
        Collections.singletonList(new Schema.Field("myint", optionalInt, null, JsonProperties.NULL_VALUE)));
    testRoundTripConversion(schema, "message record1 {\n" + "  optional int32 myint;\n" + "}\n");
  }

  @Test
  public void testOptionalMapValue() throws Exception {
    Schema schema = Schema.createRecord("record1", null, null, false);
    Schema optionalIntMap = Schema.createMap(optional(Schema.create(INT)));
    schema.setFields(Arrays.asList(new Schema.Field("myintmap", optionalIntMap, null, null)));
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
    Schema schema = Schema.createRecord("record1", null, null, false);
    Schema optionalIntArray = Schema.createArray(optional(Schema.create(INT)));
    schema.setFields(Arrays.asList(new Schema.Field("myintarray", optionalIntArray, null, null)));
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
    Schema schema = Schema.createRecord("record2", null, null, false);
    Schema multipleTypes = Schema.createUnion(
        Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(INT), Schema.create(Schema.Type.FLOAT)));
    schema.setFields(Arrays.asList(new Schema.Field("myunion", multipleTypes, null, JsonProperties.NULL_VALUE)));

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
    Schema innerRecord = Schema.createRecord("element", null, null, false);
    Schema optionalString = optional(Schema.create(Schema.Type.STRING));
    innerRecord.setFields(Arrays.asList(
        new Schema.Field("s1", optionalString, null, JsonProperties.NULL_VALUE),
        new Schema.Field("s2", optionalString, null, JsonProperties.NULL_VALUE)));
    Schema schema = Schema.createRecord("HasArray", null, null, false);
    schema.setFields(
        Arrays.asList(new Schema.Field("myarray", Schema.createArray(optional(innerRecord)), null, null)));
    System.err.println("Avro schema: " + schema.toString(true));

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
    Schema innerRecord = Schema.createRecord("InnerRecord", null, null, false);
    Schema optionalString = optional(Schema.create(Schema.Type.STRING));
    innerRecord.setFields(Arrays.asList(
        new Schema.Field("s1", optionalString, null, JsonProperties.NULL_VALUE),
        new Schema.Field("s2", optionalString, null, JsonProperties.NULL_VALUE)));
    Schema schema = Schema.createRecord("HasArray", null, null, false);
    schema.setFields(
        Arrays.asList(new Schema.Field("myarray", Schema.createArray(optional(innerRecord)), null, null)));
    System.err.println("Avro schema: " + schema.toString(true));

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
    Schema listOfLists = optional(Schema.createArray(Schema.createArray(Schema.create(INT))));
    Schema schema = Schema.createRecord("AvroCompatListInList", null, null, false);
    schema.setFields(
        Arrays.asList(new Schema.Field("listOfLists", listOfLists, null, JsonProperties.NULL_VALUE)));
    System.err.println("Avro schema: " + schema.toString(true));

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
    Schema listOfLists = optional(Schema.createArray(Schema.createArray(Schema.create(INT))));
    Schema schema = Schema.createRecord("ThriftCompatListInList", null, null, false);
    schema.setFields(
        Arrays.asList(new Schema.Field("listOfLists", listOfLists, null, JsonProperties.NULL_VALUE)));
    System.err.println("Avro schema: " + schema.toString(true));

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
    Schema listOfLists = optional(Schema.createArray(Schema.createArray(Schema.create(INT))));
    Schema schema = Schema.createRecord("UnknownTwoLevelListInList", null, null, false);
    schema.setFields(
        Arrays.asList(new Schema.Field("listOfLists", listOfLists, null, JsonProperties.NULL_VALUE)));
    System.err.println("Avro schema: " + schema.toString(true));

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
    Schema schema = Schema.createRecord("myrecord", null, null, false);
    Schema map = Schema.createMap(Schema.create(INT));
    schema.setFields(Collections.singletonList(new Schema.Field("mymap", map, null, null)));
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
    Schema schema = Schema.createRecord("myrecord", null, null, false);
    Schema decimal = LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    schema.setFields(Collections.singletonList(new Schema.Field("dec", decimal, null, null)));

    testRoundTripConversion(schema, "message myrecord {\n" + "  required binary dec (DECIMAL(9,2));\n" + "}\n");
  }

  @Test
  public void testDecimalFixedType() throws Exception {
    Schema schema = Schema.createRecord("myrecord", null, null, false);
    Schema decimal = LogicalTypes.decimal(9, 2).addToSchema(Schema.createFixed("dec", null, null, 8));
    schema.setFields(Collections.singletonList(new Schema.Field("dec", decimal, null, null)));

    testRoundTripConversion(
        schema, "message myrecord {\n" + "  required fixed_len_byte_array(8) dec (DECIMAL(9,2));\n" + "}\n");
  }

  @Test
  public void testDecimalIntegerType() throws Exception {
    Schema expected = Schema.createRecord(
        "myrecord", null, null, false, Arrays.asList(new Schema.Field("dec", Schema.create(INT), null, null)));

    // the decimal portion is lost because it isn't valid in Avro
    testParquetToAvroConversion(
        expected, "message myrecord {\n" + "  required int32 dec (DECIMAL(9,2));\n" + "}\n");
  }

  @Test
  public void testDecimalLongType() throws Exception {
    Schema expected = Schema.createRecord(
        "myrecord", null, null, false, Arrays.asList(new Schema.Field("dec", Schema.create(LONG), null, null)));

    // the decimal portion is lost because it isn't valid in Avro
    testParquetToAvroConversion(
        expected, "message myrecord {\n" + "  required int64 dec (DECIMAL(9,2));\n" + "}\n");
  }

  @Test
  public void testParquetInt96AsFixed12AvroType() throws Exception {
    Configuration enableInt96ReadingConfig = new Configuration();
    enableInt96ReadingConfig.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);

    Schema schema = Schema.createRecord("myrecord", null, null, false);
    Schema int96schema = Schema.createFixed("INT96", "INT96 represented as byte[12]", null, 12);
    schema.setFields(Collections.singletonList(
      new Schema.Field("int96_field", int96schema, null, null)));

    testParquetToAvroConversion(enableInt96ReadingConfig, schema, "message myrecord {\n"
        + "  required int96 int96_field;\n"
        + "}\n");
  }

  @Test
  public void testParquetInt96DefaultFail() throws Exception {
    Schema schema = Schema.createRecord("myrecord", null, null, false);

    MessageType parquetSchemaWithInt96 =
        MessageTypeParser.parseMessageType("message myrecord {\n  required int96 int96_field;\n}\n");

    assertThrows(
        "INT96 is deprecated. As interim enable READ_INT96_AS_FIXED  flag to read as byte array.",
        IllegalArgumentException.class,
        () -> getAvroSchemaConverter(new Configuration()).convert(parquetSchemaWithInt96));
  }

  @Test
  public void testDateType() throws Exception {
    Schema date = LogicalTypes.date().addToSchema(Schema.create(INT));
    Schema expected = Schema.createRecord(
        "myrecord", null, null, false, Arrays.asList(new Schema.Field("date", date, null, null)));

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
    Schema date = LogicalTypes.timeMillis().addToSchema(Schema.create(INT));
    Schema expected = Schema.createRecord(
        "myrecord", null, null, false, Arrays.asList(new Schema.Field("time", date, null, null)));

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
    Schema date = LogicalTypes.timeMicros().addToSchema(Schema.create(LONG));
    Schema expected = Schema.createRecord(
        "myrecord", null, null, false, Arrays.asList(new Schema.Field("time", date, null, null)));

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
    Schema date = LogicalTypes.timestampMillis().addToSchema(Schema.create(LONG));
    Schema expected = Schema.createRecord(
        "myrecord", null, null, false, Arrays.asList(new Schema.Field("timestamp", date, null, null)));

    testRoundTripConversion(
        expected, "message myrecord {\n" + "  required int64 timestamp (TIMESTAMP(MILLIS,true));\n" + "}\n");

    final Schema converted = getAvroSchemaConverter(new Configuration())
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
            .schema()
            .getLogicalType()
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
    Schema date = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(LONG));
    Schema expected = Schema.createRecord(
        "myrecord", null, null, false, Arrays.asList(new Schema.Field("timestamp", date, null, null)));

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
    Schema date = LogicalTypes.timestampMicros().addToSchema(Schema.create(LONG));
    Schema expected = Schema.createRecord(
        "myrecord", null, null, false, Arrays.asList(new Schema.Field("timestamp", date, null, null)));

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

    final Schema converted = getAvroSchemaConverter(new Configuration())
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
            .schema()
            .getLogicalType()
            .getName());
  }

  @Test
  public void testLocalTimestampMicrosType() throws Exception {
    Schema date = LogicalTypes.localTimestampMicros().addToSchema(Schema.create(LONG));
    Schema expected = Schema.createRecord(
        "myrecord", null, null, false, Arrays.asList(new Schema.Field("timestamp", date, null, null)));

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
    Schema innerA1 = record("a1", "a12", field("a4", primitive(Schema.Type.FLOAT)));

    Schema outerA1 = record("a1", field("a2", primitive(Schema.Type.FLOAT)), optionalField("a1", innerA1));
    Schema schema = record("Message", optionalField("a1", outerA1));

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
    Schema a2 = record("a2", field("a4", primitive(Schema.Type.FLOAT)));
    Schema a22 = record(
        "a2", "a22", field("a4", primitive(Schema.Type.FLOAT)), field("a5", primitive(Schema.Type.FLOAT)));

    Schema a1 = record("a1", optionalField("a2", a2));
    Schema a3 = record("a3", optionalField("a2", a22));

    Schema schema = record("Message", optionalField("a1", a1), optionalField("a3", a3));

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
    Schema fromAvro = Schema.createRecord(
        "myrecord",
        null,
        null,
        false,
        Arrays.asList(
            new Schema.Field("uuid", LogicalTypes.uuid().addToSchema(Schema.create(STRING)), null, null)));
    String parquet = "message myrecord {\n" + "  required binary uuid (STRING);\n" + "}\n";
    Schema toAvro = Schema.createRecord(
        "myrecord",
        null,
        null,
        false,
        Arrays.asList(new Schema.Field("uuid", Schema.create(STRING), null, null)));

    testAvroToParquetConversion(fromAvro, parquet);
    testParquetToAvroConversion(toAvro, parquet);

    assertEquals(
        COMPATIBLE, checkReaderWriterCompatibility(fromAvro, toAvro).getType());
  }

  @Test
  public void testUUIDTypeWithParquetUUID() throws Exception {
    Schema uuid = LogicalTypes.uuid().addToSchema(Schema.create(STRING));
    Schema expected = Schema.createRecord(
        "myrecord", null, null, false, Arrays.asList(new Schema.Field("uuid", uuid, null, null)));

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
        schema.toAvroSchema(),
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
        () -> getAvroSchemaConverter(conf).convert(schema.toAvroSchema()));
  }

  public static Schema optional(Schema original) {
    return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), original));
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

  public static Schema record(String name, String namespace, Schema.Field... fields) {
    Schema record = Schema.createRecord(name, null, namespace, false);
    record.setFields(Arrays.asList(fields));
    return record;
  }

  public static Schema record(String name, Schema.Field... fields) {
    return record(name, null, fields);
  }

  public static Schema.Field field(String name, Schema schema) {
    return new Schema.Field(name, schema, null, null);
  }

  public static Schema.Field optionalField(String name, Schema schema) {
    return new Schema.Field(name, optional(schema), null, JsonProperties.NULL_VALUE);
  }

  public static Schema array(Schema element) {
    return Schema.createArray(element);
  }

  public static Schema primitive(Schema.Type type) {
    return Schema.create(type);
  }

  public static Configuration conf(String name, boolean value) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(name, value);
    return conf;
  }

}