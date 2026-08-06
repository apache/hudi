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

package org.apache.hudi.common.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;

import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.HOODIE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link AvroOrcUtils}.
 */
public class TestAvroOrcUtils extends HoodieCommonTestHarness {

  public static final TypeDescription ORC_SCHEMA = AvroOrcUtils.createOrcSchema(HoodieSchema.parse(TRIP_EXAMPLE_SCHEMA));
  public static final TypeDescription ORC_TRIP_SCHEMA = AvroOrcUtils.createOrcSchema(HoodieSchema.parse(TRIP_SCHEMA));

  public static List<Arguments> testCreateOrcSchemaArgs() {
    // the ORC schema is constructed in the order as AVRO_SCHEMA:
    // TRIP_SCHEMA_PREFIX, EXTRA_TYPE_SCHEMA, MAP_TYPE_SCHEMA, FARE_NESTED_SCHEMA, TIP_NESTED_SCHEMA, TRIP_SCHEMA_SUFFIX
    // The following types are tested:
    // DATE, DECIMAL, LONG, INT, BYTES, ARRAY, RECORD, MAP, STRING, FLOAT, DOUBLE, ENUM
    TypeDescription orcSchema = TypeDescription.fromString("struct<"
        + "timestamp:bigint,_row_key:string,partition_path:string,"
        + "trip_type:string,rider:string,driver:string,begin_lat:double,"
        + "begin_lon:double,end_lat:double,end_lon:double,"
        + "distance_in_meters:int,seconds_since_epoch:bigint,weight:float,nation:binary,"
        + "current_date:date,current_ts:bigint,height:decimal(10,6),"
        + "city_to_state:map<string,string>,"
        + "fare:struct<amount:double,currency:string>,"
        + "tip_history:array<struct<amount:double,currency:string>>,"
        + "_hoodie_is_deleted:boolean>");

    // Tests the types FIXED, UNION
    String structField = "{\"type\":\"record\", \"name\":\"fare\",\"fields\": "
        + "[{\"name\": \"amount\",\"type\": \"double\"},{\"name\": \"currency\", \"type\": \"string\"}]}";
    HoodieSchema schemaWithMoreTypes = HoodieSchema.parse(
        "{\"type\": \"record\"," + "\"name\": \"triprec\"," + "\"fields\": [ "
            + "{\"name\" : \"age\", \"type\":{\"type\": \"fixed\", \"size\": 16, \"name\": \"fixedField\" }},"
            + "{\"name\" : \"height\", \"type\": [\"int\", \"null\"] },"
            + "{\"name\" : \"id\", \"type\": [\"int\", \"string\"] },"
            + "{\"name\" : \"fare\", \"type\": [" + structField + ", \"null\"] }]}");
    TypeDescription orcSchemaWithMoreTypes = TypeDescription.fromString(
        "struct<age:binary,height:int,id:uniontype<int,string>,fare:struct<amount:double,currency:string>>");

    return Arrays.asList(
        Arguments.of(HOODIE_SCHEMA, orcSchema),
        Arguments.of(schemaWithMoreTypes, orcSchemaWithMoreTypes)
    );
  }

  @ParameterizedTest
  @MethodSource("testCreateOrcSchemaArgs")
  public void testCreateOrcSchema(HoodieSchema avroSchema, TypeDescription orcSchema) {
    TypeDescription convertedSchema = AvroOrcUtils.createOrcSchema(avroSchema);
    assertEquals(orcSchema, convertedSchema);
  }

  /**
   * Tests that LocalTimestamp types are converted to ORC Long (not Timestamp) to preserve old behavior.
   * This ensures backward compatibility with the pre-HoodieSchema refactoring implementation
   * where LocalTimestamp logical types were not explicitly handled and fell through to LONG conversion.
   */
  @Test
  public void testLocalTimestampConvertedToLong() {
    // Create HoodieSchemas for all timestamp types
    HoodieSchema timestampMillis = HoodieSchema.createTimestampMillis();
    HoodieSchema timestampMicros = HoodieSchema.createTimestampMicros();
    HoodieSchema localTimestampMillis = HoodieSchema.createLocalTimestampMillis();
    HoodieSchema localTimestampMicros = HoodieSchema.createLocalTimestampMicros();

    // UTC-adjusted timestamps should convert to ORC Timestamp
    TypeDescription orcTimestampMillis = AvroOrcUtils.createOrcSchema(timestampMillis);
    TypeDescription orcTimestampMicros = AvroOrcUtils.createOrcSchema(timestampMicros);
    assertEquals(TypeDescription.Category.TIMESTAMP, orcTimestampMillis.getCategory(),
        "TimestampMillis should convert to ORC Timestamp");
    assertEquals(TypeDescription.Category.TIMESTAMP, orcTimestampMicros.getCategory(),
        "TimestampMicros should convert to ORC Timestamp");

    // Local timestamps should convert to ORC Long (old behavior)
    TypeDescription orcLocalTimestampMillis = AvroOrcUtils.createOrcSchema(localTimestampMillis);
    TypeDescription orcLocalTimestampMicros = AvroOrcUtils.createOrcSchema(localTimestampMicros);
    assertEquals(TypeDescription.Category.LONG, orcLocalTimestampMillis.getCategory(),
        "LocalTimestampMillis should convert to ORC Long (preserving old behavior)");
    assertEquals(TypeDescription.Category.LONG, orcLocalTimestampMicros.getCategory(),
        "LocalTimestampMicros should convert to ORC Long (preserving old behavior)");
  }

  @Test
  public void testPrimitiveValuesRoundTripThroughColumnVectors() {
    assertEquals(true, roundTrip(HoodieSchema.create(HoodieSchemaType.BOOLEAN), true));
    assertEquals(12, roundTrip(HoodieSchema.create(HoodieSchemaType.INT), 12));
    assertEquals(34L, roundTrip(HoodieSchema.create(HoodieSchemaType.LONG), 34L));
    assertEquals(1.25f, roundTrip(HoodieSchema.create(HoodieSchemaType.FLOAT), 1.25f));
    assertEquals(2.5d, roundTrip(HoodieSchema.create(HoodieSchemaType.DOUBLE), 2.5d));
    assertEquals("hoodie", roundTrip(HoodieSchema.create(HoodieSchemaType.STRING), new Utf8("hoodie")).toString());
    assertEquals(42, roundTrip(HoodieSchema.createDate(), 42));
    assertEquals(1_700_000_000_123L, roundTrip(HoodieSchema.createTimestampMillis(), 1_700_000_000_123L));
    assertEquals(1_700_000_000_123_456L,
        roundTrip(HoodieSchema.createTimestampMicros(), 1_700_000_000_123_456L));

    ByteBuffer binary = (ByteBuffer) roundTrip(
        HoodieSchema.create(HoodieSchemaType.BYTES), ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertArrayEquals(new byte[] {1, 2, 3}, toByteArray(binary));
  }

  @Test
  public void testComplexValuesRoundTripThroughColumnVectors() {
    HoodieSchema arraySchema = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT));
    assertEquals(Arrays.asList(1, 2, 3), roundTrip(arraySchema, Arrays.asList(1, 2, 3)));

    HoodieSchema mapSchema = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG));
    Map<String, Long> values = new LinkedHashMap<>();
    values.put("first", 10L);
    values.put("second", 20L);
    assertEquals(values, roundTrip(mapSchema, values));

    HoodieSchema recordSchema = HoodieSchema.createRecord("nested", null, null, Arrays.asList(
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("count", HoodieSchema.create(HoodieSchemaType.INT), null, null)));
    GenericRecord record = new GenericData.Record(recordSchema.toAvroSchema());
    record.put("name", "record-name");
    record.put("count", 7);
    GenericRecord converted = (GenericRecord) roundTrip(recordSchema, record);
    assertEquals("record-name", converted.get("name").toString());
    assertEquals(7, converted.get("count"));

    HoodieSchema unionSchema = HoodieSchema.createUnion(Arrays.asList(
        HoodieSchema.create(HoodieSchemaType.INT), HoodieSchema.create(HoodieSchemaType.STRING)));
    assertEquals(9, roundTrip(unionSchema, 9));
    assertEquals("union-value", roundTrip(unionSchema, "union-value").toString());
  }

  @Test
  public void testDecimalEnumAndFixedValuesRoundTripThroughColumnVectors() {
    HoodieSchema decimalSchema = HoodieSchema.createDecimal(10, 2);
    ByteBuffer decimalBytes = (ByteBuffer) roundTrip(decimalSchema, new BigDecimal("1234.50"));
    assertEquals(new BigInteger("123450"), new BigInteger(toByteArray(decimalBytes)));

    HoodieSchema fixedDecimalSchema = HoodieSchema.parse("{\"type\":\"fixed\",\"name\":\"amount\","
        + "\"size\":8,\"logicalType\":\"decimal\",\"precision\":12,\"scale\":2}");
    BigDecimal fixedValue = new BigDecimal("9876.50");
    GenericData.Fixed fixed = (GenericData.Fixed) new Conversions.DecimalConversion().toFixed(
        fixedValue, fixedDecimalSchema.toAvroSchema(), fixedDecimalSchema.toAvroSchema().getLogicalType());
    GenericData.Fixed convertedFixed = (GenericData.Fixed) roundTrip(fixedDecimalSchema, fixed);
    assertEquals(fixedValue, new Conversions.DecimalConversion().fromFixed(
        convertedFixed, fixedDecimalSchema.toAvroSchema(), fixedDecimalSchema.toAvroSchema().getLogicalType()));

    HoodieSchema enumSchema = HoodieSchema.parse(
        "{\"type\":\"enum\",\"name\":\"status\",\"symbols\":[\"OPEN\",\"CLOSED\"]}");
    GenericData.EnumSymbol open = new GenericData.EnumSymbol(enumSchema.toAvroSchema(), "OPEN");
    assertEquals(open, roundTrip(enumSchema, open));
  }

  @Test
  public void testNullResizeAndRepeatingVectors() {
    HoodieSchema schema = HoodieSchema.create(HoodieSchemaType.LONG);
    TypeDescription type = AvroOrcUtils.createOrcSchema(schema);
    VectorizedRowBatch batch = TypeDescription.createStruct().addField("value", type).createRowBatch();
    ColumnVector vector = batch.cols[0];

    int expandedPosition = vector.isNull.length;
    AvroOrcUtils.addToVector(type, vector, schema, null, expandedPosition);
    assertFalse(vector.noNulls);
    assertTrue(vector.isNull[expandedPosition]);
    assertNull(AvroOrcUtils.readFromVector(type, vector, schema, expandedPosition));

    ((LongColumnVector) vector).vector[0] = 99;
    vector.isNull[0] = false;
    vector.isRepeating = true;
    assertEquals(99L, AvroOrcUtils.readFromVector(type, vector, schema, 17));
  }

  @Test
  public void testCreateSchemaCoversEveryOrcCategory() {
    TypeDescription orcSchema = TypeDescription.fromString("struct<"
        + "boolean_field:boolean,byte_field:tinyint,short_field:smallint,int_field:int,long_field:bigint,"
        + "float_field:float,double_field:double,string_field:string,char_field:char(5),varchar_field:varchar(8),"
        + "date_field:date,timestamp_field:timestamp,binary_field:binary,decimal_field:decimal(12,3),"
        + "list_field:array<int>,map_field:map<string,bigint>,union_field:uniontype<int,string>>");

    HoodieSchema schema = AvroOrcUtils.createSchema(orcSchema);
    assertEquals(HoodieSchemaType.RECORD, schema.getType());
    assertEquals(17, schema.getFields().size());
    assertEquals(HoodieSchemaType.BOOLEAN, schema.getField("boolean_field").get().schema().getType());
    assertEquals(HoodieSchemaType.BYTES, schema.getField("binary_field").get().schema().getType());
    assertEquals(HoodieSchemaType.ARRAY, schema.getField("list_field").get().schema().getType());
    assertEquals(HoodieSchemaType.MAP, schema.getField("map_field").get().schema().getType());
    assertEquals(HoodieSchemaType.UNION, schema.getField("union_field").get().schema().getType());
  }

  @Test
  public void testCreateSchemaWithDefaultsAndNestedNamespaces() {
    TypeDescription orcSchema = TypeDescription.fromString(
        "struct<id:bigint,nested:struct<name:string,active:boolean>,amount:decimal(8,2),tags:array<string>>");

    HoodieSchema nullable = AvroOrcUtils.createSchemaWithDefaultValue(
        orcSchema, "root_record", "org.apache.hudi.test", true);
    assertEquals("root_record", nullable.getName());
    assertTrue(nullable.getFields().stream().allMatch(field -> field.schema().getType() == HoodieSchemaType.UNION));

    HoodieSchema required = AvroOrcUtils.createSchemaWithDefaultValue(
        orcSchema, "root_record", "org.apache.hudi.test", false);
    assertTrue(required.getFields().stream().noneMatch(field -> field.schema().getType() == HoodieSchemaType.UNION));
    assertEquals("nested", required.getField("nested").get().schema().getName());
  }

  @Test
  public void testCachingOfByteReferencesDoesNotCopyInput() {
    HoodieSchema schema = HoodieSchema.create(HoodieSchemaType.BYTES);
    TypeDescription type = AvroOrcUtils.createOrcSchema(schema);
    VectorizedRowBatch batch = TypeDescription.createStruct().addField("value", type).createRowBatch();
    byte[] input = new byte[] {4, 5, 6};

    AvroOrcUtils.addToVector(type, batch.cols[0], schema, input, 0);

    BytesColumnVector vector = (BytesColumnVector) batch.cols[0];
    assertSame(input, vector.vector[0]);
  }

  @Test
  public void testAdditionalOrcVectorRepresentationsAndFailures() {
    assertEquals((byte) 7, roundTrip(TypeDescription.createByte(), HoodieSchema.create(HoodieSchemaType.INT), (byte) 7));
    assertEquals((short) 11, roundTrip(TypeDescription.createShort(), HoodieSchema.create(HoodieSchemaType.INT), (short) 11));
    assertEquals("char", roundTrip(TypeDescription.createChar().withMaxLength(8),
        HoodieSchema.create(HoodieSchemaType.STRING), "char"));
    assertEquals("varchar", roundTrip(TypeDescription.createVarchar().withMaxLength(8),
        HoodieSchema.create(HoodieSchemaType.STRING), "varchar"));

    HoodieSchema dateSchema = HoodieSchema.create(HoodieSchemaType.INT);
    assertEquals(1, roundTrip(TypeDescription.createDate(), dateSchema, Date.valueOf("1970-01-02")));
    assertEquals(2, roundTrip(TypeDescription.createDate(), dateSchema, new java.util.Date(2L * 86_400_000L)));

    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertThrows(IllegalStateException.class,
        () -> addValue(TypeDescription.createString(), stringSchema, new Object()));
    assertThrows(IllegalStateException.class,
        () -> addValue(TypeDescription.createBinary(), HoodieSchema.create(HoodieSchemaType.BYTES), "not-binary"));
    assertThrows(IllegalStateException.class,
        () -> addValue(TypeDescription.createDecimal().withScale(2).withPrecision(8),
            HoodieSchema.createDecimal(8, 2), "not-decimal"));
    assertThrows(org.apache.hudi.exception.HoodieIOException.class,
        () -> roundTrip(TypeDescription.createVarchar().withMaxLength(3), stringSchema, "too-long"));
  }

  @Test
  public void testUnionMatchesPrimitiveAndBinaryRepresentations() {
    HoodieSchema schema = HoodieSchema.createUnion(Arrays.asList(
        HoodieSchema.create(HoodieSchemaType.BOOLEAN),
        HoodieSchema.create(HoodieSchemaType.INT),
        HoodieSchema.create(HoodieSchemaType.LONG),
        HoodieSchema.create(HoodieSchemaType.FLOAT),
        HoodieSchema.create(HoodieSchemaType.DOUBLE),
        HoodieSchema.create(HoodieSchemaType.STRING),
        HoodieSchema.create(HoodieSchemaType.BYTES)));
    TypeDescription type = TypeDescription.createUnion()
        .addUnionChild(TypeDescription.createBoolean())
        .addUnionChild(TypeDescription.createInt())
        .addUnionChild(TypeDescription.createLong())
        .addUnionChild(TypeDescription.createFloat())
        .addUnionChild(TypeDescription.createDouble())
        .addUnionChild(TypeDescription.createString())
        .addUnionChild(TypeDescription.createBinary());

    assertEquals(true, roundTrip(type, schema, true));
    assertEquals(17, roundTrip(type, schema, 17));
    assertEquals(23L, roundTrip(type, schema, 23L));
    assertEquals(1.5f, roundTrip(type, schema, 1.5f));
    assertEquals(2.5d, roundTrip(type, schema, 2.5d));
    assertEquals("text", roundTrip(type, schema, new Utf8("text")).toString());
    assertArrayEquals(new byte[] {9, 8}, toByteArray((ByteBuffer) roundTrip(
        type, schema, new byte[] {9, 8})));

    VectorizedRowBatch batch = TypeDescription.createStruct().addField("value", type).createRowBatch();
    assertFalse(AvroOrcUtils.addUnionValue((UnionColumnVector) batch.cols[0], type.getChildren(), schema,
        new Object(), 0));
  }

  @Test
  public void testTimeAndNullUnionSchemaCreation() {
    assertEquals(TypeDescription.createInt(), AvroOrcUtils.createOrcSchema(HoodieSchema.createTimeMillis()));
    assertEquals(TypeDescription.createLong(), AvroOrcUtils.createOrcSchema(HoodieSchema.createTimeMicros()));
    assertEquals(TypeDescription.createUnion(),
        AvroOrcUtils.createOrcSchema(HoodieSchema.createUnion(Arrays.asList(
            HoodieSchema.create(HoodieSchemaType.NULL)))));
  }

  private static Object roundTrip(HoodieSchema schema, Object value) {
    TypeDescription type = AvroOrcUtils.createOrcSchema(schema);
    return roundTrip(type, schema, value);
  }

  private static Object roundTrip(TypeDescription type, HoodieSchema schema, Object value) {
    VectorizedRowBatch batch = TypeDescription.createStruct().addField("value", type).createRowBatch();
    AvroOrcUtils.addToVector(type, batch.cols[0], schema, value, 0);
    return AvroOrcUtils.readFromVector(type, batch.cols[0], schema, 0);
  }

  private static void addValue(TypeDescription type, HoodieSchema schema, Object value) {
    VectorizedRowBatch batch = TypeDescription.createStruct().addField("value", type).createRowBatch();
    AvroOrcUtils.addToVector(type, batch.cols[0], schema, value, 0);
  }

  private static byte[] toByteArray(ByteBuffer buffer) {
    ByteBuffer duplicate = buffer.duplicate();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }
}
