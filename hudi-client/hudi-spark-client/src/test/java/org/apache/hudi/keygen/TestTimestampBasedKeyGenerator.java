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

package org.apache.hudi.keygen;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;

import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_DATE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_TIMEZONE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_TIMEZONE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_TYPE_FIELD;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestTimestampBasedKeyGenerator {

  private GenericRecord baseRecord;
  private TypedProperties properties = new TypedProperties();

  private HoodieSchema schema;
  private StructType structType;
  private Row baseRow;
  private InternalRow internalRow;

  @BeforeEach
  void initialize() throws IOException {
    schema = SchemaTestUtil.getTimestampEvolvedSchema();
    structType = new StructType(new StructField[] {
        new StructField("field1", StringType, true, Metadata.empty()),
        new StructField("createTime", LongType, true, Metadata.empty()),
        new StructField("createTimeString", StringType, true, Metadata.empty()),
        new StructField("createTimeDecimal", new DecimalType(20, 4), true, Metadata.empty())
    });
    baseRecord = SchemaTestUtil
        .generateAvroRecordFromJson(schema, 1, "001", "f1");
    baseRow = genericRecordToRow(baseRecord);
    internalRow = KeyGeneratorTestUtilities.getInternalRow(baseRow);

    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "field1");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "createTime");
    properties.setProperty(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "false");
  }

  private Row genericRecordToRow(GenericRecord baseRecord) {
    return KeyGeneratorTestUtilities.genericRecordToRow(baseRecord, structType);
  }

  private TypedProperties getBaseKeyConfig(String partitionPathField, String timestampType, String dateFormat, String timezone, String scalarType) {
    TypedProperties newProperties = TypedProperties.copy(this.properties);

    newProperties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), partitionPathField);
    newProperties.setProperty(TIMESTAMP_TYPE_FIELD.key(), timestampType);
    newProperties.setProperty(TIMESTAMP_OUTPUT_DATE_FORMAT.key(), dateFormat);
    newProperties.setProperty(TIMESTAMP_TIMEZONE_FORMAT.key(), timezone);

    if (scalarType != null) {
      newProperties.setProperty("hoodie.deltastreamer.keygen.timebased.timestamp.scalar.time.unit", scalarType);
    }

    return newProperties;
  }

  private TypedProperties getBaseKeyConfig(String partitionPathField,
                                           String timestampType,
                                           String inputFormatList,
                                           String inputFormatDelimiterRegex,
                                           String inputTimezone,
                                           String outputFormat,
                                           String outputTimezone) {
    TypedProperties newProperties = TypedProperties.copy(this.properties);

    newProperties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), partitionPathField);

    if (timestampType != null) {
      newProperties.setProperty(TIMESTAMP_TYPE_FIELD.key(), timestampType);
    }
    if (inputFormatList != null) {
      newProperties.setProperty(TIMESTAMP_INPUT_DATE_FORMAT.key(), inputFormatList);
    }
    if (inputFormatDelimiterRegex != null) {
      newProperties.setProperty(TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX.key(), inputFormatDelimiterRegex);
    }
    if (inputTimezone != null) {
      newProperties.setProperty(TIMESTAMP_INPUT_TIMEZONE_FORMAT.key(), inputTimezone);
    }
    if (outputFormat != null) {
      newProperties.setProperty(TIMESTAMP_OUTPUT_DATE_FORMAT.key(), outputFormat);
    }
    if (outputTimezone != null) {
      newProperties.setProperty(TIMESTAMP_OUTPUT_TIMEZONE_FORMAT.key(), outputTimezone);
    }
    return newProperties;
  }

  @Test
  void testTimestampBasedKeyGenerator() throws IOException {
    // timezone is GMT+8:00
    baseRecord.put("createTime", 1578283932000L);
    properties = getBaseKeyConfig("createTime", "EPOCHMILLISECONDS", "yyyy-MM-dd hh", "GMT+8:00", null);
    TimestampBasedKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals("2020-01-06 12", hk1.getPartitionPath());
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020-01-06 12", keyGen.getPartitionPath(baseRow));
    internalRow = KeyGeneratorTestUtilities.getInternalRow(baseRow);
    assertEquals(UTF8String.fromString("2020-01-06 12"), keyGen.getPartitionPath(internalRow, baseRow.schema()));

    // timezone is GMT+8:00, createTime is BigDecimal
    BigDecimal decimal = new BigDecimal("1578283932000.0001");
    Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
    HoodieSchema resolvedNullableSchema = HoodieSchemaUtils.getNonNullTypeFromUnion(schema.getField("createTimeDecimal").get().schema());
    GenericFixed avroDecimal = conversion.toFixed(decimal, resolvedNullableSchema.toAvroSchema(), LogicalTypes.decimal(20, 4));
    baseRecord.put("createTimeDecimal", avroDecimal);
    properties = getBaseKeyConfig("createTimeDecimal", "EPOCHMILLISECONDS", "yyyy-MM-dd hh", "GMT+8:00", null);
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey bigDecimalKey = keyGen.getKey(baseRecord);
    assertEquals("2020-01-06 12", bigDecimalKey.getPartitionPath());
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020-01-06 12", keyGen.getPartitionPath(baseRow));

    // timezone is GMT
    properties = getBaseKeyConfig("createTime", "EPOCHMILLISECONDS", "yyyy-MM-dd hh", "GMT", null);
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk2 = keyGen.getKey(baseRecord);
    assertEquals("2020-01-06 04", hk2.getPartitionPath());
    assertEquals("2020-01-06 04", keyGen.getPartitionPath(baseRow));

    // timestamp is DATE_STRING, timezone is GMT+8:00
    baseRecord.put("createTimeString", "2020-01-06 12:12:12");
    properties = getBaseKeyConfig("createTimeString", "DATE_STRING", "yyyy-MM-dd hh", "GMT+8:00", null);
    properties.setProperty("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd hh:mm:ss");
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk3 = keyGen.getKey(baseRecord);
    assertEquals("2020-01-06 12", hk3.getPartitionPath());
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020-01-06 12", keyGen.getPartitionPath(baseRow));

    // timezone is GMT
    properties = getBaseKeyConfig("createTimeString", "DATE_STRING", "yyyy-MM-dd hh", "GMT", null);
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk4 = keyGen.getKey(baseRecord);
    assertEquals("2020-01-06 12", hk4.getPartitionPath());
    assertEquals("2020-01-06 12", keyGen.getPartitionPath(baseRow));

    // timezone is GMT+8:00, createTime is null
    baseRecord.put("createTime", null);
    properties = getBaseKeyConfig("createTime", "EPOCHMILLISECONDS", "yyyy-MM-dd hh", "GMT+8:00", null);
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk5 = keyGen.getKey(baseRecord);
    assertEquals("1970-01-01 08", hk5.getPartitionPath());
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("1970-01-01 08", keyGen.getPartitionPath(baseRow));
    internalRow = KeyGeneratorTestUtilities.getInternalRow(baseRow);
    assertEquals(UTF8String.fromString("1970-01-01 08"), keyGen.getPartitionPath(internalRow, baseRow.schema()));

    // timestamp is DATE_STRING, timezone is GMT, createTime is null
    baseRecord.put("createTimeString", null);
    properties = getBaseKeyConfig("createTime", "DATE_STRING", "yyyy-MM-dd hh:mm:ss", "GMT", null);
    properties.setProperty("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd hh:mm:ss");
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk6 = keyGen.getKey(baseRecord);
    assertEquals("1970-01-01 12:00:00", hk6.getPartitionPath());
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("1970-01-01 12:00:00", keyGen.getPartitionPath(baseRow));
    internalRow = KeyGeneratorTestUtilities.getInternalRow(baseRow);
    assertEquals(UTF8String.fromString("1970-01-01 12:00:00"), keyGen.getPartitionPath(internalRow, baseRow.schema()));

    // Timestamp field is in long type, with `EPOCHMICROSECONDS` timestamp type in the key generator
    baseRecord.put("createTime", 1578283932123456L);
    properties = getBaseKeyConfig("createTime", "EPOCHMICROSECONDS", "yyyy-MM-dd hh", "GMT+8:00", null);
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey key = keyGen.getKey(baseRecord);
    assertEquals("2020-01-06 12", key.getPartitionPath());
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020-01-06 12", keyGen.getPartitionPath(baseRow));
    internalRow = KeyGeneratorTestUtilities.getInternalRow(baseRow);
    assertEquals(UTF8String.fromString("2020-01-06 12"), keyGen.getPartitionPath(internalRow, baseRow.schema()));

    // Timestamp field is in decimal type, with `EPOCHMICROSECONDS` timestamp type in the key generator
    decimal = new BigDecimal("1578283932123456.0001");
    resolvedNullableSchema = HoodieSchemaUtils.getNonNullTypeFromUnion(
        schema.getField("createTimeDecimal").get().schema());
    avroDecimal = conversion.toFixed(decimal, resolvedNullableSchema.toAvroSchema(), LogicalTypes.decimal(20, 4));
    baseRecord.put("createTimeDecimal", avroDecimal);
    properties = getBaseKeyConfig(
        "createTimeDecimal", "EPOCHMICROSECONDS", "yyyy-MM-dd hh", "GMT+8:00", null);
    keyGen = new TimestampBasedKeyGenerator(properties);
    bigDecimalKey = keyGen.getKey(baseRecord);
    assertEquals("2020-01-06 12", bigDecimalKey.getPartitionPath());
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020-01-06 12", keyGen.getPartitionPath(baseRow));
  }

  @Test
  void testScalar() throws IOException {
    // timezone is GMT+8:00
    baseRecord.put("createTime", 20000L);

    // timezone is GMT
    properties = getBaseKeyConfig("createTime", "SCALAR", "yyyy-MM-dd hh", "GMT", "days");
    TimestampBasedKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals("2024-10-04 12", hk1.getPartitionPath());

    // test w/ Row
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2024-10-04 12", keyGen.getPartitionPath(baseRow));
    internalRow = KeyGeneratorTestUtilities.getInternalRow(baseRow);
    assertEquals(UTF8String.fromString("2024-10-04 12"), keyGen.getPartitionPath(internalRow, baseRow.schema()));

    // timezone is GMT, createTime is null
    baseRecord.put("createTime", null);
    properties = getBaseKeyConfig("createTime", "SCALAR", "yyyy-MM-dd hh", "GMT", "days");
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk2 = keyGen.getKey(baseRecord);
    assertEquals("1970-01-02 12", hk2.getPartitionPath());

    // test w/ Row
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("1970-01-02 12", keyGen.getPartitionPath(baseRow));
    internalRow = KeyGeneratorTestUtilities.getInternalRow(baseRow);
    assertEquals(UTF8String.fromString("1970-01-02 12"), keyGen.getPartitionPath(internalRow, baseRow.schema()));

    // timezone is GMT. number of days store integer in mysql
    baseRecord.put("createTime", 18736L);
    properties = getBaseKeyConfig("createTime", "SCALAR", "yyyy-MM-dd", "GMT", "DAYS");
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey scalarSecondsKey = keyGen.getKey(baseRecord);
    assertEquals("2021-04-19", scalarSecondsKey.getPartitionPath());

    // test w/ Row
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2021-04-19", keyGen.getPartitionPath(baseRow));
  }

  @Test
  void testScalarWithLogicalType() throws IOException {
    schema = SchemaTestUtil.getTimestampWithLogicalTypeSchema();
    structType = new StructType(new StructField[] {
        new StructField("field1", StringType, true, Metadata.empty()),
        new StructField("createTime", TimestampType, true, Metadata.empty())
    });
    baseRecord = SchemaTestUtil.generateAvroRecordFromJson(schema, 1, "001", "f1");
    baseRecord.put("createTime", 1638513806000000L);

    properties = getBaseKeyConfig("createTime", "SCALAR", "yyyy/MM/dd", "GMT", "MICROSECONDS");
    properties.setProperty(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), "true");
    TimestampBasedKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals("2021/12/03", hk1.getPartitionPath());

    // test w/ Row
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2021/12/03", keyGen.getPartitionPath(baseRow));
    internalRow = KeyGeneratorTestUtilities.getInternalRow(baseRow);
    assertEquals(UTF8String.fromString("2021/12/03"), keyGen.getPartitionPath(internalRow, baseRow.schema()));

    // timezone is GMT, createTime is null
    baseRecord.put("createTime", null);
    properties = getBaseKeyConfig("createTime", "SCALAR", "yyyy/MM/dd", "GMT", "MICROSECONDS");
    properties.setProperty(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), "true");
    keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk2 = keyGen.getKey(baseRecord);
    assertEquals("1970/01/01", hk2.getPartitionPath());

    // test w/ Row
    baseRow = genericRecordToRow(baseRecord);
    assertEquals("1970/01/01", keyGen.getPartitionPath(baseRow));
    internalRow = KeyGeneratorTestUtilities.getInternalRow(baseRow);
    assertEquals(UTF8String.fromString("1970/01/01"), keyGen.getPartitionPath(internalRow, baseRow.schema()));
  }

  @Test
  void test_ExpectsMatch_SingleInputFormat_ISO8601WithMsZ_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTimeString", "2020-04-01T13:01:33.428Z");
    properties = this.getBaseKeyConfig(
        "createTimeString",
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "GMT");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals("2020040113", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040113", keyGen.getPartitionPath(baseRow));
  }

  @Test
  void test_ExpectsMatch_SingleInputFormats_ISO8601WithMsZ_OutputTimezoneAsInputDateTimeZone() throws IOException {
    baseRecord.put("createTimeString", "2020-04-01T13:01:33.428Z");
    properties = this.getBaseKeyConfig(
        "createTimeString",
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals("2020040113", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040113", keyGen.getPartitionPath(baseRow));
  }

  @Test
  void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsZ_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTimeString", "2020-04-01T13:01:33.428Z");
    properties = this.getBaseKeyConfig(
        "createTimeString",
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "UTC");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals("2020040113", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040113", keyGen.getPartitionPath(baseRow));
  }

  @Test
  void test_ExpectsMatch_MultipleInputFormats_ISO8601NoMsZ_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTimeString", "2020-04-01T13:01:33Z");
    properties = this.getBaseKeyConfig(
        "createTimeString",
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "UTC");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals("2020040113", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040113", keyGen.getPartitionPath(baseRow));
  }

  @Test
  void test_ExpectsMatch_MultipleInputFormats_ISO8601NoMsWithOffset_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTimeString", "2020-04-01T13:01:33-05:00");
    properties = this.getBaseKeyConfig(
        "createTimeString",
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "UTC");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals("2020040118", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040118", keyGen.getPartitionPath(baseRow));
  }

  @Test
  void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsWithOffset_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTimeString", "2020-04-01T13:01:33.123-05:00");
    properties = this.getBaseKeyConfig(
        "createTimeString",
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "UTC");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals("2020040118", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040118", keyGen.getPartitionPath(baseRow));
  }

  @Test
  void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsZ_OutputTimezoneAsEST() throws IOException {
    baseRecord.put("createTimeString", "2020-04-01T13:01:33.123Z");
    properties = this.getBaseKeyConfig(
        "createTimeString",
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "EST");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals("2020040109", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040109", keyGen.getPartitionPath(baseRow));
  }

  @Test
  void test_Throws_MultipleInputFormats_InputDateNotMatchingFormats() throws IOException {
    baseRecord.put("createTimeString", "2020-04-01 13:01:33.123-05:00");
    properties = this.getBaseKeyConfig(
        "createTimeString",
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "",
        "",
        "yyyyMMddHH",
        "UTC");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    assertThrows(HoodieKeyGeneratorException.class, () -> keyGen.getKey(baseRecord));

    baseRow = genericRecordToRow(baseRecord);
    assertThrows(HoodieKeyGeneratorException.class, () -> keyGen.getPartitionPath(baseRow));
  }

  @Test
  void test_ExpectsMatch_MultipleInputFormats_ShortDate_OutputCustomDate() throws IOException {
    baseRecord.put("createTimeString", "20200401");
    properties = this.getBaseKeyConfig(
        "createTimeString",
        "DATE_STRING",
        "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ,yyyyMMdd",
        "",
        "UTC",
        "MM/dd/yyyy",
        "UTC");
    BuiltinKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals("04/01/2020", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("04/01/2020", keyGen.getPartitionPath(baseRow));
  }
}