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

package org.apache.hudi.keygen;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.KeyGeneratorTestUtilities;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Function1;
import scala.Tuple2;

import java.io.IOException;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTimestampBasedKeyGenerator {

  private GenericRecord baseRecord;
  private TypedProperties properties = new TypedProperties();

  private Schema schema;
  private StructType structType;
  private Row baseRow;
  private InternalRow internalRow;

  @BeforeEach
  public void initialize() throws IOException {
    schema = SchemaTestUtil.getTimestampEvolvedSchema();
    structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);
    baseRecord = SchemaTestUtil
        .generateAvroRecordFromJson(schema, 1, "001", "f1");
    baseRow = genericRecordToRow(baseRecord);
    internalRow = KeyGeneratorTestUtilities.getInternalRow(baseRow);

    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "field1");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "createTime");
    properties.setProperty(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "false");
  }

  private Row genericRecordToRow(GenericRecord baseRecord) {
    Function1<GenericRecord, Row> convertor = AvroConversionUtils.createConverterToRow(baseRecord.getSchema(), structType);
    Row row = convertor.apply(baseRecord);
    int fieldCount = structType.fieldNames().length;
    Object[] values = new Object[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      values[i] = row.get(i);
    }
    return new GenericRowWithSchema(values, structType);
  }

  private TypedProperties getBaseKeyConfig(String partitionPathField, String timestampType, String dateFormat, String timezone, String scalarType) {
    TypedProperties properties = new TypedProperties(this.properties);

    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), partitionPathField);
    properties.setProperty(KeyGeneratorOptions.Config.TIMESTAMP_TYPE_FIELD_PROP, timestampType);
    properties.setProperty(KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, dateFormat);
    properties.setProperty(KeyGeneratorOptions.Config.TIMESTAMP_TIMEZONE_FORMAT_PROP, timezone);

    if (scalarType != null) {
      properties.setProperty("hoodie.deltastreamer.keygen.timebased.timestamp.scalar.time.unit", scalarType);
    }

    return properties;
  }

  private TypedProperties getBaseKeyConfig(String partitionPathField,
                                           String timestampType,
                                           String inputFormatList,
                                           String inputFormatDelimiterRegex,
                                           String inputTimezone,
                                           String outputFormat,
                                           String outputTimezone) {
    TypedProperties properties = new TypedProperties(this.properties);

    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), partitionPathField);

    if (timestampType != null) {
      properties.setProperty(KeyGeneratorOptions.Config.TIMESTAMP_TYPE_FIELD_PROP, timestampType);
    }
    if (inputFormatList != null) {
      properties.setProperty(KeyGeneratorOptions.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP, inputFormatList);
    }
    if (inputFormatDelimiterRegex != null) {
      properties.setProperty(KeyGeneratorOptions.Config.TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX_PROP, inputFormatDelimiterRegex);
    }
    if (inputTimezone != null) {
      properties.setProperty(KeyGeneratorOptions.Config.TIMESTAMP_INPUT_TIMEZONE_FORMAT_PROP, inputTimezone);
    }
    if (outputFormat != null) {
      properties.setProperty(KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, outputFormat);
    }
    if (outputTimezone != null) {
      properties.setProperty(KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP, outputTimezone);
    }
    return properties;
  }

  @Test
  public void testTimestampBasedKeyGenerator() throws IOException {
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
    Tuple2<Object, Schema> resolvedNullableSchema = AvroConversionUtils.resolveAvroTypeNullability(schema.getField("createTimeDecimal").schema());
    GenericFixed avroDecimal = conversion.toFixed(decimal, resolvedNullableSchema._2, LogicalTypes.decimal(20, 4));
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
  }

  @Test
  public void testScalar() throws IOException {
    // timezone is GMT+8:00
    baseRecord.put("createTime", 20000L);

    // timezone is GMT
    properties = getBaseKeyConfig("createTime", "SCALAR", "yyyy-MM-dd hh", "GMT", "days");
    TimestampBasedKeyGenerator keyGen = new TimestampBasedKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals(hk1.getPartitionPath(), "2024-10-04 12");

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
  public void testScalarWithLogicalType() throws IOException {
    schema = SchemaTestUtil.getTimestampWithLogicalTypeSchema();
    structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);
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
  public void test_ExpectsMatch_SingleInputFormat_ISO8601WithMsZ_OutputTimezoneAsUTC() throws IOException {
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
    Assertions.assertEquals("2020040113", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040113", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_SingleInputFormats_ISO8601WithMsZ_OutputTimezoneAsInputDateTimeZone() throws IOException {
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
    Assertions.assertEquals("2020040113", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040113", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsZ_OutputTimezoneAsUTC() throws IOException {
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
    Assertions.assertEquals("2020040113", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040113", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601NoMsZ_OutputTimezoneAsUTC() throws IOException {
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
    Assertions.assertEquals("2020040113", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040113", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601NoMsWithOffset_OutputTimezoneAsUTC() throws IOException {
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
    Assertions.assertEquals("2020040118", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040118", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsWithOffset_OutputTimezoneAsUTC() throws IOException {
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
    Assertions.assertEquals("2020040118", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040118", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsZ_OutputTimezoneAsEST() throws IOException {
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
    Assertions.assertEquals("2020040109", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("2020040109", keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_Throws_MultipleInputFormats_InputDateNotMatchingFormats() throws IOException {
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
    Assertions.assertThrows(HoodieKeyGeneratorException.class, () -> keyGen.getKey(baseRecord));

    baseRow = genericRecordToRow(baseRecord);
    Assertions.assertThrows(HoodieKeyGeneratorException.class, () -> keyGen.getPartitionPath(baseRow));
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ShortDate_OutputCustomDate() throws IOException {
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
    Assertions.assertEquals("04/01/2020", hk1.getPartitionPath());

    baseRow = genericRecordToRow(baseRecord);
    assertEquals("04/01/2020", keyGen.getPartitionPath(baseRow));
  }
}