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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.testutils.SchemaTestUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.exception.HoodieDeltaStreamerException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTimestampBasedKeyGenerator {
  private GenericRecord baseRecord;
  private TypedProperties properties = new TypedProperties();

  @BeforeEach
  public void initialize() throws IOException {
    Schema schema = SchemaTestUtil.getTimestampEvolvedSchema();
    baseRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 1, "001", "f1");

    properties.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "field1");
    properties.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "createTime");
    properties.setProperty(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(), "false");
  }

  private TypedProperties getBaseKeyConfig(String timestampType, String dateFormat, String timezone, String scalarType) {
    properties.setProperty(TimestampBasedKeyGenerator.Config.TIMESTAMP_TYPE_FIELD_PROP, timestampType);
    properties.setProperty(TimestampBasedKeyGenerator.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, dateFormat);
    properties.setProperty(TimestampBasedKeyGenerator.Config.TIMESTAMP_TIMEZONE_FORMAT_PROP, timezone);

    if (scalarType != null) {
      properties.setProperty("hoodie.deltastreamer.keygen.timebased.timestamp.scalar.time.unit", scalarType);
    }

    return properties;
  }

  private TypedProperties getBaseKeyConfig(String timestampType, String inputFormatList, String inputFormatDelimiterRegex, String inputTimezone, String outputFormat, String outputTimezone) {
    if (timestampType != null) {
      properties.setProperty(TimestampBasedKeyGenerator.Config.TIMESTAMP_TYPE_FIELD_PROP, timestampType);
    }
    if (inputFormatList != null) {
      properties.setProperty(TimestampBasedKeyGenerator.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP, inputFormatList);
    }
    if (inputFormatDelimiterRegex != null) {
      properties.setProperty(TimestampBasedKeyGenerator.Config.TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX_PROP, inputFormatDelimiterRegex);
    }
    if (inputTimezone != null) {
      properties.setProperty(TimestampBasedKeyGenerator.Config.TIMESTAMP_INPUT_TIMEZONE_FORMAT_PROP, inputTimezone);
    }
    if (outputFormat != null) {
      properties.setProperty(TimestampBasedKeyGenerator.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, outputFormat);
    }
    if (outputTimezone != null) {
      properties.setProperty(TimestampBasedKeyGenerator.Config.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP, outputTimezone);
    }
    return properties;
  }

  @Test
  public void testTimestampBasedKeyGenerator() throws IOException {
    // timezone is GMT+8:00
    baseRecord.put("createTime", 1578283932000L);
    properties = getBaseKeyConfig("EPOCHMILLISECONDS", "yyyy-MM-dd hh", "GMT+8:00", null);
    HoodieKey hk1 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);
    assertEquals("2020-01-06 12", hk1.getPartitionPath());

    // timezone is GMT
    properties = getBaseKeyConfig("EPOCHMILLISECONDS", "yyyy-MM-dd hh", "GMT", null);
    HoodieKey hk2 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);
    assertEquals("2020-01-06 04", hk2.getPartitionPath());

    // timestamp is DATE_STRING, timezone is GMT+8:00
    baseRecord.put("createTime", "2020-01-06 12:12:12");
    properties = getBaseKeyConfig("DATE_STRING", "yyyy-MM-dd hh", "GMT+8:00", null);
    properties.setProperty("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd hh:mm:ss");
    HoodieKey hk3 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);
    assertEquals("2020-01-06 12", hk3.getPartitionPath());

    // timezone is GMT
    properties = getBaseKeyConfig("DATE_STRING", "yyyy-MM-dd hh", "GMT", null);
    HoodieKey hk4 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);
    assertEquals("2020-01-06 12", hk4.getPartitionPath());
  }

  @Test
  public void testScalar() throws IOException {
    // timezone is GMT+8:00
    baseRecord.put("createTime", 20000L);

    // timezone is GMT
    properties = getBaseKeyConfig("SCALAR", "yyyy-MM-dd hh", "GMT", "days");
    HoodieKey hk5 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);
    assertEquals(hk5.getPartitionPath(), "2024-10-04 12");
  }

  @Test
  public void test_ExpectsMatch_SingleInputFormat_ISO8601WithMsZ_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33.428Z");
    properties = this.getBaseKeyConfig(
      "DATE_STRING",
      "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
      "",
      "",
      "yyyyMMddHH",
      "GMT");
    HoodieKey hk1 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);

    Assertions.assertEquals("2020040113", hk1.getPartitionPath());
  }

  @Test
  public void test_ExpectsMatch_SingleInputFormats_ISO8601WithMsZ_OutputTimezoneAsInputDateTimeZone() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33.428Z");
    properties = this.getBaseKeyConfig(
      "DATE_STRING",
      "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
      "",
      "",
      "yyyyMMddHH",
      "");
    HoodieKey hk1 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);

    Assertions.assertEquals("2020040113", hk1.getPartitionPath());
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsZ_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33.428Z");
    properties = this.getBaseKeyConfig(
      "DATE_STRING",
      "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
      "",
      "",
      "yyyyMMddHH",
      "UTC");
    HoodieKey hk1 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);

    Assertions.assertEquals("2020040113", hk1.getPartitionPath());
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601NoMsZ_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33Z");
    properties = this.getBaseKeyConfig(
      "DATE_STRING",
      "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
      "",
      "",
      "yyyyMMddHH",
      "UTC");
    HoodieKey hk1 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);

    Assertions.assertEquals("2020040113", hk1.getPartitionPath());
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601NoMsWithOffset_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33-05:00");
    properties = this.getBaseKeyConfig(
      "DATE_STRING",
      "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
      "",
      "",
      "yyyyMMddHH",
      "UTC");
    HoodieKey hk1 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);

    Assertions.assertEquals("2020040118", hk1.getPartitionPath());
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsWithOffset_OutputTimezoneAsUTC() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33.123-05:00");
    properties = this.getBaseKeyConfig(
      "DATE_STRING",
      "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
      "",
      "",
      "yyyyMMddHH",
      "UTC");
    HoodieKey hk1 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);

    Assertions.assertEquals("2020040118", hk1.getPartitionPath());
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsZ_OutputTimezoneAsEST() throws IOException {
    baseRecord.put("createTime", "2020-04-01T13:01:33.123Z");
    properties = this.getBaseKeyConfig(
      "DATE_STRING",
      "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
      "",
      "",
      "yyyyMMddHH",
      "EST");
    HoodieKey hk1 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);

    Assertions.assertEquals("2020040109", hk1.getPartitionPath());
  }

  @Test
  public void test_Throws_MultipleInputFormats_InputDateNotMatchingFormats() {
    baseRecord.put("createTime", "2020-04-01 13:01:33.123-05:00");
    properties = this.getBaseKeyConfig(
      "DATE_STRING",
      "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
      "",
      "",
      "yyyyMMddHH",
      "UTC");
    Assertions.assertThrows(HoodieDeltaStreamerException.class, () -> new TimestampBasedKeyGenerator(properties).getKey(baseRecord));
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ShortDate_OutputCustomDate() throws IOException {
    baseRecord.put("createTime", "20200401");
    properties = this.getBaseKeyConfig(
      "DATE_STRING",
      "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ,yyyyMMdd",
      "",
      "UTC",
      "MM/dd/yyyy",
      "UTC");
    HoodieKey hk1 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);

    Assertions.assertEquals("04/01/2020", hk1.getPartitionPath());
  }
}