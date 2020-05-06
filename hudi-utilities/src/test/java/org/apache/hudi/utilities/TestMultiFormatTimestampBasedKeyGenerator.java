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

package org.apache.hudi.utilities;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.SchemaTestUtil;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.utilities.keygen.MultiFormatTimestampBasedKeyGenerator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestMultiFormatTimestampBasedKeyGenerator {
  private GenericRecord baseRecord;
  private TypedProperties properties = new TypedProperties();

  @Before
  public void initialize() throws IOException {
    Schema schema = SchemaTestUtil.getTimestampEvolvedSchema();
    baseRecord = SchemaTestUtil
        .generateAvroRecordFromJson(schema, 1, "001", "f1");

    properties.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "field1");
    properties.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "createTime");
    properties.setProperty(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(), "false");
  }
  
  private TypedProperties getBaseKeyConfig(String timestampType, String inputFormatList, String inputFormatDelimiterRegex, String inputTimezone, String outputFormat, String outputTimezone) {
    if (timestampType != null) {
      properties.setProperty("hoodie.deltastreamer.keygen.timebased.timestamp.type", timestampType);
    }
    if (inputFormatList != null) {
      properties.setProperty("hoodie.deltastreamer.keygen.timebased.input.dateformatlist", inputFormatList);
    }
    if (inputFormatDelimiterRegex != null) {
      properties.setProperty("hoodie.deltastreamer.keygen.timebased.input.dateformatlistdelimiterregex", inputFormatDelimiterRegex);
    }
    if (inputTimezone != null) {
      properties.setProperty("hoodie.deltastreamer.keygen.timebased.input.timezone", inputTimezone);
    }
    if (outputFormat != null) {
      properties.setProperty("hoodie.deltastreamer.keygen.timebased.output.dateformat", outputFormat);
    }
    if (outputTimezone != null) {
      properties.setProperty("hoodie.deltastreamer.keygen.timebased.output.timezone", outputTimezone);
    }

    return properties;
  }

  @Test
  public void test_ExpectsMatch_SingleInputFormat_ISO8601WithMsZ_OutputTimezoneAsUTC() {
    baseRecord.put("createTime", "2020-04-01T13:01:33.428Z");
    properties = this.getBaseKeyConfig(
            "DATE_STRING",
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "",
            "",
            "yyyyMMddHH",
            "UTC");
    HoodieKey hk1 = new MultiFormatTimestampBasedKeyGenerator(properties).getKey(baseRecord);

    assertEquals("2020040113", hk1.getPartitionPath());
  }

  @Test
  public void test_ExpectsMatch_SingleInputFormats_ISO8601WithMsZ_OutputTimezoneAsInputDateTimeZone() {
    baseRecord.put("createTime", "2020-04-01T13:01:33.428Z");
    properties = this.getBaseKeyConfig(
            "DATE_STRING",
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "",
            "",
            "yyyyMMddHH",
            "");
    HoodieKey hk1 = new MultiFormatTimestampBasedKeyGenerator(properties).getKey(baseRecord);

    assertEquals("2020040113", hk1.getPartitionPath());
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsZ_OutputTimezoneAsUTC() {
    baseRecord.put("createTime", "2020-04-01T13:01:33.428Z");
    properties = this.getBaseKeyConfig(
            "DATE_STRING",
            "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "",
            "",
            "yyyyMMddHH",
            "UTC");
    HoodieKey hk1 = new MultiFormatTimestampBasedKeyGenerator(properties).getKey(baseRecord);

    assertEquals("2020040113", hk1.getPartitionPath());
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601NoMsZ_OutputTimezoneAsUTC() {
    baseRecord.put("createTime", "2020-04-01T13:01:33Z");
    properties = this.getBaseKeyConfig(
            "DATE_STRING",
            "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "",
            "",
            "yyyyMMddHH",
            "UTC");
    HoodieKey hk1 = new MultiFormatTimestampBasedKeyGenerator(properties).getKey(baseRecord);

    assertEquals("2020040113", hk1.getPartitionPath());
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601NoMsWithOffset_OutputTimezoneAsUTC() {
    baseRecord.put("createTime", "2020-04-01T13:01:33-05:00");
    properties = this.getBaseKeyConfig(
            "DATE_STRING",
            "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "",
            "",
            "yyyyMMddHH",
            "UTC");
    HoodieKey hk1 = new MultiFormatTimestampBasedKeyGenerator(properties).getKey(baseRecord);

    assertEquals("2020040118", hk1.getPartitionPath());
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsWithOffset_OutputTimezoneAsUTC() {
    baseRecord.put("createTime", "2020-04-01T13:01:33.123-05:00");
    properties = this.getBaseKeyConfig(
            "DATE_STRING",
            "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "",
            "",
            "yyyyMMddHH",
            "UTC");
    HoodieKey hk1 = new MultiFormatTimestampBasedKeyGenerator(properties).getKey(baseRecord);

    assertEquals("2020040118", hk1.getPartitionPath());
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ISO8601WithMsZ_OutputTimezoneAsEST() {
    baseRecord.put("createTime", "2020-04-01T13:01:33.123Z");
    properties = this.getBaseKeyConfig(
            "DATE_STRING",
            "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "",
            "",
            "yyyyMMddHH",
            "EST");
    HoodieKey hk1 = new MultiFormatTimestampBasedKeyGenerator(properties).getKey(baseRecord);

    assertEquals("2020040109", hk1.getPartitionPath());
  }

  @Test(expected = Exception.class)
  public void test_Throws_MultipleInputFormats_InputDateNotMatchingFormats() {
    baseRecord.put("createTime", "2020-04-01 13:01:33.123-05:00");
    properties = this.getBaseKeyConfig(
            "DATE_STRING",
            "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "",
            "",
            "yyyyMMddHH",
            "UTC");
    HoodieKey hk1 = new MultiFormatTimestampBasedKeyGenerator(properties).getKey(baseRecord);
  }

  @Test
  public void test_ExpectsMatch_MultipleInputFormats_ShortDate_OutputCustomDate() {
    baseRecord.put("createTime", "20200401");
    properties = this.getBaseKeyConfig(
            "DATE_STRING",
            "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ,yyyyMMdd",
            "",
            "",
            "MM/dd/yyyy",
            "UTC");
    HoodieKey hk1 = new MultiFormatTimestampBasedKeyGenerator(properties).getKey(baseRecord);

    assertEquals("04/01/2020", hk1.getPartitionPath());
  }
}
