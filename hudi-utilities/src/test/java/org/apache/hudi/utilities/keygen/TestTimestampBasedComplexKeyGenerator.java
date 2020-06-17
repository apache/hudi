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

package org.apache.hudi.utilities.keygen;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieKey;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for TimestampBasedComplexKeyGenerator.
 */
public class TestTimestampBasedComplexKeyGenerator extends TestTimestampBasedKeyGenerator {

  @Test
  public void testTimestampBasedComplexKeyGenerator() {
    properties.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "field1,createTime");
    baseRecord.put("field1", "value1");
    baseRecord.put("createTime", 1578283932000L);

    // timezone is GMT+8:00
    properties = getBaseKeyConfig("EPOCHMILLISECONDS", "yyyy-MM-dd HH", "GMT+8:00", null);
    HoodieKey hk1 = new TimestampBasedComplexKeyGenerator(properties).getKey(baseRecord);
    assertEquals("field1:value1,createTime:1578283932000", hk1.getRecordKey());
    assertEquals("2020-01-06 12", hk1.getPartitionPath());

    // timezone is GMT
    properties = getBaseKeyConfig("EPOCHMILLISECONDS", "yyyy-MM-dd HH", "GMT", null);
    HoodieKey hk2 = new TimestampBasedComplexKeyGenerator(properties).getKey(baseRecord);
    assertEquals("field1:value1,createTime:1578283932000", hk2.getRecordKey());
    assertEquals("2020-01-06 04", hk2.getPartitionPath());

    // timestamp is DATE_STRING, timezone is GMT+8:00
    baseRecord.put("createTime", "2020-01-06 12:12:12");
    properties = getBaseKeyConfig("DATE_STRING", "yyyy-MM-dd HH", "GMT+8:00", null);
    properties.setProperty("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd HH:mm:ss");
    HoodieKey hk3 = new TimestampBasedComplexKeyGenerator(properties).getKey(baseRecord);
    assertEquals("field1:value1,createTime:2020-01-06 12:12:12", hk3.getRecordKey());
    assertEquals("2020-01-06 12", hk3.getPartitionPath());

    // timezone is GMT
    properties = getBaseKeyConfig("DATE_STRING", "yyyy-MM-dd HH", "GMT", null);
    HoodieKey hk4 = new TimestampBasedComplexKeyGenerator(properties).getKey(baseRecord);
    assertEquals("field1:value1,createTime:2020-01-06 12:12:12", hk4.getRecordKey());
    assertEquals("2020-01-06 12", hk4.getPartitionPath());

    // timestamp is UNIX_TIMESTAMP, timezone is GMT+8:00
    properties = getBaseKeyConfig("UNIX_TIMESTAMP", "yyyy-MM-dd HH", "GMT+8:00", null);
    baseRecord.put("createTime", 1578283932L);
    HoodieKey hk5 = new TimestampBasedComplexKeyGenerator(properties).getKey(baseRecord);
    assertEquals("field1:value1,createTime:1578283932", hk5.getRecordKey());
    assertEquals("2020-01-06 12", hk5.getPartitionPath());

    // timezone is GMT
    properties = getBaseKeyConfig("UNIX_TIMESTAMP", "yyyy-MM-dd HH", "GMT", null);
    HoodieKey hk6 = new TimestampBasedComplexKeyGenerator(properties).getKey(baseRecord);
    assertEquals("field1:value1,createTime:1578283932", hk6.getRecordKey());
    assertEquals("2020-01-06 04", hk6.getPartitionPath());

    // DateFormat is yyyy/MM/dd
    properties = getBaseKeyConfig("UNIX_TIMESTAMP", "yyyy/MM/dd", "GMT", null);
    HoodieKey hk7 = new TimestampBasedComplexKeyGenerator(properties).getKey(baseRecord);
    assertEquals("field1:value1,createTime:1578283932", hk7.getRecordKey());
    assertEquals("2020/01/06", hk7.getPartitionPath());

    // timestampType is MIXED, TIMESTAMP_INPUT_DATE_FORMAT_PROP is yyyy-MM-dd, createTime is "2020-01-06 12:12:12"
    baseRecord.put("createTime", "2020-01-06 12:12:12");
    properties = getBaseKeyConfig("MIXED", "yyyy-MM-dd", "GMT", null);
    properties.setProperty("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd");
    HoodieKey hk8 = new TimestampBasedComplexKeyGenerator(properties).getKey(baseRecord);
    assertEquals("2020-01-06", hk8.getPartitionPath());

    // timestampType is MIXED, TIMESTAMP_INPUT_DATE_FORMAT_PROP is yyyy-MM-dd, createTime is "2020-01-06 12"
    baseRecord.put("createTime", "2020-01-06 12");
    properties = getBaseKeyConfig("MIXED", "yyyy-MM-dd", "GMT", null);
    HoodieKey hk9 = new TimestampBasedComplexKeyGenerator(properties).getKey(baseRecord);
    assertEquals("2020-01-06", hk9.getPartitionPath());

  }

  @Test
  public void testScalar() {
    // timezone is GMT+8:00
    baseRecord.put("createTime", 20000L);

    // timezone is GMT
    properties = getBaseKeyConfig("SCALAR", "yyyy-MM-dd HH", "GMT", "days");
    HoodieKey hk5 = new TimestampBasedComplexKeyGenerator(properties).getKey(baseRecord);
    assertEquals(hk5.getPartitionPath(), "2024-10-04 00");

  }

}
