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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.keygen.TimestampBasedKeyGenerator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestTimestampBasedKeyGenerator {
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
  
  private TypedProperties getBaseKeyConfig(String timestampType, String dateFormat, String timezone) {
    properties.setProperty("hoodie.deltastreamer.keygen.timebased.timestamp.type", timestampType);
    properties.setProperty("hoodie.deltastreamer.keygen.timebased.output.dateformat", dateFormat);
    properties.setProperty("hoodie.deltastreamer.keygen.timebased.timezone", timezone);
    return properties;
  }

  @Test
  public void testTimestampBasedKeyGenerator() {
    // timezone is GMT+8:00
    baseRecord.put("createTime", 1578283932000L);
    properties = getBaseKeyConfig("EPOCHMILLISECONDS", "yyyy-MM-dd hh", "GMT+8:00");
    HoodieKey hk1 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);
    assertEquals(hk1.getPartitionPath(), "2020-01-06 12");

    // timezone is GMT
    properties = getBaseKeyConfig("EPOCHMILLISECONDS", "yyyy-MM-dd hh", "GMT");
    HoodieKey hk2 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);
    assertEquals(hk2.getPartitionPath(), "2020-01-06 04");

    // timestamp is DATE_STRING, timezone is GMT+8:00
    baseRecord.put("createTime", "2020-01-06 12:12:12");
    properties = getBaseKeyConfig("DATE_STRING", "yyyy-MM-dd hh", "GMT+8:00");
    properties.setProperty("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd hh:mm:ss");
    HoodieKey hk3 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);
    assertEquals(hk3.getPartitionPath(), "2020-01-06 12");

    // timezone is GMT
    properties = getBaseKeyConfig("DATE_STRING", "yyyy-MM-dd hh", "GMT");
    HoodieKey hk4 = new TimestampBasedKeyGenerator(properties).getKey(baseRecord);
    assertEquals(hk4.getPartitionPath(), "2020-01-06 12");
  }
}
