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
import org.apache.hudi.utilities.keygen.TimestampBasedKeyGenerator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestTimestampBasedKeyGenerator {
  private Schema schema = SchemaTestUtil.getTimestampEvolvedSchema();
  private GenericRecord baseRecord = null;

  public TestTimestampBasedKeyGenerator() throws IOException {
  }

  @Before
  public void initialize() throws IOException {
    baseRecord = SchemaTestUtil
        .generateAvroRecordFromJson(schema, 1, "001", "f1");
  }

  private TypedProperties getBaseKeyConfig(String recordKeyFieldName, String partitionPathField, String hiveStylePartitioning) {
    TypedProperties props = new TypedProperties();
    props.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), recordKeyFieldName);
    props.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), partitionPathField);
    props.setProperty(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(), hiveStylePartitioning);
    props.setProperty("hoodie.deltastreamer.keygen.timebased.timestamp.type", "EPOCHMILLISECONDS");
    props.setProperty("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyy-MM-dd hh");
    props.setProperty("hoodie.deltastreamer.keygen.timebased.timezone", "GMT+8:00");
    return props;
  }

  @Test
  public void testTimestampBasedKeyGenerator() {
    // if timezone is GMT+8:00
    baseRecord.put("createTime", 1578283932000L);
    TypedProperties props = getBaseKeyConfig("field1", "createTime", "false");
    HoodieKey hk1 = new TimestampBasedKeyGenerator(props).getKey(baseRecord);
    assertEquals(hk1.getPartitionPath(), "2020-01-06 12");

    // if timezone is GMT
    props.setProperty("hoodie.deltastreamer.keygen.timebased.timezone", "GMT");
    HoodieKey hk2 = new TimestampBasedKeyGenerator(props).getKey(baseRecord);
    assertEquals(hk2.getPartitionPath(), "2020-01-06 04");

    // if timestamp is DATE_STRING, and timestamp type is DATE_STRING
    baseRecord.put("createTime", "2020-01-06 12:12:12");
    props.setProperty("hoodie.deltastreamer.keygen.timebased.timestamp.type", "DATE_STRING");
    props.setProperty("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd hh:mm:ss");
    props.setProperty("hoodie.deltastreamer.keygen.timebased.timezone", "GMT+8:00");
    HoodieKey hk3 = new TimestampBasedKeyGenerator(props).getKey(baseRecord);
    assertEquals(hk3.getPartitionPath(), "2020-01-06 12");
  }
}
