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

import static junit.framework.TestCase.assertEquals;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.junit.jupiter.api.Test;

public class TestComplexKeyGenerator {

  @Test
  public void testSingleValueKeyGenerator() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key");
    properties.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "timestamp");
    ComplexKeyGenerator compositeKeyGenerator = new ComplexKeyGenerator(properties);
    assertEquals(compositeKeyGenerator.getRecordKeyFields().size(), 1);
    assertEquals(compositeKeyGenerator.getPartitionPathFields().size(), 1);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    GenericRecord record = dataGenerator.generateGenericRecords(1).get(0);
    String rowKey = record.get("_row_key").toString();
    String partitionPath = record.get("timestamp").toString();
    HoodieKey hoodieKey = compositeKeyGenerator.getKey(record);
    assertEquals("_row_key:" + rowKey, hoodieKey.getRecordKey());
    assertEquals(partitionPath, hoodieKey.getPartitionPath());
  }

  @Test
  public void testMultipleValueKeyGenerator() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key,timestamp");
    properties.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "rider,driver");
    ComplexKeyGenerator compositeKeyGenerator = new ComplexKeyGenerator(properties);
    assertEquals(compositeKeyGenerator.getRecordKeyFields().size(), 2);
    assertEquals(compositeKeyGenerator.getPartitionPathFields().size(), 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    GenericRecord record = dataGenerator.generateGenericRecords(1).get(0);
    String rowKey =
        "_row_key" + ComplexKeyGenerator.DEFAULT_RECORD_KEY_SEPARATOR + record.get("_row_key").toString() + ","
            + "timestamp" + ComplexKeyGenerator.DEFAULT_RECORD_KEY_SEPARATOR + record.get("timestamp").toString();
    String partitionPath = record.get("rider").toString() + "/" + record.get("driver").toString();
    HoodieKey hoodieKey = compositeKeyGenerator.getKey(record);
    assertEquals(rowKey, hoodieKey.getRecordKey());
    assertEquals(partitionPath, hoodieKey.getPartitionPath());
  }

}