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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static junit.framework.TestCase.assertEquals;

public class TestComplexAvroKeyGenerator {

  @Test
  public void testSingleValueKeyGenerator() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp");
    ComplexAvroKeyGenerator compositeKeyGenerator = new ComplexAvroKeyGenerator(properties);
    assertEquals(compositeKeyGenerator.getRecordKeyFieldNames().size(), 1);
    assertEquals(compositeKeyGenerator.getPartitionPathFields().size(), 1);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    GenericRecord record = dataGenerator.generateGenericRecords(1).get(0);
    String rowKey = record.get("_row_key").toString();
    String partitionPath = record.get("timestamp").toString();
    HoodieKey hoodieKey = compositeKeyGenerator.getKey(record);
    assertEquals(rowKey, hoodieKey.getRecordKey());
    assertEquals(partitionPath, hoodieKey.getPartitionPath());
  }

  @Test
  public void testMultipleValueKeyGenerator() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key,timestamp");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "rider,driver");
    ComplexAvroKeyGenerator compositeKeyGenerator = new ComplexAvroKeyGenerator(properties);
    assertEquals(compositeKeyGenerator.getRecordKeyFieldNames().size(), 2);
    assertEquals(compositeKeyGenerator.getPartitionPathFields().size(), 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    GenericRecord record = dataGenerator.generateGenericRecords(1).get(0);
    String rowKey =
        "_row_key" + ComplexAvroKeyGenerator.DEFAULT_RECORD_KEY_SEPARATOR + record.get("_row_key").toString() + ","
            + "timestamp" + ComplexAvroKeyGenerator.DEFAULT_RECORD_KEY_SEPARATOR + record.get("timestamp").toString();
    String partitionPath = record.get("rider").toString() + "/" + record.get("driver").toString();
    HoodieKey hoodieKey = compositeKeyGenerator.getKey(record);
    assertEquals(rowKey, hoodieKey.getRecordKey());
    assertEquals(partitionPath, hoodieKey.getPartitionPath());
  }

  @Test
  public void testMultipleValueKeyGeneratorNonPartitioned() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key,timestamp");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "");
    ComplexAvroKeyGenerator compositeKeyGenerator = new ComplexAvroKeyGenerator(properties);
    assertEquals(compositeKeyGenerator.getRecordKeyFieldNames().size(), 2);
    assertEquals(compositeKeyGenerator.getPartitionPathFields().size(), 0);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    GenericRecord record = dataGenerator.generateGenericRecords(1).get(0);
    String rowKey =
        "_row_key" + ComplexAvroKeyGenerator.DEFAULT_RECORD_KEY_SEPARATOR + record.get("_row_key").toString() + ","
            + "timestamp" + ComplexAvroKeyGenerator.DEFAULT_RECORD_KEY_SEPARATOR + record.get("timestamp").toString();
    String partitionPath = "";
    HoodieKey hoodieKey = compositeKeyGenerator.getKey(record);
    assertEquals(rowKey, hoodieKey.getRecordKey());
    assertEquals(partitionPath, hoodieKey.getPartitionPath());
  }
}

