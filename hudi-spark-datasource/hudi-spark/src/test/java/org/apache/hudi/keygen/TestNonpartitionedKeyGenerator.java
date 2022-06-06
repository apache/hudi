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
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.KeyGeneratorTestUtilities;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestNonpartitionedKeyGenerator extends KeyGeneratorTestUtilities {

  private TypedProperties getCommonProps(boolean getComplexRecordKey) {
    TypedProperties properties = new TypedProperties();
    if (getComplexRecordKey) {
      properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key, pii_col");
    } else {
      properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    }
    properties.put(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");
    return properties;
  }

  private TypedProperties getPropertiesWithoutPartitionPathProp() {
    return getCommonProps(false);
  }

  private TypedProperties getPropertiesWithPartitionPathProp() {
    TypedProperties properties = getCommonProps(true);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp,ts_ms");
    return properties;
  }

  private TypedProperties getPropertiesWithoutRecordKeyProp() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp");
    return properties;
  }

  private TypedProperties getWrongRecordKeyFieldProps() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_wrong_key");
    return properties;
  }

  @Test
  public void testNullRecordKeyFields() {
    NonpartitionedKeyGenerator keyGenerator = new NonpartitionedKeyGenerator(getPropertiesWithoutRecordKeyProp());
    GenericRecord record = getRecord();
    assertEquals(EMPTY_STRING, keyGenerator.getKey(record).getRecordKey());
  }

  @Test
  public void testNonNullPartitionPathFields() {
    TypedProperties properties = getPropertiesWithPartitionPathProp();
    NonpartitionedKeyGenerator keyGenerator = new NonpartitionedKeyGenerator(properties);
    GenericRecord record = getRecord();
    Row row = KeyGeneratorTestUtilities.getRow(record);
    assertEquals(properties.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()), "timestamp,ts_ms");
    assertEquals(EMPTY_STRING, keyGenerator.getPartitionPath(row));
  }

  @Test
  public void testNullPartitionPathFields() {
    TypedProperties properties = getPropertiesWithoutPartitionPathProp();
    NonpartitionedKeyGenerator keyGenerator = new NonpartitionedKeyGenerator(properties);
    GenericRecord record = getRecord();
    Row row = KeyGeneratorTestUtilities.getRow(record);
    assertEquals(EMPTY_STRING, keyGenerator.getPartitionPath(row));
  }

  @Test
  public void testWrongRecordKeyField() {
    NonpartitionedKeyGenerator keyGenerator = new NonpartitionedKeyGenerator(getWrongRecordKeyFieldProps());
    assertThrows(HoodieKeyException.class, () -> keyGenerator.getRecordKey(getRecord()));
    assertThrows(HoodieKeyException.class, () -> keyGenerator.buildFieldSchemaInfoIfNeeded(KeyGeneratorTestUtilities.structType));
  }

  @Test
  public void testSingleValueKeyGeneratorNonPartitioned() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "timestamp");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "");
    NonpartitionedKeyGenerator keyGenerator = new NonpartitionedKeyGenerator(properties);
    assertEquals(keyGenerator.getRecordKeyFields().size(), 1);
    assertEquals(keyGenerator.getPartitionPathFields().size(), 0);

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    GenericRecord record = dataGenerator.generateGenericRecords(1).get(0);
    String rowKey = record.get("timestamp").toString();
    HoodieKey hoodieKey = keyGenerator.getKey(record);
    assertEquals(rowKey, hoodieKey.getRecordKey());
    assertEquals("", hoodieKey.getPartitionPath());
  }

  @Test
  public void testMultipleValueKeyGeneratorNonPartitioned1() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "timestamp,driver");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "");
    NonpartitionedKeyGenerator keyGenerator = new NonpartitionedKeyGenerator(properties);
    assertEquals(keyGenerator.getRecordKeyFields().size(), 2);
    assertEquals(keyGenerator.getPartitionPathFields().size(), 0);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    GenericRecord record = dataGenerator.generateGenericRecords(1).get(0);
    String rowKey =
        "timestamp" + ComplexAvroKeyGenerator.DEFAULT_RECORD_KEY_SEPARATOR + record.get("timestamp").toString() + ","
            + "driver" + ComplexAvroKeyGenerator.DEFAULT_RECORD_KEY_SEPARATOR + record.get("driver").toString();
    String partitionPath = "";
    HoodieKey hoodieKey = keyGenerator.getKey(record);
    assertEquals(rowKey, hoodieKey.getRecordKey());
    assertEquals(partitionPath, hoodieKey.getPartitionPath());
  }
}
