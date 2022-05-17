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

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.KeyGeneratorTestUtilities;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static junit.framework.TestCase.assertEquals;

public class TestComplexKeyGenerator extends KeyGeneratorTestUtilities {

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

  private TypedProperties getPropertiesWithoutRecordKeyProp() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp");
    return properties;
  }

  private TypedProperties getWrongRecordKeyFieldProps() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp");
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_wrong_key");
    return properties;
  }

  private TypedProperties getProps() {
    TypedProperties properties = getCommonProps(true);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp,ts_ms");
    return properties;
  }

  @Test
  public void testNullPartitionPathFields() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new ComplexKeyGenerator(getPropertiesWithoutPartitionPathProp()));
  }

  @Test
  public void testNullRecordKeyFields() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new ComplexKeyGenerator(getPropertiesWithoutRecordKeyProp()));
  }

  @Test
  public void testWrongRecordKeyField() {
    ComplexKeyGenerator keyGenerator = new ComplexKeyGenerator(getWrongRecordKeyFieldProps());
    Assertions.assertThrows(HoodieKeyException.class, () -> keyGenerator.getRecordKey(getRecord()));
    Assertions.assertThrows(HoodieKeyException.class, () -> keyGenerator.buildFieldSchemaInfoIfNeeded(KeyGeneratorTestUtilities.structType));
  }

  @Test
  public void testHappyFlow() {
    ComplexKeyGenerator keyGenerator = new ComplexKeyGenerator(getProps());
    GenericRecord record = getRecord();
    HoodieKey key = keyGenerator.getKey(record);
    Assertions.assertEquals(key.getRecordKey(), "_row_key:key1,pii_col:pi");
    Assertions.assertEquals(key.getPartitionPath(), "timestamp=4357686/ts_ms=2020-03-21");
    Row row = KeyGeneratorTestUtilities.getRow(record);
    Assertions.assertEquals(keyGenerator.getRecordKey(row), "_row_key:key1,pii_col:pi");
    Assertions.assertEquals(keyGenerator.getPartitionPath(row), "timestamp=4357686/ts_ms=2020-03-21");

    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    Assertions.assertEquals(keyGenerator.getPartitionPath(internalRow, row.schema()), "timestamp=4357686/ts_ms=2020-03-21");
  }

  @Test
  public void testSingleValueKeyGenerator() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp");
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

    Row row = KeyGeneratorTestUtilities.getRow(record, HoodieTestDataGenerator.AVRO_SCHEMA,
        AvroConversionUtils.convertAvroSchemaToStructType(HoodieTestDataGenerator.AVRO_SCHEMA));
    Assertions.assertEquals(compositeKeyGenerator.getPartitionPath(row), partitionPath);
    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    Assertions.assertEquals(compositeKeyGenerator.getPartitionPath(internalRow, row.schema()), partitionPath);
  }

  @Test
  public void testMultipleValueKeyGenerator() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key,timestamp");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "rider,driver");
    ComplexKeyGenerator compositeKeyGenerator = new ComplexKeyGenerator(properties);
    assertEquals(compositeKeyGenerator.getRecordKeyFields().size(), 2);
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

    Row row = KeyGeneratorTestUtilities.getRow(record, HoodieTestDataGenerator.AVRO_SCHEMA,
        AvroConversionUtils.convertAvroSchemaToStructType(HoodieTestDataGenerator.AVRO_SCHEMA));
    Assertions.assertEquals(compositeKeyGenerator.getPartitionPath(row), partitionPath);

    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    Assertions.assertEquals(compositeKeyGenerator.getPartitionPath(internalRow, row.schema()), partitionPath);
  }

  @Test
  public void testMultipleValueKeyGeneratorNonPartitioned() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key,timestamp");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "");
    ComplexKeyGenerator compositeKeyGenerator = new ComplexKeyGenerator(properties);
    assertEquals(compositeKeyGenerator.getRecordKeyFields().size(), 2);
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

    Row row = KeyGeneratorTestUtilities.getRow(record, HoodieTestDataGenerator.AVRO_SCHEMA,
        AvroConversionUtils.convertAvroSchemaToStructType(HoodieTestDataGenerator.AVRO_SCHEMA));
    Assertions.assertEquals(compositeKeyGenerator.getPartitionPath(row), partitionPath);

    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    Assertions.assertEquals(compositeKeyGenerator.getPartitionPath(internalRow, row.schema()), partitionPath);
  }
}
