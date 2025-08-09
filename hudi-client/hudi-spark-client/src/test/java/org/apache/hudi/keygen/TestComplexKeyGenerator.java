/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.keygen;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
  void testNullPartitionPathFields() {
    assertThrows(IllegalArgumentException.class, () -> new ComplexKeyGenerator(getPropertiesWithoutPartitionPathProp()));
  }

  @Test
  void testNullRecordKeyFields() {
    GenericRecord avroRecord = getRecord();
    assertThrows(HoodieKeyException.class, () -> {
      ComplexKeyGenerator keyGenerator = new ComplexKeyGenerator(getPropertiesWithoutRecordKeyProp());
      keyGenerator.getRecordKey(avroRecord);
    });
  }

  @Test
  void testWrongRecordKeyField() {
    ComplexKeyGenerator keyGenerator = new ComplexKeyGenerator(getWrongRecordKeyFieldProps());
    assertThrows(HoodieKeyException.class, () -> keyGenerator.getRecordKey(getRecord()));
  }

  @Test
  void testHappyFlow() {
    ComplexKeyGenerator keyGenerator = new ComplexKeyGenerator(getProps());
    GenericRecord avroRecord = getRecord();
    HoodieKey key = keyGenerator.getKey(avroRecord);
    assertEquals("_row_key:key1,pii_col:pi", key.getRecordKey());
    assertEquals("timestamp=4357686/ts_ms=2020-03-21", key.getPartitionPath());
    Row row = KeyGeneratorTestUtilities.getRow(avroRecord);
    assertEquals("_row_key:key1,pii_col:pi", keyGenerator.getRecordKey(row));
    assertEquals("timestamp=4357686/ts_ms=2020-03-21", keyGenerator.getPartitionPath(row));

    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    assertEquals(UTF8String.fromString("timestamp=4357686/ts_ms=2020-03-21"), keyGenerator.getPartitionPath(internalRow, row.schema()));
  }

  @ParameterizedTest
  @CsvSource(value = {"false,true", "true,false", "true,true"})
  void testSingleValueKeyGenerator(boolean setEncodeSingleKeyFieldNameConfig,
                                   boolean encodeSingleKeyFieldName) {
    String recordKeyFieldName = "_row_key";
    TypedProperties properties = new TypedProperties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), recordKeyFieldName);
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp");
    if (setEncodeSingleKeyFieldNameConfig) {
      properties.setProperty(
          HoodieWriteConfig.COMPLEX_KEYGEN_ENCODE_SINGLE_RECORD_KEY_FIELD_NAME.key(),
          String.valueOf(encodeSingleKeyFieldName));
    }
    ComplexKeyGenerator compositeKeyGenerator = new ComplexKeyGenerator(properties);
    assertEquals(1, compositeKeyGenerator.getRecordKeyFieldNames().size());
    assertEquals(1, compositeKeyGenerator.getPartitionPathFields().size());
    try (HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(System.currentTimeMillis())) {
      GenericRecord avroRecord = dataGenerator.generateGenericRecords(1).get(0);
      String rowKey = avroRecord.get(recordKeyFieldName).toString();
      String partitionPath = avroRecord.get("timestamp").toString();
      HoodieKey hoodieKey = compositeKeyGenerator.getKey(avroRecord);
      String expectedRecordKey = !setEncodeSingleKeyFieldNameConfig || encodeSingleKeyFieldName
          ? recordKeyFieldName + ":" + rowKey : rowKey;
      assertEquals(expectedRecordKey, hoodieKey.getRecordKey());
      assertEquals(partitionPath, hoodieKey.getPartitionPath());

      Row row = KeyGeneratorTestUtilities.getRow(avroRecord, TRIP_STRUCT_TYPE);
      assertEquals(partitionPath, compositeKeyGenerator.getPartitionPath(row));
      InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
      assertEquals(UTF8String.fromString(expectedRecordKey), compositeKeyGenerator.getRecordKey(internalRow, TRIP_STRUCT_TYPE));
      assertEquals(UTF8String.fromString(partitionPath), compositeKeyGenerator.getPartitionPath(internalRow, row.schema()));
    }
  }

  @ParameterizedTest
  @CsvSource(value = {"false,true", "true,false", "true,true"})
  void testMultipleValueKeyGenerator(boolean setEncodeSingleKeyFieldNameConfig,
                                     boolean encodeSingleKeyFieldName) {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key,timestamp");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "rider,driver");
    if (setEncodeSingleKeyFieldNameConfig) {
      properties.setProperty(
          HoodieWriteConfig.COMPLEX_KEYGEN_ENCODE_SINGLE_RECORD_KEY_FIELD_NAME.key(),
          String.valueOf(encodeSingleKeyFieldName));
    }
    ComplexKeyGenerator compositeKeyGenerator = new ComplexKeyGenerator(properties);
    assertEquals(2, compositeKeyGenerator.getRecordKeyFieldNames().size());
    assertEquals(2, compositeKeyGenerator.getPartitionPathFields().size());
    try (HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(System.currentTimeMillis())) {
      GenericRecord avroRecord = dataGenerator.generateGenericRecords(1).get(0);
      String rowKey =
          "_row_key" + ComplexAvroKeyGenerator.DEFAULT_RECORD_KEY_SEPARATOR + avroRecord.get("_row_key").toString() + ","
              + "timestamp" + ComplexAvroKeyGenerator.DEFAULT_RECORD_KEY_SEPARATOR + avroRecord.get("timestamp").toString();
      String partitionPath = avroRecord.get("rider").toString() + "/" + avroRecord.get("driver").toString();
      HoodieKey hoodieKey = compositeKeyGenerator.getKey(avroRecord);
      assertEquals(rowKey, hoodieKey.getRecordKey());
      assertEquals(partitionPath, hoodieKey.getPartitionPath());

      Row row = KeyGeneratorTestUtilities.getRow(avroRecord, TRIP_STRUCT_TYPE);
      assertEquals(partitionPath, compositeKeyGenerator.getPartitionPath(row));

      InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
      assertEquals(UTF8String.fromString(rowKey), compositeKeyGenerator.getRecordKey(internalRow, TRIP_STRUCT_TYPE));
      assertEquals(UTF8String.fromString(partitionPath), compositeKeyGenerator.getPartitionPath(internalRow, row.schema()));
    }
  }

  @ParameterizedTest
  @CsvSource(value = {"false,true", "true,false", "true,true"})
  void testMultipleValueKeyGeneratorNonPartitioned(boolean setEncodeSingleKeyFieldNameConfig,
                                                   boolean encodeSingleKeyFieldName) {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key,timestamp");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "");
    if (setEncodeSingleKeyFieldNameConfig) {
      properties.setProperty(
          HoodieWriteConfig.COMPLEX_KEYGEN_ENCODE_SINGLE_RECORD_KEY_FIELD_NAME.key(),
          String.valueOf(encodeSingleKeyFieldName));
    }
    ComplexKeyGenerator compositeKeyGenerator = new ComplexKeyGenerator(properties);
    assertEquals(2, compositeKeyGenerator.getRecordKeyFieldNames().size());
    assertEquals(0, compositeKeyGenerator.getPartitionPathFields().size());
    try (HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(System.currentTimeMillis())) {
      GenericRecord avroRecord = dataGenerator.generateGenericRecords(1).get(0);
      String rowKey =
          "_row_key" + ComplexAvroKeyGenerator.DEFAULT_RECORD_KEY_SEPARATOR + avroRecord.get("_row_key").toString() + ","
              + "timestamp" + ComplexAvroKeyGenerator.DEFAULT_RECORD_KEY_SEPARATOR + avroRecord.get("timestamp").toString();
      String partitionPath = "";
      HoodieKey hoodieKey = compositeKeyGenerator.getKey(avroRecord);
      assertEquals(rowKey, hoodieKey.getRecordKey());
      assertEquals(partitionPath, hoodieKey.getPartitionPath());

      Row row = KeyGeneratorTestUtilities.getRow(avroRecord, TRIP_STRUCT_TYPE);
      assertEquals(partitionPath, compositeKeyGenerator.getPartitionPath(row));

      InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
      assertEquals(UTF8String.fromString(rowKey), compositeKeyGenerator.getRecordKey(internalRow, TRIP_STRUCT_TYPE));
      assertEquals(UTF8String.fromString(partitionPath), compositeKeyGenerator.getPartitionPath(internalRow, row.schema()));
    }
  }
}
