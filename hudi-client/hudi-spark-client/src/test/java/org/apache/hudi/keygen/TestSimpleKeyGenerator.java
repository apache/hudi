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
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.hudi.keygen.KeyGenUtils.HUDI_DEFAULT_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestSimpleKeyGenerator extends KeyGeneratorTestUtilities {
  private TypedProperties getCommonProps() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    properties.put(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");
    return properties;
  }

  private TypedProperties getPropertiesWithoutPartitionPathProp() {
    return getCommonProps();
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

  private TypedProperties getWrongPartitionPathFieldProps() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "_wrong_partition_path");
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    return properties;
  }

  private TypedProperties getComplexRecordKeyProp() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp");
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key,pii_col");
    return properties;
  }

  private TypedProperties getProps() {
    TypedProperties properties = getCommonProps();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp");
    return properties;
  }

  private TypedProperties getPropsWithNestedPartitionPathField() {
    TypedProperties properties = getCommonProps();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "nested_col.prop1");
    return properties;
  }

  @Test
  void testNullPartitionPathFields() {
    assertThrows(IllegalArgumentException.class, () -> new SimpleKeyGenerator(getPropertiesWithoutPartitionPathProp()));
  }

  @Test
  void testNullRecordKeyFields() {
    GenericRecord avroRecord = getRecord();
    Assertions.assertThrows(IndexOutOfBoundsException.class, () ->  {
      BaseKeyGenerator keyGenerator = new SimpleKeyGenerator(getPropertiesWithoutRecordKeyProp());
      keyGenerator.getRecordKey(avroRecord);
    });
  }

  @Test
  void testWrongRecordKeyField() {
    SimpleKeyGenerator keyGenerator = new SimpleKeyGenerator(getWrongRecordKeyFieldProps());
    assertThrows(HoodieKeyException.class, () -> keyGenerator.getRecordKey(getRecord()));
  }

  @Test
  void testWrongPartitionPathField() {
    SimpleKeyGenerator keyGenerator = new SimpleKeyGenerator(getWrongPartitionPathFieldProps());
    GenericRecord avroRecord = getRecord();
    // TODO this should throw as well
    //assertThrows(HoodieException.class, () -> {
    //  keyGenerator.getPartitionPath(record);
    //});
    assertThrows(HoodieException.class,
        () -> keyGenerator.getPartitionPath(KeyGeneratorTestUtilities.getRow(avroRecord)));
  }

  @Test
  void testComplexRecordKeyField() {
    assertThrows(IllegalArgumentException.class,
        () -> new SimpleKeyGenerator(getComplexRecordKeyProp()));
  }

  @Test
  void testHappyFlow() {
    SimpleKeyGenerator keyGenerator = new SimpleKeyGenerator(getProps());
    GenericRecord avroRecord = getRecord();
    HoodieKey key = keyGenerator.getKey(getRecord());
    Assertions.assertEquals("key1", key.getRecordKey());
    Assertions.assertEquals("timestamp=4357686", key.getPartitionPath());

    Row row = KeyGeneratorTestUtilities.getRow(avroRecord);
    Assertions.assertEquals("key1", keyGenerator.getRecordKey(row));
    Assertions.assertEquals("timestamp=4357686", keyGenerator.getPartitionPath(row));

    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    Assertions.assertEquals(UTF8String.fromString("timestamp=4357686"), keyGenerator.getPartitionPath(internalRow, row.schema()));
  }

  private static Stream<GenericRecord> nestedColTestRecords() {
    return Stream.of(null, getNestedColRecord(null, 10L),
        getNestedColRecord("", 10L), getNestedColRecord("val1", 10L));
  }

  @ParameterizedTest
  @MethodSource("nestedColTestRecords")
  void testNestedPartitionPathField(GenericRecord nestedColRecord) {
    SimpleKeyGenerator keyGenerator = new SimpleKeyGenerator(getPropsWithNestedPartitionPathField());
    GenericRecord avroRecord = getRecord(nestedColRecord);
    String partitionPathFieldValue = null;
    if (nestedColRecord != null) {
      partitionPathFieldValue = (String) nestedColRecord.get("prop1");
    }
    String expectedPartitionPath = "nested_col.prop1="
        + (partitionPathFieldValue != null && !partitionPathFieldValue.isEmpty() ? partitionPathFieldValue : HUDI_DEFAULT_PARTITION_PATH);
    HoodieKey key = keyGenerator.getKey(avroRecord);
    Assertions.assertEquals("key1", key.getRecordKey());
    Assertions.assertEquals(expectedPartitionPath, key.getPartitionPath());

    Row row = KeyGeneratorTestUtilities.getRow(avroRecord);
    Assertions.assertEquals("key1", keyGenerator.getRecordKey(row));
    Assertions.assertEquals(expectedPartitionPath, keyGenerator.getPartitionPath(row));
  }
}
