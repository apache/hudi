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
import org.apache.hudi.exception.HoodieKeyException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.KeyGeneratorTestUtilities;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSimpleKeyGenerator extends KeyGeneratorTestUtilities {

  private TypedProperties getCommonProps() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key");
    properties.put(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, "true");
    return properties;
  }

  private TypedProperties getPropertiesWithoutPartitionPathProp() {
    return getCommonProps();
  }

  private TypedProperties getPropertiesWithoutRecordKeyProp() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "timestamp");
    return properties;
  }

  private TypedProperties getWrongRecordKeyFieldProps() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "timestamp");
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY, "_wrong_key");
    return properties;
  }

  private TypedProperties getWrongPartitionPathFieldProps() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "_wrong_partition_path");
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key");
    return properties;
  }

  private TypedProperties getComplexRecordKeyProp() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "timestamp");
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key,pii_col");
    return properties;
  }

  private TypedProperties getProps() {
    TypedProperties properties = getCommonProps();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "timestamp");
    return properties;
  }

  @Test
  public void testNullPartitionPathFields() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new SimpleKeyGenerator(getPropertiesWithoutPartitionPathProp()));
  }

  @Test
  public void testNullRecordKeyFields() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new SimpleKeyGenerator(getPropertiesWithoutRecordKeyProp()));
  }

  @Test
  public void testWrongRecordKeyField() {
    SimpleKeyGenerator keyGenerator = new SimpleKeyGenerator(getWrongRecordKeyFieldProps());
    Assertions.assertThrows(HoodieKeyException.class, () -> keyGenerator.getRecordKey(getRecord()));
    Assertions.assertThrows(HoodieKeyException.class, () -> keyGenerator.buildFieldPositionMapIfNeeded(KeyGeneratorTestUtilities.structType));
  }

  @Test
  public void testWrongPartitionPathField() {
    SimpleKeyGenerator keyGenerator = new SimpleKeyGenerator(getWrongPartitionPathFieldProps());
    GenericRecord record = getRecord();
    Assertions.assertEquals(keyGenerator.getPartitionPath(record), KeyGenUtils.DEFAULT_PARTITION_PATH);
    Assertions.assertEquals(keyGenerator.getPartitionPath(KeyGeneratorTestUtilities.getRow(record)),
        KeyGenUtils.DEFAULT_PARTITION_PATH);
  }

  @Test
  public void testComplexRecordKeyField() {
    SimpleKeyGenerator keyGenerator = new SimpleKeyGenerator(getComplexRecordKeyProp());
    Assertions.assertThrows(HoodieKeyException.class, () -> keyGenerator.getRecordKey(getRecord()));
    Assertions.assertThrows(HoodieKeyException.class, () -> keyGenerator.buildFieldPositionMapIfNeeded(KeyGeneratorTestUtilities.structType));
  }

  @Test
  public void testHappyFlow() {
    SimpleKeyGenerator keyGenerator = new SimpleKeyGenerator(getProps());
    GenericRecord record = getRecord();
    HoodieKey key = keyGenerator.getKey(getRecord());
    Assertions.assertEquals(key.getRecordKey(), "key1");
    Assertions.assertEquals(key.getPartitionPath(), "timestamp=4357686");

    Row row = KeyGeneratorTestUtilities.getRow(record);
    Assertions.assertEquals(keyGenerator.getRecordKey(row), "key1");
    Assertions.assertEquals(keyGenerator.getPartitionPath(row), "timestamp=4357686");
  }

}
