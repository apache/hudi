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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.config.TypedProperties;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.KeyGeneratorTestUtilities;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCustomKeyGenerator extends KeyGeneratorTestUtilities {

  private TypedProperties getCommonProps(boolean getComplexRecordKey) {
    TypedProperties properties = new TypedProperties();
    if (getComplexRecordKey) {
      properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key, pii_col");
    } else {
      properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key");
    }
    properties.put(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, "true");
    return properties;
  }

  private TypedProperties getPropertiesForSimpleKeyGen() {
    TypedProperties properties = getCommonProps(false);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "timestamp:simple");
    return properties;
  }

  private TypedProperties getImproperPartitionFieldFormatProp() {
    TypedProperties properties = getCommonProps(false);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "timestamp");
    return properties;
  }

  private TypedProperties getInvalidPartitionKeyTypeProps() {
    TypedProperties properties = getCommonProps(false);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "timestamp:dummy");
    return properties;
  }

  private TypedProperties getComplexRecordKeyWithSimplePartitionProps() {
    TypedProperties properties = getCommonProps(true);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "timestamp:simple");
    return properties;
  }

  private TypedProperties getComplexRecordKeyAndPartitionPathProps() {
    TypedProperties properties = getCommonProps(true);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "timestamp:simple,ts_ms:timestamp");
    populateNecessaryPropsForTimestampBasedKeyGen(properties);
    return properties;
  }

  private TypedProperties getPropsWithoutRecordKeyFieldProps() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "timestamp:simple");
    return properties;
  }

  private void populateNecessaryPropsForTimestampBasedKeyGen(TypedProperties properties) {
    properties.put("hoodie.deltastreamer.keygen.timebased.timestamp.type", "DATE_STRING");
    properties.put("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd");
    properties.put("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyyMMdd");
  }

  private TypedProperties getPropertiesForTimestampBasedKeyGen() {
    TypedProperties properties = getCommonProps(false);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "ts_ms:timestamp");
    populateNecessaryPropsForTimestampBasedKeyGen(properties);
    return properties;
  }

  private TypedProperties getPropertiesForNonPartitionedKeyGen() {
    TypedProperties properties = getCommonProps(false);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "");
    return properties;
  }

  @Test
  public void testSimpleKeyGenerator() {
    BuiltinKeyGenerator keyGenerator = new CustomKeyGenerator(getPropertiesForSimpleKeyGen());
    GenericRecord record = getRecord();
    HoodieKey key = keyGenerator.getKey(record);
    Assertions.assertEquals(key.getRecordKey(), "key1");
    Assertions.assertEquals(key.getPartitionPath(), "timestamp=4357686");
    Row row = KeyGeneratorTestUtilities.getRow(record);
    Assertions.assertEquals(keyGenerator.getRecordKey(row), "key1");
    Assertions.assertEquals(keyGenerator.getPartitionPath(row), "timestamp=4357686");
  }

  @Test
  public void testTimestampBasedKeyGenerator() {
    BuiltinKeyGenerator keyGenerator = new CustomKeyGenerator(getPropertiesForTimestampBasedKeyGen());
    GenericRecord record = getRecord();
    HoodieKey key = keyGenerator.getKey(record);
    Assertions.assertEquals(key.getRecordKey(), "key1");
    Assertions.assertEquals(key.getPartitionPath(), "ts_ms=20200321");
    Row row = KeyGeneratorTestUtilities.getRow(record);
    Assertions.assertEquals(keyGenerator.getRecordKey(row), "key1");
    Assertions.assertEquals(keyGenerator.getPartitionPath(row), "ts_ms=20200321");
  }

  @Test
  public void testNonPartitionedKeyGenerator() {
    BuiltinKeyGenerator keyGenerator = new CustomKeyGenerator(getPropertiesForNonPartitionedKeyGen());
    GenericRecord record = getRecord();
    HoodieKey key = keyGenerator.getKey(record);
    Assertions.assertEquals(key.getRecordKey(), "key1");
    Assertions.assertTrue(key.getPartitionPath().isEmpty());
    Row row = KeyGeneratorTestUtilities.getRow(record);
    Assertions.assertEquals(keyGenerator.getRecordKey(row), "key1");
    Assertions.assertTrue(keyGenerator.getPartitionPath(row).isEmpty());
  }

  @Test
  public void testInvalidPartitionKeyType() {
    try {
      BuiltinKeyGenerator keyGenerator = new CustomKeyGenerator(getInvalidPartitionKeyTypeProps());
      keyGenerator.getKey(getRecord());
      Assertions.fail("should fail when invalid PartitionKeyType is provided!");
    } catch (Exception e) {
      Assertions.assertTrue(e.getMessage().contains("No enum constant org.apache.hudi.keygen.CustomAvroKeyGenerator.PartitionKeyType.DUMMY"));
    }

    try {
      BuiltinKeyGenerator keyGenerator = new CustomKeyGenerator(getInvalidPartitionKeyTypeProps());
      GenericRecord record = getRecord();
      Row row = KeyGeneratorTestUtilities.getRow(record);
      keyGenerator.getPartitionPath(row);
      Assertions.fail("should fail when invalid PartitionKeyType is provided!");
    } catch (Exception e) {
      Assertions.assertTrue(e.getMessage().contains("No enum constant org.apache.hudi.keygen.CustomAvroKeyGenerator.PartitionKeyType.DUMMY"));
    }
  }

  @Test
  public void testNoRecordKeyFieldProp() {
    try {
      BuiltinKeyGenerator keyGenerator = new CustomKeyGenerator(getPropsWithoutRecordKeyFieldProps());
      keyGenerator.getKey(getRecord());
      Assertions.fail("should fail when record key field is not provided!");
    } catch (Exception e) {
      Assertions.assertTrue(e.getMessage().contains("Property hoodie.datasource.write.recordkey.field not found"));
    }

    try {
      BuiltinKeyGenerator keyGenerator = new CustomKeyGenerator(getPropsWithoutRecordKeyFieldProps());
      GenericRecord record = getRecord();
      Row row = KeyGeneratorTestUtilities.getRow(record);
      keyGenerator.getRecordKey(row);
      Assertions.fail("should fail when record key field is not provided!");
    } catch (Exception e) {
      Assertions.assertTrue(e.getMessage().contains("Property hoodie.datasource.write.recordkey.field not found"));
    }
  }

  @Test
  public void testPartitionFieldsInImproperFormat() {
    try {
      BuiltinKeyGenerator keyGenerator = new CustomKeyGenerator(getImproperPartitionFieldFormatProp());
      keyGenerator.getKey(getRecord());
      Assertions.fail("should fail when partition key field is provided in improper format!");
    } catch (Exception e) {
      Assertions.assertTrue(e.getMessage().contains("Unable to find field names for partition path in proper format"));
    }

    try {
      BuiltinKeyGenerator keyGenerator = new CustomKeyGenerator(getImproperPartitionFieldFormatProp());
      GenericRecord record = getRecord();
      Row row = KeyGeneratorTestUtilities.getRow(record);
      keyGenerator.getPartitionPath(row);
      Assertions.fail("should fail when partition key field is provided in improper format!");
    } catch (Exception e) {
      Assertions.assertTrue(e.getMessage().contains("Unable to find field names for partition path in proper format"));
    }
  }

  @Test
  public void testComplexRecordKeyWithSimplePartitionPath() {
    BuiltinKeyGenerator keyGenerator = new CustomKeyGenerator(getComplexRecordKeyWithSimplePartitionProps());
    GenericRecord record = getRecord();
    HoodieKey key = keyGenerator.getKey(record);
    Assertions.assertEquals(key.getRecordKey(), "_row_key:key1,pii_col:pi");
    Assertions.assertEquals(key.getPartitionPath(), "timestamp=4357686");

    Row row = KeyGeneratorTestUtilities.getRow(record);
    Assertions.assertEquals(keyGenerator.getRecordKey(row), "_row_key:key1,pii_col:pi");
    Assertions.assertEquals(keyGenerator.getPartitionPath(row), "timestamp=4357686");
  }

  @Test
  public void testComplexRecordKeysWithComplexPartitionPath() {
    BuiltinKeyGenerator keyGenerator = new CustomKeyGenerator(getComplexRecordKeyAndPartitionPathProps());
    GenericRecord record = getRecord();
    HoodieKey key = keyGenerator.getKey(record);
    Assertions.assertEquals(key.getRecordKey(), "_row_key:key1,pii_col:pi");
    Assertions.assertEquals(key.getPartitionPath(), "timestamp=4357686/ts_ms=20200321");

    Row row = KeyGeneratorTestUtilities.getRow(record);
    Assertions.assertEquals(keyGenerator.getRecordKey(row), "_row_key:key1,pii_col:pi");
    Assertions.assertEquals(keyGenerator.getPartitionPath(row), "timestamp=4357686/ts_ms=20200321");
  }
}
