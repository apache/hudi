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
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.KeyGeneratorTestUtilities;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestGlobalDeleteRecordGenerator extends KeyGeneratorTestUtilities {

  private TypedProperties getCommonProps(boolean getComplexRecordKey) {
    TypedProperties properties = new TypedProperties();
    if (getComplexRecordKey) {
      properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key,pii_col");
    } else {
      properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    }
    properties.put(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");
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

  private TypedProperties getProps() {
    TypedProperties properties = getCommonProps(true);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp,ts_ms");
    return properties;
  }

  @Test
  void testNullRecordKeyFields() {
    GenericRecord avroRecord = getRecord();
    assertThrows(StringIndexOutOfBoundsException.class, () -> {
      BaseKeyGenerator keyGenerator = new GlobalDeleteKeyGenerator(getPropertiesWithoutRecordKeyProp());
      keyGenerator.getRecordKey(avroRecord);
    });
  }

  @Test
  void testWrongRecordKeyField() {
    GlobalDeleteKeyGenerator keyGenerator = new GlobalDeleteKeyGenerator(getWrongRecordKeyFieldProps());
    assertThrows(HoodieKeyException.class, () -> keyGenerator.getRecordKey(getRecord()));
  }

  @Test
  void testHappyFlow() {
    GlobalDeleteKeyGenerator keyGenerator = new GlobalDeleteKeyGenerator(getProps());
    GenericRecord avroRecord = getRecord();
    HoodieKey key = keyGenerator.getKey(avroRecord);
    assertEquals("_row_key:key1,pii_col:pi", key.getRecordKey());
    assertEquals("", key.getPartitionPath());
    Row row = KeyGeneratorTestUtilities.getRow(avroRecord);
    assertEquals("_row_key:key1,pii_col:pi", keyGenerator.getRecordKey(row));
    assertEquals("", keyGenerator.getPartitionPath(row));
  }
}
