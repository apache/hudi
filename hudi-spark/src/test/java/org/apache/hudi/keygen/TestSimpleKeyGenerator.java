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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.exception.HoodieKeyException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSimpleKeyGenerator extends TestKeyGeneratorUtilities {

  private TypedProperties getCommonProps() {
    TypedProperties properties = new TypedProperties();
    properties.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key");
    properties.put(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(), "true");
    return properties;
  }

  private TypedProperties getPropertiesWithoutPartitionPathProp() {
    return getCommonProps();
  }

  private TypedProperties getPropertiesWithoutRecordKeyProp() {
    TypedProperties properties = new TypedProperties();
    properties.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "timestamp");
    return properties;
  }

  private TypedProperties getWrongRecordKeyFieldProps() {
    TypedProperties properties = new TypedProperties();
    properties.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "timestamp");
    properties.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_wrong_key");
    return properties;
  }

  private TypedProperties getComplexRecordKeyProp() {
    TypedProperties properties = new TypedProperties();
    properties.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "timestamp");
    properties.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key,pii_col");
    return properties;
  }

  private TypedProperties getProps() {
    TypedProperties properties = getCommonProps();
    properties.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "timestamp");
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
  }

  @Test
  public void testComplexRecordKeyField() {
    SimpleKeyGenerator keyGenerator = new SimpleKeyGenerator(getComplexRecordKeyProp());
    Assertions.assertThrows(HoodieKeyException.class, () -> keyGenerator.getRecordKey(getRecord()));
  }

  @Test
  public void testHappyFlow() {
    SimpleKeyGenerator keyGenerator = new SimpleKeyGenerator(getProps());
    HoodieKey key = keyGenerator.getKey(getRecord());
    Assertions.assertEquals(key.getRecordKey(), "key1");
    Assertions.assertEquals(key.getPartitionPath(), "timestamp=4357686");
  }
}
