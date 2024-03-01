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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for testing different implementation of {@link StorageConfiguration}.
 *
 * @param <T> configuration type.
 */
public abstract class BaseTestStorageConfiguration<T> {
  private static final Map<String, String> EMPTY_MAP = new HashMap<>();
  private static final String KEY_STRING = "hudi.key.string";
  private static final String KEY_BOOLEAN = "hudi.key.boolean";
  private static final String KEY_LONG = "hudi.key.long";
  private static final String KEY_ENUM = "hudi.key.enum";
  private static final String KEY_NON_EXISTENT = "hudi.key.non_existent";
  private static final String VALUE_STRING = "string_value";
  private static final String VALUE_BOOLEAN = "true";
  private static final String VALUE_LONG = "12309120";
  private static final String VALUE_ENUM = TestEnum.ENUM2.toString();

  /**
   * @return instance of {@link StorageConfiguration} implementation class.
   */
  protected abstract StorageConfiguration<T> getStorageConfiguration(T conf);

  /**
   * @param mapping configuration in key-value pairs.
   * @return underlying configuration instance.
   */
  protected abstract T getConf(Map<String, String> mapping);

  @Test
  public void testConstructorGetNewCopy() {
    T conf = getConf(EMPTY_MAP);
    StorageConfiguration<T> storageConf = getStorageConfiguration(conf);
    assertSame(storageConf.get(), storageConf.get());
    assertNotSame(storageConf.get(), storageConf.newCopy());
  }

  @Test
  public void testSet() {
    StorageConfiguration<T> storageConf = getStorageConfiguration(getConf(EMPTY_MAP));
    assertFalse(storageConf.getString(KEY_STRING).isPresent());
    assertFalse(storageConf.getString(KEY_BOOLEAN).isPresent());

    storageConf.set(KEY_STRING, VALUE_STRING);
    storageConf.set(KEY_BOOLEAN, VALUE_BOOLEAN);
    assertEquals(Option.of(VALUE_STRING), storageConf.getString(KEY_STRING));
    assertTrue(storageConf.getBoolean(KEY_BOOLEAN, false));
  }

  @Test
  public void testGet() {
    StorageConfiguration<?> storageConf = getStorageConfiguration(getConf(prepareConfigs()));
    validateConfigs(storageConf);
  }

  private Map<String, String> prepareConfigs() {
    Map<String, String> conf = new HashMap<>();
    conf.put(KEY_STRING, VALUE_STRING);
    conf.put(KEY_BOOLEAN, VALUE_BOOLEAN);
    conf.put(KEY_LONG, VALUE_LONG);
    conf.put(KEY_ENUM, VALUE_ENUM);
    return conf;
  }

  private void validateConfigs(StorageConfiguration<?> storageConf) {
    assertEquals(Option.of(VALUE_STRING), storageConf.getString(KEY_STRING));
    assertEquals(VALUE_STRING, storageConf.getString(KEY_STRING, ""));
    assertTrue(storageConf.getBoolean(KEY_BOOLEAN, false));
    assertFalse(storageConf.getBoolean(KEY_NON_EXISTENT, false));
    assertEquals(Long.parseLong(VALUE_LONG), storageConf.getLong(KEY_LONG, 0));
    assertEquals(30L, storageConf.getLong(KEY_NON_EXISTENT, 30L));
    assertEquals(TestEnum.valueOf(VALUE_ENUM), storageConf.getEnum(KEY_ENUM, TestEnum.ENUM1));
    assertEquals(TestEnum.ENUM1, storageConf.getEnum(KEY_NON_EXISTENT, TestEnum.ENUM1));
    assertFalse(storageConf.getString(KEY_NON_EXISTENT).isPresent());
    assertEquals(VALUE_STRING, storageConf.getString(KEY_NON_EXISTENT, VALUE_STRING));
  }

  enum TestEnum {
    ENUM1, ENUM2, ENUM3
  }
}
