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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for testing different implementation of {@link StorageConfiguration}.
 *
 * @param <T> configuration type.
 */
public abstract class BaseTestStorageConfiguration<T> {
  private static final Map<String, String> EMPTY_MAP = new HashMap<>();
  private static final String KEY_STRING = "hudi.key.string";
  private static final String KEY_STRING_OTHER = "hudi.key.string.other";
  private static final String KEY_BOOLEAN = "hudi.key.boolean";
  private static final String KEY_LONG = "hudi.key.long";
  private static final String KEY_ENUM = "hudi.key.enum";
  private static final String KEY_NON_EXISTENT = "hudi.key.non_existent";
  private static final String VALUE_STRING = "string_value";
  private static final String VALUE_STRING_1 = "string_value_1";
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
  public void testConstructorNewInstanceUnwrapCopy() {
    T conf = getConf(prepareConfigs());
    StorageConfiguration<T> storageConf = getStorageConfiguration(conf);
    StorageConfiguration<T> newStorageConf = storageConf.newInstance();
    Class unwrapperConfClass = storageConf.unwrap().getClass();
    assertNotSame(storageConf, newStorageConf,
        "storageConf.newInstance() should return a different StorageConfiguration instance.");
    validateConfigs(newStorageConf);
    assertNotSame(storageConf.unwrap(), newStorageConf.unwrap(),
        "storageConf.newInstance() should contain a new copy of the underlying configuration instance.");
    assertSame(storageConf.unwrap(), storageConf.unwrap(),
        "storageConf.unwrap() should return the same underlying configuration instance.");
    assertSame(storageConf.unwrap(), storageConf.unwrapAs(unwrapperConfClass),
        "storageConf.unwrapAs(unwrapperConfClass) should return the same underlying configuration instance.");
    assertNotSame(storageConf.unwrap(), storageConf.unwrapCopy(),
        "storageConf.unwrapCopy() should return a new copy of the underlying configuration instance.");
    validateConfigs(getStorageConfiguration(storageConf.unwrapCopy()));
    assertNotSame(storageConf.unwrap(), storageConf.unwrapCopyAs(unwrapperConfClass),
        "storageConf.unwrapCopyAs(unwrapperConfClass) should return a new copy of the underlying configuration instance.");
    validateConfigs(getStorageConfiguration((T) storageConf.unwrapCopyAs(unwrapperConfClass)));
    assertThrows(
        IllegalArgumentException.class,
        () -> storageConf.unwrapAs(Integer.class));
    assertThrows(
        IllegalArgumentException.class,
        () -> storageConf.unwrapCopyAs(Integer.class));
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

    storageConf.setIfUnset(KEY_STRING, VALUE_STRING + "_1");
    storageConf.setIfUnset(KEY_STRING_OTHER, VALUE_STRING_1);
    assertEquals(Option.of(VALUE_STRING), storageConf.getString(KEY_STRING));
    assertEquals(Option.of(VALUE_STRING_1), storageConf.getString(KEY_STRING_OTHER));
  }

  @Test
  public void testGet() {
    StorageConfiguration<?> storageConf = getStorageConfiguration(getConf(prepareConfigs()));
    validateConfigs(storageConf);
  }

  @Test
  public void testSerializability() throws IOException, ClassNotFoundException {
    StorageConfiguration<?> storageConf = getStorageConfiguration(getConf(prepareConfigs()));
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(storageConf);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
           ObjectInputStream ois = new ObjectInputStream(bais)) {
        StorageConfiguration<?> deserialized = (StorageConfiguration) ois.readObject();
        assertNotNull(deserialized.unwrap());
        validateConfigs(deserialized);
      }
    }
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
