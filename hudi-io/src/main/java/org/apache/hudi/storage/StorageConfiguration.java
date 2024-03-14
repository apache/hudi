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

package org.apache.hudi.storage;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Interface providing the storage configuration in type {@link T}.
 *
 * @param <T> type of storage configuration to provide.
 */
public abstract class StorageConfiguration<T> implements Serializable {
  /**
   * @return the storage configuration.
   */
  public abstract T get();

  /**
   * @return a new copy of the storage configuration.
   */
  public abstract T newCopy();

  /**
   * Serializes the storage configuration.
   * DO NOT change the signature, as required by {@link Serializable}.
   *
   * @param out stream to write.
   * @throws IOException on I/O error.
   */
  public abstract void writeObject(ObjectOutputStream out) throws IOException;

  /**
   * Deserializes the storage configuration.
   * DO NOT change the signature, as required by {@link Serializable}.
   *
   * @param in stream to read.
   * @throws IOException on I/O error.
   */
  public abstract void readObject(ObjectInputStream in) throws IOException;

  /**
   * Sets the configuration key-value pair.
   *
   * @param key   in String.
   * @param value in String.
   */
  public abstract void set(String key, String value);

  /**
   * Gets the String value of a property key.
   *
   * @param key property key in String.
   * @return the property value if present, or {@code Option.empty()}.
   */
  public abstract Option<String> getString(String key);

  /**
   * Gets the String value of a property key if present, or the default value if not.
   *
   * @param key          property key in String.
   * @param defaultValue default value is the property does not exist.
   * @return the property value if present, or the default value.
   */
  public final String getString(String key, String defaultValue) {
    Option<String> value = getString(key);
    return value.isPresent() ? value.get() : defaultValue;
  }

  /**
   * Gets the boolean value of a property key if present, or the default value if not.
   *
   * @param key          property key in String.
   * @param defaultValue default value is the property does not exist.
   * @return the property value if present, or the default value.
   */
  public final boolean getBoolean(String key, boolean defaultValue) {
    Option<String> value = getString(key);
    return value.isPresent()
        ? (!StringUtils.isNullOrEmpty(value.get()) ? Boolean.parseBoolean(value.get()) : defaultValue)
        : defaultValue;
  }

  /**
   * Gets the long value of a property key if present, or the default value if not.
   *
   * @param key          property key in String.
   * @param defaultValue default value is the property does not exist.
   * @return the property value if present, or the default value.
   */
  public final long getLong(String key, long defaultValue) {
    Option<String> value = getString(key);
    return value.isPresent() ? Long.parseLong(value.get()) : defaultValue;
  }

  /**
   * Gets the Enum value of a property key if present, or the default value if not.
   *
   * @param key          property key in String.
   * @param defaultValue default value is the property does not exist.
   * @param <T>          Enum.
   * @return the property value if present, or the default value.
   */
  public <T extends Enum<T>> T getEnum(String key, T defaultValue) {
    Option<String> value = getString(key);
    return value.isPresent()
        ? Enum.valueOf(defaultValue.getDeclaringClass(), value.get())
        : defaultValue;
  }
}
