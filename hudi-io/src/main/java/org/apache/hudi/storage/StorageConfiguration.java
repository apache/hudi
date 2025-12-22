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
import org.apache.hudi.common.util.ValidationUtils;

import java.io.Serializable;

/**
 * Interface providing the storage configuration in type {@link T}.
 *
 * @param <T> type of storage configuration to provide.
 */
public abstract class StorageConfiguration<T> implements Serializable {
  /**
   * @return a new {@link StorageConfiguration} instance with a new copy of
   * the configuration of type {@link T}.
   */
  public abstract StorageConfiguration<T> newInstance();

  /**
   * @return the underlying configuration of type {@link T}.
   */
  public abstract T unwrap();

  /**
   * @return a new copy of the underlying configuration of type {@link T}.
   */
  public abstract T unwrapCopy();
  
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
   * Gets an inline version of this storage configuration
   *
   * @return copy of this storage configuration that is inline
   */
  public abstract StorageConfiguration<T> getInline();

  /**
   * @param clazz class of U, which is assignable from T.
   * @param <U>   type to return.
   * @return the underlying configuration cast to type {@link U}.
   */
  public final <U> U unwrapAs(Class<U> clazz) {
    return castConfiguration(unwrap(), clazz);
  }

  /**
   * @param clazz class of U, which is assignable from T.
   * @param <U>   type to return.
   * @return a new copy of the underlying configuration cast to type {@link U}.
   */
  public final <U> U unwrapCopyAs(Class<U> clazz) {
    return castConfiguration(unwrapCopy(), clazz);
  }

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

  /**
   * Sets a property key with a value in the configuration, if the property key
   * does not already exist.
   *
   * @param key   property key.
   * @param value property value.
   */
  public final void setIfUnset(String key, String value) {
    if (getString(key).isEmpty()) {
      set(key, value);
    }
  }

  /**
   * @param conf  configuration object.
   * @param clazz class of U.
   * @param <U>   type to return.
   * @return the configuration cast to type {@link U}.
   */
  public static <U> U castConfiguration(Object conf, Class<U> clazz) {
    ValidationUtils.checkArgument(
        clazz.isAssignableFrom(conf.getClass()),
        "Cannot cast the underlying configuration to type " + clazz);
    return (U) conf;
  }
}
