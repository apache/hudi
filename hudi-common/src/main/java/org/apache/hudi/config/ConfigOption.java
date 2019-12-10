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

package org.apache.hudi.config;

import java.util.Objects;

/**
 * A ConfigOption contains the key and an optional default value.
 *
 * @param <T> The type of the default value.
 */
public class ConfigOption<T> {

  private final String key;

  private final T defaultValue;

  ConfigOption(String key, T defaultValue) {
    this.key = Objects.requireNonNull(key);
    this.defaultValue = Objects.requireNonNull(defaultValue);
  }

  public String key() {
    return key;
  }

  public T defaultValue() {
    return defaultValue;
  }

  /**
   * Create a OptionBuilder with key.
   *
   * @param key The key of the option
   * @return Return a OptionBuilder.
   */
  public static ConfigOption.OptionBuilder key(String key) {
    Objects.requireNonNull(key);
    return new ConfigOption.OptionBuilder(key);
  }

  /**
   * The OptionBuilder is used to build the ConfigOption.
   */
  public static final class OptionBuilder {

    private final String key;

    OptionBuilder(String key) {
      this.key = key;
    }

    public <T> ConfigOption<T> defaultValue(T value) {
      Objects.requireNonNull(value);
      return new ConfigOption<>(key, value);
    }
  }
}
