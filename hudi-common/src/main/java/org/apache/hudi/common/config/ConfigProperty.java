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

package org.apache.hudi.common.config;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * ConfigProperty describes a configuration property. It contains the configuration
 * key, deprecated older versions of the key, and an optional default value for the configuration,
 * configuration descriptions and also an inferring mechanism to infer the configuration value
 * based on other configurations.
 *
 * @param <T> The type of the default value.
 */
public class ConfigProperty<T> implements Serializable {

  private final String key;

  private final T defaultValue;

  private final String docOnDefaultValue;

  private final String doc;

  private final Option<String> sinceVersion;

  private final Option<String> deprecatedVersion;

  private final Set<String> validValues;

  private final String[] alternatives;

  // provide the ability to infer config value based on other configs
  private final Option<Function<HoodieConfig, Option<T>>> inferFunction;

  ConfigProperty(String key, T defaultValue, String docOnDefaultValue, String doc,
                 Option<String> sinceVersion, Option<String> deprecatedVersion,
                 Option<Function<HoodieConfig, Option<T>>> inferFunc, Set<String> validValues,
                 String... alternatives) {
    this.key = Objects.requireNonNull(key);
    this.defaultValue = defaultValue;
    this.docOnDefaultValue = docOnDefaultValue;
    this.doc = doc;
    this.sinceVersion = sinceVersion;
    this.deprecatedVersion = deprecatedVersion;
    this.inferFunction = inferFunc;
    this.validValues = validValues;
    this.alternatives = alternatives;
  }

  public String key() {
    return key;
  }

  public T defaultValue() {
    if (defaultValue == null) {
      throw new HoodieException(String.format("There's no default value for this config: %s", key));
    }
    return defaultValue;
  }

  public boolean hasDefaultValue() {
    return defaultValue != null;
  }

  public String getDocOnDefaultValue() {
    return StringUtils.isNullOrEmpty(docOnDefaultValue)
        ? StringUtils.EMPTY_STRING : docOnDefaultValue;
  }

  public String doc() {
    return StringUtils.isNullOrEmpty(doc) ? StringUtils.EMPTY_STRING : doc;
  }

  public Option<String> getSinceVersion() {
    return sinceVersion;
  }

  public Option<String> getDeprecatedVersion() {
    return deprecatedVersion;
  }

  public boolean hasInferFunction() {
    return getInferFunction().isPresent();
  }

  Option<Function<HoodieConfig, Option<T>>> getInferFunction() {
    return inferFunction;
  }

  public void checkValues(String value) {
    if (validValues != null && !validValues.isEmpty() && !validValues.contains(value)) {
      throw new IllegalArgumentException(
          "The value of " + key + " should be one of "
              + String.join(",", validValues) + ", but was " + value);
    }
  }

  public List<String> getAlternatives() {
    return Arrays.asList(alternatives);
  }

  public ConfigProperty<T> withDocumentation(String doc) {
    Objects.requireNonNull(doc);
    return new ConfigProperty<>(key, defaultValue, docOnDefaultValue, doc, sinceVersion, deprecatedVersion, inferFunction, validValues, alternatives);
  }

  public ConfigProperty<T> withValidValues(String... validValues) {
    Objects.requireNonNull(validValues);
    return new ConfigProperty<>(key, defaultValue, docOnDefaultValue, doc, sinceVersion, deprecatedVersion, inferFunction, new HashSet<>(Arrays.asList(validValues)), alternatives);
  }

  public ConfigProperty<T> withAlternatives(String... alternatives) {
    Objects.requireNonNull(alternatives);
    return new ConfigProperty<>(key, defaultValue, docOnDefaultValue, doc, sinceVersion, deprecatedVersion, inferFunction, validValues, alternatives);
  }

  public ConfigProperty<T> sinceVersion(String sinceVersion) {
    Objects.requireNonNull(sinceVersion);
    return new ConfigProperty<>(key, defaultValue, docOnDefaultValue, doc, Option.of(sinceVersion), deprecatedVersion, inferFunction, validValues, alternatives);
  }

  public ConfigProperty<T> deprecatedAfter(String deprecatedVersion) {
    Objects.requireNonNull(deprecatedVersion);
    return new ConfigProperty<>(key, defaultValue, docOnDefaultValue, doc, sinceVersion, Option.of(deprecatedVersion), inferFunction, validValues, alternatives);
  }

  public ConfigProperty<T> withInferFunction(Function<HoodieConfig, Option<T>> inferFunction) {
    Objects.requireNonNull(inferFunction);
    return new ConfigProperty<>(key, defaultValue, docOnDefaultValue, doc, sinceVersion, deprecatedVersion, Option.of(inferFunction), validValues, alternatives);
  }

  /**
   * Create a OptionBuilder with key.
   *
   * @param key The key of the option
   * @return Return a OptionBuilder.
   */
  public static PropertyBuilder key(String key) {
    Objects.requireNonNull(key);
    return new PropertyBuilder(key);
  }

  @Override
  public String toString() {
    return String.format(
        "Key: '%s' , default: %s description: %s since version: %s deprecated after: %s)",
        key, defaultValue, doc, sinceVersion.isPresent() ? sinceVersion.get() : "version is not defined",
        deprecatedVersion.isPresent() ? deprecatedVersion.get() : "version is not defined");
  }

  /**
   * The PropertyBuilder is used to build the ConfigProperty.
   */
  public static final class PropertyBuilder {

    private final String key;

    PropertyBuilder(String key) {
      this.key = key;
    }

    public <T> ConfigProperty<T> defaultValue(T value) {
      return defaultValue(value, "");
    }

    public <T> ConfigProperty<T> defaultValue(T value, String docOnDefaultValue) {
      Objects.requireNonNull(value);
      Objects.requireNonNull(docOnDefaultValue);
      ConfigProperty<T> configProperty = new ConfigProperty<>(key, value, docOnDefaultValue, "", Option.empty(), Option.empty(), Option.empty(), Collections.emptySet());
      return configProperty;
    }

    public ConfigProperty<String> noDefaultValue() {
      return noDefaultValue("");
    }

    public ConfigProperty<String> noDefaultValue(String docOnDefaultValue) {
      ConfigProperty<String> configProperty = new ConfigProperty<>(key, null, docOnDefaultValue, "", Option.empty(),
          Option.empty(), Option.empty(), Collections.emptySet());
      return configProperty;
    }
  }
}