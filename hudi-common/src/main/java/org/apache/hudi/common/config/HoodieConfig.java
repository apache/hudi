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

import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.util.ConfigUtils.getRawValueWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.loadGlobalProperties;

/**
 * This class deals with {@link ConfigProperty} and provides get/set functionalities.
 */
@Getter
public class HoodieConfig implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieConfig.class);

  protected static final String CONFIG_VALUES_DELIMITER = ",";
  // Number of retries while reading the properties file to deal with parallel updates
  protected static final int MAX_READ_RETRIES = 5;
  // Delay between retries while reading the properties file
  protected static final int READ_RETRY_DELAY_MSEC = 1000;

  protected TypedProperties props;

  public HoodieConfig() {
    this.props = new TypedProperties();
  }

  public HoodieConfig(TypedProperties props) {
    this.props = props;
  }

  protected HoodieConfig(Properties props) {
    this.props = TypedProperties.copy(props);
  }

  public <T> void setValue(ConfigProperty<T> cfg, String val) {
    cfg.checkValues(val);
    props.setProperty(cfg.key(), val);
  }

  public <T> void setValue(String key, String val) {
    props.setProperty(key, val);
  }

  public <T> void clearValue(ConfigProperty<T> cfg) {
    ConfigUtils.removeConfigFromProps(props, cfg);
  }

  public void setAll(Properties properties) {
    props.putAll(properties);
  }

  /**
   * Sets the default value of a config if user does not set it already.
   * The default value can only be set if the config property has a built-in
   * default value or an infer function.  When the infer function is present,
   * the infer function is used first to derive the config value based on other
   * configs.  If the config value cannot be inferred, the built-in default value
   * is used if present.
   *
   * @param configProperty Config to set a default value.
   * @param <T>            Data type of the config.
   */
  public <T> void setDefaultValue(ConfigProperty<T> configProperty) {
    if (!contains(configProperty)) {
      Option<T> inferValue = Option.empty();
      if (configProperty.hasInferFunction()) {
        inferValue = configProperty.getInferFunction().get().apply(this);
      }
      if (inferValue.isPresent() || configProperty.hasDefaultValue()) {
        props.setProperty(
            configProperty.key(),
            inferValue.isPresent()
                ? inferValue.get().toString()
                : configProperty.defaultValue().toString());
      }
    }
  }

  public <T> void setDefaultValue(ConfigProperty<T> configProperty, T defaultVal) {
    if (!contains(configProperty)) {
      props.setProperty(configProperty.key(), defaultVal.toString());
    }
  }

  public boolean contains(String key) {
    return props.containsKey(key);
  }

  public <T> boolean contains(ConfigProperty<T> configProperty) {
    return contains(configProperty, this);
  }

  public static <T> boolean contains(ConfigProperty<T> configProperty, HoodieConfig config) {
    if (config.getProps().containsKey(configProperty.key())) {
      return true;
    }
    return configProperty.getAlternatives().stream().anyMatch(k -> config.getProps().containsKey(k));
  }

  private <T> Option<Object> getRawValue(ConfigProperty<T> configProperty) {
    return getRawValueWithAltKeys(props, configProperty);
  }

  protected void setDefaults(String configClassName) {
    Class<?> configClass = ReflectionUtils.getClass(configClassName);
    Arrays.stream(configClass.getDeclaredFields())
        .filter(f -> Modifier.isStatic(f.getModifiers()))
        .filter(f -> f.getType().isAssignableFrom(ConfigProperty.class))
        .forEach(f -> {
          try {
            ConfigProperty<?> cfgProp = (ConfigProperty<?>) f.get("null");
            if (cfgProp.hasDefaultValue() || cfgProp.hasInferFunction()) {
              setDefaultValue(cfgProp);
            }
          } catch (IllegalAccessException e) {
            e.printStackTrace();
          }
        });
  }

  public <T> String getString(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(Object::toString).orElse(null);
  }

  public <T> List<String> getSplitStrings(ConfigProperty<T> configProperty) {
    return getSplitStrings(configProperty, ",");
  }

  public <T> List<String> getSplitStrings(ConfigProperty<T> configProperty, String delimiter) {
    return StringUtils.split(getString(configProperty), delimiter);
  }

  public String getString(String key) {
    return props.getProperty(key);
  }

  public <T> Integer getInt(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Integer.parseInt(v.toString())).orElse(null);
  }

  public <T> Integer getIntOrDefault(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Integer.parseInt(v.toString()))
        .orElseGet(() -> Integer.parseInt(configProperty.defaultValue().toString()));
  }

  public <T> Boolean getBoolean(ConfigProperty<T> configProperty) {
    if (configProperty.hasDefaultValue()) {
      return getBooleanOrDefault(configProperty);
    }
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Boolean.parseBoolean(v.toString())).orElse(null);
  }

  public boolean getBooleanOrDefault(String key, boolean defaultVal) {
    return Option.ofNullable(props.getProperty(key)).map(Boolean::parseBoolean).orElse(defaultVal);
  }

  public <T> boolean getBooleanOrDefault(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Boolean.parseBoolean(v.toString()))
            .orElseGet(() -> Boolean.parseBoolean(configProperty.defaultValue().toString()));
  }

  public <T> boolean getBooleanOrDefault(ConfigProperty<T> configProperty, boolean defaultVal) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Boolean.parseBoolean(v.toString())).orElse(defaultVal);
  }

  public <T> Long getLong(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Long.parseLong(v.toString())).orElse(null);
  }

  public <T> Long getLongOrDefault(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Long.parseLong(v.toString()))
            .orElseGet(() -> Long.parseLong(configProperty.defaultValue().toString()));
  }

  public <T> Float getFloat(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Float.parseFloat(v.toString())).orElse(null);
  }

  public <T> Float getFloatOrDefault(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Float.parseFloat(v.toString()))
            .orElseGet(() -> Float.parseFloat(configProperty.defaultValue().toString()));
  }

  public <T> Double getDouble(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Double.parseDouble(v.toString())).orElse(null);
  }

  public <T> Double getDoubleOrDefault(ConfigProperty<T> configProperty) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(v -> Double.parseDouble(v.toString()))
            .orElseGet(() -> Double.parseDouble(configProperty.defaultValue().toString()));
  }

  public <T> String getStringOrDefault(ConfigProperty<T> configProperty) {
    return getStringOrDefault(configProperty, configProperty.defaultValue().toString());
  }

  public <T> String getStringOrDefault(ConfigProperty<T> configProperty, String defaultVal) {
    Option<Object> rawValue = getRawValue(configProperty);
    return rawValue.map(Object::toString).orElse(defaultVal);
  }

  public String getStringOrDefault(String key, String defaultVal) {
    return Option.ofNullable(props.getProperty(key)).orElse(defaultVal);
  }

  public TypedProperties getProps(boolean includeGlobalProps) {
    if (includeGlobalProps) {
      TypedProperties mergedProps = loadGlobalProperties();
      mergedProps.putAll(props);
      return mergedProps;
    } else {
      return props;
    }
  }

  public void setDefaultOnCondition(boolean condition, HoodieConfig config) {
    if (condition) {
      setDefault(config);
    }
  }

  public void setDefault(HoodieConfig config) {
    props.putAll(config.getProps());
  }

  public <T> String getStringOrThrow(ConfigProperty<T> configProperty, String errorMessage) throws HoodieException {
    Option<Object> rawValue = getRawValue(configProperty);
    if (rawValue.isPresent()) {
      return rawValue.get().toString();
    } else {
      throw new HoodieException(errorMessage);
    }
  }

  public static HoodieConfig copy(Properties props) {
    return new HoodieConfig(props);
  }
}
