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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.util.ConfigUtils.getRawValueWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.loadGlobalProperties;

/**
 * This class deals with {@link ConfigProperty} and provides get/set functionalities.
 */
@Slf4j
public class HoodieConfig implements Serializable {

  protected static final String CONFIG_VALUES_DELIMITER = ",";
  // Number of retries while reading the properties file to deal with parallel updates
  protected static final int MAX_READ_RETRIES = 5;
  // Delay between retries while reading the properties file
  protected static final int READ_RETRY_DELAY_MSEC = 1000;

  // Matches the value computed from the members of this class in earlier releases, so their serialized configs stay readable.
  private static final long serialVersionUID = 449277607721112139L;
  // The props of the enclosing config and their snapshot, while it writes its nested configs on this thread
  private static final ThreadLocal<Pair<TypedProperties, TypedProperties>> NESTED_CONFIG_BASE_PROPS = new ThreadLocal<>();

  @Getter
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

  /**
   * Writes the fields of the calling subclass as {@link ObjectOutputStream#defaultWriteObject()} does, except that
   * the props of each {@link HoodieConfig} written meanwhile are written as their difference from a snapshot of
   * the props of this config when that is smaller. Call it from the {@code writeObject} method of a config that
   * holds nested configs built from its own props, so that the stream carries the shared entries once.
   */
  protected final void defaultWriteObjectSharingProps(ObjectOutputStream out) throws IOException {
    Pair<TypedProperties, TypedProperties> previous = NESTED_CONFIG_BASE_PROPS.get();
    NESTED_CONFIG_BASE_PROPS.set(Pair.of(props, PropertiesDelta.snapshot(props)));
    try {
      out.defaultWriteObject();
    } finally {
      if (previous == null) {
        NESTED_CONFIG_BASE_PROPS.remove();
      } else {
        NESTED_CONFIG_BASE_PROPS.set(previous);
      }
    }
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    Pair<TypedProperties, TypedProperties> base = NESTED_CONFIG_BASE_PROPS.get();
    // A nested config sharing the props instance of the enclosing config keeps sharing it after deserialization.
    PropertiesDelta delta = base == null || base.getLeft() == props ? null : PropertiesDelta.of(base.getRight(), props);
    // The delta takes the place of the props field, so a reader without PropertiesDelta fails to resolve it
    // instead of leaving props unset.
    ObjectOutputStream.PutField fields = out.putFields();
    fields.put("props", delta == null ? props : delta);
    out.writeFields();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    Object value = in.readFields().get("props", null);
    props = value instanceof PropertiesDelta ? ((PropertiesDelta) value).apply() : (TypedProperties) value;
  }

  /**
   * Serialized form of properties as their difference from a base that is never modified.
   */
  private static final class PropertiesDelta implements Serializable {
    private static final long serialVersionUID = 1L;

    private final TypedProperties base;
    private final HashMap<Object, Object> changed;
    private final ArrayList<Object> removed;

    private PropertiesDelta(TypedProperties base, HashMap<Object, Object> changed, ArrayList<Object> removed) {
      this.base = base;
      this.changed = changed;
      this.removed = removed;
    }

    static TypedProperties snapshot(TypedProperties props) {
      if (props == null) {
        return null;
      }
      TypedProperties snapshot = new TypedProperties();
      // Properties synchronizes its mutators on itself, so this copies a consistent view.
      synchronized (props) {
        snapshot.putAll(props);
      }
      return snapshot;
    }

    /**
     * Returns the difference of {@code props} from {@code base}, or null if {@code props} should be written in full.
     */
    static PropertiesDelta of(TypedProperties base, TypedProperties props) {
      if (base == null || props == null || props.getClass() != TypedProperties.class) {
        return null;
      }
      HashMap<Object, Object> changed = new HashMap<>();
      ArrayList<Object> removed = new ArrayList<>();
      synchronized (props) {
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
          if (!entry.getValue().equals(base.get(entry.getKey()))) {
            changed.put(entry.getKey(), entry.getValue());
          }
        }
        for (Object key : base.keySet()) {
          if (!props.containsKey(key)) {
            removed.add(key);
          }
        }
        return changed.size() + removed.size() < props.size() ? new PropertiesDelta(base, changed, removed) : null;
      }
    }

    TypedProperties apply() {
      TypedProperties props = new TypedProperties();
      props.putAll(base);
      removed.forEach(props::remove);
      props.putAll(changed);
      return props;
    }
  }
}
