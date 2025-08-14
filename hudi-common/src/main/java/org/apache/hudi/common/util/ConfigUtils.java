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

package org.apache.hudi.common.util;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.PropertiesConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieReaderConfig.USE_NATIVE_HFILE_READER;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_CHECKSUM;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED;

public class ConfigUtils {
  public static final String STREAMER_CONFIG_PREFIX = "hoodie.streamer.";
  @Deprecated
  public static final String DELTA_STREAMER_CONFIG_PREFIX = "hoodie.deltastreamer.";
  public static final String SCHEMAPROVIDER_CONFIG_PREFIX = STREAMER_CONFIG_PREFIX + "schemaprovider.";
  @Deprecated
  public static final String OLD_SCHEMAPROVIDER_CONFIG_PREFIX = DELTA_STREAMER_CONFIG_PREFIX + "schemaprovider.";
  /**
   * Config stored in hive serde properties to tell query engine (spark/flink) to
   * read the table as a read-optimized table when this config is true.
   */
  public static final String IS_QUERY_AS_RO_TABLE = "hoodie.query.as.ro.table";

  /**
   * Config stored in hive serde properties to tell query engine (spark) the
   * location to read.
   */
  public static final String TABLE_SERDE_PATH = "path";

  public static final HoodieConfig DEFAULT_HUDI_CONFIG_FOR_READER = new HoodieConfig();

  private static final Logger LOG = LoggerFactory.getLogger(ConfigUtils.class);

  /**
   * Get ordering field.
   */
  @Nullable
  public static String[] getOrderingFields(Properties properties) {
    String orderField = null;
    if (properties.containsKey("hoodie.datasource.write.precombine.field")) {
      orderField = properties.getProperty("hoodie.datasource.write.precombine.field");
    } else if (properties.containsKey(HoodieTableConfig.PRECOMBINE_FIELDS.key())) {
      orderField = properties.getProperty(HoodieTableConfig.PRECOMBINE_FIELDS.key());
    } else if (properties.containsKey(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY)) {
      orderField = properties.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY);
    }
    return orderField == null ? null : orderField.split(",");
  }

  /**
   * Ensures that ordering field is populated for mergers and legacy payloads.
   *
   * <p> See also {@link #getOrderingFields(Properties)}.
   */
  public static TypedProperties supplementOrderingFields(TypedProperties props, List<String> orderingFields) {
    String orderingFieldsAsString = String.join(",", orderingFields);
    props.putIfAbsent(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, orderingFieldsAsString);
    props.putIfAbsent(HoodieTableConfig.PRECOMBINE_FIELDS.key(), orderingFieldsAsString);
    props.putIfAbsent("hoodie.datasource.write.precombine.field", orderingFieldsAsString);
    return props;
  }

  /**
   * Get payload class.
   */
  public static String getPayloadClass(Properties props) {
    return HoodieRecordPayload.getPayloadClassName(props);
  }

  /**
   * Check if event time metadata should be tracked.
   */
  public static boolean isTrackingEventTimeWatermark(TypedProperties props) {
    return props.getBoolean("hoodie.write.track.event.time.watermark", false);
  }

  /**
   * Check if logical timestamp should be made consistent.
   */
  public static boolean shouldKeepConsistentLogicalTimestamp(TypedProperties props) {
    return Boolean.parseBoolean(props.getProperty(
        KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
  }

  /**
   * Extract event_time field name from configuration.
   */
  @Nullable
  public static String getEventTimeFieldName(TypedProperties props) {
    return props.getProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY);
  }

  public static List<String> split2List(String param) {
    return StringUtils.split(param, ",").stream()
        .map(String::trim).distinct().collect(Collectors.toList());
  }

  /**
   * Convert the key-value config to a map.  The format of the config
   * is a key-value pair just like "k1=v1\nk2=v2\nk3=v3".
   *
   * @param keyValueConfig Key-value configs in properties format, i.e., multiple lines of
   *                       `key=value`.
   * @return A {@link Map} of key-value configs.
   */
  public static Map<String, String> toMap(String keyValueConfig) {
    return toMap(keyValueConfig, "\n");
  }

  /**
   * Convert the key-value config to a map. The format of the config is a key-value pair
   * with defined separator.  For example, if the separator is a comma, the input is
   * "k1=v1,k2=v2,k3=v3".
   *
   * @param keyValueConfig key-value configs in properties format, with defined separator.
   * @param separator      the separator.
   * @return A {@link Map} of key-value configs.
   */
  public static Map<String, String> toMap(String keyValueConfig, String separator) {
    if (StringUtils.isNullOrEmpty(keyValueConfig)) {
      return new HashMap<>();
    }
    String[] keyvalues = keyValueConfig.split(separator);
    Map<String, String> tableProperties = new HashMap<>();
    for (String keyValue : keyvalues) {
      // Handle multiple new lines and lines that contain only spaces after splitting
      if (keyValue.trim().isEmpty()) {
        continue;
      }
      String[] keyValueArray = keyValue.split("=");
      if (keyValueArray.length == 1 || keyValueArray.length == 2) {
        String key = keyValueArray[0].trim();
        String value = keyValueArray.length == 2 ? keyValueArray[1].trim() : "";
        tableProperties.put(key, value);
      } else {
        throw new IllegalArgumentException("Bad key-value config: " + keyValue + ", must be the"
            + " format 'key = value'");
      }
    }
    return tableProperties;
  }

  /**
   * Convert map config to key-value string.The format of the config
   * is a key-value pair just like "k1=v1\nk2=v2\nk3=v3".
   *
   * @param config A {@link Map} of key-value configs.
   * @return Key-value configs in properties format, i.e., multiple lines of `key=value`.
   */
  public static String configToString(Map<String, String> config) {
    if (config == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : config.entrySet()) {
      if (sb.length() > 0) {
        sb.append("\n");
      }
      sb.append(entry.getKey()).append("=").append(entry.getValue());
    }
    return sb.toString();
  }

  /**
   * Case-insensitive resolution of input enum name to the enum type
   */
  public static <T extends Enum<T>> T resolveEnum(Class<T> enumType,
                                                  String name) {
    T[] enumConstants = enumType.getEnumConstants();
    for (T constant : enumConstants) {
      if (constant.name().equalsIgnoreCase(name)) {
        return constant;
      }
    }

    throw new IllegalArgumentException("No enum constant found " + enumType.getName() + "." + name);
  }

  public static <T extends Enum<T>> String[] enumNames(Class<T> enumType) {
    T[] enumConstants = enumType.getEnumConstants();
    return Arrays.stream(enumConstants).map(Enum::name).toArray(String[]::new);
  }

  /**
   * Strips the prefix from a config key. The prefix is defined by a {@link ConfigProperty}
   * which can have alternatives.  The method strips any matching prefix.
   *
   * @param prop         The config key for stripping
   * @param prefixConfig The prefix.
   * @return An {@link Option} of the config key after stripping, if any prefix matches the key;
   * empty {@link Option} otherwise.
   */
  public static Option<String> stripPrefix(String prop, ConfigProperty<String> prefixConfig) {
    if (prop.startsWith(prefixConfig.key())) {
      return Option.of(String.join("", prop.split(prefixConfig.key())));
    }
    for (String altPrefix : prefixConfig.getAlternatives()) {
      if (prop.startsWith(altPrefix)) {
        return Option.of(String.join("", prop.split(altPrefix)));
      }
    }
    return Option.empty();
  }

  /**
   * Whether the properties contain a config. If any of the key or alternative keys of the
   * {@link ConfigProperty} exists in the properties, this method returns {@code true}.
   *
   * @param props          Configs in {@link Properties}
   * @param configProperty Config to look up.
   * @return {@code true} if exists; {@code false} otherwise.
   */
  public static boolean containsConfigProperty(Properties props,
                                               ConfigProperty<?> configProperty) {
    if (!props.containsKey(configProperty.key())) {
      for (String alternative : configProperty.getAlternatives()) {
        if (props.containsKey(alternative)) {
          return true;
        }
      }
      return false;
    }
    return true;
  }

  /**
   * Whether the properties contain a config. If any of the key or alternative keys of the
   * {@link ConfigProperty} exists in the properties, this method returns {@code true}.
   *
   * @param props          Configs in {@link Map}
   * @param configProperty Config to look up.
   * @return {@code true} if exists; {@code false} otherwise.
   */
  public static boolean containsConfigProperty(Map<String, Object> props,
                                               ConfigProperty<?> configProperty) {
    return containsConfigProperty(props::containsKey, configProperty);
  }

  /**
   * Whether the properties contain a config. If any of the key or alternative keys of the
   * {@link ConfigProperty} exists, this method returns {@code true}.
   *
   * @param keyExistsFn    Function to check if key exists
   * @param configProperty Config to look up.
   * @return {@code true} if exists; {@code false} otherwise.
   */
  public static boolean containsConfigProperty(Function<String, Boolean> keyExistsFn,
                                               ConfigProperty<?> configProperty) {
    if (!keyExistsFn.apply(configProperty.key())) {
      for (String alternative : configProperty.getAlternatives()) {
        if (keyExistsFn.apply(alternative)) {
          return true;
        }
      }
      return false;
    }
    return true;
  }

  /**
   * Validates that config String keys exist in the properties.
   *
   * @param props          Configs in {@link TypedProperties} to validate.
   * @param checkPropNames List of String keys that must exist.
   */
  public static void checkRequiredProperties(TypedProperties props, List<String> checkPropNames) {
    checkPropNames.forEach(prop -> {
      if (!props.containsKey(prop)) {
        throw new HoodieNotSupportedException("Required property " + prop + " is missing");
      }
    });
  }

  /**
   * Validates that all {@link ConfigProperty} configs exist in the properties. For each
   * {@link ConfigProperty} config, if any of the key or alternative keys of the
   * {@link ConfigProperty} exists in the properties, the validation of this config passes.
   *
   * @param props              Configs in {@link TypedProperties} to validate.
   * @param configPropertyList List of {@link ConfigProperty} configs that must exist.
   */
  public static void checkRequiredConfigProperties(TypedProperties props,
                                                   List<ConfigProperty<?>> configPropertyList) {
    configPropertyList.forEach(configProperty -> {
      if (!containsConfigProperty(props, configProperty)) {
        throw new HoodieNotSupportedException("Required property " + configProperty.key() + " is missing");
      }
    });
  }

  /**
   * Gets the raw value for a {@link ConfigProperty} config from properties. The key and
   * alternative keys are used to fetch the config.
   *
   * @param props          Configs in {@link Properties}.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return {@link Option} of value if the config exists; empty {@link Option} otherwise.
   */
  public static Option<Object> getRawValueWithAltKeys(Properties props,
                                                      ConfigProperty<?> configProperty) {
    if (props.containsKey(configProperty.key())) {
      return Option.ofNullable(props.get(configProperty.key()));
    }
    for (String alternative : configProperty.getAlternatives()) {
      if (props.containsKey(alternative)) {
        LOG.warn(String.format("The configuration key '%s' has been deprecated "
                + "and may be removed in the future. Please use the new key '%s' instead.",
            alternative, configProperty.key()));
        return Option.ofNullable(props.get(alternative));
      }
    }
    return Option.empty();
  }

  /**
   * Gets the raw value for a {@link ConfigProperty<T>} config from properties with the option
   * of using default value.
   *
   * @param props           Configs in {@link Properties}.
   * @param configProperty  {@link ConfigProperty} config to fetch.
   * @param useDefaultValue If enabled, uses default value for configProperty.
   * @return raw value of the config.
   * @param <T> type of the value.
   */
  public static <T> T getRawValueWithAltKeys(Properties props, ConfigProperty<T> configProperty, boolean useDefaultValue) {
    Option<T> rawValue = (Option<T>) getRawValueWithAltKeys(props, configProperty);
    if (rawValue.isPresent()) {
      return rawValue.get();
    }
    if (useDefaultValue) {
      return configProperty.defaultValue();
    }
    throw new IllegalArgumentException("Property " + configProperty.key() + " not found");
  }

  /**
   * Gets the String value for a {@link ConfigProperty} config from properties. The key and
   * alternative keys are used to fetch the config. If the config is not found, an
   * {@link IllegalArgumentException} is thrown.
   *
   * @param props          Configs in {@link Properties}.
   * @param configProperty {@link ConfigProperty} config of String type to fetch.
   * @return String value if the config exists.
   */
  public static String getStringWithAltKeys(Properties props,
                                            ConfigProperty<String> configProperty) {
    return getStringWithAltKeys(props, configProperty, false);
  }

  /**
   * Gets the String value for a {@link ConfigProperty} config from properties. The key and
   * alternative keys are used to fetch the config. If using default value, the default value
   * of {@link ConfigProperty} config, if exists, is returned if the config is not found in
   * the properties. If not using default value, if the config is not found, an
   * {@link IllegalArgumentException} is thrown.
   *
   * @param props           Configs in {@link Properties}.
   * @param configProperty  {@link ConfigProperty} config of String type to fetch.
   * @param useDefaultValue Whether to use default value from {@link ConfigProperty}.
   * @return String value if the config exists; otherwise, if the config does not exist and
   * {@code useDefaultValue} is true, returns default String value if there is default value
   * defined in the {@link ConfigProperty} config and {@code null} otherwise.
   */
  public static String getStringWithAltKeys(Properties props,
                                            ConfigProperty<String> configProperty,
                                            boolean useDefaultValue) {
    if (useDefaultValue) {
      return getStringWithAltKeys(
          props, configProperty, configProperty.hasDefaultValue() ? configProperty.defaultValue() : null);
    }
    Option<Object> rawValue = getRawValueWithAltKeys(props, configProperty);
    if (!rawValue.isPresent()) {
      throw new IllegalArgumentException("Property " + configProperty.key() + " not found");
    }
    return rawValue.get().toString();
  }

  /**
   * Gets the String value for a {@link ConfigProperty} config from properties. The key and
   * alternative keys are used to fetch the config. The default value as the input of the method
   * is returned if the config is not found in the properties.
   *
   * @param props          Configs in {@link Properties}.
   * @param configProperty {@link ConfigProperty} config of String type to fetch.
   * @param defaultValue   Default value.
   * @return String value if the config exists; default value otherwise.
   */
  public static String getStringWithAltKeys(Properties props,
                                            ConfigProperty<?> configProperty,
                                            String defaultValue) {
    Option<Object> rawValue = getRawValueWithAltKeys(props, configProperty);
    return rawValue.map(Object::toString).orElse(defaultValue);
  }

  /**
   * Gets the String value for a {@link ConfigProperty} config from a {@link Map}. The key
   * and alternative keys are used to fetch the config. The default value of {@link ConfigProperty}
   * config, if exists, is returned if the config is not found in the properties.
   *
   * @param props          Configs in {@link Map}.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return String value if the config exists; default String value if the config does not exist
   * and there is default value defined in the {@link ConfigProperty} config and is convertible to
   * String type; {@code null} otherwise.
   */
  public static String getStringWithAltKeys(Map<String, Object> props,
                                            ConfigProperty<?> configProperty) {
    return getStringWithAltKeys(props::get, configProperty);
  }

  /**
   * Gets the String value for a {@link ConfigProperty} config using a key mapping function. The key
   * and alternative keys are used to fetch the config. The default value of {@link ConfigProperty}
   * config, if exists, is returned if the config is not found in the properties.
   *
   * @param keyMapper      Mapper function to map the key to values.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return String value if the config exists; default String value if the config does not exist
   * and there is default value defined in the {@link ConfigProperty} config and is convertible to
   * String type; {@code null} otherwise.
   */
  public static String getStringWithAltKeys(Function<String, Object> keyMapper,
                                            ConfigProperty<?> configProperty) {
    Object value = keyMapper.apply(configProperty.key());
    if (value != null) {
      return value.toString();
    }
    for (String alternative : configProperty.getAlternatives()) {
      value = keyMapper.apply(alternative);
      if (value != null) {
        LOG.warn(String.format("The configuration key '%s' has been deprecated "
                + "and may be removed in the future. Please use the new key '%s' instead.",
            alternative, configProperty.key()));
        return value.toString();
      }
    }
    return configProperty.hasDefaultValue() ? configProperty.defaultValue().toString() : null;
  }

  /**
   * Gets String value from properties with alternative keys.
   *
   * @param props        Configs in {@link TypedProperties}.
   * @param key          String key.
   * @param altKey       Alternative String key.
   * @param defaultValue Default String value.
   * @return String value if the config exists; default value otherwise.
   */
  public static String getStringWithAltKeys(TypedProperties props,
                                            String key, String altKey,
                                            String defaultValue) {
    if (props.containsKey(altKey)) {
      return props.getString(altKey);
    }
    if (props.containsKey(key)) {
      return props.getString(key);
    }
    return defaultValue;
  }

  /**
   * Gets the boolean value for a {@link ConfigProperty} config from properties. The key and
   * alternative keys are used to fetch the config. The default value of {@link ConfigProperty}
   * config, if exists, is returned if the config is not found in the properties.
   *
   * @param props          Configs in {@link Properties}.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return boolean value if the config exists; default boolean value if the config does not exist
   * and there is default value defined in the {@link ConfigProperty} config; {@code false} otherwise.
   */
  public static boolean getBooleanWithAltKeys(Properties props,
                                              ConfigProperty<?> configProperty) {
    Option<Object> rawValue = getRawValueWithAltKeys(props, configProperty);
    boolean defaultValue = configProperty.hasDefaultValue() && Boolean.parseBoolean(configProperty.defaultValue().toString());
    return rawValue.map(v -> Boolean.parseBoolean(v.toString())).orElse(defaultValue);
  }

  /**
   * Gets the integer value for a {@link ConfigProperty} config from properties. The key and
   * alternative keys are used to fetch the config. The default value of {@link ConfigProperty}
   * config, if exists, is returned if the config is not found in the properties.
   *
   * @param props          Configs in {@link Properties}.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return integer value if the config exists; default integer value if the config does not exist
   * and there is default value defined in the {@link ConfigProperty} config; {@code 0} otherwise.
   */
  public static int getIntWithAltKeys(Properties props,
                                      ConfigProperty<?> configProperty) {
    Option<Object> rawValue = getRawValueWithAltKeys(props, configProperty);
    int defaultValue = configProperty.hasDefaultValue()
        ? Integer.parseInt(configProperty.defaultValue().toString()) : 0;
    return rawValue.map(v -> Integer.parseInt(v.toString())).orElse(defaultValue);
  }

  /**
   * Gets the long value for a {@link ConfigProperty} config from properties. The key and
   * alternative keys are used to fetch the config. The default value of {@link ConfigProperty}
   * config, if exists, is returned if the config is not found in the properties.
   *
   * @param props          Configs in {@link TypedProperties}.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return long value if the config exists; default long value if the config does not exist
   * and there is default value defined in the {@link ConfigProperty} config; {@code 0} otherwise.
   */
  public static long getLongWithAltKeys(TypedProperties props,
                                        ConfigProperty<Long> configProperty) {
    Option<Object> rawValue = getRawValueWithAltKeys(props, configProperty);
    long defaultValue = configProperty.hasDefaultValue()
        ? configProperty.defaultValue() : 0L;
    return rawValue.map(v -> Long.parseLong(v.toString())).orElse(defaultValue);
  }

  /**
   * Removes a {@link ConfigProperty} config from properties. This removes all possible keys
   * including the alternatives from the properties.
   *
   * @param props          Configs in {@link TypedProperties}.
   * @param configProperty {@link ConfigProperty} config to remove.
   */
  public static void removeConfigFromProps(TypedProperties props,
                                           ConfigProperty<?> configProperty) {
    props.remove(configProperty.key());
    for (String alternative : configProperty.getAlternatives()) {
      props.remove(alternative);
    }
  }

  /**
   * Returns filtered properties based on the given {@link ConfigProperty} config list to keep.
   *
   * @param props              Configs in {@link TypedProperties}.
   * @param configPropertyList List of {@link ConfigProperty} configs to keep.
   * @return Filtered configs in {@link Map} with {@link ConfigProperty} configs to keep.
   */
  public static Map<String, Object> filterProperties(TypedProperties props,
                                                     List<ConfigProperty<String>> configPropertyList) {
    Set<String> persistedKeys = getAllConfigKeys(configPropertyList);
    return props.entrySet().stream()
        .filter(p -> persistedKeys.contains(String.valueOf(p.getKey())))
        .collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue())));
  }

  /**
   * @param configPropertyList List of {@link ConfigProperty} configs to keep.
   * @return all String keys of {@link ConfigProperty} configs.
   */
  public static Set<String> getAllConfigKeys(List<ConfigProperty<String>> configPropertyList) {
    return configPropertyList.stream().flatMap(configProperty -> {
      List<String> keys = new ArrayList<>();
      keys.add(configProperty.key());
      keys.addAll(configProperty.getAlternatives());
      return keys.stream();
    }).collect(Collectors.toSet());
  }

  public static TypedProperties fetchConfigs(
      HoodieStorage storage,
      StoragePath metaPath,
      String propertiesFile,
      String propertiesBackupFile,
      int maxReadRetries,
      int maxReadRetryDelayInMs) throws IOException {
    StoragePath cfgPath = new StoragePath(metaPath, propertiesFile);
    StoragePath backupCfgPath = new StoragePath(metaPath, propertiesBackupFile);
    int readRetryCount = 0;
    boolean found = false;

    TypedProperties props = new TypedProperties();
    while (readRetryCount++ < maxReadRetries) {
      for (StoragePath path : Arrays.asList(cfgPath, backupCfgPath)) {
        // Read the properties and validate that it is a valid file
        try (InputStream is = storage.open(path)) {
          props.clear();
          props.load(is);
          found = true;
          if (props.containsKey(TABLE_CHECKSUM.key())) {
            ValidationUtils.checkArgument(HoodieTableConfig.validateChecksum(props));
          }
          return props;
        } catch (IOException e) {
          LOG.warn(String.format("Could not read properties from %s: %s", path, e));
        } catch (IllegalArgumentException e) {
          LOG.warn(String.format("Invalid properties file %s: %s", path, props));
        }
      }

      // Failed to read all files so wait before retrying. This can happen in cases of parallel updates to the properties.
      try {
        Thread.sleep(maxReadRetryDelayInMs);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting");
      }
    }

    // If we are here then after all retries either no properties file was found or only an invalid file was found.
    if (found) {
      throw new IllegalArgumentException(
          "hoodie.properties file seems invalid. Please check for left over `.updated` files if any, manually copy it to hoodie.properties and retry");
    } else if (!storage.exists(metaPath)) {
      throw new TableNotFoundException(metaPath.toString());
    } else {
      throw new HoodieIOException("Could not load Hoodie properties from " + cfgPath);
    }
  }

  public static void recoverIfNeeded(HoodieStorage storage, StoragePath cfgPath,
                                     StoragePath backupCfgPath) throws IOException {
    if (!storage.exists(cfgPath)) {
      // copy over from backup
      try (InputStream in = storage.open(backupCfgPath);
           OutputStream out = storage.create(cfgPath, false)) {
        FileIOUtils.copy(in, out);
      }
    }
    // regardless, we don't need the backup anymore.
    storage.deleteFile(backupCfgPath);
  }

  public static void upsertProperties(Properties current, Properties updated) {
    updated.forEach((k, v) -> current.setProperty(k.toString(), v.toString()));
  }

  public static void deleteProperties(Properties current, Properties deleted) {
    deleted.forEach((k, v) -> current.remove(k.toString()));
  }

  public static HoodieConfig getReaderConfigs(StorageConfiguration<?> storageConf) {
    HoodieConfig config = new HoodieConfig();
    config.setAll(DEFAULT_HUDI_CONFIG_FOR_READER.getProps());
    config.setValue(USE_NATIVE_HFILE_READER,
        Boolean.toString(storageConf.getBoolean(USE_NATIVE_HFILE_READER.key(), USE_NATIVE_HFILE_READER.defaultValue())));
    return config;
  }

  public static TypedProperties loadGlobalProperties() {
    return ((PropertiesConfig) ReflectionUtils.loadClass("org.apache.hudi.common.config.DFSPropertiesConfiguration")).getGlobalProperties();
  }

  /**
   * Extract all properties whose keys start with a given prefix.
   * E.g., if the prefix is "a.b.c.", and the props contain:
   * "a.b.c.K1=V1", "a.b.c.K2=V2", "a.b.c.K3=V3".
   * Then the output is:
   * Map(K1->V1, K2->V2, K3->V3).
   */
  public static Map<String, String> extractWithPrefix(TypedProperties props, String prefix) {
    if (props == null || props.isEmpty()) {
      return Collections.emptyMap();
    }

    int prefixLength = prefix.length();
    Map<String, String> mergeProperties = new HashMap<>();
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      String key = entry.getKey().toString();
      // Early exit if key is shorter than prefix or doesn't start with prefix
      if (key.length() <= prefixLength || !key.startsWith(prefix)) {
        continue;
      }
      // Extract and validate the property key
      String propKey = key.substring(prefixLength).trim();
      if (propKey.isEmpty()) {
        continue;
      }
      // Extract and trim the value
      Object value = entry.getValue();
      String stringValue = (value != null) ? value.toString().trim() : "";
      mergeProperties.put(propKey, stringValue);
    }
    return mergeProperties;
  }
}
