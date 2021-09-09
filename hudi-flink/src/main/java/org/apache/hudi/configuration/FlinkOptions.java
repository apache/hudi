/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.configuration;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hoodie flink sql base config options.
 *
 * <p>It has the base options for Hoodie table. It also defines some utilities.
 */
@ConfigClassProperty(name = "Flink Options",
    groupName = ConfigGroups.Names.FLINK_SQL,
    description = "Flink jobs using the SQL can be configured through the options in WITH clause."
        + "Base configurations on Hudi tables are listed below.")
public class FlinkOptions extends HoodieConfig {
  private FlinkOptions() {
  }

  // ------------------------------------------------------------------------
  //  Base Options
  // ------------------------------------------------------------------------
  public static final ConfigOption<String> PATH = ConfigOptions
      .key("path")
      .stringType()
      .noDefaultValue()
      .withDescription("Base path for the target hoodie table.\n"
          + "The path would be created if it does not exist,\n"
          + "otherwise a Hoodie table expects to be initialized successfully");

  // ------------------------------------------------------------------------
  //  Common Options
  // ------------------------------------------------------------------------

  public static final ConfigOption<String> PARTITION_DEFAULT_NAME = ConfigOptions
      .key("partition.default_name")
      .stringType()
      .defaultValue("__DEFAULT_PARTITION__")
      .withDescription("The default partition name in case the dynamic partition"
          + " column value is null/empty string");

  public static final ConfigOption<Boolean> CHANGELOG_ENABLED = ConfigOptions
      .key("changelog.enabled")
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether to keep all the intermediate changes, "
          + "we try to keep all the changes of a record when enabled:\n"
          + "1). The sink accept the UPDATE_BEFORE message;\n"
          + "2). The source try to emit every changes of a record.\n"
          + "The semantics is best effort because the compaction job would finally merge all changes of a record into one.\n"
          + " default false to have UPSERT semantics");

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  // Prefix for Hoodie specific properties.
  private static final String PROPERTIES_PREFIX = "properties.";

  /**
   * Collects the config options that start with 'properties.' into a 'key'='value' list.
   */
  public static Map<String, String> getHoodieProperties(Map<String, String> options) {
    return getHoodiePropertiesWithPrefix(options, PROPERTIES_PREFIX);
  }

  /**
   * Collects the config options that start with specified prefix {@code prefix} into a 'key'='value' list.
   */
  public static Map<String, String> getHoodiePropertiesWithPrefix(Map<String, String> options, String prefix) {
    final Map<String, String> hoodieProperties = new HashMap<>();

    if (hasPropertyOptions(options)) {
      options.keySet().stream()
          .filter(key -> key.startsWith(PROPERTIES_PREFIX))
          .forEach(key -> {
            final String value = options.get(key);
            final String subKey = key.substring((prefix).length());
            hoodieProperties.put(subKey, value);
          });
    }
    return hoodieProperties;
  }

  /**
   * Collects all the config options, the 'properties.' prefix would be removed if the option key starts with it.
   */
  public static Configuration flatOptions(Configuration conf) {
    final Map<String, String> propsMap = new HashMap<>();

    conf.toMap().forEach((key, value) -> {
      final String subKey = key.startsWith(PROPERTIES_PREFIX)
          ? key.substring((PROPERTIES_PREFIX).length())
          : key;
      propsMap.put(subKey, value);
    });
    return fromMap(propsMap);
  }

  private static boolean hasPropertyOptions(Map<String, String> options) {
    return options.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
  }

  /**
   * Creates a new configuration that is initialized with the options of the given map.
   */
  public static Configuration fromMap(Map<String, String> map) {
    final Configuration configuration = new Configuration();
    map.forEach(configuration::setString);
    return configuration;
  }

  /**
   * Returns whether the given conf defines default value for the option {@code option}.
   */
  public static <T> boolean isDefaultValueDefined(Configuration conf, ConfigOption<T> option) {
    return !conf.getOptional(option).isPresent()
        || conf.get(option).equals(option.defaultValue());
  }

  /**
   * Returns all the optional config options.
   */
  public static Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>(allOptions());
    options.remove(PATH);
    return options;
  }

  /**
   * Returns all the config options.
   */
  public static List<ConfigOption<?>> allOptions() {
    Field[] declaredFields = FlinkOptions.class.getDeclaredFields();
    List<ConfigOption<?>> options = new ArrayList<>();
    for (Field field : declaredFields) {
      if (java.lang.reflect.Modifier.isStatic(field.getModifiers())
          && field.getType().equals(ConfigOption.class)) {
        try {
          options.add((ConfigOption<?>) field.get(ConfigOption.class));
        } catch (IllegalAccessException e) {
          throw new HoodieException("Error while fetching static config option", e);
        }
      }
    }
    return options;
  }
}
