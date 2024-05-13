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

package org.apache.hudi.common.util;

import org.apache.hudi.common.config.ConfigProperty;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Utils on Hadoop {@link Configuration}.
 */
public class HadoopConfigUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopConfigUtils.class);

  /**
   * Creates a Hadoop {@link Configuration} instance with the properties.
   *
   * @param props {@link Properties} instance.
   * @return Hadoop {@link Configuration} instance.
   */
  public static Configuration createHadoopConf(Properties props) {
    Configuration hadoopConf = new Configuration();
    props.stringPropertyNames().forEach(k -> hadoopConf.set(k, props.getProperty(k)));
    return hadoopConf;
  }

  /**
   * Gets the raw value for a {@link ConfigProperty} config from Hadoop configuration. The key and
   * alternative keys are used to fetch the config.
   *
   * @param conf           Configs in Hadoop {@link Configuration}.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return {@link Option} of value if the config exists; empty {@link Option} otherwise.
   */
  public static Option<String> getRawValueWithAltKeys(Configuration conf,
                                                      ConfigProperty<?> configProperty) {
    String value = conf.get(configProperty.key());
    if (value != null) {
      return Option.of(value);
    }
    for (String alternative : configProperty.getAlternatives()) {
      String altValue = conf.get(alternative);
      if (altValue != null) {
        LOG.warn(String.format("The configuration key '%s' has been deprecated "
                + "and may be removed in the future. Please use the new key '%s' instead.",
            alternative, configProperty.key()));
        return Option.of(altValue);
      }
    }
    return Option.empty();
  }

  /**
   * Gets the boolean value for a {@link ConfigProperty} config from Hadoop configuration. The key and
   * alternative keys are used to fetch the config. The default value of {@link ConfigProperty}
   * config, if exists, is returned if the config is not found in the configuration.
   *
   * @param conf           Configs in Hadoop {@link Configuration}.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return boolean value if the config exists; default boolean value if the config does not exist
   * and there is default value defined in the {@link ConfigProperty} config; {@code false} otherwise.
   */
  public static boolean getBooleanWithAltKeys(Configuration conf,
                                              ConfigProperty<?> configProperty) {
    Option<String> rawValue = getRawValueWithAltKeys(conf, configProperty);
    boolean defaultValue = configProperty.hasDefaultValue()
        ? Boolean.parseBoolean(configProperty.defaultValue().toString()) : false;
    return rawValue.map(Boolean::parseBoolean).orElse(defaultValue);
  }
}
