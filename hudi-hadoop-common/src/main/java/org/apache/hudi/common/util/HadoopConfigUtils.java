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

import java.util.Properties;

/**
 * Utils on Hadoop {@link Configuration}.
 */
public class HadoopConfigUtils {

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
    return ConfigUtils.getRawValueWithAltKeys(conf::get, configProperty).map(Object::toString);
  }
}
