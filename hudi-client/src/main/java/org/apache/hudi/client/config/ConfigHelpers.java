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

package org.apache.hudi.client.config;

import org.apache.hudi.common.model.HoodieEngineType;

import java.util.Properties;

import static org.apache.hudi.config.HoodieEngineConfig.DEFAULT_HOODIE_ENGINE_TYPE;
import static org.apache.hudi.config.HoodieEngineConfig.HOODIE_ENGINE_TYPE_PROP;

/**
 * Config Helpers to create config.
 */
public class ConfigHelpers {
  /**
   * Create Config according to the props.
   * @param props
   * @return
   */
  public static <T> AbstractConfig<T> createConfig(Properties props) {
    String hoodieEngineType = props.getProperty(HOODIE_ENGINE_TYPE_PROP, DEFAULT_HOODIE_ENGINE_TYPE);
    HoodieEngineType engineType = HoodieEngineType.valueOf(hoodieEngineType);
    AbstractConfig abstractConfig;
    switch (engineType) {
      case SPARK:
        abstractConfig = new SparkConfig<T>(props);
        break;
      default:
        throw new UnsupportedOperationException(engineType + " not supported yet.");
    }
    return abstractConfig;
  }
}
