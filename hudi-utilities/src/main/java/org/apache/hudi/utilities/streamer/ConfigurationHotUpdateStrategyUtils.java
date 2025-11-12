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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;

public class ConfigurationHotUpdateStrategyUtils {

  /**
   * Creates a {@link ConfigurationHotUpdateStrategy} class via reflection.
   *
   * <p>If the class name of {@link ConfigurationHotUpdateStrategy} is configured
   * through the {@link HoodieStreamer.Config#configHotUpdateStrategyClass}.
   */
  public static Option<ConfigurationHotUpdateStrategy> createConfigurationHotUpdateStrategy(
      String strategyClass,
      HoodieStreamer.Config cfg,
      TypedProperties properties) throws HoodieException {
    try {
      return StringUtils.isNullOrEmpty(strategyClass)
          ? Option.empty() :
          Option.of((ConfigurationHotUpdateStrategy) ReflectionUtils.loadClass(strategyClass, cfg, properties));
    } catch (Throwable e) {
      throw new HoodieException("Could not create configuration hot update strategy class " + strategyClass, e);
    }
  }
}
