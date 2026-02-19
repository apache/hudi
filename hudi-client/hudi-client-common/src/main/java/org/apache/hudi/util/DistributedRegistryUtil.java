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

package org.apache.hudi.util;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;

/**
 * Utility class for creating and initializing distributed metric registries.
 */
public class DistributedRegistryUtil {

  /**
   * Creates and sets metrics registries for HoodieWrapperFileSystem.
   * When executor metrics are enabled, this creates distributed registries that can collect
   * metrics from Spark executors. Otherwise, it creates local registries.
   *
   * @param context The engine context
   * @param config The write configuration
   */
  public static void createWrapperFileSystemRegistries(HoodieEngineContext context, HoodieWriteConfig config) {
    if (config.isMetricsOn()) {
      Registry registry;
      Registry registryMeta;
      if (config.isExecutorMetricsEnabled()) {
        // Create and set distributed registry for HoodieWrapperFileSystem
        registry = context.getMetricRegistry(config.getTableName(), HoodieWrapperFileSystem.REGISTRY_NAME);
        registryMeta = context.getMetricRegistry(config.getTableName(), HoodieWrapperFileSystem.REGISTRY_META_NAME);
      } else {
        registry = Registry.getRegistryOfClass(config.getTableName(), HoodieWrapperFileSystem.REGISTRY_NAME,
            Registry.getRegistry(HoodieWrapperFileSystem.REGISTRY_NAME).getClass().getName());
        registryMeta = Registry.getRegistryOfClass(config.getTableName(), HoodieWrapperFileSystem.REGISTRY_META_NAME,
            Registry.getRegistry(HoodieWrapperFileSystem.REGISTRY_META_NAME).getClass().getName());
      }
      HoodieWrapperFileSystem.setMetricsRegistry(registry, registryMeta);
    }
  }
}
