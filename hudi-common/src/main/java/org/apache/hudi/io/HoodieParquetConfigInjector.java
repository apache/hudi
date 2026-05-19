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

package org.apache.hudi.io;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

/**
 * A pluggable interface that all parquet-based writers (Spark/Flink) will invoke before creating write support
 * or parquet file writer objects.
 * <p>
 * This allows users to inject custom configurations into the Parquet writer pipeline at runtime, enabling
 * fine-grained control over Parquet file properties such as bloom filters, compression settings, encoding
 * options, and other advanced Parquet configurations.
 * <p>
 * <strong>Important:</strong> Implementations must NOT mutate the input {@code storageConf} or
 * {@code hoodieConfig} objects directly. Instead, they should create copies, apply the desired
 * modifications to the copies, and return them. The caller retains references to the original
 * objects, so in-place mutations would have unintended side effects on other components sharing
 * the same configuration instances.
 * <p>
 * Example use cases:
 * <ul>
 *   <li>Enabling column-specific Parquet bloom filters</li>
 *   <li>Setting custom compression codecs per file or partition</li>
 *   <li>Adjusting page sizes or row group sizes based on data characteristics</li>
 *   <li>Injecting custom metadata into Parquet files</li>
 * </ul>
 *
 * @since 1.2.0
 */
public interface HoodieParquetConfigInjector {

  /**
   * Injects custom configurations into the Parquet writer pipeline.
   * <p>
   * This method is invoked before creating the Parquet write support and writer objects, allowing
   * implementations to modify both the storage-level and Hudi-level configurations.
   * <p>
   * Implementations must not mutate the input parameters. Instead, create copies of {@code storageConf}
   * and {@code hoodieConfig}, apply modifications to the copies, and return them in the result pair.
   *
   * @param path the file path where the Parquet file will be written
   * @param storageConf the storage configuration (e.g., Hadoop Configuration) — must not be mutated
   * @param hoodieConfig the Hudi configuration containing write settings and table properties — must not be mutated
   * @return a pair containing new (or copied) storage configuration and Hudi configuration with the injected properties.
   *         Both configurations will be used to create the Parquet writer.
   */
  Pair<StorageConfiguration, HoodieConfig> injectConfig(StoragePath path, StorageConfiguration storageConf, HoodieConfig hoodieConfig);

  /**
   * Applies the configured {@link HoodieParquetConfigInjector} (if any) to the given storage and Hudi configurations.
   * If no injector class is configured, returns the original configurations unchanged.
   *
   * @param path the file path where the Parquet file will be written
   * @param storageConf the storage configuration
   * @param hoodieConfig the Hudi configuration
   * @return a pair containing the (potentially modified) storage configuration and Hudi configuration
   */
  static Pair<StorageConfiguration, HoodieConfig> applyConfigInjector(StoragePath path, StorageConfiguration storageConf, HoodieConfig hoodieConfig) {
    String configInjectorClass = hoodieConfig.getStringOrDefault(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS, StringUtils.EMPTY_STRING);
    if (StringUtils.isNullOrEmpty(configInjectorClass)) {
      return Pair.of(storageConf, hoodieConfig);
    }
    try {
      HoodieParquetConfigInjector injector = (HoodieParquetConfigInjector) ReflectionUtils.loadClass(configInjectorClass);
      return injector.injectConfig(path, storageConf, hoodieConfig);
    } catch (Exception e) {
      throw new HoodieException("Failed to instantiate or invoke parquet config injector class: " + configInjectorClass, e);
    }
  }
}
