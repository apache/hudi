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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.HoodieParquetConfigInjector;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

/**
 * Test implementation of {@link HoodieParquetConfigInjector} that disables dictionary encoding.
 */
public class DisableDictionaryInjector implements HoodieParquetConfigInjector {
  @Override
  public Pair<StorageConfiguration, HoodieConfig> injectConfig(StoragePath path,
                                                               StorageConfiguration storageConf,
                                                               HoodieConfig hoodieConfig) {
    hoodieConfig.setValue(HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED, "false");
    return Pair.of(storageConf, hoodieConfig);
  }
}
