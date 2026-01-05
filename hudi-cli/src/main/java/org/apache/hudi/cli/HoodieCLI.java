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

package org.apache.hudi.cli;

import org.apache.hudi.cli.utils.SparkTempViewProvider;
import org.apache.hudi.cli.utils.TempViewProvider;
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.HoodieStorageUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * This class is responsible to load table metadata and hoodie related configs.
 */
public class HoodieCLI {

  public static StorageConfiguration<Configuration> conf;
  public static ConsistencyGuardConfig consistencyGuardConfig = ConsistencyGuardConfig.newBuilder().build();
  public static HoodieTimeGeneratorConfig timeGeneratorConfig;
  public static HoodieStorage storage;
  public static CLIState state = CLIState.INIT;
  public static String basePath;
  protected static HoodieTableMetaClient tableMetadata;
  public static HoodieTableMetaClient syncTableMetadata;
  public static TempViewProvider tempViewProvider;

  /**
   * Enum for CLI state.
   */
  public enum CLIState {
    INIT, TABLE, SYNC
  }

  public static void setConsistencyGuardConfig(ConsistencyGuardConfig config) {
    consistencyGuardConfig = config;
  }

  public static void setTimeGeneratorConfig(HoodieTimeGeneratorConfig config) {
    timeGeneratorConfig = config;
  }

  private static void setTableMetaClient(HoodieTableMetaClient tableMetadata) {
    HoodieCLI.tableMetadata = tableMetadata;
  }

  private static void setBasePath(String basePath) {
    HoodieCLI.basePath = basePath;
  }

  public static boolean initConf() {
    if (HoodieCLI.conf == null) {
      HoodieCLI.conf = HadoopFSUtils.getStorageConf(
          HadoopFSUtils.prepareHadoopConf(new Configuration()));
      return true;
    }
    return false;
  }

  public static void initFS(boolean force) throws IOException {
    if (storage == null || force) {
      storage = (tableMetadata != null)
          ? tableMetadata.getStorage()
          : HoodieStorageUtils.getStorage(
              HadoopFSUtils.convertToStoragePath(FileSystem.get(conf.unwrap()).getWorkingDirectory()),
              conf);
    }
  }

  public static void refreshTableMetadata() {
    setTableMetaClient(HoodieTableMetaClient.builder()
        .setConf(HoodieCLI.conf.newInstance()).setBasePath(basePath).setLoadActiveTimelineOnLoad(false)
        .setConsistencyGuardConfig(HoodieCLI.consistencyGuardConfig)
        .setTimeGeneratorConfig(timeGeneratorConfig == null ? HoodieTimeGeneratorConfig.defaultConfig(basePath) : timeGeneratorConfig)
        .build());
  }

  public static void connectTo(String basePath) {
    setBasePath(basePath);
    refreshTableMetadata();
  }

  /**
   * Get tableMetadata, throw NullPointerException when it is null.
   *
   * @return tableMetadata which is instance of HoodieTableMetaClient
   */
  public static HoodieTableMetaClient getTableMetaClient() {
    if (tableMetadata == null) {
      throw new NullPointerException("There is no hudi table. Please use connect command to set table first");
    }
    return tableMetadata;
  }

  public static synchronized TempViewProvider getTempViewProvider() {
    if (tempViewProvider == null) {
      tempViewProvider = new SparkTempViewProvider(HoodieCLI.class.getSimpleName());
    }

    return tempViewProvider;
  }
}
