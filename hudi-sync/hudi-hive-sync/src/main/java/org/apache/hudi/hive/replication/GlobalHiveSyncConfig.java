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

package org.apache.hudi.hive.replication;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.hive.HiveSyncConfig;

import com.beust.jcommander.Parameter;

public class GlobalHiveSyncConfig extends HiveSyncConfig {
  @Parameter(names = {"--replicated-timestamp"}, description = "Add globally replicated timestamp to enable consistent reads across clusters")
  public String globallyReplicatedTimeStamp;

  public GlobalHiveSyncConfig() {
  }

  public GlobalHiveSyncConfig(TypedProperties props) {
    super(props);
  }

  public static GlobalHiveSyncConfig copy(GlobalHiveSyncConfig cfg) {
    GlobalHiveSyncConfig newConfig = new GlobalHiveSyncConfig(cfg.getProps());
    newConfig.basePath = cfg.basePath;
    newConfig.assumeDatePartitioning = cfg.assumeDatePartitioning;
    newConfig.databaseName = cfg.databaseName;
    newConfig.hivePass = cfg.hivePass;
    newConfig.hiveUser = cfg.hiveUser;
    newConfig.partitionFields = cfg.partitionFields;
    newConfig.partitionValueExtractorClass = cfg.partitionValueExtractorClass;
    newConfig.jdbcUrl = cfg.jdbcUrl;
    newConfig.tableName = cfg.tableName;
    newConfig.usePreApacheInputFormat = cfg.usePreApacheInputFormat;
    newConfig.useFileListingFromMetadata = cfg.useFileListingFromMetadata;
    newConfig.supportTimestamp = cfg.supportTimestamp;
    newConfig.decodePartition = cfg.decodePartition;
    newConfig.batchSyncNum = cfg.batchSyncNum;
    newConfig.globallyReplicatedTimeStamp = cfg.globallyReplicatedTimeStamp;
    return newConfig;
  }

  @Override
  public String toString() {
    return "GlobalHiveSyncConfig{" + super.toString()
        + " globallyReplicatedTimeStamp=" + globallyReplicatedTimeStamp + "}";
  }

}
