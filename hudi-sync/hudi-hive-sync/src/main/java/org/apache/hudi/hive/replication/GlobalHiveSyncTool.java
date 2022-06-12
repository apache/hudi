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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.HiveSyncTool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class GlobalHiveSyncTool extends HiveSyncTool {

  private static final Logger LOG = LogManager.getLogger(HiveSyncTool.class);

  public GlobalHiveSyncTool(GlobalHiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    super(cfg, configuration, fs);
  }

  @Override
  public void syncHoodieTable() {
    doSync();
  }

  @Override
  protected void syncHoodieTable(String tableName, boolean useRealtimeInputFormat, boolean readAsOptimized) {
    super.syncHoodieTable(tableName, useRealtimeInputFormat, readAsOptimized);
    if (((GlobalHiveSyncConfig) hiveSyncConfig).globallyReplicatedTimeStamp != null) {
      hoodieHiveClient.updateLastReplicatedTimeStamp(tableName,
          ((GlobalHiveSyncConfig) hiveSyncConfig).globallyReplicatedTimeStamp);
    }
    LOG.info("Sync complete for " + tableName);
  }

  public Map<String, Option<String>> getLastReplicatedTimeStampMap() {
    Map<String, Option<String>> timeStampMap = new HashMap<>();
    Option<String> timeStamp = hoodieHiveClient.getLastReplicatedTime(snapshotTableName);
    timeStampMap.put(snapshotTableName, timeStamp);
    if (HoodieTableType.MERGE_ON_READ.equals(hoodieHiveClient.getTableType())) {
      Option<String> roTimeStamp = hoodieHiveClient.getLastReplicatedTime(roTableName.get());
      timeStampMap.put(roTableName.get(), roTimeStamp);
    }
    return timeStampMap;
  }

  public void setLastReplicatedTimeStamp(Map<String, Option<String>> timeStampMap) {
    for (String tableName : timeStampMap.keySet()) {
      Option<String> timestamp = timeStampMap.get(tableName);
      if (timestamp.isPresent()) {
        hoodieHiveClient.updateLastReplicatedTimeStamp(tableName, timestamp.get());
        LOG.info("updated timestamp for " + tableName + " to: " + timestamp.get());
      } else {
        hoodieHiveClient.deleteLastReplicatedTimeStamp(tableName);
        LOG.info("deleted timestamp for " + tableName);
      }
    }
  }

  public static GlobalHiveSyncTool buildGlobalHiveSyncTool(GlobalHiveSyncConfig cfg, HiveConf hiveConf) {
    FileSystem fs = FSUtils.getFs(cfg.hoodieSyncConfigParams.basePath, new Configuration());
    hiveConf.addResource(fs.getConf());
    return new GlobalHiveSyncTool(cfg, hiveConf, fs);
  }
}
