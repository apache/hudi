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

import org.apache.hudi.common.util.Option;

import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;
import java.util.Properties;

public class ReplicationStateSync implements AutoCloseable {

  protected GlobalHiveSyncTool globalHiveSyncTool;
  private Map<String, Option<String>> replicatedTimeStampMap;
  private Map<String, Option<String>> oldReplicatedTimeStampMap;
  private final String clusterId;

  ReplicationStateSync(Properties props, HiveConf hiveConf, String uid) {
    globalHiveSyncTool = new GlobalHiveSyncTool(props, hiveConf);
    replicatedTimeStampMap = globalHiveSyncTool.getLastReplicatedTimeStampMap();
    clusterId = uid;
  }

  public void sync() throws Exception {
    // the cluster maybe down by the time we reach here so we refresh our replication
    // state right before we set the oldReplicatedTimeStamp to narrow this window. this is a
    // liveliness check right before we start.
    replicatedTimeStampMap = globalHiveSyncTool.getLastReplicatedTimeStampMap();
    // it is possible sync fails midway and corrupts the table property therefore we should set
    // the oldReplicatedTimeStampMap before the sync start so that we attempt to rollback
    // this will help in scenario where sync failed due to some bug in hivesync but in case where
    // cluster went down halfway through or before sync in this case rollback may also fail and
    // that is ok and we want to be alerted to such scenarios.
    oldReplicatedTimeStampMap = replicatedTimeStampMap;
    globalHiveSyncTool.syncHoodieTable();
    replicatedTimeStampMap = globalHiveSyncTool.getLastReplicatedTimeStampMap();
  }

  public boolean rollback() {
    if (oldReplicatedTimeStampMap != null) {
      globalHiveSyncTool.setLastReplicatedTimeStamp(oldReplicatedTimeStampMap);
      oldReplicatedTimeStampMap = null;
    }
    return true;
  }

  public boolean replicationStateIsInSync(ReplicationStateSync other) {
    return globalHiveSyncTool.getLastReplicatedTimeStampMap()
        .equals(other.globalHiveSyncTool.getLastReplicatedTimeStampMap());
  }

  @Override
  public String toString() {
    return  "{ clusterId: " + clusterId + " replicatedState: " + replicatedTimeStampMap + " }";
  }

  public String getClusterId() {
    return clusterId;
  }

  @Override
  public void close() {
    if (globalHiveSyncTool != null) {
      globalHiveSyncTool.close();
      globalHiveSyncTool = null;
    }
  }

}
