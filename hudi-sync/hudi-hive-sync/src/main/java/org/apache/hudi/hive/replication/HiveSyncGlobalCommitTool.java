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

import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.LOCAL_HIVE_SITE_URI;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.REMOTE_HIVE_SITE_URI;

import com.beust.jcommander.JCommander;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HiveSyncGlobalCommitTool implements HiveSyncGlobalCommit, AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(HiveSyncGlobalCommitTool.class);
  private final HiveSyncGlobalCommitConfig config;
  private List<ReplicationStateSync> replicationStateSyncList;

  private ReplicationStateSync getReplicatedState(boolean forRemote) {
    HiveConf hiveConf = new HiveConf();
    // we probably just need to set the metastore URIs
    // TODO: figure out how to integrate this in production
    // how to load balance between piper HMS,HS2
    // if we have list of uris, we can do something similar to createHiveConf in reairsync
    hiveConf.addResource(new Path(config.properties.getProperty(
        forRemote ? REMOTE_HIVE_SITE_URI : LOCAL_HIVE_SITE_URI)));
    // TODO: get clusterId as input parameters
    ReplicationStateSync state = new ReplicationStateSync(config.mkGlobalHiveSyncConfig(forRemote),
        hiveConf, forRemote ? "REMOTESYNC" : "LOCALSYNC");
    return state;
  }

  @Override
  public boolean commit() {
    // TODO: add retry attempts
    String name = Thread.currentThread().getName();
    try {
      for (ReplicationStateSync stateSync : replicationStateSyncList) {
        Thread.currentThread().setName(stateSync.getClusterId());
        LOG.info("starting sync for state " + stateSync);
        stateSync.sync();
        LOG.info("synced state " + stateSync);
      }
    } catch (Exception e) {
      Thread.currentThread().setName(name);
      LOG.error(String.format("Error while trying to commit replication state %s", e.getMessage()), e);
      return false;
    } finally {
      Thread.currentThread().setName(name);
    }

    LOG.info("done syncing to all tables, verifying the timestamps...");
    ReplicationStateSync base = replicationStateSyncList.get(0);
    boolean success = true;
    LOG.info("expecting all timestamps to be similar to: " + base);
    for (int idx = 1; idx < replicationStateSyncList.size(); ++idx) {
      ReplicationStateSync other = replicationStateSyncList.get(idx);
      if (!base.replicationStateIsInSync(other)) {
        LOG.error("the timestamp of other : " + other + " is not matching with base: " + base);
        success = false;
      }
    }
    return success;
  }

  @Override
  public boolean rollback() {
    for (ReplicationStateSync stateSync : replicationStateSyncList) {
      stateSync.rollback();
    }
    return true;
  }

  public HiveSyncGlobalCommitTool(HiveSyncGlobalCommitConfig config) {
    this.config = config;
    this.replicationStateSyncList = new ArrayList<>(2);
    this.replicationStateSyncList.add(getReplicatedState(false));
    this.replicationStateSyncList.add(getReplicatedState(true));
  }

  private static HiveSyncGlobalCommitConfig getHiveSyncGlobalCommitConfig(String[] args)
      throws IOException {
    HiveSyncGlobalCommitConfig cfg = new HiveSyncGlobalCommitConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    cfg.load();
    return cfg;
  }

  @Override
  public void close() {
    for (ReplicationStateSync stateSync : replicationStateSyncList) {
      stateSync.close();
    }
  }

  public static void main(String[] args) throws IOException, HoodieHiveSyncException {
    final HiveSyncGlobalCommitConfig cfg = getHiveSyncGlobalCommitConfig(args);
    try (final HiveSyncGlobalCommitTool globalCommitTool = new HiveSyncGlobalCommitTool(cfg)) {
      boolean success = globalCommitTool.commit();
      if (!success) {
        if (!globalCommitTool.rollback()) {
          throw new RuntimeException("not able to rollback failed commit");
        }
      }
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "not able to commit replicated timestamp", e);
    }
  }
}
