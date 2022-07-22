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

package org.apache.hudi.hive.replication;

import org.apache.hudi.hive.testutils.HiveTestCluster;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.apache.hudi.hadoop.utils.HoodieHiveUtils.GLOBALLY_CONSISTENT_READ_TIMESTAMP;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USER;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USE_PRE_APACHE_INPUT_FORMAT;
import static org.apache.hudi.hive.replication.GlobalHiveSyncConfig.META_SYNC_GLOBAL_REPLICATE_TIMESTAMP;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitParams.LOCAL_BASE_PATH;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitParams.LOCAL_HIVE_SERVER_JDBC_URLS;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitParams.LOCAL_HIVE_SITE_URI;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitParams.REMOTE_BASE_PATH;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitParams.REMOTE_HIVE_SERVER_JDBC_URLS;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitParams.REMOTE_HIVE_SITE_URI;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_ASSUME_DATE_PARTITION;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class TestHiveSyncGlobalCommitTool {

  @RegisterExtension
  public static HiveTestCluster localCluster = new HiveTestCluster();
  @RegisterExtension
  public static HiveTestCluster remoteCluster = new HiveTestCluster();

  private static final String DB_NAME = "foo";
  private static final String TBL_NAME = "bar";

  private HiveSyncGlobalCommitParams getGlobalCommitConfig(String commitTime) throws Exception {
    HiveSyncGlobalCommitParams params = new HiveSyncGlobalCommitParams();
    params.loadedProps.setProperty(LOCAL_HIVE_SITE_URI, localCluster.getHiveSiteXmlLocation());
    params.loadedProps.setProperty(REMOTE_HIVE_SITE_URI, remoteCluster.getHiveSiteXmlLocation());
    params.loadedProps.setProperty(LOCAL_HIVE_SERVER_JDBC_URLS, localCluster.getHiveJdBcUrl());
    params.loadedProps.setProperty(REMOTE_HIVE_SERVER_JDBC_URLS, remoteCluster.getHiveJdBcUrl());
    params.loadedProps.setProperty(LOCAL_BASE_PATH, localCluster.tablePath(DB_NAME, TBL_NAME));
    params.loadedProps.setProperty(REMOTE_BASE_PATH, remoteCluster.tablePath(DB_NAME, TBL_NAME));
    params.loadedProps.setProperty(META_SYNC_GLOBAL_REPLICATE_TIMESTAMP.key(), commitTime);
    params.loadedProps.setProperty(HIVE_USER.key(), System.getProperty("user.name"));
    params.loadedProps.setProperty(HIVE_PASS.key(), "");
    params.loadedProps.setProperty(META_SYNC_DATABASE_NAME.key(), DB_NAME);
    params.loadedProps.setProperty(META_SYNC_TABLE_NAME.key(), TBL_NAME);
    params.loadedProps.setProperty(META_SYNC_BASE_PATH.key(), localCluster.tablePath(DB_NAME, TBL_NAME));
    params.loadedProps.setProperty(META_SYNC_ASSUME_DATE_PARTITION.key(), "true");
    params.loadedProps.setProperty(HIVE_USE_PRE_APACHE_INPUT_FORMAT.key(), "false");
    params.loadedProps.setProperty(META_SYNC_PARTITION_FIELDS.key(), "datestr");
    return params;
  }

  private void compareEqualLastReplicatedTimeStamp(HiveSyncGlobalCommitParams config) throws Exception {
    assertEquals(localCluster.getHMSClient()
        .getTable(DB_NAME, TBL_NAME).getParameters()
        .get(GLOBALLY_CONSISTENT_READ_TIMESTAMP), remoteCluster.getHMSClient()
        .getTable(DB_NAME, TBL_NAME).getParameters()
        .get(GLOBALLY_CONSISTENT_READ_TIMESTAMP), "compare replicated timestamps");
  }

  @BeforeEach
  public void setUp() throws Exception {
    localCluster.forceCreateDb(DB_NAME);
    remoteCluster.forceCreateDb(DB_NAME);
    localCluster.dfsCluster.getFileSystem().delete(new Path(localCluster.tablePath(DB_NAME, TBL_NAME)), true);
    remoteCluster.dfsCluster.getFileSystem().delete(new Path(remoteCluster.tablePath(DB_NAME, TBL_NAME)), true);
  }

  @AfterEach
  public void clear() throws Exception {
    localCluster.getHMSClient().dropTable(DB_NAME, TBL_NAME);
    remoteCluster.getHMSClient().dropTable(DB_NAME, TBL_NAME);
  }

  @Test
  public void testHiveConfigShouldMatchClusterConf() throws Exception {
    String commitTime = "100";
    localCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    // simulate drs
    remoteCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    HiveSyncGlobalCommitParams params = getGlobalCommitConfig(commitTime);
    HiveSyncGlobalCommitTool tool = new HiveSyncGlobalCommitTool(params);
    ReplicationStateSync localReplicationStateSync = tool.getReplicatedState(false);
    ReplicationStateSync remoteReplicationStateSync = tool.getReplicatedState(true);
    assertEquals(localReplicationStateSync.globalHiveSyncTool.config.getHiveConf().get("hive.metastore.uris"),
        localCluster.getHiveConf().get("hive.metastore.uris"));
    assertEquals(remoteReplicationStateSync.globalHiveSyncTool.config.getHiveConf().get("hive.metastore.uris"),
        remoteCluster.getHiveConf().get("hive.metastore.uris"));
  }

  @Test
  public void testBasicGlobalCommit() throws Exception {
    String commitTime = "100";
    localCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    // simulate drs
    remoteCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    HiveSyncGlobalCommitParams params = getGlobalCommitConfig(commitTime);
    HiveSyncGlobalCommitTool tool = new HiveSyncGlobalCommitTool(params);
    assertTrue(tool.commit());
    compareEqualLastReplicatedTimeStamp(params);
  }

  @Test
  public void testBasicRollback() throws Exception {
    String commitTime = "100";
    localCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    // simulate drs
    remoteCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    HiveSyncGlobalCommitParams params = getGlobalCommitConfig(commitTime);
    HiveSyncGlobalCommitTool tool = new HiveSyncGlobalCommitTool(params);
    assertFalse(localCluster.getHMSClient().tableExists(DB_NAME, TBL_NAME));
    assertFalse(remoteCluster.getHMSClient().tableExists(DB_NAME, TBL_NAME));
    // stop the remote cluster hive server to simulate cluster going down
    remoteCluster.stopHiveServer2();
    assertFalse(tool.commit());
    assertEquals(commitTime, localCluster.getHMSClient()
        .getTable(DB_NAME, TBL_NAME).getParameters()
        .get(GLOBALLY_CONSISTENT_READ_TIMESTAMP));
    assertTrue(tool.rollback()); // do a rollback
    assertNotEquals(commitTime, localCluster.getHMSClient()
        .getTable(DB_NAME, TBL_NAME).getParameters()
        .get(GLOBALLY_CONSISTENT_READ_TIMESTAMP));
    assertFalse(remoteCluster.getHMSClient().tableExists(DB_NAME, TBL_NAME));
    remoteCluster.startHiveServer2();
  }
}
