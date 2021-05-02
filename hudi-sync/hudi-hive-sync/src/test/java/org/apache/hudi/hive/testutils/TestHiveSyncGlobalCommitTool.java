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

package org.apache.hudi.hive.testutils;

import org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig;
import org.apache.hudi.hive.replication.HiveSyncGlobalCommitTool;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.hudi.common.table.HoodieTableGloballyConsistentMetaClient.GLOBALLY_CONSISTENT_READ_TIMESTAMP;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.LOCAL_BASE_PATH;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.LOCAL_HIVE_SERVER_JDBC_URLS;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.LOCAL_HIVE_SITE_URI;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.REMOTE_BASE_PATH;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.REMOTE_HIVE_SERVER_JDBC_URLS;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.REMOTE_HIVE_SITE_URI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHiveSyncGlobalCommitTool {

  TestCluster localCluster;
  TestCluster remoteCluster;

  private static String DB_NAME = "foo";
  private static String TBL_NAME = "bar";

  private HiveSyncGlobalCommitConfig getGlobalCommitConfig(
      String commitTime, String dbName, String tblName) throws Exception {
    HiveSyncGlobalCommitConfig config = new HiveSyncGlobalCommitConfig();
    config.properties.setProperty(LOCAL_HIVE_SITE_URI, localCluster.getHiveSiteXmlLocation());
    config.properties.setProperty(REMOTE_HIVE_SITE_URI, remoteCluster.getHiveSiteXmlLocation());
    config.properties.setProperty(LOCAL_HIVE_SERVER_JDBC_URLS, localCluster.getHiveJdBcUrl());
    config.properties.setProperty(REMOTE_HIVE_SERVER_JDBC_URLS, remoteCluster.getHiveJdBcUrl());
    config.properties.setProperty(LOCAL_BASE_PATH, localCluster.tablePath(dbName, tblName));
    config.properties.setProperty(REMOTE_BASE_PATH, remoteCluster.tablePath(dbName, tblName));
    config.globallyReplicatedTimeStamp = commitTime;
    config.hiveUser = System.getProperty("user.name");
    config.hivePass = "";
    config.databaseName = dbName;
    config.tableName = tblName;
    config.basePath = localCluster.tablePath(dbName, tblName);
    config.assumeDatePartitioning = true;
    config.usePreApacheInputFormat = false;
    config.partitionFields = Collections.singletonList("datestr");
    return config;
  }

  private void compareEqualLastReplicatedTimeStamp(HiveSyncGlobalCommitConfig config) throws Exception {
    assertEquals(localCluster.getHMSClient()
        .getTable(config.databaseName, config.tableName).getParameters()
        .get(GLOBALLY_CONSISTENT_READ_TIMESTAMP), remoteCluster.getHMSClient()
        .getTable(config.databaseName, config.tableName).getParameters()
        .get(GLOBALLY_CONSISTENT_READ_TIMESTAMP), "compare replicated timestamps");
  }

  @BeforeEach
  public void setUp() throws Exception {
    localCluster = new TestCluster();
    localCluster.setup();
    remoteCluster = new TestCluster();
    remoteCluster.setup();
    localCluster.forceCreateDb(DB_NAME);
    remoteCluster.forceCreateDb(DB_NAME);
    localCluster.dfsCluster.getFileSystem().delete(new Path(localCluster.tablePath(DB_NAME, TBL_NAME)), true);
    remoteCluster.dfsCluster.getFileSystem().delete(new Path(remoteCluster.tablePath(DB_NAME, TBL_NAME)), true);
  }

  @AfterEach
  public void clear() throws Exception {
    localCluster.getHMSClient().dropTable(DB_NAME, TBL_NAME);
    remoteCluster.getHMSClient().dropTable(DB_NAME, TBL_NAME);
    localCluster.shutDown();
    remoteCluster.shutDown();
  }

  @Test
  public void testBasicGlobalCommit() throws Exception {
    String commitTime = "100";
    localCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    // simulate drs
    remoteCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    HiveSyncGlobalCommitConfig config = getGlobalCommitConfig(commitTime, DB_NAME, TBL_NAME);
    HiveSyncGlobalCommitTool tool = new HiveSyncGlobalCommitTool(config);
    assertTrue(tool.commit());
    compareEqualLastReplicatedTimeStamp(config);
  }

  @Test
  public void testBasicRollback() throws Exception {
    String commitTime = "100";
    localCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    // simulate drs
    remoteCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    HiveSyncGlobalCommitConfig config = getGlobalCommitConfig(commitTime, DB_NAME, TBL_NAME);
    HiveSyncGlobalCommitTool tool = new HiveSyncGlobalCommitTool(config);
    assertFalse(localCluster.getHMSClient().tableExists(DB_NAME, TBL_NAME));
    assertFalse(remoteCluster.getHMSClient().tableExists(DB_NAME, TBL_NAME));
    // stop the remote cluster hive server to simulate cluster going down
    remoteCluster.stopHiveServer2();
    assertFalse(tool.commit());
    assertEquals(commitTime, localCluster.getHMSClient()
        .getTable(config.databaseName, config.tableName).getParameters()
        .get(GLOBALLY_CONSISTENT_READ_TIMESTAMP));
    assertTrue(tool.rollback()); // do a rollback
    assertNotEquals(commitTime, localCluster.getHMSClient()
        .getTable(config.databaseName, config.tableName).getParameters()
        .get(GLOBALLY_CONSISTENT_READ_TIMESTAMP));
    assertFalse(remoteCluster.getHMSClient().tableExists(DB_NAME, TBL_NAME));
    remoteCluster.startHiveServer2();
  }
}
