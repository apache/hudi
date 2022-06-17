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

package org.apache.hudi.hive;

import org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig;
import org.apache.hudi.hive.replication.HiveSyncGlobalCommitTool;
import org.apache.hudi.hive.testutils.TestCluster;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.apache.hudi.hadoop.utils.HoodieHiveUtils.GLOBALLY_CONSISTENT_READ_TIMESTAMP;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_USER;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_USE_PRE_APACHE_INPUT_FORMAT;
import static org.apache.hudi.hive.replication.GlobalHiveSyncConfig.META_SYNC_GLOBAL_REPLICATE_TIMESTAMP;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.LOCAL_BASE_PATH;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.LOCAL_HIVE_SERVER_JDBC_URLS;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.LOCAL_HIVE_SITE_URI;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.REMOTE_BASE_PATH;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.REMOTE_HIVE_SERVER_JDBC_URLS;
import static org.apache.hudi.hive.replication.HiveSyncGlobalCommitConfig.REMOTE_HIVE_SITE_URI;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_ASSUME_DATE_PARTITION;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;

public class TestHiveSyncGlobalCommitTool {

  @RegisterExtension
  public static TestCluster localCluster = new TestCluster();
  @RegisterExtension
  public static TestCluster remoteCluster = new TestCluster();

  private static final String DB_NAME = "foo";
  private static final String TBL_NAME = "bar";

  private HiveSyncGlobalCommitConfig getGlobalCommitConfig(String commitTime) throws Exception {
    HiveSyncGlobalCommitConfig config = new HiveSyncGlobalCommitConfig();
    config.properties.setProperty(LOCAL_HIVE_SITE_URI, localCluster.getHiveSiteXmlLocation());
    config.properties.setProperty(REMOTE_HIVE_SITE_URI, remoteCluster.getHiveSiteXmlLocation());
    config.properties.setProperty(LOCAL_HIVE_SERVER_JDBC_URLS, localCluster.getHiveJdBcUrl());
    config.properties.setProperty(REMOTE_HIVE_SERVER_JDBC_URLS, remoteCluster.getHiveJdBcUrl());
    config.properties.setProperty(LOCAL_BASE_PATH, localCluster.tablePath(DB_NAME, TBL_NAME));
    config.properties.setProperty(REMOTE_BASE_PATH, remoteCluster.tablePath(DB_NAME, TBL_NAME));
    config.properties.setProperty(META_SYNC_GLOBAL_REPLICATE_TIMESTAMP.key(), commitTime);
    config.properties.setProperty(HIVE_USER.key(), System.getProperty("user.name"));
    config.properties.setProperty(HIVE_PASS.key(), "");
    config.properties.setProperty(META_SYNC_DATABASE_NAME.key(), DB_NAME);
    config.properties.setProperty(META_SYNC_TABLE_NAME.key(), TBL_NAME);
    config.properties.setProperty(META_SYNC_BASE_PATH.key(), localCluster.tablePath(DB_NAME, TBL_NAME));
    config.properties.setProperty(META_SYNC_ASSUME_DATE_PARTITION.key(), "true");
    config.properties.setProperty(HIVE_USE_PRE_APACHE_INPUT_FORMAT.key(), "false");
    config.properties.setProperty(META_SYNC_PARTITION_FIELDS.key(), "datestr");
    return config;
  }

  private void compareEqualLastReplicatedTimeStamp(HiveSyncGlobalCommitConfig config) throws Exception {
    Assertions.assertEquals(localCluster.getHMSClient()
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
  public void testBasicGlobalCommit() throws Exception {
    String commitTime = "100";
    localCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    // simulate drs
    remoteCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    HiveSyncGlobalCommitConfig config = getGlobalCommitConfig(commitTime);
    HiveSyncGlobalCommitTool tool = new HiveSyncGlobalCommitTool(config);
    Assertions.assertTrue(tool.commit());
    compareEqualLastReplicatedTimeStamp(config);
  }

  @Test
  public void testBasicRollback() throws Exception {
    String commitTime = "100";
    localCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    // simulate drs
    remoteCluster.createCOWTable(commitTime, 5, DB_NAME, TBL_NAME);
    HiveSyncGlobalCommitConfig config = getGlobalCommitConfig(commitTime);
    HiveSyncGlobalCommitTool tool = new HiveSyncGlobalCommitTool(config);
    Assertions.assertFalse(localCluster.getHMSClient().tableExists(DB_NAME, TBL_NAME));
    Assertions.assertFalse(remoteCluster.getHMSClient().tableExists(DB_NAME, TBL_NAME));
    // stop the remote cluster hive server to simulate cluster going down
    remoteCluster.stopHiveServer2();
    Assertions.assertFalse(tool.commit());
    Assertions.assertEquals(commitTime, localCluster.getHMSClient()
        .getTable(DB_NAME, TBL_NAME).getParameters()
        .get(GLOBALLY_CONSISTENT_READ_TIMESTAMP));
    Assertions.assertTrue(tool.rollback()); // do a rollback
    Assertions.assertNotEquals(commitTime, localCluster.getHMSClient()
        .getTable(DB_NAME, TBL_NAME).getParameters()
        .get(GLOBALLY_CONSISTENT_READ_TIMESTAMP));
    Assertions.assertFalse(remoteCluster.getHMSClient().tableExists(DB_NAME, TBL_NAME));
    remoteCluster.startHiveServer2();
  }
}
