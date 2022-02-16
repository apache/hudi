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

package org.apache.hudi.utilities;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.hive.HoodieHiveClient;
import org.apache.hudi.hive.testutils.HiveTestUtil;
import org.apache.hudi.utilities.exception.HoodieIncrementalPullSQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;

import static org.apache.hudi.hive.testutils.HiveTestUtil.fileSystem;
import static org.apache.hudi.hive.testutils.HiveTestUtil.hiveSyncConfig;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHiveIncrementalPuller {

  private HiveIncrementalPuller.Config config;
  private String targetBasePath = null;

  @BeforeEach
  public void setup() throws HiveException, IOException, InterruptedException, MetaException {
    config = new HiveIncrementalPuller.Config();
    HiveTestUtil.setUp();
  }

  @AfterEach
  public void teardown() throws Exception {
    HiveTestUtil.clearIncrementalPullSetup(config.hoodieTmpDir, targetBasePath);
  }

  @Test
  public void testInitHiveIncrementalPuller() {

    assertDoesNotThrow(() -> {
      new HiveIncrementalPuller(config);
    }, "Unexpected exception while initing HiveIncrementalPuller.");

  }

  private HiveIncrementalPuller.Config getHivePullerConfig(String incrementalSql) throws IOException {
    config.hiveJDBCUrl = hiveSyncConfig.jdbcUrl;
    config.hiveUsername = hiveSyncConfig.hiveUser;
    config.hivePassword = hiveSyncConfig.hivePass;
    config.hoodieTmpDir = Files.createTempDirectory("hivePullerTest").toUri().toString();
    config.sourceDb = hiveSyncConfig.databaseName;
    config.sourceTable = hiveSyncConfig.tableName;
    config.targetDb = "tgtdb";
    config.targetTable = "test2";
    config.tmpDb = "tmp_db";
    config.fromCommitTime = "100";
    createIncrementalSqlFile(incrementalSql, config);
    return config;
  }

  private void createIncrementalSqlFile(String text, HiveIncrementalPuller.Config cfg) throws IOException {
    java.nio.file.Path path = Paths.get(cfg.hoodieTmpDir + "/incremental_pull.txt");
    Files.createDirectories(path.getParent());
    Files.createFile(path);
    try (FileWriter fr = new FileWriter(new File(path.toUri()))) {
      fr.write(text);
    } catch (Exception e) {
      // no-op
    }
    cfg.incrementalSQLFile = path.toString();
  }

  private void createSourceTable() throws IOException, URISyntaxException {
    String instantTime = "101";
    HiveTestUtil.createCOWTable(instantTime, 5, true);
    hiveSyncConfig.syncMode = "jdbc";
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 3;
    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, HiveTestUtil.getHiveConf(), fileSystem);
    tool.syncHoodieTable();
  }

  private void createTargetTable() throws IOException, URISyntaxException {
    String instantTime = "100";
    targetBasePath = Files.createTempDirectory("hivesynctest1" + Instant.now().toEpochMilli()).toUri().toString();
    HiveTestUtil.createCOWTable(instantTime, 5, true,
            targetBasePath, "tgtdb", "test2");
    HiveSyncTool tool = new HiveSyncTool(getTargetHiveSyncConfig(targetBasePath), HiveTestUtil.getHiveConf(), fileSystem);
    tool.syncHoodieTable();
  }

  private HiveSyncConfig getTargetHiveSyncConfig(String basePath) {
    HiveSyncConfig config = HiveSyncConfig.copy(hiveSyncConfig);
    config.databaseName = "tgtdb";
    config.tableName = "test2";
    config.basePath = basePath;
    config.batchSyncNum = 3;
    config.syncMode = "jdbc";
    return config;
  }

  private HiveSyncConfig getAssertionSyncConfig(String databaseName) {
    HiveSyncConfig config = HiveSyncConfig.copy(hiveSyncConfig);
    config.databaseName = databaseName;
    return config;
  }

  private void createTables() throws IOException, URISyntaxException {
    createSourceTable();
    createTargetTable();
  }

  @Test
  public void testPullerWithoutIncrementalClause() throws IOException, URISyntaxException {
    createTables();
    HiveIncrementalPuller puller = new HiveIncrementalPuller(getHivePullerConfig(
            "select name from testdb.test1"));
    Exception e = assertThrows(HoodieIncrementalPullSQLException.class, puller::saveDelta,
            "Should fail when incremental clause not provided!");
    assertTrue(e.getMessage().contains("Incremental SQL does not have clause `_hoodie_commit_time` > '%s', which means its not pulling incrementally"));
  }

  @Test
  public void testPullerWithoutSourceInSql() throws IOException, URISyntaxException {
    createTables();
    HiveIncrementalPuller puller = new HiveIncrementalPuller(getHivePullerConfig(
            "select name from tgtdb.test2 where `_hoodie_commit_time` > '%s'"));
    Exception e = assertThrows(HoodieIncrementalPullSQLException.class, puller::saveDelta,
            "Should fail when source db and table names not provided!");
    assertTrue(e.getMessage().contains("Incremental SQL does not have testdb.test1"));
  }

  @Test
  public void testPuller() throws IOException, URISyntaxException {
    createTables();
    HiveIncrementalPuller.Config cfg = getHivePullerConfig("select name from testdb.test1 where `_hoodie_commit_time` > '%s'");
    HoodieHiveClient hiveClient = new HoodieHiveClient(hiveSyncConfig, HiveTestUtil.getHiveConf(), fileSystem);
    hiveClient.createDatabase(cfg.tmpDb);
    HiveIncrementalPuller puller = new HiveIncrementalPuller(cfg);
    puller.saveDelta();
    HiveSyncConfig assertingConfig = getAssertionSyncConfig(cfg.tmpDb);
    HoodieHiveClient assertingClient = new HoodieHiveClient(assertingConfig, HiveTestUtil.getHiveConf(), fileSystem);
    String tmpTable = cfg.targetTable + "__" + cfg.sourceTable;
    assertTrue(assertingClient.doesTableExist(tmpTable));
  }

}
