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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.hive.HoodieHiveSyncClient;
import org.apache.hudi.hive.testutils.HiveTestUtil;
import org.apache.hudi.utilities.exception.HoodieIncrementalPullSQLException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_MODE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_URL;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USER;
import static org.apache.hudi.hive.testutils.HiveTestUtil.hiveSyncProps;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class TestHiveIncrementalPuller {

  private final HiveIncrementalPuller.Config config = new HiveIncrementalPuller.Config();
  @TempDir
  java.nio.file.Path tempDir;

  @AfterAll
  public static void cleanUpClass() throws Exception {
    HiveTestUtil.shutdown();
  }

  @BeforeEach
  public void setUp() throws Exception {
    HiveTestUtil.setUp(Option.empty(), true);
  }

  @AfterEach
  public void tearDown() throws Exception {
    HiveTestUtil.clear();
  }

  @Test
  public void testInitHiveIncrementalPuller() {
    assertDoesNotThrow(() -> {
      new HiveIncrementalPuller(config);
    }, "Unexpected exception while initializing HiveIncrementalPuller.");
  }

  private HiveIncrementalPuller.Config getHivePullerConfig(String incrementalSql) throws IOException {
    config.hiveJDBCUrl = hiveSyncProps.getString(HIVE_URL.key());
    config.hiveUsername = hiveSyncProps.getString(HIVE_USER.key());
    config.hivePassword = hiveSyncProps.getString(HIVE_PASS.key());
    config.hoodieTmpDir = tempDir.resolve("hivePullerTest").toAbsolutePath().toString();
    config.sourceDb = hiveSyncProps.getString(META_SYNC_DATABASE_NAME.key());
    config.sourceTable = hiveSyncProps.getString(META_SYNC_TABLE_NAME.key());
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
    hiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), "jdbc");
    try (HiveSyncTool tool = new HiveSyncTool(hiveSyncProps, HiveTestUtil.getHiveConf())) {
      tool.syncHoodieTable();
    }
  }

  private void createTargetTable() throws IOException, URISyntaxException {
    String instantTime = "100";
    String targetBasePath = tempDir.resolve("target_table").toAbsolutePath().toString();
    HiveTestUtil.createCOWTable(instantTime, 5, true,
        targetBasePath, "tgtdb", "test2");
    try (HiveSyncTool tool = new HiveSyncTool(getTargetHiveSyncConfig(targetBasePath), HiveTestUtil.getHiveConf())) {
      tool.syncHoodieTable();
    }
  }

  private TypedProperties getTargetHiveSyncConfig(String basePath) {
    TypedProperties targetHiveSyncProps = TypedProperties.copy(hiveSyncProps);
    targetHiveSyncProps.setProperty(META_SYNC_DATABASE_NAME.key(), "tgtdb");
    targetHiveSyncProps.setProperty(META_SYNC_TABLE_NAME.key(), "test2");
    targetHiveSyncProps.setProperty(META_SYNC_BASE_PATH.key(), basePath);
    targetHiveSyncProps.setProperty(HIVE_SYNC_MODE.key(), "jdbc");

    return targetHiveSyncProps;
  }

  private TypedProperties getAssertionSyncConfig(String databaseName) {
    TypedProperties assertHiveSyncProps = TypedProperties.copy(hiveSyncProps);
    assertHiveSyncProps.setProperty(META_SYNC_DATABASE_NAME.key(), databaseName);
    return assertHiveSyncProps;
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
  @Disabled("Disable due to hive not support avro 1.10.2.")
  public void testPuller() throws IOException, URISyntaxException {
    createTables();
    HiveIncrementalPuller.Config cfg = getHivePullerConfig("select name from testdb.test1 where `_hoodie_commit_time` > '%s'");
    HoodieHiveSyncClient hiveClient = new HoodieHiveSyncClient(new HiveSyncConfig(hiveSyncProps, HiveTestUtil.getHiveConf()), mock(HoodieTableMetaClient.class));
    hiveClient.createDatabase(cfg.tmpDb);
    HiveIncrementalPuller puller = new HiveIncrementalPuller(cfg);
    puller.saveDelta();
    HoodieHiveSyncClient assertingClient = new HoodieHiveSyncClient(new HiveSyncConfig(getAssertionSyncConfig(cfg.tmpDb), HiveTestUtil.getHiveConf()), mock(HoodieTableMetaClient.class));
    String tmpTable = cfg.targetTable + "__" + cfg.sourceTable;
    assertTrue(assertingClient.tableExists(tmpTable));
  }

}
