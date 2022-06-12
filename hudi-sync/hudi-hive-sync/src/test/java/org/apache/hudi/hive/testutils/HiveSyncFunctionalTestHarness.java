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

package org.apache.hudi.hive.testutils;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.minicluster.ZookeeperTestService;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveClient;
import org.apache.hudi.hive.ddl.HiveQueryDDLExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Collections;

public class HiveSyncFunctionalTestHarness {

  private static transient Configuration hadoopConf;
  private static transient HiveTestService hiveTestService;
  private static transient ZookeeperTestService zookeeperTestService;

  /**
   * An indicator of the initialization status.
   */
  protected boolean initialized = false;
  @TempDir
  protected java.nio.file.Path tempDir;

  public String basePath() {
    return tempDir.toAbsolutePath().toString();
  }

  public Configuration hadoopConf() {
    return hadoopConf;
  }

  public FileSystem fs() throws IOException {
    return FileSystem.get(hadoopConf);
  }

  public HiveTestService hiveService() {
    return hiveTestService;
  }

  public HiveConf hiveConf() {
    return hiveTestService.getHiveServer().getHiveConf();
  }

  public ZookeeperTestService zkService() {
    return zookeeperTestService;
  }

  public HiveSyncConfig hiveSyncConf() throws IOException {
    HiveSyncConfig conf = new HiveSyncConfig();
    conf.hiveSyncConfigParams.jdbcUrl = hiveTestService.getJdbcHive2Url();
    conf.hiveSyncConfigParams.hiveUser = "";
    conf.hiveSyncConfigParams.hivePass = "";
    conf.hoodieSyncConfigParams.databaseName = "hivesynctestdb";
    conf.hoodieSyncConfigParams.tableName = "hivesynctesttable";
    conf.hoodieSyncConfigParams.basePath = Files.createDirectories(tempDir.resolve("hivesynctestcase-" + Instant.now().toEpochMilli())).toUri().toString();
    conf.hoodieSyncConfigParams.assumeDatePartitioning = true;
    conf.hiveSyncConfigParams.usePreApacheInputFormat = false;
    conf.hoodieSyncConfigParams.partitionFields = Collections.singletonList("datestr");
    return conf;
  }

  public HoodieHiveClient hiveClient(HiveSyncConfig hiveSyncConfig) throws IOException {
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(hiveSyncConfig.hoodieSyncConfigParams.tableName)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(hadoopConf, hiveSyncConfig.hoodieSyncConfigParams.basePath);
    return new HoodieHiveClient(hiveSyncConfig, hiveConf(), fs());
  }

  public void dropTables(String database, String... tables) throws IOException, HiveException, MetaException {
    HiveSyncConfig hiveSyncConfig = hiveSyncConf();
    hiveSyncConfig.hoodieSyncConfigParams.databaseName = database;
    for (String table : tables) {
      hiveSyncConfig.hoodieSyncConfigParams.tableName = table;
      new HiveQueryDDLExecutor(hiveSyncConfig, fs(), hiveConf()).runSQL("drop table if exists " + table);
    }
  }

  public void dropDatabases(String... databases) throws IOException, HiveException, MetaException {
    HiveSyncConfig hiveSyncConfig = hiveSyncConf();
    for (String database : databases) {
      hiveSyncConfig.hoodieSyncConfigParams.databaseName = database;
      new HiveQueryDDLExecutor(hiveSyncConfig, fs(), hiveConf()).runSQL("drop database if exists " + database);
    }
  }

  @BeforeEach
  public synchronized void runBeforeEach() throws IOException, InterruptedException {
    initialized = hiveTestService != null && zookeeperTestService != null;
    if (!initialized) {
      hadoopConf = new Configuration();
      zookeeperTestService = new ZookeeperTestService(hadoopConf);
      zookeeperTestService.start();
      hiveTestService = new HiveTestService(hadoopConf);
      hiveTestService.start();
    }
  }

  @AfterAll
  public static synchronized void cleanUpAfterAll() {
    if (hiveTestService != null) {
      hiveTestService.stop();
      hiveTestService = null;
    }
    if (zookeeperTestService != null) {
      zookeeperTestService.stop();
      zookeeperTestService = null;
    }
    if (hadoopConf != null) {
      hadoopConf.clear();
      hadoopConf = null;
    }
  }
}
