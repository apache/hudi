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
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncClient;
import org.apache.hudi.hive.ddl.HiveQueryDDLExecutor;
import org.apache.hudi.hive.util.IMetaStoreClientUtil;
import org.apache.hudi.storage.StorageConfiguration;

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
import java.util.Properties;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_URL;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USER;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USE_PRE_APACHE_INPUT_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;

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

  public StorageConfiguration<Configuration> storageConf() {
    return HadoopFSUtils.getStorageConf(hiveConf());
  }

  public ZookeeperTestService zkService() {
    return zookeeperTestService;
  }

  public HiveSyncConfig hiveSyncConf() throws IOException {
    Properties props = new Properties();
    props.setProperty(HIVE_URL.key(), hiveTestService.getJdbcHive2Url());
    props.setProperty(HIVE_USER.key(), "");
    props.setProperty(HIVE_PASS.key(), "");
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "hivesynctestdb");
    props.setProperty(META_SYNC_TABLE_NAME.key(), "hivesynctesttable");
    props.setProperty(META_SYNC_BASE_PATH.key(), Files.createDirectories(tempDir.resolve("hivesynctestcase-" + Instant.now().toEpochMilli())).toUri().toString());
    props.setProperty(HIVE_USE_PRE_APACHE_INPUT_FORMAT.key(), "false");
    props.setProperty(META_SYNC_PARTITION_FIELDS.key(), "datestr");
    return new HiveSyncConfig(props, hiveConf());
  }

  public HoodieHiveSyncClient hiveClient(HiveSyncConfig hiveSyncConfig) throws IOException {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(hiveSyncConfig.getString(META_SYNC_TABLE_NAME))
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(HadoopFSUtils.getStorageConfWithCopy(hadoopConf), hiveSyncConfig.getString(META_SYNC_BASE_PATH));
    return new HoodieHiveSyncClient(hiveSyncConfig, metaClient);
  }

  public void dropTables(String database, String... tables) throws IOException, HiveException, MetaException {
    HiveSyncConfig hiveSyncConfig = hiveSyncConf();
    hiveSyncConfig.setValue(META_SYNC_DATABASE_NAME, database);
    for (String table : tables) {
      hiveSyncConfig.setValue(META_SYNC_TABLE_NAME, table);
      new HiveQueryDDLExecutor(hiveSyncConfig, IMetaStoreClientUtil.getMSC(hiveSyncConfig.getHiveConf())).runSQL("drop table if exists " + table);
    }
  }

  public void dropDatabases(String... databases) throws IOException, HiveException, MetaException {
    HiveSyncConfig hiveSyncConfig = hiveSyncConf();
    for (String database : databases) {
      hiveSyncConfig.setValue(META_SYNC_DATABASE_NAME, database);
      new HiveQueryDDLExecutor(hiveSyncConfig, IMetaStoreClientUtil.getMSC(hiveSyncConfig.getHiveConf())).runSQL("drop database if exists " + database);
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
