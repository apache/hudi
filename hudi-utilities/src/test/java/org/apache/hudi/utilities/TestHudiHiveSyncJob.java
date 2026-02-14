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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncClient;
import org.apache.hudi.hive.testutils.HiveTestUtil;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.nio.file.Path;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_MODE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_URL;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USER;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test cases for {@link HudiHiveSyncJob}.
 */
public class TestHudiHiveSyncJob {

  @TempDir
  Path tempDir;

  @BeforeAll
  static void setUpClass() throws Exception {
    HiveTestUtil.setUp(Option.empty(), true);
  }

  @AfterAll
  static void cleanUpClass() {
    try {
      HiveTestUtil.shutdown();
    } catch (Throwable t) {
      // no-op for cleanup failures in tests
    }
  }

  @Test
  void testRunRegistersUnregisteredHudiDatasetInMetastore() throws Exception {
    String tableName = "hive_sync_job_" + UUID.randomUUID().toString().replace("-", "");
    String basePath = Files.createDirectory(tempDir.resolve("hudi-table")).toUri().toString();
    String databaseName = "default";
    HudiHiveSyncJob.Config cfg = new HudiHiveSyncJob.Config();
    cfg.basePath = basePath;
    cfg.baseFileFormat = "PARQUET";
    cfg.configs.add("hoodie.datasource.hive_sync.database=" + databaseName);
    cfg.configs.add("hoodie.datasource.hive_sync.table=" + tableName);
    cfg.configs.add("hoodie.datasource.meta.sync.database=" + databaseName);
    cfg.configs.add("hoodie.datasource.meta.sync.table=" + tableName);
    cfg.configs.add(HIVE_SYNC_MODE.key() + "=jdbc");
    cfg.configs.add(HIVE_URL.key() + "=" + HiveTestUtil.hiveSyncProps.getString(HIVE_URL.key()));
    cfg.configs.add(HIVE_USER.key() + "=" + HiveTestUtil.hiveSyncProps.getString(HIVE_USER.key()));
    cfg.configs.add(HIVE_PASS.key() + "=" + HiveTestUtil.hiveSyncProps.getString(HIVE_PASS.key()));

    JavaSparkContext jsc = null;
    SparkSession spark = null;
    try {
      jsc = UtilHelpers.buildSparkContext("test-hudi-hive-sync-job", "local[2]", false);
      spark = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate();

      StructType schema = new StructType()
          .add("id", DataTypes.StringType, false)
          .add("name", DataTypes.StringType, true)
          .add("ts", DataTypes.LongType, false);
      Dataset<Row> source = spark.createDataFrame(
          Arrays.asList(
              RowFactory.create("1", "a1", 1000L),
              RowFactory.create("2", "a2", 1001L)),
          schema);

      // Write Hudi dataset by path only: this should create commits but not register a metastore table.
      source.write().format("hudi")
          .option("hoodie.table.name", tableName)
          .option("hoodie.datasource.write.recordkey.field", "id")
          .option("hoodie.datasource.write.precombine.field", "ts")
          .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
          .option("hoodie.datasource.write.partitionpath.field", "")
          .mode("overwrite")
          .save(basePath);

      assertFalse(tableExists(databaseName, tableName, basePath, spark));

      new HudiHiveSyncJob(jsc, cfg).run();

      assertTrue(tableExists(databaseName, tableName, basePath, spark));
    } finally {
      if (spark != null) {
        spark.close();
      }
      if (jsc != null) {
        jsc.stop();
      }
    }
  }

  private boolean tableExists(String dbName, String tableName, String basePath, SparkSession spark) {
    String catalogImpl = spark.conf().get("spark.sql.catalogImplementation", "in-memory")
        .toLowerCase(Locale.ROOT);
    if ("hive".equals(catalogImpl)) {
      return spark.catalog().tableExists(dbName, tableName);
    }
    return tableExistsInMetastore(dbName, tableName, basePath);
  }

  private boolean tableExistsInMetastore(String dbName, String tableName, String basePath) {
    TypedProperties props = TypedProperties.copy(HiveTestUtil.hiveSyncProps);
    props.setProperty(META_SYNC_DATABASE_NAME.key(), dbName);
    props.setProperty(META_SYNC_TABLE_NAME.key(), tableName);
    props.setProperty(META_SYNC_BASE_PATH.key(), basePath);
    try (HoodieHiveSyncClient client = new HoodieHiveSyncClient(
        new HiveSyncConfig(props, HiveTestUtil.getHiveConf()),
        mock(HoodieTableMetaClient.class))) {
      return client.tableExists(tableName);
    }
  }
}
