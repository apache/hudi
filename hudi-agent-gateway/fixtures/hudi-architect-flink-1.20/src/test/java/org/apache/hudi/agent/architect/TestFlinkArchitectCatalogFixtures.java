/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.agent.architect;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieCatalogException;
import org.apache.hudi.hive.HiveSyncConfigHolder;
import org.apache.hudi.sink.utils.HiveSyncContext;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.hudi.table.catalog.CatalogOptions;
import org.apache.hudi.table.catalog.HoodieCatalog;
import org.apache.hudi.table.catalog.HoodieCatalogFactory;
import org.apache.hudi.table.catalog.HoodieHiveCatalog;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Released-bundle evidence for the bounded PR6 catalog composition contract. */
class TestFlinkArchitectCatalogFixtures {

  @Test
  void testPinnedCatalogFactorySelectsDfsAndHms() {
    HoodieCatalogFactory factory = new HoodieCatalogFactory();

    Catalog dfs = factory.createCatalog(context("lake", Map.of(
        "mode", "dfs",
        "catalog.path", "file:///tmp/lake")));
    Catalog hms = factory.createCatalog(context("hms", Map.of(
        "mode", "hms",
        "hadoop.hive.metastore.uris", "thrift://hms.example:9083",
        "table.external", "true")));

    assertInstanceOf(HoodieCatalog.class, dfs);
    assertInstanceOf(HoodieHiveCatalog.class, hms);
  }

  @Test
  void testPinnedCatalogFactoryRejectsUnknownMode() {
    HoodieCatalogFactory factory = new HoodieCatalogFactory();

    HoodieCatalogException exception = assertThrows(
        HoodieCatalogException.class,
        () -> factory.createCatalog(context("unsupported", Map.of("mode", "unknown"))));

    assertTrue(exception.getMessage().contains("supported modes: [hms, dfs]"));
  }

  @Test
  void testPinnedDfsCatalogPathDerivation() {
    Configuration options = new Configuration();
    options.set(CatalogOptions.CATALOG_PATH, "file:///tmp/lake");
    ExposedHoodieCatalog catalog = new ExposedHoodieCatalog("lake", options);

    assertEquals(
        "file:///tmp/lake/analytics/orders",
        catalog.infer("file:///tmp/lake", new ObjectPath("analytics", "orders")));
  }

  @Test
  void testPinnedHmsCatalogExternalFlag() throws Exception {
    Configuration options = new Configuration();
    options.set(CatalogOptions.TABLE_EXTERNAL, true);
    HoodieHiveCatalog catalog = new HoodieHiveCatalog(
        "hms", options, new HiveConf(), true);
    Field external = HoodieHiveCatalog.class.getDeclaredField("external");
    external.setAccessible(true);

    assertTrue(external.getBoolean(catalog));
  }

  @Test
  void testPinnedHmsCatalogRejectsEmbeddedMetastore() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HoodieHiveCatalog("hms", new Configuration()));
  }

  @Test
  void testPinnedDfsCatalogDdlAndInsertPlan(@TempDir Path tempDir) {
    StreamExecutionEnvironment environment =
        StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
        environment, EnvironmentSettings.newInstance().inStreamingMode().build());
    String catalogPath = tempDir.toUri().toString();
    tableEnv.executeSql(
        "CREATE CATALOG `lake` WITH ("
            + "'type'='hudi','mode'='dfs','catalog.path'='"
            + catalogPath
            + "','default-database'='analytics')");
    tableEnv.executeSql("USE CATALOG `lake`");
    tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS `analytics`");
    tableEnv.executeSql("USE `analytics`");
    tableEnv.createTemporaryView(
        "orders_source",
        tableEnv.fromValues(
            DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                DataTypes.FIELD("payload", DataTypes.STRING())),
            Row.of("order-1", "created")));
    tableEnv.executeSql(
        "CREATE TABLE `orders` ("
            + "`id` STRING NOT NULL, `payload` STRING, "
            + "PRIMARY KEY (`id`) NOT ENFORCED) WITH ("
            + "'connector'='hudi','table.type'='COPY_ON_WRITE',"
            + "'write.operation'='insert','write.insert.cluster'='false')");

    String plan = tableEnv.explainSql(
        "INSERT INTO `orders` (`id`, `payload`) "
            + "SELECT `id`, `payload` FROM `orders_source`");

    assertTrue(plan.contains("lake.analytics.orders"), plan);
    assertTrue(Files.exists(tempDir.resolve("analytics/orders/.hoodie")));
  }

  @Test
  void testPinnedMorHiveSyncNamingDefaults() {
    Configuration configuration = new Configuration();
    configuration.set(FlinkOptions.PATH, "file:///tmp/orders");
    configuration.set(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");
    configuration.set(FlinkOptions.HIVE_SYNC_DB, "analytics");
    configuration.set(FlinkOptions.HIVE_SYNC_TABLE, "orders");
    Properties properties = HiveSyncContext.buildSyncConfig(configuration);

    assertEquals(
        "false",
        properties.getProperty(HiveSyncConfigHolder.HIVE_IGNORE_EXCEPTIONS.key()));
    assertEquals(
        "false",
        properties.getProperty(
            HiveSyncConfigHolder.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE.key()));
    assertEquals(
        "true",
        properties.getProperty(HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE.key()));
    assertEquals(
        "ALL",
        properties.getProperty(HiveSyncConfigHolder.HIVE_SYNC_TABLE_STRATEGY.key()));
    assertEquals(
        "true",
        HoodieSyncConfig.META_SYNC_SNAPSHOT_WITH_TABLE_NAME.defaultValue());
  }

  private static CatalogFactory.Context context(String name, Map<String, String> values) {
    Map<String, String> options = new HashMap<>(values);
    Configuration configuration = Configuration.fromMap(options);
    return new CatalogFactory.Context() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public Map<String, String> getOptions() {
        return options;
      }

      @Override
      public ReadableConfig getConfiguration() {
        return configuration;
      }

      @Override
      public ClassLoader getClassLoader() {
        return TestFlinkArchitectCatalogFixtures.class.getClassLoader();
      }
    };
  }

  private static final class ExposedHoodieCatalog extends HoodieCatalog {
    private ExposedHoodieCatalog(String name, Configuration options) {
      super(name, options);
    }

    private String infer(String catalogPath, ObjectPath tablePath) {
      return inferTablePath(catalogPath, tablePath);
    }
  }
}
