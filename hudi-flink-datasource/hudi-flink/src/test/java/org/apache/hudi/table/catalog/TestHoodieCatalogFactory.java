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

package org.apache.hudi.table.catalog;

import org.apache.hudi.adapter.TestTableEnvs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.table.catalog.CatalogOptions.CATALOG_PATH;
import static org.apache.hudi.table.catalog.CatalogOptions.DEFAULT_DATABASE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test cases for {@link HoodieCatalogFactory}.
 */
public class TestHoodieCatalogFactory {
  private static final URL CONF_DIR =
      Thread.currentThread().getContextClassLoader().getResource("test-catalog-factory-conf");

  @TempDir
  File tempFile;

  @Test
  void testCreateCatalogThroughSQL() {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String catalogDDL = ""
        + "create catalog hudi_catalog\n"
        + "  with(\n"
        + "    'type' = 'hudi',\n"
        + "    'catalog.path' = '" + tempFile.getAbsolutePath() + "/warehouse',\n"
        + "    'mode' = 'hms',\n"
        + "    'hive.conf.dir' = '" + CONF_DIR.getPath() + "',\n"
        + "    'table.external' = 'true'\n"
        + "  )\n";
    RuntimeException exception = assertThrows(RuntimeException.class, () -> tableEnv.executeSql(catalogDDL));
    assertThat(exception.getMessage(), containsString("hive metastore"));
  }

  @Test
  void testCreateHMSCatalog() {
    final String catalogName = "mycatalog";

    final HoodieHiveCatalog expectedCatalog = HoodieCatalogTestUtils.createHiveCatalog(catalogName);

    final Map<String, String> options = new HashMap<>();
    options.put(CommonCatalogOptions.CATALOG_TYPE.key(), HoodieCatalogFactory.IDENTIFIER);
    options.put(CatalogOptions.HIVE_CONF_DIR.key(), CONF_DIR.getPath());
    options.put(CatalogOptions.MODE.key(), "hms");
    options.put(CatalogOptions.TABLE_EXTERNAL.key(), "false");

    final Catalog actualCatalog =
        FactoryUtil.createCatalog(
            catalogName, options, null, Thread.currentThread().getContextClassLoader());

    assertEquals(
        ((HoodieHiveCatalog) actualCatalog)
            .getHiveConf()
            .getVar(HiveConf.ConfVars.METASTOREURIS), "dummy-hms");
    checkEquals(expectedCatalog, (HoodieHiveCatalog) actualCatalog);
  }

  @Test
  void testCreateDFSCatalog() {
    final String catalogName = "mycatalog";

    Map<String, String> catalogOptions = new HashMap<>();
    catalogOptions.put(CATALOG_PATH.key(), tempFile.getAbsolutePath());
    catalogOptions.put(DEFAULT_DATABASE.key(), "test_db");
    HoodieCatalog expectedCatalog = new HoodieCatalog(catalogName, Configuration.fromMap(catalogOptions));

    final Map<String, String> options = new HashMap<>();
    options.put(CommonCatalogOptions.CATALOG_TYPE.key(), HoodieCatalogFactory.IDENTIFIER);
    options.put(CATALOG_PATH.key(), tempFile.getAbsolutePath());
    options.put(DEFAULT_DATABASE.key(), "test_db");
    options.put(CatalogOptions.MODE.key(), "dfs");

    final Catalog actualCatalog =
        FactoryUtil.createCatalog(
            catalogName, options, null, Thread.currentThread().getContextClassLoader());

    checkEquals(expectedCatalog, (AbstractCatalog) actualCatalog);
  }

  private static void checkEquals(AbstractCatalog c1, AbstractCatalog c2) {
    // Only assert a few selected properties for now
    assertEquals(c2.getName(), c1.getName());
    assertEquals(c2.getDefaultDatabase(), c1.getDefaultDatabase());
  }
}
