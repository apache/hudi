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

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.hadoop.hive.conf.HiveConf;

import java.io.IOException;

/**
 * Test utils for Hoodie catalog.
 */
public class HoodieCatalogTestUtils {
  private static final String HIVE_WAREHOUSE_URI_FORMAT =
      "jdbc:derby:;databaseName=%s;create=true";

  private static final String TEST_CATALOG_NAME = "test_catalog";

  private static final org.junit.rules.TemporaryFolder TEMPORARY_FOLDER = new org.junit.rules.TemporaryFolder();

  /**
   * Create a HiveCatalog with an embedded Hive Metastore.
   */
  public static HoodieHiveCatalog createHiveCatalog() {
    return createHiveCatalog(TEST_CATALOG_NAME);
  }

  public static HoodieHiveCatalog createHiveCatalog(String name) {
    return new HoodieHiveCatalog(
        name,
        null,
        null,
        createHiveConf(),
        true,
        false);
  }

  public static HiveConf createHiveConf() {
    ClassLoader classLoader = HoodieCatalogTestUtils.class.getClassLoader();
    try {
      TEMPORARY_FOLDER.create();
      String warehouseDir = TEMPORARY_FOLDER.newFolder().getAbsolutePath() + "/metastore_db";
      String warehouseUri = String.format(HIVE_WAREHOUSE_URI_FORMAT, warehouseDir);

      HiveConf.setHiveSiteLocation(classLoader.getResource(CatalogOptions.HIVE_SITE_FILE));
      HiveConf hiveConf = new HiveConf();
      hiveConf.setVar(
          HiveConf.ConfVars.METASTOREWAREHOUSE,
          TEMPORARY_FOLDER.newFolder("hive_warehouse").getAbsolutePath());
      hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, warehouseUri);
      return hiveConf;
    } catch (IOException e) {
      throw new CatalogException("Failed to create test HiveConf to HiveCatalog.", e);
    }
  }
}
