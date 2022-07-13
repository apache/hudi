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

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for {@link HoodieCatalogFactory}.
 */
public class TestHoodieCatalogFactory {
  private static final URL CONF_DIR =
      Thread.currentThread().getContextClassLoader().getResource("test-catalog-factory-conf");

  @Test
  public void testCreateHiveCatalog() {
    final String catalogName = "mycatalog";

    final HoodieHiveCatalog expectedCatalog = HoodieCatalogTestUtils.createHiveCatalog(catalogName);

    final Map<String, String> options = new HashMap<>();
    options.put(CommonCatalogOptions.CATALOG_TYPE.key(), HoodieCatalogFactory.IDENTIFIER);
    options.put(CatalogOptions.HIVE_CONF_DIR.key(), CONF_DIR.getPath());
    options.put(CatalogOptions.MODE.key(), "hms");

    final Catalog actualCatalog =
        FactoryUtil.createCatalog(
            catalogName, options, null, Thread.currentThread().getContextClassLoader());

    assertEquals(
        ((HoodieHiveCatalog) actualCatalog)
            .getHiveConf()
            .getVar(HiveConf.ConfVars.METASTOREURIS), "dummy-hms");
    checkEquals(expectedCatalog, (HoodieHiveCatalog) actualCatalog);
  }

  private static void checkEquals(HoodieHiveCatalog c1, HoodieHiveCatalog c2) {
    // Only assert a few selected properties for now
    assertEquals(c2.getName(), c1.getName());
    assertEquals(c2.getDefaultDatabase(), c1.getDefaultDatabase());
  }
}
