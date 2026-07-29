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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.utils.CatalogUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieCatalogUtil}.
 */
class TestHoodieCatalogUtil {

  @Test
  void testPartitionKeyExtractionAndPathInference() {
    Schema schema = Schema.newBuilder()
        .column("id", DataTypes.INT())
        .column("region", DataTypes.STRING())
        .column("day", DataTypes.STRING())
        .build();
    Map<String, String> options = Collections.singletonMap(
        FlinkOptions.PARTITION_PATH_FIELD.key(), "region,day");
    CatalogTable optionPartitionedTable =
        CatalogUtils.createCatalogTable(schema, Collections.emptyList(), options, null);
    CatalogTable declaredPartitionedTable =
        CatalogUtils.createCatalogTable(schema, Collections.singletonList("day"), options, null);

    assertEquals(
        Arrays.asList("region", "day"),
        HoodieCatalogUtil.getPartitionKeys(optionPartitionedTable));
    assertEquals(
        Collections.singletonList("day"),
        HoodieCatalogUtil.getPartitionKeys(declaredPartitionedTable));

    Map<String, String> spec = new LinkedHashMap<>();
    spec.put("region", "apac");
    spec.put("day", "2026-07-28");
    CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(spec);
    assertEquals("region=apac/day=2026-07-28", HoodieCatalogUtil.inferPartitionPath(true, partitionSpec));
    assertEquals("apac/2026-07-28", HoodieCatalogUtil.inferPartitionPath(false, partitionSpec));
  }

  @Test
  void testOrderedPartitionValuesAndValidation() throws Exception {
    HiveConf hiveConf = HoodieCatalogTestUtils.createHiveConf();
    ObjectPath tablePath = new ObjectPath("default", "tbl");
    Map<String, String> spec = new LinkedHashMap<>();
    spec.put("day", null);
    spec.put("region", "apac");

    assertEquals(
        Arrays.asList("apac", hiveConf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME)),
        HoodieCatalogUtil.getOrderedPartitionValues(
            "catalog",
            hiveConf,
            new CatalogPartitionSpec(spec),
            Arrays.asList("region", "day"),
            tablePath));

    assertThrows(
        PartitionSpecInvalidException.class,
        () -> HoodieCatalogUtil.getOrderedPartitionValues(
            "catalog",
            hiveConf,
            new CatalogPartitionSpec(Collections.singletonMap("region", "apac")),
            Arrays.asList("region", "day"),
            tablePath));

    Map<String, String> wrongKey = new LinkedHashMap<>();
    wrongKey.put("region", "apac");
    wrongKey.put("month", "07");
    assertThrows(
        PartitionSpecInvalidException.class,
        () -> HoodieCatalogUtil.getOrderedPartitionValues(
            "catalog",
            hiveConf,
            new CatalogPartitionSpec(wrongKey),
            Arrays.asList("region", "day"),
            tablePath));
  }

  @Test
  void testHiveConfLoadingFailureAndEmbeddedDetection() {
    CatalogException exception = assertThrows(
        CatalogException.class,
        () -> HoodieCatalogUtil.createHiveConf(
            "target/does-not-exist-" + System.nanoTime(),
            new Configuration()));
    assertTrue(exception.getMessage().contains("Failed to load hive-site.xml"));

    HiveConf embedded = new HiveConf();
    embedded.setVar(HiveConf.ConfVars.METASTOREURIS, "");
    assertTrue(HoodieCatalogUtil.isEmbeddedMetastore(embedded));

    assertNotNull(HoodieCatalogUtil.createHiveConf(null, new Configuration()));
  }
}
