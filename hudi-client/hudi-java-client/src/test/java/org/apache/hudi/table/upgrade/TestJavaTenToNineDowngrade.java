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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestJavaTenToNineDowngrade {

  @Test
  void refusedDowngradeLeavesTheMetadataTableAtItsVersion(@TempDir Path basePath) throws Exception {
    String schema = "{\"type\":\"record\",\"name\":\"rec\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"string\"},"
        + "{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]}]}";
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("test_table")
        .setTableVersion(HoodieTableVersion.TEN)
        .setRecordKeyFields("id")
        .setPartitionFields("ts")
        .setTableCreateSchema(schema)
        .setTimestampPartitionFractionalSeconds(true)
        .initTable(HoodieTestUtils.getDefaultStorageConf(), basePath.toString());
    String metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(basePath.toString());
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setTableName("test_table_metadata")
        .setTableVersion(HoodieTableVersion.TEN)
        .initTable(HoodieTestUtils.getDefaultStorageConf(), metadataTablePath);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath.toString()).withSchema(schema).forTable("test_table").build();

    HoodieUpgradeDowngradeException ex = assertThrows(HoodieUpgradeDowngradeException.class,
        () -> new UpgradeDowngrade(metaClient, writeConfig, new HoodieJavaEngineContext(HoodieTestUtils.getDefaultStorageConf()),
            JavaUpgradeDowngradeHelper.getInstance()).run(HoodieTableVersion.NINE, null));
    assertTrue(ex.getMessage().contains(HoodieTableConfig.TIMESTAMP_PARTITION_FRACTIONAL_SECONDS.key()),
        "the message must name the property: " + ex.getMessage());
    assertEquals(HoodieTableVersion.TEN, HoodieTestUtils.createMetaClient(basePath.toString()).getTableConfig().getTableVersion());
    assertEquals(HoodieTableVersion.TEN, HoodieTestUtils.createMetaClient(metadataTablePath).getTableConfig().getTableVersion());
  }
}
