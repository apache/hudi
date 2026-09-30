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

package org.apache.hudi.utilities.health;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Predicate;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link MetadataTableSyncCheck} against a table with a real metadata table, which needs a
 * Spark write client to produce. The skip paths that need no metadata table live in the lighter
 * {@link TestMetadataTableSyncCheck}.
 */
public class TestMetadataTableSyncCheckFunctional extends HoodieSparkClientTestBase {

  private final MetadataTableSyncCheck check = new MetadataTableSyncCheck();

  private TypedProperties propsWithMetadataEnabled() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieMetadataConfig.ENABLE.key(), "true");
    return props;
  }

  private HealthCheckResult runCheck() throws Exception {
    HoodieTableMetaClient freshMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();
    return check.check(new HealthCheckContext(freshMetaClient, propsWithMetadataEnabled(), false));
  }

  private HoodieWriteConfig writeConfigWithMetadataTable() {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .forTable("health_check_table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withInlineCompaction(false).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .build();
  }

  /** A Merge-on-Read table with a metadata table, holding both base files and log files. */
  private void writeTableWithMetadataTable() throws Exception {
    initMetaClient(HoodieTableType.MERGE_ON_READ);
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfigWithMetadataTable())) {
      String insertInstant = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> inserts = dataGen.generateInserts(insertInstant, 30);
      WriteClientTestUtils.startCommitWithTime(client, insertInstant);
      JavaRDD<WriteStatus> insertStatuses = client.bulkInsert(jsc.parallelize(inserts, 1), insertInstant);
      client.commit(insertInstant, insertStatuses);

      String updateInstant = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> updates = dataGen.generateUpdates(updateInstant, 15);
      WriteClientTestUtils.startCommitWithTime(client, updateInstant);
      JavaRDD<WriteStatus> updateStatuses = client.upsert(jsc.parallelize(updates, 1), updateInstant);
      client.commit(updateInstant, updateStatuses);
    }
  }

  /** Removes one file of the given kind directly on storage, bypassing the timeline and the metadata table. */
  private StoragePath deleteOneFileFromStorage(String partition, Predicate<StoragePath> kind) throws Exception {
    List<StoragePathInfo> entries = storage.listDirectEntries(new StoragePath(basePath, partition));
    StoragePathInfo victim = entries.stream()
        .filter(e -> kind.test(e.getPath()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("no matching file in " + partition));
    assertTrue(storage.deleteFile(victim.getPath()));
    return victim.getPath();
  }

  private void assertInconsistencyNames(HealthCheckResult result, String partition, StoragePath file, String fileKind) {
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
    assertEquals("1", result.getEffectiveConfigs().get("observed.partitions.inconsistent"));
    assertTrue(result.getFindings().stream().anyMatch(
            f -> f.contains("'" + partition + "'") && f.contains(file.getName()) && f.contains(fileKind + " files listed by the metadata table")),
        "expected the partition and " + fileKind + " file to be named, got: " + result.getFindings());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("HoodieMetadataTableValidator")),
        "expected a pointer to the full validator, got: " + result.getFindings());
  }

  @Test
  public void caughtUpTableWithConsistentListingIsHealthy() throws Exception {
    // given a table whose every write was mirrored into the metadata table
    writeTableWithMetadataTable();

    // when the check runs
    HealthCheckResult result = runCheck();

    // then the metadata table is synced and agrees with storage in every sampled partition
    assertEquals(HealthStatus.HEALTHY, result.getStatus(), result.getSummary() + " " + result.getFindings());
    assertEquals("0", result.getEffectiveConfigs().get("observed.data.table.writes.behind"));
    assertEquals("0", result.getEffectiveConfigs().get("observed.partitions.inconsistent"));
    assertEquals(result.getEffectiveConfigs().get("observed.data.table.latest.completed.instant"),
        result.getEffectiveConfigs().get("observed.mdt.synced.instant"));
    assertTrue(Integer.parseInt(result.getEffectiveConfigs().get("observed.partitions.sampled")) > 0);
  }

  @Test
  public void baseFileMissingFromStorageIsUnhealthyNamingThePartitionAndFile() throws Exception {
    // given a consistent table, with one base file then removed directly on storage
    writeTableWithMetadataTable();
    StoragePath deleted = deleteOneFileFromStorage(DEFAULT_FIRST_PARTITION_PATH, FSUtils::isBaseFile);

    // when the check runs
    HealthCheckResult result = runCheck();

    // then the metadata table and storage disagree, and the finding says where and on what
    assertInconsistencyNames(result, DEFAULT_FIRST_PARTITION_PATH, deleted, "base");
  }

  @Test
  public void logFileMissingFromStorageIsUnhealthyNamingThePartitionAndFile() throws Exception {
    // given a consistent table, with one log file then removed directly on storage
    writeTableWithMetadataTable();
    StoragePath deleted = deleteOneFileFromStorage(DEFAULT_FIRST_PARTITION_PATH, FSUtils::isLogFile);

    // when the check runs
    HealthCheckResult result = runCheck();

    // then log files are compared too, not only base files
    assertInconsistencyNames(result, DEFAULT_FIRST_PARTITION_PATH, deleted, "log");
  }

  @Test
  public void dataTableWriteMissingFromMetadataTableIsUnhealthyWithoutRecommendingRebuild() throws Exception {
    // given a consistent table, then a completed data-table write the metadata table never saw
    writeTableWithMetadataTable();
    HoodieTableMetaClient dataMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance()).setBasePath(basePath).build();
    String unmirroredInstant = dataMetaClient.createNewInstantTime(false);
    HoodieTestTable.of(dataMetaClient).addCommit(unmirroredInstant);

    // when the check runs
    HealthCheckResult result = runCheck();

    // then the lag is reported, and the operator is steered away from rebuilding for lag alone
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
    assertEquals("1", result.getEffectiveConfigs().get("observed.data.table.writes.behind"));
    assertEquals(unmirroredInstant, result.getEffectiveConfigs().get("observed.data.table.latest.completed.instant"));
    assertEquals("0", result.getEffectiveConfigs().get("observed.partitions.inconsistent"));
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("Do not delete or rebuild")),
        "expected a warning against rebuilding, got: " + result.getFindings());
    assertFalse(result.getFindings().stream().anyMatch(f -> f.contains("HoodieMetadataTableValidator")),
        "listing was consistent, so the full validator should not be recommended: " + result.getFindings());
  }
}
