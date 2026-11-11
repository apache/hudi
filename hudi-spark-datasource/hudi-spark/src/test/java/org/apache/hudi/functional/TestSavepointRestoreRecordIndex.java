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

package org.apache.hudi.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Savepoint restores of a table whose metadata table holds a record index built after the restore target. The record
 * index needs the Spark adapter to initialize, so these tests live with the Spark datasource tests.
 */
@Tag("functional")
public class TestSavepointRestoreRecordIndex extends HoodieClientTestBase {

  @ParameterizedTest
  @ValueSource(strings = {"compactedMetadata", "metadataEnabledAfterSavepoint", "metadataEnabledAtRestore"})
  void testRestoreLeavesNoRecordIndexEntryOfRolledBackCommits(String metadataHistory) throws Exception {
    HoodieWriteConfig configWithoutMetadata = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    HoodieWriteConfig configWithRecordIndex = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withEnableGlobalRecordLevelIndex(true)
            .withMetadataIndexColumnStats(true)
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .build())
        .build();
    String savepointCommit;
    List<HoodieRecord> savepointed;
    List<HoodieRecord> rolledBack = new ArrayList<>();
    try (SparkRDDWriteClient client = getHoodieWriteClient(
        metadataHistory.equals("compactedMetadata") ? configWithRecordIndex : configWithoutMetadata)) {
      savepointCommit = client.startCommit();
      savepointed = dataGen.generateInserts(savepointCommit, 10);
      client.commit(savepointCommit, client.insert(jsc.parallelize(savepointed, 1), savepointCommit));
      client.savepoint("user1", "Savepoint for 1st commit");
      rolledBack.addAll(insertNewRecords(client));
    }
    try (SparkRDDWriteClient client = getHoodieWriteClient(configWithRecordIndex)) {
      if (metadataHistory.equals("metadataEnabledAtRestore")) {
        metadataWriter(configWithRecordIndex).close();
        assertFalse(client.deleteMdtIfNecessaryBeforeRestore(savepointCommit), "a metadata table holding only its initialization is restored in place");
      } else {
        rolledBack.addAll(insertNewRecords(client));
      }
      client.restoreToSavepoint(savepointCommit);
      assertRowNumberEqualsTo(10);
      Set<String> partitionsAfterRestore = HoodieTableMetaClient.reload(metaClient).getTableConfig().getMetadataPartitions();
      assertFalse(partitionsAfterRestore.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath()), "the record index is dropped");
      assertFalse(partitionsAfterRestore.contains(MetadataPartitionType.PARTITION_STATS.getPartitionPath()),
          "an index partition the restore dropped is not listed again");

      String commitAfterRestore = client.startCommit();
      List<HoodieRecord> reinserted = dataGen.generateUpdates(commitAfterRestore, rolledBack.subList(0, 5));
      List<WriteStatus> statuses = client.upsert(jsc.parallelize(reinserted, 1), commitAfterRestore).collect();
      assertNoWriteErrors(statuses);
      client.commit(commitAfterRestore, jsc.parallelize(statuses, 1));
      assertRowNumberEqualsTo(15);

      Map<String, String> indexedKeys = new HashMap<>();
      try (HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(
          context, metaClient.getStorage(), configWithRecordIndex.getMetadataConfig(), basePath)) {
        List<String> keys = Stream.concat(savepointed.stream(), rolledBack.stream()).map(HoodieRecord::getRecordKey).collect(Collectors.toList());
        tableMetadata.readRecordIndexLocationsWithKeys(HoodieListData.eager(keys)).collectAsList()
            .forEach(entry -> indexedKeys.put(entry.getKey(), entry.getValue().getInstantTime()));
      }
      savepointed.forEach(record -> assertTrue(indexedKeys.containsKey(record.getRecordKey())));
      rolledBack.subList(0, 5).forEach(record -> assertEquals(commitAfterRestore, indexedKeys.get(record.getRecordKey())));
      rolledBack.subList(5, rolledBack.size()).forEach(record -> assertFalse(indexedKeys.containsKey(record.getRecordKey()),
          "the record index has no entry for a key inserted only by a rolled back commit"));
    }
  }

  /**
   * The first attempt of a restore rolls back the data and stops before the metadata step; its retry rolls back nothing
   * and still drops the record index of a metadata table built after the target.
   */
  @Test
  void testRetriedRestoreDropsTheRecordIndexAfterAnEarlierAttemptRolledBackTheData() throws Exception {
    HoodieWriteConfig configWithoutMetadata = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    HoodieWriteConfig configWithRecordIndex = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withEnableGlobalRecordLevelIndex(true).build())
        .build();
    String savepointCommit;
    List<HoodieRecord> rolledBack;
    try (SparkRDDWriteClient client = getHoodieWriteClient(configWithoutMetadata)) {
      savepointCommit = client.startCommit();
      client.commit(savepointCommit, client.insert(jsc.parallelize(dataGen.generateInserts(savepointCommit, 10), 1), savepointCommit));
      client.savepoint("user1", "Savepoint for 1st commit");
      rolledBack = insertNewRecords(client);
    }
    metadataWriter(configWithRecordIndex).close();
    java.nio.file.Path hoodieFolder = Paths.get(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    java.nio.file.Path metadataTable = hoodieFolder.resolve("metadata");
    java.nio.file.Path tableConfig = hoodieFolder.resolve(HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    java.nio.file.Path saved = tempDir.resolve("saved-" + UUID.randomUUID());
    copyDirectory(metadataTable, saved.resolve("metadata"));
    copyFileAndChecksum(tableConfig, saved);

    try (SparkRDDWriteClient client = getHoodieWriteClient(configWithRecordIndex)) {
      client.restoreToSavepoint(savepointCommit);
    }
    // Back to the state of an attempt that rolled back the data and stopped before the metadata step.
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant restore = metaClient.getActiveTimeline().getRestoreTimeline().filterCompletedInstants().lastInstant().get();
    metaClient.getStorage().deleteFile(new StoragePath(metaClient.getTimelinePath(), metaClient.getInstantFileNameGenerator().getFileName(restore)));
    FileIOUtils.deleteDirectory(metadataTable.toFile());
    copyDirectory(saved.resolve("metadata"), metadataTable);
    copyFileAndChecksum(saved.resolve(HoodieTableConfig.HOODIE_PROPERTIES_FILE), hoodieFolder);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTrue(metaClient.getTableConfig().getMetadataPartitions().contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath()));
    assertFalse(metaClient.getActiveTimeline().getRestoreTimeline().filterInflightsAndRequested().empty(), "the restore is pending");

    try (SparkRDDWriteClient client = getHoodieWriteClient(configWithRecordIndex)) {
      client.restoreToSavepoint(savepointCommit);
      assertRowNumberEqualsTo(10);
      assertFalse(HoodieTableMetaClient.reload(metaClient).getTableConfig().getMetadataPartitions()
          .contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath()), "the retried restore drops the record index");

      String commitAfterRestore = client.startCommit();
      List<HoodieRecord> reinserted = dataGen.generateUpdates(commitAfterRestore, rolledBack.subList(0, 5));
      List<WriteStatus> statuses = client.upsert(jsc.parallelize(reinserted, 1), commitAfterRestore).collect();
      assertNoWriteErrors(statuses);
      client.commit(commitAfterRestore, jsc.parallelize(statuses, 1));
      assertRowNumberEqualsTo(15);
    }
  }

  private static void copyFileAndChecksum(java.nio.file.Path file, java.nio.file.Path targetFolder) throws IOException {
    Files.createDirectories(targetFolder);
    Files.copy(file, targetFolder.resolve(file.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
    java.nio.file.Path checksum = file.resolveSibling("." + file.getFileName() + ".crc");
    java.nio.file.Path targetChecksum = targetFolder.resolve(checksum.getFileName().toString());
    if (Files.exists(checksum)) {
      Files.copy(checksum, targetChecksum, StandardCopyOption.REPLACE_EXISTING);
    } else {
      Files.deleteIfExists(targetChecksum);
    }
  }

  private static void copyDirectory(java.nio.file.Path source, java.nio.file.Path target) throws IOException {
    Files.createDirectories(target.getParent());
    try (Stream<java.nio.file.Path> paths = Files.walk(source)) {
      for (java.nio.file.Path path : paths.collect(Collectors.toList())) {
        Files.copy(path, target.resolve(source.relativize(path).toString()), StandardCopyOption.COPY_ATTRIBUTES);
      }
    }
  }

  private List<HoodieRecord> insertNewRecords(SparkRDDWriteClient client) {
    String commitTime = client.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 10);
    client.commit(commitTime, client.insert(jsc.parallelize(records, 1), commitTime));
    return records;
  }
}
