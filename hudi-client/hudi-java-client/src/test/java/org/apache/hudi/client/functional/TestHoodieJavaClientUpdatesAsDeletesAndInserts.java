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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.GenericRecordValidationTestUtils;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Functional tests for {@link HoodieWriteConfig#WRITE_UPDATES_AS_DELETES_AND_INSERTS} on
 * merge-on-read storage: an update becomes a positional delete in the record's current file group
 * plus an insert of the new version into a different file group of the same partition.
 */
public class TestHoodieJavaClientUpdatesAsDeletesAndInserts extends HoodieJavaClientTestHarness {

  @BeforeEach
  public void setUpTestTable() {
    testTable = HoodieTestTable.of(metaClient);
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  private HoodieWriteConfig buildConfig(boolean updatesAsDeletesAndInserts) {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE)
        // route all inserts to new file groups so log files carry deletes only
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(10).compactionSmallFileSize(0).build())
        .withWriteUpdatesAsDeletesAndInserts(updatesAsDeletesAndInserts)
        .build();
  }

  @Test
  public void testUpdatesBecomePositionalDeletesAndInserts() throws Exception {
    HoodieWriteConfig config = buildConfig(true);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, insertTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);

    Set<String> fileIdsAfterInsert = collectFileIds();

    String updateTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, updateTime, insertTime, Option.of(Arrays.asList(insertTime)),
        "000", 50, HoodieJavaWriteClient::upsert, false, false, 50, 100, 2,
        config.populateMetaFields(), INSTANT_GENERATOR);

    // Every log file written by the update must be a delete log carrying valid record positions
    // that reference the file slice's base file instant; no update record lands in a log.
    AtomicLong deleteRecordCount = new AtomicLong();
    HoodieSchema schema = HoodieSchema.parse(config.getSchema());
    try (SyncableFileSystemView fsView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      for (String partition : dataGen.getPartitionPaths()) {
        for (FileSlice slice : fsView.getLatestFileSlices(partition).collect(Collectors.toList())) {
          List<HoodieLogFile> logFiles = slice.getLogFiles().collect(Collectors.toList());
          if (logFiles.isEmpty()) {
            continue;
          }
          assertTrue(slice.getBaseFile().isPresent(), "a file group with delete logs must have a base file");
          for (HoodieLogFile logFile : logFiles) {
            assertTrue(logFile.getFileName().contains(".deletes."),
                "log files must only contain deletes but got: " + logFile.getFileName());
            try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(metaClient, logFile, schema)) {
              while (reader.hasNext()) {
                HoodieLogBlock block = reader.next();
                assertTrue(block instanceof HoodieDeleteBlock,
                    "expected only delete blocks but got: " + block.getClass().getSimpleName());
                HoodieDeleteBlock deleteBlock = (HoodieDeleteBlock) block;
                deleteRecordCount.addAndGet(deleteBlock.getRecordsToDelete().length);
                List<Long> positions = deleteBlock.getRecordPositionList();
                assertEquals(deleteBlock.getRecordsToDelete().length, positions.size(),
                    "every delete record must carry a position");
                positions.forEach(position ->
                    assertTrue(position >= 0, "delete record positions must be valid"));
                assertEquals(slice.getBaseFile().get().getCommitTime(),
                    deleteBlock.getBaseFileInstantTimeOfPositions(),
                    "positions must reference the file slice's base file");
              }
            }
          }
        }
      }
    }
    assertEquals(50, deleteRecordCount.get(), "each update must produce exactly one positional delete");

    // The new versions must land in new file groups of the same partitions.
    Set<String> newFileIds = new HashSet<>(collectFileIds());
    newFileIds.removeAll(fileIdsAfterInsert);
    assertFalse(newFileIds.isEmpty(), "updates must insert the new version into new file groups");

    // The merged view must expose all 100 records with the 50 new versions served from the new
    // file groups (their file name meta field points outside the original file groups).
    Map<String, GenericRecord> recordMap =
        GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen);
    assertEquals(100, recordMap.size());
    long updatedRecords = recordMap.values().stream()
        .filter(r -> r.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString().equals(updateTime))
        .peek(r -> {
          String fileName = r.get(HoodieRecord.FILENAME_METADATA_FIELD).toString();
          assertTrue(newFileIds.stream().anyMatch(fileName::contains),
              "an updated record must be served from a new file group but came from " + fileName);
        }).count();
    assertEquals(50, updatedRecords);
  }

  @Test
  public void testDefaultBehaviorKeepsUpdatesInLogFiles() throws Exception {
    HoodieWriteConfig config = buildConfig(false);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, insertTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);
    Set<String> fileIdsAfterInsert = collectFileIds();

    String updateTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, updateTime, insertTime, Option.of(Arrays.asList(insertTime)),
        "000", 50, HoodieJavaWriteClient::upsert, false, false, 50, 100, 2,
        config.populateMetaFields(), INSTANT_GENERATOR);

    // Updates stay in their file groups as data log files; no new file groups appear.
    assertEquals(fileIdsAfterInsert, collectFileIds());
    try (SyncableFileSystemView fsView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      long dataLogFiles = Arrays.stream(dataGen.getPartitionPaths())
          .flatMap(fsView::getLatestFileSlices)
          .flatMap(FileSlice::getLogFiles)
          .filter(logFile -> !logFile.getFileName().contains(".deletes."))
          .count();
      assertTrue(dataLogFiles > 0, "updates must be appended to data log files by default");
    }
  }

  @Test
  public void testCompactionAfterDecomposedUpdates() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(2).compactionSmallFileSize(0).build())
        .withWriteUpdatesAsDeletesAndInserts(true)
        .build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, insertTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);

    String updateTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, updateTime, insertTime, Option.of(Arrays.asList(insertTime)),
        "000", 50, HoodieJavaWriteClient::upsert, false, false, 50, 100, 2,
        config.populateMetaFields(), INSTANT_GENERATOR);

    Option<String> compactionTime = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTime.isPresent(), "expected a compaction plan for the delete logs");
    HoodieWriteMetadata writeMetadata = client.compact(compactionTime.get());
    client.commitCompaction(compactionTime.get(), writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants()
        .containsInstant(compactionTime.get()));

    // Compaction applies the positional deletes: the merged view is unchanged and no log files remain.
    Map<String, GenericRecord> recordMap =
        GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen);
    assertEquals(100, recordMap.size());
    assertEquals(50, recordMap.values().stream()
        .filter(r -> r.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString().equals(updateTime))
        .count());
    try (SyncableFileSystemView fsView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      long remainingLogFiles = Arrays.stream(dataGen.getPartitionPaths())
          .flatMap(fsView::getLatestFileSlices)
          .flatMap(FileSlice::getLogFiles)
          .count();
      assertEquals(0, remainingLogFiles, "compaction must consume all delete log files");
    }
  }

  private Set<String> collectFileIds() throws Exception {
    try (SyncableFileSystemView fsView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      return Arrays.stream(dataGen.getPartitionPaths())
          .flatMap(fsView::getLatestFileSlices)
          .map(FileSlice::getFileId)
          .collect(Collectors.toSet());
    }
  }
}
