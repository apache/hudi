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

import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteCommitCallbackConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.deltacommit.JavaUpsertDeltaCommitActionExecutor;
import org.apache.hudi.testutils.GenericRecordValidationTestUtils;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;
import org.apache.hudi.testutils.RecordingCommitCallback;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.apache.hudi.testutils.GenericRecordValidationTestUtils.assertDataInMORTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieJavaClientOnMergeOnReadStorage extends HoodieJavaClientTestHarness {

  @BeforeEach
  public void setUpTestTable() {
    testTable = HoodieTestTable.of(metaClient);
  }

  @Test
  public void testReadingMORTableWithoutBaseFile() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    // Do insert and updates thrice one after the other.
    // Insert
    String commitTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, commitTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);
    // check that only log files exist
    try (SyncableFileSystemView fileSystemView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      Arrays.stream(dataGen.getPartitionPaths())
          .forEach(partition -> fileSystemView.getLatestFileSlices(partition).forEach(fileSlice -> assertTrue(fileSlice.getBaseFile().isEmpty())));
    }

    // Update
    String commitTimeBetweenPrevAndNew = commitTime;
    commitTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, commitTime, commitTimeBetweenPrevAndNew,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", 50, HoodieJavaWriteClient::upsert,
        false, false, 50, 100, 2, config.populateMetaFields(), INSTANT_GENERATOR);

    // Delete 5 records
    String prevCommitTime = commitTime;
    commitTime = WriteClientTestUtils.createNewInstantTime();
    deleteBatch(config, client, commitTime, prevCommitTime, "000", 25, false, false,
        0, 100, TIMELINE_FACTORY, INSTANT_GENERATOR);

    // Verify all the records.
    metaClient.reloadActiveTimeline();
    Map<String, GenericRecord> recordMap = GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen);
    assertEquals(75, recordMap.size());
  }

  @Test
  public void testCompactionOnMORTable() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    // Do insert and updates thrice one after the other.
    // Insert
    String commitTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, commitTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);

    // Update
    String commitTimeBetweenPrevAndNew = commitTime;
    commitTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, commitTime, commitTimeBetweenPrevAndNew,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", 50, HoodieJavaWriteClient::upsert,
        false, false, 5, 100, 2, config.populateMetaFields(), INSTANT_GENERATOR);

    // Schedule and execute compaction.
    Option<String> timeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(timeStamp.isPresent());
    HoodieWriteMetadata writeMetadata = client.compact(timeStamp.get());
    client.commitCompaction(timeStamp.get(), writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(timeStamp.get()));
    // Verify all the records.
    metaClient.reloadActiveTimeline();
    assertDataInMORTable(config, commitTime, timeStamp.get(), storageConf, Arrays.asList(dataGen.getPartitionPaths()));
  }

  @Test
  public void testAsyncCompactionOnMORTable() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    // Do insert and updates thrice one after the other.
    // Insert
    String commitTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, commitTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);

    // Update
    String commitTimeBetweenPrevAndNew = commitTime;
    commitTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, commitTime, commitTimeBetweenPrevAndNew,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", 50, HoodieJavaWriteClient::upsert,
        false, false, 5, 100, 2, config.populateMetaFields(), INSTANT_GENERATOR);

    // Schedule compaction but do not run it
    Option<String> timeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(timeStamp.isPresent());

    commitTimeBetweenPrevAndNew = commitTime;
    commitTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, commitTime, commitTimeBetweenPrevAndNew,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", 50, HoodieJavaWriteClient::upsert,
        false, false, 5, 150, 2, config.populateMetaFields(), INSTANT_GENERATOR);
    // Verify all the records.
    metaClient.reloadActiveTimeline();
    assertDataInMORTable(config, commitTime, timeStamp.get(), storageConf, Arrays.asList(dataGen.getPartitionPaths()));

    // now run compaction
    HoodieWriteMetadata writeMetadata = client.compact(timeStamp.get());
    client.commitCompaction(timeStamp.get(), writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(timeStamp.get()));

    // Verify all the records.
    metaClient.reloadActiveTimeline();
    assertDataInMORTable(config, commitTime, timeStamp.get(), storageConf, Arrays.asList(dataGen.getPartitionPaths()));

    commitTimeBetweenPrevAndNew = commitTime;
    commitTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, commitTime, commitTimeBetweenPrevAndNew,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", 50, HoodieJavaWriteClient::upsert,
        false, false, 5, 200, 2, config.populateMetaFields(), INSTANT_GENERATOR);
    // Verify all the records.
    metaClient.reloadActiveTimeline();
    assertDataInMORTable(config, commitTime, timeStamp.get(), storageConf, Arrays.asList(dataGen.getPartitionPaths()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testInsertIntoSmallFileGroupKeepsLoggedUpdates(boolean preppedInsert) throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.BLOOM).build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);
    String partition = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    HoodieTestDataGenerator singlePartitionDataGen = new HoodieTestDataGenerator(new String[] {partition});

    // Inserts go to a base file since the index cannot look up records in log files.
    String firstInsertTime = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, firstInsertTime);
    client.commit(firstInsertTime, client.insert(singlePartitionDataGen.generateInserts(firstInsertTime, 20), firstInsertTime));

    // Updates go to a log file of the same file group.
    String updateTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> updates = singlePartitionDataGen.generateUniqueUpdates(updateTime, 5);
    WriteClientTestUtils.startCommitWithTime(client, updateTime);
    client.commit(updateTime, client.upsert(updates, updateTime));

    String secondInsertTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> secondInserts = singlePartitionDataGen.generateInserts(secondInsertTime, 20);
    WriteClientTestUtils.startCommitWithTime(client, secondInsertTime);
    client.commit(secondInsertTime, preppedInsert
        ? client.insertPreppedRecords(secondInserts, secondInsertTime)
        : client.insert(secondInserts, secondInsertTime));

    metaClient.reloadActiveTimeline();
    Map<String, GenericRecord> snapshot =
        GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, singlePartitionDataGen);
    assertEquals(40, snapshot.size());
    updates.forEach(update -> assertEquals(updateTime,
        snapshot.get(update.getRecordKey()).get(COMMIT_TIME_METADATA_FIELD).toString(),
        "Snapshot must return the logged update of " + update.getRecordKey()));
  }

  @Test
  public void testInsertsWithAnIndexOnLogFilesFillTheSmallFileGroup() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);
    String partition = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    HoodieTestDataGenerator singlePartitionDataGen = new HoodieTestDataGenerator(new String[] {partition});

    for (int i = 0; i < 2; i++) {
      String insertTime = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, insertTime);
      client.commit(insertTime, client.insert(singlePartitionDataGen.generateInserts(insertTime, 20), insertTime));
    }

    try (SyncableFileSystemView fileSystemView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      assertEquals(1, fileSystemView.getLatestFileSlices(partition).count(), "the second insert goes to the small file group");
    }
    assertEquals(40, GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, singlePartitionDataGen).size());
  }

  @Test
  public void testInsertsSkipABaseFileAtTheSmallFileLimit() throws Exception {
    assertSecondInsertSkipsTheFirstFileGroup(HoodieIndex.IndexType.BLOOM, (config, firstSlice) ->
        getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .compactionSmallFileSize(firstSlice.getBaseFile().get().getFileSize()).build())
            .build());
  }

  @Test
  public void testInsertsWithAnIndexOnLogFilesSkipAFileGroupAtTheMaxFileSize() throws Exception {
    assertSecondInsertSkipsTheFirstFileGroup(HoodieIndex.IndexType.INMEMORY, (config, firstSlice) ->
        getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
            .withStorageConfig(HoodieStorageConfig.newBuilder()
                .parquetMaxFileSize(firstSlice.getTotalFileSizeAsParquetFormat(config)).build())
            .build());
  }

  private void assertSecondInsertSkipsTheFirstFileGroup(HoodieIndex.IndexType indexType,
      BiFunction<HoodieWriteConfig, FileSlice, HoodieWriteConfig> secondConfig) throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, indexType).build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);
    String partition = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    HoodieTestDataGenerator singlePartitionDataGen = new HoodieTestDataGenerator(new String[] {partition});

    String firstInsertTime = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, firstInsertTime);
    client.commit(firstInsertTime, client.insert(singlePartitionDataGen.generateInserts(firstInsertTime, 20), firstInsertTime));
    FileSlice firstSlice;
    try (SyncableFileSystemView fileSystemView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      firstSlice = fileSystemView.getLatestFileSlices(partition).findFirst().get();
    }

    HoodieWriteConfig limitedConfig = secondConfig.apply(config, firstSlice);
    client = getHoodieWriteClient(limitedConfig);
    String secondInsertTime = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, secondInsertTime);
    client.commit(secondInsertTime, client.insert(singlePartitionDataGen.generateInserts(secondInsertTime, 20), secondInsertTime));

    try (SyncableFileSystemView fileSystemView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      assertTrue(fileSystemView.getLatestFileSlices(partition).count() > 1, "the second insert goes to a new file group");
      FileSlice slice = fileSystemView.getLatestFileSlice(partition, firstSlice.getFileId()).get();
      assertEquals(firstInsertTime, slice.getBaseInstantTime());
      assertEquals(firstSlice.getLogFiles().count(), slice.getLogFiles().count());
    }
    assertEquals(40, GenericRecordValidationTestUtils.getRecordsMap(limitedConfig, storageConf, singlePartitionDataGen).size());
  }

  @Test
  public void testUpsertPartitionerNeedsAWorkloadProfile() {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM).build();
    JavaUpsertDeltaCommitActionExecutor<?> executor = new JavaUpsertDeltaCommitActionExecutor<>(context, config,
        HoodieJavaTable.create(config, context, metaClient), WriteClientTestUtils.createNewInstantTime(), Collections.emptyList());
    assertThrows(HoodieUpsertException.class, () -> executor.getUpsertPartitioner(null));
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @Test
  public void testWriteCommitCallbackFiresOnCompaction() throws Exception {
    RecordingCommitCallback.reset();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .withCallbackConfig(HoodieWriteCommitCallbackConfig.newBuilder()
            .writeCommitCallbackOn("true")
            .withCallbackClass(RecordingCommitCallback.class.getName())
            .build())
        .build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    // Two delta commits through the auto-commit path.
    String commitTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, commitTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);
    String prevCommit = commitTime;
    commitTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, commitTime, prevCommit,
        Option.of(Arrays.asList(prevCommit)), "000", 50, HoodieJavaWriteClient::upsert,
        false, false, 5, 100, 2, config.populateMetaFields(), INSTANT_GENERATOR);

    // The callback must fire for the auto-committed delta commits with the deltacommit action.
    assertTrue(RecordingCommitCallback.messages().stream().anyMatch(m ->
            HoodieTimeline.DELTA_COMMIT_ACTION.equals(m.getCommitActionType().orElse(null))),
        "callback must fire for delta commits");

    // Schedule, execute and commit compaction.
    Option<String> compactionTime = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTime.isPresent());
    HoodieWriteMetadata writeMetadata = client.compact(compactionTime.get());
    client.commitCompaction(compactionTime.get(), writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime.get()));

    // The callback must fire exactly once for the compaction completion, reporting the completed
    // timeline action (commit).
    List<HoodieWriteCommitCallbackMessage> compactionMessages = RecordingCommitCallback.messages().stream()
        .filter(m -> m.getCommitTime().equals(compactionTime.get()))
        .collect(Collectors.toList());
    assertEquals(1, compactionMessages.size(), "callback must fire once for the compaction commit");
    assertEquals(HoodieTimeline.COMMIT_ACTION, compactionMessages.get(0).getCommitActionType().orElse(null));
    assertNotNull(compactionMessages.get(0).getPrevFilePaths(), "prevFilePaths must never be null");
  }

  @Test
  public void testWriteCommitCallbackFiresOnClustering() throws Exception {
    RecordingCommitCallback.reset();
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder()
        .withClusteringMaxNumGroups(10)
        .withClusteringSortColumns("_row_key")
        .withClusteringTargetPartitions(0)
        .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
        .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName())
        .build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY)
        .withClusteringConfig(clusteringConfig)
        .withCallbackConfig(HoodieWriteCommitCallbackConfig.newBuilder()
            .writeCommitCallbackOn("true")
            .withCallbackClass(RecordingCommitCallback.class.getName())
            .build())
        .build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    // Two inserts create base-file groups that clustering can rewrite.
    String commitTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, commitTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);
    commitTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, commitTime, "001", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 200, 2, Option.empty(), INSTANT_GENERATOR);

    // Schedule and execute clustering inline (shouldComplete = true completes the commit).
    Option<String> clusteringTime = client.scheduleClustering(Option.empty());
    assertTrue(clusteringTime.isPresent(), "expected a clustering plan to be scheduled");
    client.cluster(clusteringTime.get(), true);
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(clusteringTime.get()));

    // The callback must fire once for the clustering completion, reporting the action actually on
    // the timeline (replacecommit).
    List<HoodieWriteCommitCallbackMessage> clusteringMessages = RecordingCommitCallback.messages().stream()
        .filter(m -> m.getCommitTime().equals(clusteringTime.get()))
        .collect(Collectors.toList());
    assertEquals(1, clusteringMessages.size(), "callback must fire once for the clustering commit");
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, clusteringMessages.get(0).getCommitActionType().orElse(null));
    // Clustering writes new file groups, so every stat carries NULL_COMMIT and no prev path resolves.
    assertTrue(clusteringMessages.get(0).getPrevFilePaths().isEmpty());
  }

}
