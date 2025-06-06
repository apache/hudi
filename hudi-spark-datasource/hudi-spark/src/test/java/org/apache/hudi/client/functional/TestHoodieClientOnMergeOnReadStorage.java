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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.log.HoodieLogBlockMetadataScanner;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.GenericRecordValidationTestUtils;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.apache.hudi.testutils.GenericRecordValidationTestUtils.assertDataInMORTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieClientOnMergeOnReadStorage extends HoodieClientTestBase {

  private HoodieTestTable testTable;

  @BeforeEach
  public void setUpTestTable() {
    testTable = HoodieSparkWriteableTestTable.of(metaClient);
  }

  @Test
  public void testReadingMORTableWithoutBaseFile() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Do insert and updates thrice one after the other.
    // Insert
    String commitTime = client.createNewInstantTime();
    insertBatch(config, client, commitTime, "000", 100, SparkRDDWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);

    // Update
    String commitTimeBetweenPrevAndNew = commitTime;
    commitTime = client.createNewInstantTime();
    updateBatch(config, client, commitTime, commitTimeBetweenPrevAndNew,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", 50, SparkRDDWriteClient::upsert,
        false, false, 50, 100, 2, config.populateMetaFields(), INSTANT_GENERATOR);

    // Delete 5 records
    String prevCommitTime = commitTime;
    commitTime = client.createNewInstantTime();
    deleteBatch(config, client, commitTime, prevCommitTime, "000", 25, false, false,
        0, 100, TIMELINE_FACTORY, INSTANT_GENERATOR);

    // Verify all the records.
    metaClient.reloadActiveTimeline();
    Map<String, GenericRecord> recordMap = GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen);
    assertEquals(75, recordMap.size());
  }

  @ParameterizedTest
  @EnumSource(value = HoodieIndex.IndexType.class, names = {"INMEMORY", "SIMPLE"})
  public void testClusteringOnMORTable(HoodieIndex.IndexType indexType) throws Exception {
    // INMEMORY index will only generate log files, SIMPLE will generate a base file and log files.
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, indexType)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder().withInlineClusteringNumCommits(3).build())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Insert
    String commitTime = client.createNewInstantTime();
    insertBatch(config, client, commitTime, "000", 100, SparkRDDWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);

    // Update
    String commitTimeBetweenPrevAndNew = commitTime;
    commitTime = client.createNewInstantTime();
    updateBatch(config, client, commitTime, commitTimeBetweenPrevAndNew,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", 50, SparkRDDWriteClient::upsert,
        false, false, 5, 100, 2, config.populateMetaFields(), INSTANT_GENERATOR);

    // Delete 5 records
    String prevCommitTime = commitTime;
    commitTime = client.createNewInstantTime();
    deleteBatch(config, client, commitTime, prevCommitTime, "000", 25, false, false,
        0, 100, TIMELINE_FACTORY, INSTANT_GENERATOR);

    // Schedule and execute compaction.
    Option<String> timeStamp = client.scheduleClustering(Option.empty());
    assertTrue(timeStamp.isPresent());
    client.cluster(timeStamp.get());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(timeStamp.get()));

    // Verify all the records.
    metaClient.reloadActiveTimeline();
    assertDataInMORTable(config, commitTime, timeStamp.get(), storageConf, Arrays.asList(dataGen.getPartitionPaths()));
  }

  @Test
  public void testCompactionOnMORTable() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Do insert and updates thrice one after the other.
    // Insert
    String commitTime = client.createNewInstantTime();
    insertBatch(config, client, commitTime, "000", 100, SparkRDDWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);

    // Update
    String commitTimeBetweenPrevAndNew = commitTime;
    commitTime = client.createNewInstantTime();
    updateBatch(config, client, commitTime, commitTimeBetweenPrevAndNew,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", 50, SparkRDDWriteClient::upsert,
        false, false, 5, 100, 2, config.populateMetaFields(), INSTANT_GENERATOR);

    // Schedule and execute compaction.
    Option<String> timeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(timeStamp.isPresent());
    HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionWriteMetadata = client.compact(timeStamp.get());
    client.commitCompaction(timeStamp.get(), compactionWriteMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(timeStamp.get()));

    // Verify all the records.
    metaClient.reloadActiveTimeline();
    assertDataInMORTable(config, commitTime, timeStamp.get(), storageConf, Arrays.asList(dataGen.getPartitionPaths()));
  }

  @Test
  public void testLogCompactionOnMORTable() throws Exception {
    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withMaxNumDeltaCommitsBeforeCompaction(1)
        .withLogCompactionBlocksThreshold(1)
        .build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withCompactionConfig(compactionConfig).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // First insert
    int expectedTotalRecs = 100;
    String newCommitTime = client.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", expectedTotalRecs,
        SparkRDDWriteClient::insert, false, false, expectedTotalRecs, expectedTotalRecs,
        1, Option.empty(), INSTANT_GENERATOR);

    String prevCommitTime = newCommitTime;
    for (int i = 0; i < 5; i++) {
      // Upsert
      newCommitTime = client.createNewInstantTime();
      expectedTotalRecs += 50;
      updateBatch(config, client, newCommitTime, prevCommitTime,
          Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
          false, false, 50, expectedTotalRecs, i + 2, config.populateMetaFields(), INSTANT_GENERATOR);
      prevCommitTime = newCommitTime;
    }

    // Schedule and execute compaction.
    Option<String> compactionTimeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTimeStamp.isPresent());
    HoodieWriteMetadata result = client.compact(compactionTimeStamp.get());
    client.commitCompaction(compactionTimeStamp.get(), result, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTimeStamp.get()));

    prevCommitTime = compactionTimeStamp.get();
    for (int i = 0; i < 2; i++) {
      // Upsert
      newCommitTime = client.createNewInstantTime();
      expectedTotalRecs += 50;
      updateBatch(config, client, newCommitTime, prevCommitTime,
          Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
          false, false, 50, expectedTotalRecs, i + 8, config.populateMetaFields(), INSTANT_GENERATOR);
      prevCommitTime = newCommitTime;
    }
    String lastCommitBeforeLogCompaction = prevCommitTime;

    // Schedule and execute compaction.
    Option<String> logCompactionTimeStamp = client.scheduleLogCompaction(Option.empty());
    assertTrue(logCompactionTimeStamp.isPresent());
    result = client.logCompact(logCompactionTimeStamp.get());
    client.commitLogCompaction(logCompactionTimeStamp.get(), result, Option.empty());
    // Verify all the records.
    assertDataInMORTable(config, lastCommitBeforeLogCompaction, logCompactionTimeStamp.get(),
        storageConf, Arrays.asList(dataGen.getPartitionPaths()));
  }

  /**
   * Test log-compaction before any compaction is scheduled. Here base file is not yet created.
   */
  @Test
  public void testLogCompactionOnMORTableWithoutBaseFile() throws Exception {
    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withEnableOptimizedLogBlocksScan("true")
        .withMaxNumDeltaCommitsBeforeCompaction(1)
        .withLogCompactionBlocksThreshold(1)
        .build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withCompactionConfig(compactionConfig).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // First insert 10 records
    String newCommitTime = client.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", 100,
        SparkRDDWriteClient::insert, false, false, 100, 100,
        1, Option.of(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH), INSTANT_GENERATOR);

    // Upsert 5 records
    String prevCommitTime = newCommitTime;
    newCommitTime = client.createNewInstantTime();
    updateBatch(config, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
        false, false, 50, 100, 2, config.populateMetaFields(), INSTANT_GENERATOR);
    prevCommitTime = newCommitTime;

    // Delete 3 records
    newCommitTime = client.createNewInstantTime();
    deleteBatch(config, client, newCommitTime, prevCommitTime, "000", 30, false, false,
        0, 70, TIMELINE_FACTORY, INSTANT_GENERATOR);

    String lastCommitBeforeLogCompaction = newCommitTime;
    // Schedule and execute compaction.
    Option<String> timeStamp = client.scheduleLogCompaction(Option.empty());
    assertTrue(timeStamp.isPresent());
    HoodieWriteMetadata result = client.logCompact(timeStamp.get());
    client.commitLogCompaction(timeStamp.get(), result, Option.empty());
    // Verify all the records.
    assertDataInMORTable(config, lastCommitBeforeLogCompaction, timeStamp.get(),
        storageConf, Arrays.asList(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH));
  }

  /**
   * Test scheduling log-compaction right after scheduling compaction. This should fail.
   */
  @Test
  public void testSchedulingLogCompactionAfterSchedulingCompaction() throws Exception {
    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withMaxNumDeltaCommitsBeforeCompaction(1)
        .withLogCompactionBlocksThreshold(1)
        .build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withCompactionConfig(compactionConfig).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // First insert
    String newCommitTime = client.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", 100,
        SparkRDDWriteClient::insert, false, false, 100, 100,
        1, Option.empty(), INSTANT_GENERATOR);

    String prevCommitTime = newCommitTime;
    // Upsert
    newCommitTime = client.createNewInstantTime();
    updateBatch(config, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
        false, false, 50, 100, 2, config.populateMetaFields(), INSTANT_GENERATOR);

    // Schedule compaction
    Option<String> compactionTimeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTimeStamp.isPresent());

    // Try scheduling log compaction, it won't succeed. 
    Option<String> logCompactionTimeStamp = client.scheduleLogCompaction(Option.empty());
    assertFalse(logCompactionTimeStamp.isPresent());
  }

  /**
   * Test scheduling compaction right after scheduling log-compaction. This should fail.
   */
  @Test
  public void testSchedulingCompactionAfterSchedulingLogCompaction() throws Exception {
    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withMaxNumDeltaCommitsBeforeCompaction(1)
        .withLogCompactionBlocksThreshold(1)
        .build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(compactionConfig)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class)
            .build())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    // First insert
    String newCommitTime = client.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", 100,
        SparkRDDWriteClient::insert, false, false, 10, 100,
        1, Option.empty(), INSTANT_GENERATOR);

    String prevCommitTime = newCommitTime;
    // Upsert
    newCommitTime = client.createNewInstantTime();
    updateBatch(config, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
        false, false, 50, 10, 2, config.populateMetaFields(), INSTANT_GENERATOR);

    // Schedule log compaction
    Option<String> logCompactionTimeStamp = client.scheduleLogCompaction(Option.empty());
    assertTrue(logCompactionTimeStamp.isPresent());

    // Even if pending logcompaction plans are in the timeline, compaction plan can be created.
    Option<String> compactionTimeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTimeStamp.isPresent());
  }

  @Test
  public void testCleanFunctionalityWhenCompactionRequestedInstantIsPresent() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(4).build())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // First insert. Here first file slice gets added to file group.
    String newCommitTime = client.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", 100,
        SparkRDDWriteClient::insert, false, false, 100, 100,
        1, Option.empty(), INSTANT_GENERATOR);

    // Schedule and execute compaction. Here, second file slice gets added.
    Option<String> compactionTimeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTimeStamp.isPresent());
    HoodieWriteMetadata result = client.compact(compactionTimeStamp.get());
    client.commitCompaction(compactionTimeStamp.get(), result, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTimeStamp.get()));
    String prevCommitTime = compactionTimeStamp.get();

    // First upsert on  second file slice.
    newCommitTime = client.createNewInstantTime();
    updateBatch(config, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
        false, false, 50, 100, 2, config.populateMetaFields(), INSTANT_GENERATOR);
    prevCommitTime = newCommitTime;

    // Schedule compaction. Third file slice gets added, compaction is not complete so base file is not created yet.
    compactionTimeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTimeStamp.isPresent());
    prevCommitTime = compactionTimeStamp.get();

    for (int i = 0; i < 6; i++) {
      // First upsert on third file slice.
      newCommitTime = client.createNewInstantTime();
      updateBatch(config, client, newCommitTime, prevCommitTime,
          Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
          false, false, 50, 100, 2, config.populateMetaFields(), INSTANT_GENERATOR);
      prevCommitTime = newCommitTime;
      if (i == 2) {
        // Since retain commits is 4 exactly after 6th completed commit there will be some files to be cleaned,
        // since a version older than the earliest commit is also retained.
        HoodieInstant cleanInstant = metaClient.reloadActiveTimeline().lastInstant().get();
        assertEquals(HoodieTimeline.CLEAN_ACTION, cleanInstant.getAction());
      } else {
        // Make sure clean is never triggered for other commits. The cleaner is blocked due to pending compaction instant.
        assertEquals(HoodieTimeline.DELTA_COMMIT_ACTION, metaClient.reloadActiveTimeline().lastInstant().get().getAction());
      }
    }
  }

  @Test
  public void testRollbackOnLogCompaction() throws Exception {
    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withMaxNumDeltaCommitsBeforeCompaction(1)
        .withLogCompactionBlocksThreshold(1)
        .build();
    HoodieWriteConfig lcConfig = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(compactionConfig).build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
        .build();
    try (SparkRDDWriteClient lcClient = new SparkRDDWriteClient(context, lcConfig);
         SparkRDDWriteClient client = new SparkRDDWriteClient(context, config)) {

      // First insert
      String newCommitTime = client.createNewInstantTime();
      insertBatch(config, client, newCommitTime, "000", 100,
          SparkRDDWriteClient::insert, false, false, 100, 100,
          1, Option.empty(), INSTANT_GENERATOR);
      String prevCommitTime = newCommitTime;

      // Upsert
      newCommitTime = client.createNewInstantTime();
      updateBatch(config, client, newCommitTime, prevCommitTime,
          Option.of(Arrays.asList(prevCommitTime)), "000", 10, SparkRDDWriteClient::upsert,
          false, false, 10, 100, 4, config.populateMetaFields(), INSTANT_GENERATOR);
      prevCommitTime = newCommitTime;

      // Schedule and execute log-compaction but do not commit.
      Option<String> logCompactionTimeStamp = lcClient.scheduleLogCompaction(Option.empty());
      assertTrue(logCompactionTimeStamp.isPresent());
      lcClient.logCompact(logCompactionTimeStamp.get());

      // Rollback the log compaction commit.
      HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.LOG_COMPACTION_ACTION, logCompactionTimeStamp.get());
      getHoodieTable(metaClient, config).rollbackInflightLogCompaction(instant);

      // Validate timeline.
      HoodieTimeline activeTimeline = metaClient.reloadActiveTimeline();
      HoodieInstant rollbackInstant = activeTimeline.lastInstant().get();
      assertEquals(3, activeTimeline.countInstants());
      assertEquals(HoodieTimeline.ROLLBACK_ACTION, rollbackInstant.getAction());

      // Validate block instant times.
      validateBlockInstantsBeforeAndAfterRollback(config, prevCommitTime, rollbackInstant.requestedTime());
      prevCommitTime = rollbackInstant.requestedTime();

      // Do one more upsert
      newCommitTime = client.createNewInstantTime();
      updateBatch(config, client, newCommitTime, prevCommitTime,
          Option.of(Arrays.asList(prevCommitTime)), "000", 10, SparkRDDWriteClient::upsert,
          false, false, 10, 100, 4, config.populateMetaFields(), INSTANT_GENERATOR);
      prevCommitTime = newCommitTime;

      // Complete log-compaction now.
      logCompactionTimeStamp = lcClient.scheduleLogCompaction(Option.empty());
      assertTrue(logCompactionTimeStamp.isPresent());
      HoodieWriteMetadata metadata = lcClient.logCompact(logCompactionTimeStamp.get());
      lcClient.commitLogCompaction(logCompactionTimeStamp.get(), metadata, Option.empty());
      assertDataInMORTable(config, prevCommitTime, logCompactionTimeStamp.get(), storageConf, Arrays.asList(dataGen.getPartitionPaths()));
    }
  }

  private void validateBlockInstantsBeforeAndAfterRollback(HoodieWriteConfig config, String instant, String currentInstant) {
    HoodieTable table = getHoodieTable(metaClient, config);
    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) table.getSliceView();
    List<String> partitionPaths = Stream.of(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS).collect(Collectors.toList());
    for (String partitionPath: partitionPaths) {
      fileSystemView.getLatestFileSlices(partitionPath).forEach(slice -> {
        HoodieLogBlockMetadataScanner scanner = new HoodieLogBlockMetadataScanner(
            table.getMetaClient(),
            slice.getLogFiles()
                .sorted(HoodieLogFile.getLogFileComparator())
                .map(file -> file.getPath().toString())
                .collect(Collectors.toList()),
            config.getMaxDFSStreamBufferSize(),
            instant,
            Option.empty());
        List<String> prevInstants = scanner.getValidBlockInstants();
        HoodieLogBlockMetadataScanner scanner2 = new HoodieLogBlockMetadataScanner(
            table.getMetaClient(),
            slice.getLogFiles()
                .sorted(HoodieLogFile.getLogFileComparator())
                .map(file -> file.getPath().toString())
                .collect(Collectors.toList()),
            config.getMaxDFSStreamBufferSize(),
            currentInstant,
            Option.empty());
        List<String> currentInstants = scanner2.getValidBlockInstants();
        assertEquals(prevInstants, currentInstants);
      });
    }
  }

  @Test
  public void testArchivalOnLogCompaction() throws Exception {
    HoodieCompactionConfig logCompactionConfig = HoodieCompactionConfig.newBuilder()
        .withMaxNumDeltaCommitsBeforeCompaction(1)
        .withLogCompactionBlocksThreshold(1)
        .build();
    HoodieWriteConfig lcWriteConfig = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withCompactionConfig(logCompactionConfig).build();

    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withMaxNumDeltaCommitsBeforeCompaction(1)
        .build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(compactionConfig)
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(2).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(4, 5).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .build();

    try (SparkRDDWriteClient lcWriteClient = new SparkRDDWriteClient(context, lcWriteConfig);
         SparkRDDWriteClient client = new SparkRDDWriteClient(context, config)) {

      // First insert
      String newCommitTime = client.createNewInstantTime();
      insertBatch(config, client, newCommitTime, "000", 100,
          SparkRDDWriteClient::insert, false, false, 10, 100,
          1, Option.empty(), INSTANT_GENERATOR);
      String prevCommitTime = newCommitTime;
      List<String> logCompactionInstantTimes = new ArrayList<>();

      for (int i = 0; i < 6; i++) {
        if (i % 4 == 0) {
          // Schedule compaction.
          Option<String> compactionTimeStamp = client.scheduleCompaction(Option.empty());
          assertTrue(compactionTimeStamp.isPresent());
          HoodieWriteMetadata result = client.compact(compactionTimeStamp.get());
          client.commitCompaction(compactionTimeStamp.get(), result, Option.empty());
          assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTimeStamp.get()));
          prevCommitTime = compactionTimeStamp.get();
        }

        // Upsert
        newCommitTime = client.createNewInstantTime();
        updateBatch(config, client, newCommitTime, prevCommitTime,
            Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
            false, false, 50, 10, 0, config.populateMetaFields(), INSTANT_GENERATOR);
        // Schedule log compaction.
        Option<String> logCompactionTimeStamp = lcWriteClient.scheduleLogCompaction(Option.empty());
        if (logCompactionTimeStamp.isPresent()) {
          logCompactionInstantTimes.add(logCompactionTimeStamp.get());
          HoodieWriteMetadata result = lcWriteClient.logCompact(logCompactionTimeStamp.get());
          lcWriteClient.commitLogCompaction(logCompactionTimeStamp.get(), result, Option.empty());
          prevCommitTime = logCompactionTimeStamp.get();
        }
      }
      boolean logCompactionInstantArchived = false;
      Map<String, List<HoodieInstant>> instantsMap = metaClient.getArchivedTimeline().getInstantsAsStream()
          .collect(Collectors.groupingBy(HoodieInstant::requestedTime));
      for (String logCompactionTimeStamp : logCompactionInstantTimes) {
        List<HoodieInstant> instants = instantsMap.get(logCompactionTimeStamp);
        if (instants == null) {
          continue;
        }
        assertEquals(1, instants.size());
        assertEquals(HoodieTimeline.DELTA_COMMIT_ACTION, instants.get(0).getAction());
        logCompactionInstantArchived = true;
      }
      assertTrue(logCompactionInstantArchived);
    }
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

}
