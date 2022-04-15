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

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.log.HoodieUnMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.action.compact.strategy.SpecificPartitionCompactionStrategy;
import org.apache.hudi.table.action.compact.strategy.SpecificPartitionLogCompactionStrategy;
import org.apache.hudi.testutils.GenericRecordValidationTestUtils;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        HoodieIndex.IndexType.INMEMORY).withAutoCommit(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Do insert and updates thrice one after the other.
    // Insert
    String commitTime = HoodieActiveTimeline.createNewInstantTime();
    insertBatch(config, client, commitTime, "000", 10, SparkRDDWriteClient::insert,
        false, false, 10, 10, 1, Option.empty());

    // Update
    String commitTimeBetweenPrevAndNew = commitTime;
    commitTime = HoodieActiveTimeline.createNewInstantTime();
    updateBatch(config, client, commitTime, commitTimeBetweenPrevAndNew,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", 5, SparkRDDWriteClient::upsert,
        false, false, 5, 10, 2, config.populateMetaFields());

    // Delete 5 records
    String prevCommitTime = commitTime;
    commitTime = HoodieActiveTimeline.createNewInstantTime();
    deleteBatch(config, client, commitTime, prevCommitTime,
        "000", 2, SparkRDDWriteClient::delete, false, false,
        0, 150);

    // Verify all the records.
    metaClient.reloadActiveTimeline();
    Map<String, GenericRecord> recordMap = GenericRecordValidationTestUtils.getRecordsMap(config, hadoopConf, dataGen);
    assertEquals(8, recordMap.size());
  }

  @Test
  public void testCompactionOnMORTable() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withAutoCommit(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Do insert and updates thrice one after the other.
    // Insert
    String commitTime = HoodieActiveTimeline.createNewInstantTime();
    insertBatch(config, client, commitTime, "000", 10, SparkRDDWriteClient::insert,
        false, false, 10, 10, 1, Option.empty());

    // Update
    String commitTimeBetweenPrevAndNew = commitTime;
    commitTime = HoodieActiveTimeline.createNewInstantTime();
    updateBatch(config, client, commitTime, commitTimeBetweenPrevAndNew,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), "000", 5, SparkRDDWriteClient::upsert,
        false, false, 5, 10, 2, config.populateMetaFields());

    // Schedule and execute compaction.
    Option<String> timeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(timeStamp.isPresent());
    client.compact(timeStamp.get());

    // Verify all the records.
    metaClient.reloadActiveTimeline();
    assertDataInMORTable(config, commitTime, timeStamp.get(), hadoopConf, Arrays.asList(dataGen.getPartitionPaths()));
  }

  @Test
  public void testLogCompactionOnMORTable() throws Exception {
    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.ALWAYS_ALLOW)
        .withLogCompactionBlocksThreshold("1")
        .build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withAutoCommit(true).withCompactionConfig(compactionConfig).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // First insert
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", 10,
        SparkRDDWriteClient::insert, false, false, 10, 100,
        1, Option.empty());

    String prevCommitTime = newCommitTime;
    for (int i = 0; i < 5; i++) {
      // Upsert
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      updateBatch(config, client, newCommitTime, prevCommitTime,
          Option.of(Arrays.asList(prevCommitTime)), "000", 2, SparkRDDWriteClient::upsert,
          false, false, 50, 10, i + 2, config.populateMetaFields());
      prevCommitTime = newCommitTime;
    }

    // Schedule and execute compaction.
    Option<String> compactionTimeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTimeStamp.isPresent());
    client.compact(compactionTimeStamp.get());

    prevCommitTime = compactionTimeStamp.get();
    //TODO: Below commits are creating duplicates when all the tests are run together. but individually they are passing.
    for (int i = 0; i < 2; i++) {
      // Upsert
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      updateBatch(config, client, newCommitTime, prevCommitTime,
          Option.of(Arrays.asList(prevCommitTime)), "000", 2, SparkRDDWriteClient::upsert,
          false, false, 50, 10, i + 8, config.populateMetaFields());
      prevCommitTime = newCommitTime;
    }
    String lastCommitBeforeLogCompaction = prevCommitTime;

    // Schedule and execute compaction.
    Option<String> logCompactionTimeStamp = client.scheduleLogCompaction(Option.empty());
    assertTrue(logCompactionTimeStamp.isPresent());
    client.logCompact(logCompactionTimeStamp.get());

    // Verify all the records.
    assertDataInMORTable(config, lastCommitBeforeLogCompaction, logCompactionTimeStamp.get(),
        hadoopConf, Arrays.asList(dataGen.getPartitionPaths()));
  }

  /**
   * Test logcompaction before any compaction is scheduled. Here base file is not yet created.
   */
  @Test
  public void testLogCompactionOnMORTableWithoutBaseFile() throws Exception {
    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.ALWAYS_ALLOW)
        .withLogCompactionBlocksThreshold("1")
        .build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withAutoCommit(true).withCompactionConfig(compactionConfig).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // First insert 10 records
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", 10,
        SparkRDDWriteClient::insert, false, false, 100, 100,
        1, Option.empty());

    // Upsert 5 records
    String prevCommitTime = newCommitTime;
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    updateBatch(config, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(prevCommitTime)), "000", 5, SparkRDDWriteClient::upsert,
        false, false, 50, 100, 2, config.populateMetaFields());
    prevCommitTime = newCommitTime;

    // Delete 5 records
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    deleteBatch(config, client, newCommitTime, prevCommitTime,
        "000", 3, SparkRDDWriteClient::delete, false, false,
        0, 150);
    prevCommitTime = newCommitTime;

    String lastCommitBeforeLogCompaction = prevCommitTime;
    // Schedule and execute compaction.
    Option<String> timeStamp = client.scheduleLogCompaction(Option.empty());
    assertTrue(timeStamp.isPresent());
    client.logCompact(timeStamp.get());
    // Verify all the records.
    assertDataInMORTable(config, lastCommitBeforeLogCompaction, timeStamp.get(),
        hadoopConf, Arrays.asList(dataGen.getPartitionPaths()));
  }

  /**
   * Test scheduling logcompaction right after scheduling compaction. This should fail.
   */
  @Test
  public void testSchedulingLogCompactionAfterSchedulingCompaction() throws Exception {
    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.ALWAYS_ALLOW)
        .withLogCompactionBlocksThreshold("1")
        .build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withAutoCommit(true).withCompactionConfig(compactionConfig).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // First insert
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", 100,
        SparkRDDWriteClient::insert, false, false, 10, 100,
        1, Option.empty());

    String prevCommitTime = newCommitTime;
    // Upsert
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    updateBatch(config, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
        false, false, 50, 10, 2, config.populateMetaFields());

    // Schedule compaction
    Option<String> compactionTimeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTimeStamp.isPresent());

    // Try scheduing log compaction, it wont succeed
    Option<String> logCompactionTimeStamp = client.scheduleLogCompaction(Option.empty());
    assertFalse(logCompactionTimeStamp.isPresent());
  }

  /**
   * Test scheduling compaction right after scheduling logcompaction. This should fail.
   */
  @Test
  public void testSchedulingCompactionAfterSchedulingLogCompaction() throws Exception {
    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.ALWAYS_ALLOW)
        .withLogCompactionBlocksThreshold("1")
        .build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withAutoCommit(true).withCompactionConfig(compactionConfig).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // First insert
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", 100,
        SparkRDDWriteClient::insert, false, false, 10, 100,
        1, Option.empty());

    String prevCommitTime = newCommitTime;
    // Upsert
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    updateBatch(config, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
        false, false, 50, 10, 2, config.populateMetaFields());

    // Schedule log compaction
    Option<String> logCompactionTimeStamp = client.scheduleLogCompaction(Option.empty());
    assertTrue(logCompactionTimeStamp.isPresent());

    // Try scheduling compaction, it wont succeed
    Option<String> compactionTimeStamp = client.scheduleCompaction(Option.empty());
    assertFalse(compactionTimeStamp.isPresent());
  }

  /**
   * Test scheduling and execution of logcompaction during a pending compaction execution on a different file group.
   */
  @Test
  public void testRunningCompactionAndLogCompactionInparallelForDifferentFileGroups() throws Exception {
    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.ALWAYS_ALLOW)
        .withPartitionsForCompaction(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)
        .withCompactionStrategy(new SpecificPartitionCompactionStrategy())
        .build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withAutoCommit(true).withCompactionConfig(compactionConfig).build();
    SparkRDDWriteClient client = new SparkRDDWriteClient(context, config);

    HoodieCompactionConfig logCompactionConfig = HoodieCompactionConfig.newBuilder()
        .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.ALWAYS_ALLOW)
        .withPartitionsForLogCompaction(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
        .withCompactionStrategy(new SpecificPartitionLogCompactionStrategy())
        .withLogCompactionBlocksThreshold("1")
        .build();
    HoodieWriteConfig lcConfig = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withAutoCommit(true).withCompactionConfig(logCompactionConfig).build();
    SparkRDDWriteClient lcClient = new SparkRDDWriteClient(context, lcConfig);

    // First insert
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", 100,
        SparkRDDWriteClient::insert, false, false, 10, 100,
        1, Option.empty());

    String prevCommitTime = newCommitTime;
    // Upsert
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    updateBatch(config, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
        false, false, 50, 10, 2, config.populateMetaFields());

    // Schedule compaction.
    Option<String> logCompactionTimeStamp = lcClient.scheduleLogCompaction(Option.empty());
    assertTrue(logCompactionTimeStamp.isPresent());

    // Schedule log compaction.
    Option<String> compactionTimeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTimeStamp.isPresent());

    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);
    Stream<Pair<String, CompactionOperation>> compactionOperations = table.getFileSystemView().getPendingCompactionOperations();
    List<String> partitionsUndergoingCompaction = compactionOperations.map(op -> op.getValue().getPartitionPath())
        .collect(Collectors.toList());
    assertEquals(Collections.singletonList(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH), partitionsUndergoingCompaction);
    Stream<Pair<String, CompactionOperation>> logCompactionOperations = table.getFileSystemView().getPendingLogCompactionOperations();
    List<String> partitionsUndergoingLogCompaction = logCompactionOperations.map(op -> op.getValue().getPartitionPath())
        .collect(Collectors.toList());
    assertEquals(Collections.singletonList(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH), partitionsUndergoingLogCompaction);

    // Now execute both the operations in reverse order.
    client.compact(compactionTimeStamp.get());
    lcClient.logCompact(logCompactionTimeStamp.get());
    assertDataInMORTable(config, newCommitTime, compactionTimeStamp.get(), hadoopConf, Arrays.asList(dataGen.getPartitionPaths()));
    assertDataInMORTable(config, newCommitTime, logCompactionTimeStamp.get(), hadoopConf, Arrays.asList(dataGen.getPartitionPaths()));
  }

  @Test
  public void testCleanFunctionalityWhenCompactionRequestedInstantIsPresent() throws Exception {
    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.ALWAYS_ALLOW)
        .build();

    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withAutoCommit(true)
        .withCompactionConfig(compactionConfig)
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(4).build())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // First insert. Here First file slice gets added to file group.
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", 100,
        SparkRDDWriteClient::insert, false, false, 10, 100,
        1, Option.empty());

    // Schedule and execute compaction. Here, second file slice gets added.
    Option<String> compactionTimeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTimeStamp.isPresent());
    client.compact(compactionTimeStamp.get());
    String prevCommitTime = compactionTimeStamp.get();

    // First upsert on  second file slice.
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    updateBatch(config, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
        false, false, 50, 10, 2, config.populateMetaFields());
    prevCommitTime = newCommitTime;

    // Schedule compaction. Third file slice gets added, compaction is not complete so base file is not created yet.
    compactionTimeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTimeStamp.isPresent());
    prevCommitTime = compactionTimeStamp.get();

    for (int i = 0; i < 6; i++) {
      // First upsert on third file slice.
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      updateBatch(config, client, newCommitTime, prevCommitTime,
          Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
          false, false, 50, 10, 2, config.populateMetaFields());
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
  public void testCleanFunctionalityWhenLogCompactionRequestedInstantIsPresent() throws Exception {
    HoodieCompactionConfig logCompactionConfig = HoodieCompactionConfig.newBuilder()
        .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.ALWAYS_ALLOW)
        .withLogCompactionBlocksThreshold("0")
        .withCompactionStrategy(new SpecificPartitionLogCompactionStrategy())
        .withPartitionsForLogCompaction(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
        .build();
    HoodieWriteConfig lcWriteConfig = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withAutoCommit(true).withCompactionConfig(logCompactionConfig).build();
    SparkRDDWriteClient lcWriteClient = new SparkRDDWriteClient(context, lcWriteConfig);

    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.ALWAYS_ALLOW)
        .withCompactionStrategy(new SpecificPartitionCompactionStrategy())
        .build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withAutoCommit(true)
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(4).build())
        .withCompactionConfig(compactionConfig).build();
    SparkRDDWriteClient client = new SparkRDDWriteClient(context, config);

    // First insert. Here First file slice gets added to file group.
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", 100,
        SparkRDDWriteClient::insert, false, false, 10, 100,
        1, Option.empty());

    // Schedule Logcompaction. This leaves a logcompaction instant in the timeline.
    // So, cleaner cannot create a plan until this is complete.
    Option<String> logCompactionTimeStamp = lcWriteClient.scheduleLogCompaction(Option.empty());
    assertTrue(logCompactionTimeStamp.isPresent());

    // Schedule and execute compaction. Here, second file slice gets added.
    Option<String> compactionTimeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTimeStamp.isPresent());
    client.compact(compactionTimeStamp.get());
    String prevCommitTime = compactionTimeStamp.get();

    // First upsert on  second file slice.
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    updateBatch(config, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
        false, false, 50, 10, 2, config.populateMetaFields());
    prevCommitTime = newCommitTime;

    // Schedule compaction. Third file slice gets added, compaction is not complete so base file is not created yet.
    compactionTimeStamp = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTimeStamp.isPresent());
    client.compact(compactionTimeStamp.get());
    prevCommitTime = compactionTimeStamp.get();

    for (int i = 0; i < 6; i++) {
      // First upsert on third file slice.
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      updateBatch(config, client, newCommitTime, prevCommitTime,
          Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
          false, false, 50, 10, 2, config.populateMetaFields());
      prevCommitTime = newCommitTime;
      // Since pending logcompaction instants is present cleaner will never get invoked.
      HoodieInstant lastInstant = metaClient.reloadActiveTimeline().lastInstant().get();
      if (HoodieTimeline.CLEAN_ACTION.equals(lastInstant.getAction())) {
        HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils
            .deserializeHoodieCleanMetadata(getHoodieTable(metaClient, config).getActiveTimeline().getInstantDetails(lastInstant).get());
        HoodieCleanPartitionMetadata cleanPartitionMetadata = cleanMetadata.getPartitionMetadata().get(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
        if (cleanPartitionMetadata != null) {
          assertTrue(cleanPartitionMetadata.getDeletePathPatterns().isEmpty());
          assertTrue(cleanPartitionMetadata.getSuccessDeleteFiles().isEmpty());
          assertTrue(cleanPartitionMetadata.getFailedDeleteFiles().isEmpty());
        }
      }
    }
  }

  @Test
  public void testRollbackOnLogCompaction() throws Exception {
    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.ALWAYS_ALLOW)
        .withLogCompactionBlocksThreshold("1")
        .build();
    HoodieWriteConfig lcConfig = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
        .withAutoCommit(false).withCompactionConfig(compactionConfig).build();
    SparkRDDWriteClient lcClient = new SparkRDDWriteClient(context, lcConfig);
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
        .withAutoCommit(true).build();
    SparkRDDWriteClient client = new SparkRDDWriteClient(context, config);

    // First insert
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", 100,
        SparkRDDWriteClient::insert, false, false, 10, 100,
        1, Option.empty());
    String prevCommitTime = newCommitTime;

    // Upsert
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    updateBatch(config, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(prevCommitTime)), "000", 10, SparkRDDWriteClient::upsert,
        false, false, 10, 10, 4, config.populateMetaFields());
    prevCommitTime = newCommitTime;

    // Schedule and execute logcompaction but do not commit.
    Option<String> logCompactionTimeStamp = lcClient.scheduleLogCompaction(Option.empty());
    assertTrue(logCompactionTimeStamp.isPresent());
    lcClient.logCompact(logCompactionTimeStamp.get());

    // Rollback the log compaction commit.
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.LOG_COMPACTION_ACTION, logCompactionTimeStamp.get());
    getHoodieTable(metaClient, config).rollbackInflightLogCompaction(instant);

    // Validate timeline.
    HoodieTimeline activeTimeline = metaClient.reloadActiveTimeline();
    HoodieInstant rollbackInstant = activeTimeline.lastInstant().get();
    assertEquals(3, activeTimeline.countInstants());
    assertEquals(HoodieTimeline.ROLLBACK_ACTION, rollbackInstant.getAction());

    // Validate block instant times.
    validateBlockInstantsBeforeAndAfterRollback(config, prevCommitTime, rollbackInstant.getTimestamp());
    prevCommitTime = rollbackInstant.getTimestamp();

    // Do one more upsert
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    updateBatch(config, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(prevCommitTime)), "000", 10, SparkRDDWriteClient::upsert,
        false, false, 10, 10, 4, config.populateMetaFields());
    prevCommitTime = newCommitTime;

    // Complete logcompaction now.
    logCompactionTimeStamp = lcClient.scheduleLogCompaction(Option.empty());
    assertTrue(logCompactionTimeStamp.isPresent());
    HoodieWriteMetadata metadata = lcClient.logCompact(logCompactionTimeStamp.get());
    lcClient.commitLogCompaction(logCompactionTimeStamp.get(), (HoodieCommitMetadata) metadata.getCommitMetadata().get(), Option.empty());
    assertDataInMORTable(config, prevCommitTime, logCompactionTimeStamp.get(), hadoopConf, Arrays.asList(dataGen.getPartitionPaths()));
  }

  private void validateBlockInstantsBeforeAndAfterRollback(HoodieWriteConfig config, String instant, String currentInstant) {
    HoodieTable table = getHoodieTable(metaClient, config);
    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) table.getSliceView();
    List<String> partitionPaths = Stream.of(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS).collect(Collectors.toList());
    for (String partitionPath: partitionPaths) {
      fileSystemView.getLatestFileSlices(partitionPath).forEach(slice -> {
        HoodieUnMergedLogRecordScanner scanner = HoodieUnMergedLogRecordScanner.newBuilder()
            .withFileSystem(metaClient.getFs())
            .withBasePath(table.getMetaClient().getBasePath())
            .withLogFilePaths(slice.getLogFiles()
                .sorted(HoodieLogFile.getLogFileComparator())
                .map(file -> file.getPath().toString())
                .collect(Collectors.toList()))
            .withLatestInstantTime(instant)
            .withBufferSize(config.getMaxDFSStreamBufferSize())
            .withUseScanV2(true)
            .build();
        scanner.scanInternal(Option.empty(), true);
        List<String> prevInstants = scanner.getValidBlockInstants();
        HoodieUnMergedLogRecordScanner scanner2 = HoodieUnMergedLogRecordScanner.newBuilder()
            .withFileSystem(metaClient.getFs())
            .withBasePath(table.getMetaClient().getBasePath())
            .withLogFilePaths(slice.getLogFiles()
                .sorted(HoodieLogFile.getLogFileComparator())
                .map(file -> file.getPath().toString())
                .collect(Collectors.toList()))
            .withLatestInstantTime(currentInstant)
            .withBufferSize(config.getMaxDFSStreamBufferSize())
            .withUseScanV2(true)
            .build();
        scanner2.scanInternal(Option.empty(), true);
        List<String> currentInstants = scanner2.getValidBlockInstants();
        assertEquals(prevInstants, currentInstants);
      });
    }
  }

  @Test
  public void testArchivalOnLogCompaction() throws Exception {
    HoodieCompactionConfig logCompactionConfig = HoodieCompactionConfig.newBuilder()
        .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.ALWAYS_ALLOW)
        .withLogCompactionBlocksThreshold("1")
        .build();
    HoodieWriteConfig lcWriteConfig = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.INMEMORY).withAutoCommit(true).withCompactionConfig(logCompactionConfig).build();
    SparkRDDWriteClient lcWriteClient = new SparkRDDWriteClient(context, lcWriteConfig);

    Properties metadataProps = new Properties();
    metadataProps.put("hoodie.metadata.compact.max.delta.commits", "1");
    HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder()
        .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.ALWAYS_ALLOW)
        .build();
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
        .withAutoCommit(true).withCompactionConfig(compactionConfig)
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(2).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(3, 4).build().newBuilder().build())
        .withProperties(metadataProps).build();
    SparkRDDWriteClient client = new SparkRDDWriteClient(context, config);

    // First insert
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    insertBatch(config, client, newCommitTime, "000", 100,
        SparkRDDWriteClient::insert, false, false, 10, 100,
        1, Option.empty());
    String prevCommitTime = newCommitTime;
    List<String> logCompactionInstantTimes = new ArrayList<>();

    for (int i = 0; i < 6; i++) {
      if (i % 4 == 0) {
        // Schedule compaction.
        Option<String> compactionTimeStamp = client.scheduleCompaction(Option.empty());
        assertTrue(compactionTimeStamp.isPresent());
        client.compact(compactionTimeStamp.get());
        prevCommitTime = compactionTimeStamp.get();
      }

      // Upsert
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      updateBatch(config, client, newCommitTime, prevCommitTime,
          Option.of(Arrays.asList(prevCommitTime)), "000", 50, SparkRDDWriteClient::upsert,
          false, false, 50, 10, 0, config.populateMetaFields());
      // Schedule compaction.
      Option<String> logCompactionTimeStamp = lcWriteClient.scheduleLogCompaction(Option.empty());
      if (logCompactionTimeStamp.isPresent()) {
        logCompactionInstantTimes.add(logCompactionTimeStamp.get());
        lcWriteClient.logCompact(logCompactionTimeStamp.get());
        prevCommitTime = logCompactionTimeStamp.get();
      }
    }
    boolean logCompactionInstantArchived = false;
    Map<String, List<HoodieInstant>> instantsMap = metaClient.getArchivedTimeline().getInstants()
        .collect(Collectors.groupingBy(HoodieInstant::getTimestamp));
    for (String logCompactionTimeStamp: logCompactionInstantTimes) {
      List<HoodieInstant> instants = instantsMap.get(logCompactionTimeStamp);
      if (instants == null) {
        continue;
      }
      assertEquals(3, instants.size());
      for (HoodieInstant instant: instants) {
        if (instant.isCompleted()) {
          assertEquals(HoodieTimeline.DELTA_COMMIT_ACTION, instant.getAction());
        } else {
          assertEquals(HoodieTimeline.LOG_COMPACTION_ACTION, instant.getAction());
        }
      }
      logCompactionInstantArchived = true;
    }
    assertTrue(logCompactionInstantArchived);
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

}
