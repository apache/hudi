/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.compact;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bloom.HoodieBloomIndex;
import org.apache.hudi.index.bloom.SparkHoodieBloomIndexHelper;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.strategy.PartitionRegexBasedCompactionStrategy;
import org.apache.hudi.table.action.compact.strategy.SmallBoundedIOCompactionStrategy;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import com.codahale.metrics.Counter;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieCompactor extends HoodieSparkClientTestHarness {

  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws Exception {
    // Initialize a local spark env
    initSparkContexts();

    // Create a temp folder as the base path
    initPath();
    storage = HoodieStorageUtils.getStorage(basePath, storageConf);
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ);
    initTestDataGenerator();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  public HoodieWriteConfig getConfig() {
    return getConfig(1);
  }

  public HoodieWriteConfig getConfig(int numCommitsBeforeCompaction) {
    return getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(numCommitsBeforeCompaction).build())
        .withMetricsConfig(getMetricsConfig())
        .build();
  }

  private static HoodieMetricsConfig getMetricsConfig() {
    return HoodieMetricsConfig.newBuilder().on(true).withReporterType("INMEMORY").build();
  }

  private long getCompactionMetricCount(String metric) {
    HoodieMetrics metrics = writeClient.getMetrics();
    String metricName = metrics.getMetricsName(HoodieTimeline.COMPACTION_ACTION, metric + ".counter");
    SortedMap<String, Counter> counters = metrics.getMetrics().getRegistry().getCounters();

    return counters.containsKey(metricName) ? counters.get(metricName).getCount() : 0;
  }

  public HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024)
            .withInlineCompaction(false).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).orcMaxFileSize(1024 * 1024).build())
        .withMemoryConfig(HoodieMemoryConfig.newBuilder().withMaxDFSStreamBufferSize(1 * 1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build());
  }

  @Test
  public void testCompactionOnCopyOnWriteFail() throws Exception {
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(getConfig());) {
      HoodieTable table = HoodieSparkTable.create(getConfig(), context, metaClient);
      String compactionInstantTime = writeClient.createNewInstantTime();
      assertThrows(HoodieNotSupportedException.class, () -> {
        table.scheduleCompaction(context, compactionInstantTime, Option.empty());
        table.compact(context, compactionInstantTime);
      });

      // Verify compaction.requested, compaction.completed metrics counts.
      assertEquals(0, getCompactionMetricCount(HoodieTimeline.REQUESTED_COMPACTION_SUFFIX));
      assertEquals(0, getCompactionMetricCount(HoodieTimeline.COMPLETED_COMPACTION_SUFFIX));
    }
  }

  @Test
  public void testCompactionEmpty() {
    HoodieWriteConfig config = getConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(getConfig(), context, metaClient);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {

      String newCommitTime = writeClient.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      String compactionInstantTime = writeClient.createNewInstantTime();
      Option<HoodieCompactionPlan> plan = table.scheduleCompaction(context, compactionInstantTime, Option.empty());
      assertFalse(plan.isPresent(), "If there is nothing to compact, result will be empty");

      // Verify compaction.requested, compaction.completed metrics counts.
      assertEquals(0, getCompactionMetricCount(HoodieTimeline.REQUESTED_COMPACTION_SUFFIX));
      assertEquals(0, getCompactionMetricCount(HoodieTimeline.COMPLETED_COMPACTION_SUFFIX));
    }
  }

  @Test
  public void testScheduleCompactionWithInflightInstant() {
    HoodieWriteConfig config = getConfig();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      // insert 100 records.
      String newCommitTime = "100";
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // create one inflight instance.
      newCommitTime = "102";
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);
      metaClient.getActiveTimeline().transitionRequestedToInflight(INSTANT_GENERATOR.createNewInstant(State.REQUESTED,
          HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime), Option.empty());

      // create one compaction instance before exist inflight instance.
      String compactionTime = "101";
      writeClient.scheduleCompactionAtInstant(compactionTime, Option.empty());
    }
  }

  @Test
  public void testNeedCompactionCondition() throws Exception {
    HoodieWriteConfig config = getConfig(3);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      // insert 100 records.
      String newCommitTime = "100";
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

      // commit 1
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // commit 2
      updateRecords(config, "101", records);

      // commit 3 (inflight)
      newCommitTime = "102";
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);
      metaClient.getActiveTimeline().transitionRequestedToInflight(INSTANT_GENERATOR.createNewInstant(State.REQUESTED,
          HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime), Option.empty());

      // check that compaction will not be scheduled
      String compactionTime = "107";
      assertFalse(writeClient.scheduleCompactionAtInstant(compactionTime, Option.empty()));
    }
  }

  @Test
  public void testWriteStatusContentsAfterCompaction() throws Exception {
    // insert 100 records
    HoodieWriteConfig config = getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withMetricsConfig(getMetricsConfig())
        .build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = "100";
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 1000);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // Update all the 1000 records across 5 commits to generate sufficient log files.
      int i = 1;
      for (; i < 5; i++) {
        newCommitTime = String.format("10%s", i);
        updateRecords(config, newCommitTime, records);
        assertLogFilesNumEqualsTo(config, i);
      }
      HoodieWriteMetadata result = compact(writeClient, String.format("10%s", i));
      verifyCompaction(result, 4000L);

      // Verify compaction.requested, compaction.completed metrics counts.
      assertEquals(1, getCompactionMetricCount(HoodieTimeline.REQUESTED_COMPACTION_SUFFIX));
      assertEquals(1, getCompactionMetricCount(HoodieTimeline.COMPLETED_COMPACTION_SUFFIX));
    }
  }

  @Test
  public void testSpillingWhenCompaction() throws Exception {
    // insert 100 records
    HoodieWriteConfig config = getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withMemoryConfig(HoodieMemoryConfig.newBuilder()
            .withMaxMemoryMaxSize(1L, 1L).build()) // force spill
        .withMetricsConfig(getMetricsConfig())
        .build();

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = writeClient.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // trigger 2 updates following with compaction
      for (int i = 1; i < 5; i += 2) {
        // Update all the 100 records
        newCommitTime = writeClient.createNewInstantTime();
        updateRecords(config, newCommitTime, records);

        assertLogFilesNumEqualsTo(config, 1);

        HoodieWriteMetadata result = compact(writeClient, writeClient.createNewInstantTime());
        verifyCompaction(result, 100L);

        // Verify compaction.requested, compaction.completed metrics counts.
        assertEquals(i / 2 + 1, getCompactionMetricCount(HoodieTimeline.REQUESTED_COMPACTION_SUFFIX));
        assertEquals(i / 2 + 1, getCompactionMetricCount(HoodieTimeline.COMPLETED_COMPACTION_SUFFIX));
      }
    }
  }

  private static Stream<Arguments> regexTestParameters() {
    Object[][] data = new Object[][] {
        {
          ".*", Arrays.asList("2015/03/16", "2015/03/17", "2016/03/15")
        },
        {
          "2017/.*/.*", Collections.emptyList()
        },
        {
          "2015/03/.*", Arrays.asList("2015/03/16", "2015/03/17")
        },
        {
          "2016/.*/.*", Arrays.asList("2016/03/15")
        }
    };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("regexTestParameters")
  public void testCompactionSpecifyPartition(String regex, List<String> expectedCompactedPartition) throws Exception {
    HoodieCompactionConfig.Builder builder = HoodieCompactionConfig.newBuilder()
        .withCompactionStrategy(new PartitionRegexBasedCompactionStrategy()).withMaxNumDeltaCommitsBeforeCompaction(1);
    builder.withCompactionSpecifyPartitionPathRegex(regex);
    HoodieWriteConfig config = getConfigBuilder()
        .withCompactionConfig(builder.build())
        .withMetricsConfig(getMetricsConfig()).build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = writeClient.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 10);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // update 1 time
      newCommitTime = writeClient.createNewInstantTime();
      updateRecords(config, newCommitTime, records);
      assertLogFilesNumEqualsTo(config, 1);

      // schedule compaction
      String compactionInstant = writeClient.createNewInstantTime();
      boolean scheduled = writeClient.scheduleCompactionAtInstant(compactionInstant, Option.empty());
      if (expectedCompactedPartition.isEmpty()) {
        assertFalse(scheduled);
        return;
      }

      HoodieWriteMetadata result = compact(writeClient, compactionInstant);

      assertTrue(result.getWriteStats().isPresent());
      List<HoodieWriteStat> stats = (List<HoodieWriteStat>) result.getWriteStats().get();
      assertEquals(expectedCompactedPartition.size(), stats.size());
      expectedCompactedPartition.forEach(expectedPartition -> {
        assertTrue(stats.stream().anyMatch(stat -> stat.getPartitionPath().contentEquals(expectedPartition)));
      });
    }
  }

  @Test
  public void testPartitionsForIncrCompaction() throws Exception {
    HoodieWriteConfig config = getConfigBuilder()
        .withIncrementalTableServiceEnabled(true)
        .withCompactionConfig(HoodieCompactionConfig
            .newBuilder()
            .withCompactionStrategy(new SmallBoundedIOCompactionStrategy())
            .withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .build();
    String[] partitions = {"20250115"};

    HoodieWriteConfig config2 = getConfigBuilder()
        .withIncrementalTableServiceEnabled(true)
        .withCompactionConfig(HoodieCompactionConfig
            .newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .build();
    String[] partitions2 = {"20250116"};

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      prepareRecords(writeClient, config, partitions);
      prepareRecords(writeClient, config, partitions);
      compact(writeClient, writeClient.createNewInstantTime());
      HoodieCompactionPlan compactionPlan1 = getLatestCompactionPlan();

      List<String> affectedPartitions = compactionPlan1.getOperations().stream()
          .map(HoodieCompactionOperation::getPartitionPath).collect(Collectors.toList());
      // compaction including 20250115
      assertTrue(affectedPartitions.contains(partitions[0]));
      List<String> missingPartitions = compactionPlan1.getMissingSchedulePartitions();
      // compaction missing 20250115 because not all the fileSlices are all processed.
      assertTrue(missingPartitions.contains(partitions[0]));
    }

    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config2)) {
      prepareRecords(writeClient, config2, partitions2);
      prepareRecords(writeClient, config2, partitions2);
      compact(writeClient, writeClient.createNewInstantTime());
      HoodieCompactionPlan compactionPlan2 = getLatestCompactionPlan();
      List<String> affectedPartitions2 = compactionPlan2.getOperations().stream()
          .map(HoodieCompactionOperation::getPartitionPath).collect(Collectors.toList());
      // compaction including 20250115 (fetched from recorded missing partitions)
      assertTrue(affectedPartitions2.contains(partitions[0]));
      // compaction including 20250116 (fetched from commit meta)
      assertTrue(affectedPartitions2.contains(partitions2[0]));
      // no missing partitions because all the fileSlices are all processed.
      List<String> missingPartitions2 = compactionPlan2.getMissingSchedulePartitions();
      assertTrue(missingPartitions2.isEmpty());
    }
  }

  private void prepareRecords(SparkRDDWriteClient writeClient, HoodieWriteConfig config, String[] partitions) throws Exception {
    initTestDataGenerator(partitions);
    String newCommitTime = writeClient.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

    // insert
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
    JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
    writeClient.insert(recordsRDD, newCommitTime).collect();

    // update
    newCommitTime = writeClient.createNewInstantTime();
    updateRecords(config, newCommitTime, records);
  }

  private HoodieCompactionPlan getLatestCompactionPlan() {
    metaClient.reloadActiveTimeline();
    Option<HoodieInstant> latestCompactionInstant = metaClient.getActiveTimeline().getCommitTimeline().lastInstant();
    HoodieInstant compactionPlanInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION,
        latestCompactionInstant.get().requestedTime(), InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    return CompactionUtils.getCompactionPlan(metaClient, compactionPlanInstant);
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  private void updateRecords(HoodieWriteConfig config, String newCommitTime, List<HoodieRecord> records) throws IOException {
    HoodieTable table = HoodieSparkTable.create(config, context);
    List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
    JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
    HoodieIndex index = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    JavaRDD<HoodieRecord> updatedTaggedRecordsRDD = tagLocation(index, updatedRecordsRDD, table);

    WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);
    writeClient.upsertPreppedRecords(updatedTaggedRecordsRDD, newCommitTime).collect();
    metaClient.reloadActiveTimeline();
  }

  /**
   * Verify that all data file has {@code expected} number of log files.
   *
   * @param config   The writer config
   * @param expected The expected number of log files
   */
  private void assertLogFilesNumEqualsTo(HoodieWriteConfig config, int expected) {
    HoodieTable table = HoodieSparkTable.create(config, context);
    for (String partitionPath : dataGen.getPartitionPaths()) {
      List<FileSlice> groupedLogFiles =
          table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
      for (FileSlice fileSlice : groupedLogFiles) {
        assertEquals(expected, fileSlice.getLogFiles().count(), "There should be " + expected + " log file written for every data file");
      }
    }
  }

  /**
   * Do a compaction.
   */
  private HoodieWriteMetadata compact(SparkRDDWriteClient writeClient, String compactionInstantTime) {
    writeClient.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());
    HoodieWriteMetadata compactMetadata = writeClient.compact(compactionInstantTime);
    return compactMetadata;
  }

  /**
   * Verify that all partition paths are present in the HoodieWriteMetadata result.
   */
  private void verifyCompaction(HoodieWriteMetadata compactionMetadata, long expectedTotalLogRecords) {
    assertTrue(compactionMetadata.getWriteStats().isPresent());
    List<HoodieWriteStat> stats = (List<HoodieWriteStat>) compactionMetadata.getWriteStats().get();
    assertEquals(dataGen.getPartitionPaths().length, stats.size());
    for (String partitionPath : dataGen.getPartitionPaths()) {
      assertTrue(stats.stream().anyMatch(stat -> stat.getPartitionPath().contentEquals(partitionPath)));
    }

    stats.forEach(stat -> {
      HoodieWriteStat.RuntimeStats runtimeStats = stat.getRuntimeStats();
      assertNotNull(runtimeStats);
      assertEquals(0, runtimeStats.getTotalCreateTime());
      assertTrue(runtimeStats.getTotalUpsertTime() > 0);
      assertTrue(runtimeStats.getTotalScanTime() > 0);
    });

    // Verify the number of log records processed during the compaction.
    long actualTotalLogRecords =
        stats.stream().mapToLong(HoodieWriteStat::getTotalLogRecords).sum();
    assertEquals(expectedTotalLogRecords, actualTotalLogRecords);

    // Verify the number of records written during compaction.
    long actualNumWritten =
        stats.stream().mapToLong(HoodieWriteStat::getNumWrites).sum();
    long actualNumUpdates =
        stats.stream().mapToLong(HoodieWriteStat::getNumUpdateWrites).sum();
    long actualInserts =
        stats.stream().mapToLong(HoodieWriteStat::getNumInserts).sum();
    assertTrue(actualNumWritten > 0
        && actualNumWritten == actualNumUpdates + actualInserts);
  }
}
