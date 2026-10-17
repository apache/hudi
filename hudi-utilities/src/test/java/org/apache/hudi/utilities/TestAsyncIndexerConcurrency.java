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

package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.INDEXING_ACTION;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.utilities.UtilHelpers.EXECUTE;
import static org.apache.hudi.utilities.UtilHelpers.SCHEDULE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the interleavings between a regular writer and the async indexer that decide whether a
 * commit lands in a newly built metadata partition.
 *
 * <p>The existing {@code TestHoodieIndexer} coverage runs the indexer to completion and then
 * rewinds instants to inflight, which exercises rollback handling but never places a writer and the
 * indexer in flight at the same time. The races here depend on the ordering between the indexer's
 * timeline snapshot, its unlocked write of the {@code partitions.inflight} table-config marker, and
 * the writer's choice of which metadata partitions to update, so they are only reachable when both
 * are genuinely concurrent.
 *
 * <p>Each test asserts on index content rather than on the presence of a metadata deltacommit: a
 * deltacommit written by a writer that only knew about {@code files} is indistinguishable from one
 * that wrote the new partition, which is the assumption these tests exist to check.
 */
public class TestAsyncIndexerConcurrency extends SparkClientFunctionalTestHarness implements SparkProvider {

  private static final HoodieTestDataGenerator DATA_GENERATOR = new HoodieTestDataGenerator(0L);
  private static final String COLUMN_STATS_PROPS = "streamer-config/indexer.properties";
  private static final String RECORD_INDEX_PROPS = "streamer-config/indexer-record-index.properties";

  private HoodieTableMetaClient metaClient;
  /** Every record key committed by this test, used to probe the record index. */
  private final Set<String> committedKeys = new HashSet<>();

  @BeforeEach
  public void init() throws IOException {
    metaClient = getHoodieMetaClient(storageConf(), basePath());
  }

  /**
   * A commit that lands between scheduling and running the indexer must appear in the new
   * partition. It is after the plan's base instant, so the bootstrap excludes it and catch-up is
   * solely responsible for it.
   */
  @Test
  public void testCommitBetweenScheduleAndExecuteIsIndexed() throws Exception {
    String tableName = "indexer_commit_between_schedule_and_execute";
    Set<String> keysBefore = upsertToTable(filesOnlyMetadataConfig(), tableName);

    scheduleIndexing(RECORD_INDEX, tableName);
    HoodieInstant indexInstant = pendingIndexInstant();

    // Commits while the index request is pending but before the run starts.
    Set<String> keysDuring = upsertToTable(filesOnlyMetadataConfig(), tableName);

    executeIndexing(indexInstant.requestedTime(), RECORD_INDEX, tableName);

    Set<String> indexedKeys = readRecordIndexKeys();
    assertTrue(indexedKeys.containsAll(keysBefore), "keys committed before scheduling must be indexed");
    assertTrue(indexedKeys.containsAll(keysDuring),
        "keys committed between schedule and execute are missing from the record index: "
            + missing(keysDuring, indexedKeys));
  }

  /**
   * A writer whose commit straddles the start of the indexer run must appear in the new partition.
   * The writer selects its metadata partitions from the table config before the indexer writes the
   * inflight marker, so it writes only {@code files}; catch-up then sees a metadata deltacommit for
   * the instant and must not treat that as proof the instant was indexed.
   */
  @Test
  public void testCommitOverlappingRunStartIsIndexed() throws Exception {
    String tableName = "indexer_commit_overlapping_run_start";
    Set<String> keysBefore = upsertToTable(filesOnlyMetadataConfig(), tableName);

    scheduleIndexing(RECORD_INDEX, tableName);
    HoodieInstant indexInstant = pendingIndexInstant();

    CountDownLatch writerStarted = new CountDownLatch(1);
    CountDownLatch indexerStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Set<String>> writerFuture = executor.submit(() -> {
        HoodieWriteConfig writeConfig = writeConfigBuilder(tableName)
            .withMetadataConfig(filesOnlyMetadataConfig()).build();
        try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient<>(context(), writeConfig)) {
          String instant = writeClient.startCommit();
          List<HoodieRecord> records = DATA_GENERATOR.generateInserts(instant, 50);
          // Write the data files first: the write client has now fixed its view of which metadata
          // partitions to update, taken from the table config before the indexer marks the new one
          // inflight. Doing the data write before releasing the indexer keeps the executors off a
          // half-initialized metadata table, which is a single-JVM test artifact rather than a
          // production race.
          List<WriteStatus> statusList =
              writeClient.upsert(jsc().parallelize(records, 1), instant).collect();
          assertNoWriteErrors(statusList);
          // Let the indexer run, then commit into the window it opens.
          writerStarted.countDown();
          assertTrue(indexerStarted.await(60, TimeUnit.SECONDS), "indexer did not start");
          writeClient.commit(instant, jsc().parallelize(statusList));
          return recordKeys(records);
        }
      });

      Future<?> indexerFuture = executor.submit(() -> {
        assertTrue(writerStarted.await(60, TimeUnit.SECONDS), "writer did not start");
        indexerStarted.countDown();
        executeIndexing(indexInstant.requestedTime(), RECORD_INDEX, tableName);
        return null;
      });

      Set<String> keysDuring = writerFuture.get(300, TimeUnit.SECONDS);
      indexerFuture.get(300, TimeUnit.SECONDS);

      Set<String> indexedKeys = readRecordIndexKeys();
      assertTrue(indexedKeys.containsAll(keysBefore), "keys committed before scheduling must be indexed");
      assertTrue(indexedKeys.containsAll(keysDuring),
          "keys from a commit overlapping the indexer run are missing from the record index: "
              + missing(keysDuring, indexedKeys));
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Catch-up must not accept a metadata deltacommit as evidence that an instant was indexed when
   * that deltacommit only carries {@code files} entries. This is the same interleaving as above,
   * asserted directly on the metadata partition rather than on query results.
   */
  @Test
  public void testFilesOnlyDeltacommitIsNotTreatedAsIndexed() throws Exception {
    String tableName = "indexer_files_only_deltacommit";
    upsertToTable(filesOnlyMetadataConfig(), tableName);

    scheduleIndexing(COLUMN_STATS, tableName);
    HoodieInstant indexInstant = pendingIndexInstant();

    Set<String> keysDuring = upsertToTable(filesOnlyMetadataConfig(), tableName);
    String straddlingInstant = latestCompletedWriteInstant();

    executeIndexing(indexInstant.requestedTime(), COLUMN_STATS, tableName);

    // The instant has a metadata deltacommit, written before the new partition existed.
    HoodieTableMetaClient metadataMetaClient = metadataMetaClient();
    assertTrue(metadataMetaClient.getActiveTimeline().containsInstant(straddlingInstant),
        "precondition: the straddling commit has a metadata deltacommit");
    assertFalse(keysDuring.isEmpty());

    // Every base file that commit wrote must have a column stats entry. Checking the entries, not
    // the deltacommit, is the whole point: the deltacommit predates the new partition.
    List<Pair<String, String>> partitionAndFileNames = baseFilesWrittenBy(straddlingInstant);
    assertFalse(partitionAndFileNames.isEmpty(), "precondition: the commit wrote base files");
    Set<Pair<String, String>> withStats = columnStatsEntriesFor(partitionAndFileNames);
    List<Pair<String, String>> withoutStats = partitionAndFileNames.stream()
        .filter(pair -> !withStats.contains(pair)).collect(Collectors.toList());
    assertTrue(withoutStats.isEmpty(),
        "column stats are missing for files written by a commit whose metadata deltacommit predates "
            + "the new partition; catch-up accepted deltacommit existence as proof of indexing: "
            + withoutStats);
  }

  /**
   * An instant that is inflight when indexing is scheduled is excluded from the bootstrap by the
   * no-holes base instant and delegated to catch-up, so catch-up owns it entirely. It must be
   * indexed whether it completes before or after the run begins.
   */
  @Test
  public void testInflightAtScheduleIsIndexedAfterCompletion() throws Exception {
    String tableName = "indexer_inflight_at_schedule";
    Set<String> keysBefore = upsertToTable(filesOnlyMetadataConfig(), tableName);

    HoodieWriteConfig writeConfig = writeConfigBuilder(tableName)
        .withMetadataConfig(filesOnlyMetadataConfig()).build();
    Set<String> keysInflight;
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient<>(context(), writeConfig)) {
      String instant = writeClient.startCommit();
      List<HoodieRecord> records = DATA_GENERATOR.generateInserts(instant, 50);
      List<WriteStatus> statusList =
          writeClient.upsert(jsc().parallelize(records, 1), instant).collect();
      assertNoWriteErrors(statusList);
      keysInflight = recordKeys(records);

      // Schedule while the write above is still inflight: the plan's base instant is cut below it.
      scheduleIndexing(RECORD_INDEX, tableName);
      HoodieInstant indexInstant = pendingIndexInstant();
      assertBaseInstantPrecedes(indexInstant, instant);

      writeClient.commit(instant, jsc().parallelize(statusList));
      executeIndexing(indexInstant.requestedTime(), RECORD_INDEX, tableName);
    }

    Set<String> indexedKeys = readRecordIndexKeys();
    assertTrue(indexedKeys.containsAll(keysBefore), "keys committed before scheduling must be indexed");
    assertTrue(indexedKeys.containsAll(keysInflight),
        "keys from an instant that was inflight at schedule time are missing from the record index: "
            + missing(keysInflight, indexedKeys));
  }

  /**
   * Two writes completing out of requested-time order must both be indexed. The timeline orders
   * instants by completion time while the catch-up range is computed with requested-time
   * comparisons, so the later-requested-but-earlier-completed instant is the one at risk.
   */
  @Test
  public void testOutOfOrderCompletionIsIndexed() throws Exception {
    String tableName = "indexer_out_of_order_completion";
    Set<String> keysBefore = upsertToTable(filesOnlyMetadataConfig(), tableName);

    HoodieWriteConfig writeConfig = writeConfigBuilder(tableName)
        .withMetadataConfig(filesOnlyMetadataConfig()).build();
    Set<String> keysEarlierRequested;
    Set<String> keysLaterRequested;
    try (SparkRDDWriteClient first = new SparkRDDWriteClient<>(context(), writeConfig);
         SparkRDDWriteClient second = new SparkRDDWriteClient<>(context(), writeConfig)) {
      String earlierInstant = first.startCommit();
      List<HoodieRecord> earlierRecords = DATA_GENERATOR.generateInsertsForPartition(
          earlierInstant, 40, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
      List<WriteStatus> earlierStatus =
          first.upsert(jsc().parallelize(earlierRecords, 1), earlierInstant).collect();
      assertNoWriteErrors(earlierStatus);
      keysEarlierRequested = recordKeys(earlierRecords);

      String laterInstant = second.startCommit();
      List<HoodieRecord> laterRecords = DATA_GENERATOR.generateInsertsForPartition(
          laterInstant, 40, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH);
      List<WriteStatus> laterStatus =
          second.upsert(jsc().parallelize(laterRecords, 1), laterInstant).collect();
      assertNoWriteErrors(laterStatus);
      keysLaterRequested = recordKeys(laterRecords);

      // The later-requested instant completes first.
      second.commit(laterInstant, jsc().parallelize(laterStatus));

      scheduleIndexing(RECORD_INDEX, tableName);
      HoodieInstant indexInstant = pendingIndexInstant();

      first.commit(earlierInstant, jsc().parallelize(earlierStatus));
      executeIndexing(indexInstant.requestedTime(), RECORD_INDEX, tableName);
    }

    Set<String> indexedKeys = readRecordIndexKeys();
    assertTrue(indexedKeys.containsAll(keysBefore), "keys committed before scheduling must be indexed");
    assertTrue(indexedKeys.containsAll(keysLaterRequested),
        "keys from the earlier-completing instant are missing: " + missing(keysLaterRequested, indexedKeys));
    assertTrue(indexedKeys.containsAll(keysEarlierRequested),
        "keys from the later-completing instant are missing: " + missing(keysEarlierRequested, indexedKeys));
  }

  /**
   * Scheduling the indexer and scheduling a table service both take the state-change lock, so they
   * serialize. Either ordering must leave the table-service instant covered: scheduled first, it
   * becomes a hole that lowers the plan's base instant; scheduled second, it is after the base.
   */
  @Test
  public void testConcurrentTableServiceSchedulingIsCovered() throws Exception {
    String tableName = "indexer_concurrent_table_service";
    Set<String> keysBefore = upsertToTable(filesOnlyMetadataConfig(), tableName);

    HoodieWriteConfig writeConfig = writeConfigBuilder(tableName)
        .withMetadataConfig(filesOnlyMetadataConfig()).build();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient<>(context(), writeConfig)) {
      CountDownLatch bothReady = new CountDownLatch(2);
      Future<?> cleanFuture = executor.submit(() -> {
        bothReady.countDown();
        assertTrue(bothReady.await(60, TimeUnit.SECONDS), "peer not ready");
        writeClient.scheduleTableService(Option.empty(), TableServiceType.CLEAN);
        return null;
      });
      Future<?> indexFuture = executor.submit(() -> {
        bothReady.countDown();
        assertTrue(bothReady.await(60, TimeUnit.SECONDS), "peer not ready");
        scheduleIndexing(RECORD_INDEX, tableName);
        return null;
      });
      cleanFuture.get(180, TimeUnit.SECONDS);
      indexFuture.get(180, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    HoodieInstant indexInstant = pendingIndexInstant();
    Set<String> keysAfter = upsertToTable(filesOnlyMetadataConfig(), tableName);
    executeIndexing(indexInstant.requestedTime(), RECORD_INDEX, tableName);

    Set<String> indexedKeys = readRecordIndexKeys();
    assertTrue(indexedKeys.containsAll(keysBefore), "keys committed before scheduling must be indexed");
    assertTrue(indexedKeys.containsAll(keysAfter),
        "keys committed after concurrent scheduling are missing: " + missing(keysAfter, indexedKeys));
  }

  /**
   * After a successful run the partition is advertised as completed in table config and is no
   * longer marked inflight.
   */
  @Test
  public void testPartitionMarkedCompleteAfterSuccessfulRun() throws Exception {
    String tableName = "indexer_partition_completion_marker";
    upsertToTable(filesOnlyMetadataConfig(), tableName);

    scheduleIndexing(RECORD_INDEX, tableName);
    HoodieInstant indexInstant = pendingIndexInstant();
    upsertToTable(filesOnlyMetadataConfig(), tableName);
    executeIndexing(indexInstant.requestedTime(), RECORD_INDEX, tableName);

    metaClient.reloadTableConfig();
    Set<String> completed = metaClient.getTableConfig().getMetadataPartitions();
    Set<String> inflight = metaClient.getTableConfig().getMetadataPartitionsInflight();
    assertTrue(completed.contains(RECORD_INDEX.getPartitionPath()),
        "record index should be marked completed after a successful run");
    assertFalse(inflight.contains(RECORD_INDEX.getPartitionPath()),
        "record index should no longer be marked inflight after a successful run");
    assertTrue(completed.contains(FILES.getPartitionPath()));
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private HoodieMetadataConfig filesOnlyMetadataConfig() {
    return HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withMetadataIndexColumnStats(false)
        .withMetadataIndexBloomFilter(false)
        .build();
  }

  private HoodieWriteConfig.Builder writeConfigBuilder(String tableName) {
    // Match the indexer's concurrency setup: both sides must use the same lock provider for the
    // state-change lock to mean anything between them.
    return HoodieWriteConfig.newBuilder()
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class).build())
        .withPath(basePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .forTable(tableName);
  }

  private Set<String> upsertToTable(HoodieMetadataConfig metadataConfig, String tableName) {
    HoodieWriteConfig writeConfig = writeConfigBuilder(tableName).withMetadataConfig(metadataConfig).build();
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient<>(context(), writeConfig)) {
      String instant = writeClient.startCommit();
      List<HoodieRecord> records = DATA_GENERATOR.generateInserts(instant, 100);
      List<WriteStatus> statusList = writeClient.upsert(jsc().parallelize(records, 1), instant).collect();
      assertNoWriteErrors(statusList);
      writeClient.commit(instant, jsc().parallelize(statusList));
      return recordKeys(records);
    }
  }

  private void scheduleIndexing(MetadataPartitionType partitionType, String tableName) {
    runIndexer(partitionType, tableName, SCHEDULE, null);
  }

  private void executeIndexing(String indexInstantTime, MetadataPartitionType partitionType, String tableName) {
    runIndexer(partitionType, tableName, EXECUTE, indexInstantTime);
  }

  private void runIndexer(MetadataPartitionType partitionType, String tableName, String mode, String instantTime) {
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    config.basePath = basePath();
    config.tableName = tableName;
    config.indexTypes = partitionType.name();
    config.runningMode = mode;
    String propsFile = RECORD_INDEX == partitionType ? RECORD_INDEX_PROPS : COLUMN_STATS_PROPS;
    config.propsFilePath =
        Objects.requireNonNull(getClass().getClassLoader().getResource(propsFile)).getPath();
    if (instantTime != null) {
      config.indexInstantTime = instantTime;
    }
    HoodieIndexer indexer = new HoodieIndexer(jsc(), config);
    assertEquals(0, indexer.start(0), "indexer run in mode " + mode + " failed");
    metaClient = HoodieTableMetaClient.reload(metaClient);
  }

  private HoodieInstant pendingIndexInstant() {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    Option<HoodieInstant> instant = metaClient.getActiveTimeline()
        .filter(i -> INDEXING_ACTION.equals(i.getAction()))
        .filterInflightsAndRequested()
        .lastInstant();
    assertTrue(instant.isPresent(), "expected a pending indexing instant");
    return instant.get();
  }

  private String latestCompletedWriteInstant() {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    Option<HoodieInstant> instant = metaClient.getActiveTimeline()
        .getWriteTimeline().filterCompletedInstants().lastInstant();
    assertTrue(instant.isPresent(), "expected a completed write instant");
    return instant.get().requestedTime();
  }

  private void assertBaseInstantPrecedes(HoodieInstant indexInstant, String inflightInstant) throws IOException {
    String baseInstant = metaClient.getActiveTimeline().readIndexPlan(indexInstant)
        .getIndexPartitionInfos().get(0).getIndexUptoInstant();
    assertTrue(compareTimestamps(baseInstant, LESSER_THAN, inflightInstant),
        "the plan's base instant " + baseInstant + " should precede the inflight instant " + inflightInstant);
  }

  private HoodieTableMetaClient metadataMetaClient() {
    HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(
        context(), metaClient.getStorage(), filesOnlyMetadataConfig(), metaClient.getBasePath().toString());
    return metadata.getMetadataMetaClient();
  }

  /**
   * Returns the subset of this test's committed keys that the record index can resolve. A key the
   * index never received simply does not come back, which is the loss these tests look for.
   */
  private Set<String> readRecordIndexKeys() {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieMetadataConfig readConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(false).build();
    try (HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(
        context(), metaClient.getStorage(), readConfig, metaClient.getBasePath().toString())) {
      HoodieData<String> keys = context().parallelize(new ArrayList<>(committedKeys), 1);
      return new HashSet<>(metadata.readRecordIndexLocationsWithKeys(keys).keys().collectAsList());
    } catch (Exception e) {
      throw new RuntimeException("failed to read the record index", e);
    }
  }

  /** The (partition, file name) pairs for every base file the given commit wrote. */
  private List<Pair<String, String>> baseFilesWrittenBy(String instantTime) throws IOException {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant instant = metaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants()
        .filter(i -> i.requestedTime().equals(instantTime)).firstInstant()
        .orElseThrow(() -> new IllegalStateException("no completed instant " + instantTime));
    HoodieCommitMetadata commitMetadata = metaClient.getActiveTimeline().readCommitMetadata(instant);
    return commitMetadata.getPartitionToWriteStats().entrySet().stream()
        .flatMap(entry -> entry.getValue().stream()
            .map(stat -> Pair.of(entry.getKey(), new StoragePath(stat.getPath()).getName())))
        .collect(Collectors.toList());
  }

  /** The subset of the given files that the column stats partition has an entry for. */
  private Set<Pair<String, String>> columnStatsEntriesFor(List<Pair<String, String>> partitionAndFileNames) {
    HoodieMetadataConfig readConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true).build();
    try (HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(
        context(), metaClient.getStorage(), readConfig, metaClient.getBasePath().toString())) {
      return metadata.getColumnStats(partitionAndFileNames, "_row_key").keySet();
    } catch (Exception e) {
      throw new RuntimeException("failed to read column stats", e);
    }
  }

  private Set<String> recordKeys(List<HoodieRecord> records) {
    Set<String> keys = records.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet());
    committedKeys.addAll(keys);
    return keys;
  }

  private static String missing(Set<String> expected, Set<String> actual) {
    Set<String> missing = new HashSet<>(expected);
    missing.removeAll(actual);
    return missing.size() + " of " + expected.size() + " keys, e.g. "
        + missing.stream().limit(5).collect(Collectors.toList());
  }
}
