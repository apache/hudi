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

package org.apache.hudi.utilities;

import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.HoodieTableMetaClient.reload;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.INFLIGHT;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.config.HoodieWriteConfig.CLIENT_HEARTBEAT_INTERVAL_IN_MS;
import static org.apache.hudi.config.HoodieWriteConfig.CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getFileSystemViewForMetadataTable;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.metadataPartitionExists;
import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.EXPRESSION_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.SECONDARY_INDEX;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.utilities.HoodieIndexer.DROP_INDEX;
import static org.apache.hudi.utilities.UtilHelpers.SCHEDULE;
import static org.apache.hudi.utilities.UtilHelpers.SCHEDULE_AND_EXECUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieIndexer extends SparkClientFunctionalTestHarness implements SparkProvider {

  private static final HoodieTestDataGenerator DATA_GENERATOR = new HoodieTestDataGenerator(0L);
  private static int colStatsFileGroupCount = HoodieMetadataConfig.METADATA_INDEX_COLUMN_STATS_FILE_GROUP_COUNT.defaultValue();
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void init() throws IOException {
    this.metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
  }

  @AfterAll
  public static void cleanup() {
    DATA_GENERATOR.close();
  }

  @Test
  public void testGetRequestedPartitionTypes() {
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    config.basePath = basePath();
    config.tableName = "indexer_test";
    config.indexTypes = "FILES,BLOOM_FILTERS,COLUMN_STATS";
    HoodieIndexer indexer = new HoodieIndexer(jsc(), config);
    List<MetadataPartitionType> partitionTypes = indexer.getRequestedPartitionTypes(config.indexTypes, Option.empty());
    assertTrue(partitionTypes.contains(FILES));
    assertTrue(partitionTypes.contains(BLOOM_FILTERS));
    assertTrue(partitionTypes.contains(COLUMN_STATS));
  }

  @Test
  public void testIsIndexBuiltForAllRequestedTypes() {
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    config.basePath = basePath();
    config.tableName = "indexer_test";
    config.indexTypes = "BLOOM_FILTERS,COLUMN_STATS";
    HoodieIndexer indexer = new HoodieIndexer(jsc(), config);
    HoodieIndexCommitMetadata commitMetadata = HoodieIndexCommitMetadata.newBuilder()
        .setIndexPartitionInfos(Arrays.asList(new HoodieIndexPartitionInfo(
            1,
            COLUMN_STATS.getPartitionPath(),
            "0000",
            Collections.emptyMap())))
        .build();
    assertFalse(indexer.isIndexBuiltForAllRequestedTypes(commitMetadata.getIndexPartitionInfos()));

    config.indexTypes = "COLUMN_STATS";
    indexer = new HoodieIndexer(jsc(), config);
    assertTrue(indexer.isIndexBuiltForAllRequestedTypes(commitMetadata.getIndexPartitionInfos()));
  }

  @Test
  public void testIndexerWithNotAllIndexesEnabled() {
    String tableName = "indexer_test";
    // enable files and bloom_filters only w/ the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = getMetadataConfigBuilder(true, false).withMetadataIndexBloomFilter(true).withMetadataIndexColumnStats(false);
    upsertToTable(metadataConfigBuilder.build(), tableName);

    // validate table config
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));

    // build indexer config which has only column_stats enabled (files and bloom filter is already enabled)
    indexMetadataPartitionsAndAssert(COLUMN_STATS.getPartitionPath(), Arrays.asList(new MetadataPartitionType[] {FILES, BLOOM_FILTERS}), Collections.emptyList(), tableName,
        "streamer-config/indexer.properties");
  }

  @Test
  public void testIndexerWithFilesPartition() {
    String tableName = "indexer_test";
    // enable files and bloom_filters only with the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = getMetadataConfigBuilder(false, false).withMetadataIndexBloomFilter(true);
    upsertToTable(metadataConfigBuilder.build(), tableName);

    // validate table config
    assertFalse(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));

    // build indexer config which has only files enabled
    indexMetadataPartitionsAndAssert(FILES.getPartitionPath(), Collections.emptyList(), Arrays.asList(new MetadataPartitionType[] {COLUMN_STATS, BLOOM_FILTERS}), tableName,
        "streamer-config/indexer.properties");
  }

  /**
   * Upsert with metadata table (FILES partition) enabled and then run indexer for RECORD_INDEX.
   */
  @Test
  public void testIndexerForRecordIndex() {
    String tableName = "indexer_test";
    // enable files and bloom_filters only with the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withAsyncIndex(false).withMetadataIndexColumnStats(false);
    upsertToTable(metadataConfigBuilder.build(), tableName);

    // validate table config
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));

    // build indexer config which has only files enabled
    indexMetadataPartitionsAndAssert(RECORD_INDEX.getPartitionPath(), Collections.singletonList(FILES), Arrays.asList(new MetadataPartitionType[] {COLUMN_STATS, BLOOM_FILTERS}), tableName,
        "streamer-config/indexer-record-index.properties");
  }

  /**
   * Test indexer for RLI and secondary index.
   */
  @Test
  public void testIndexerForSecondaryIndex() {
    String tableName = "indexer_test_rli_si";
    // enable files only with the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withAsyncIndex(false).withMetadataIndexColumnStats(false);
    upsertToTable(metadataConfigBuilder.build(), tableName);

    // validate table config
    metaClient = reload(metaClient);
    assertTrue(metaClient.getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));

    // build RLI with the indexer
    indexMetadataPartitionsAndAssert(RECORD_INDEX.getPartitionPath(), Collections.singletonList(FILES), Arrays.asList(COLUMN_STATS, BLOOM_FILTERS), tableName,
        "streamer-config/indexer-record-index.properties");
    // build SI with the indexer
    String indexName = "idx_rider";
    indexMetadataPartitionsAndAssert(SECONDARY_INDEX.getPartitionPath() + indexName, Arrays.asList(FILES, RECORD_INDEX),
        Arrays.asList(COLUMN_STATS, BLOOM_FILTERS), tableName, "streamer-config/indexer-secondary-index.properties");
    // validate the secondary index is built
    assertTrue(metadataPartitionExists(basePath(), context(), SECONDARY_INDEX.getPartitionPath() + indexName));
  }

  /**
   * Test partitions created by async indexer is not deleted by regular writers if the partition is not enabled in the regular writer config.
   * <p>
   * 1. Upsert with metadata enabled with default configs (RECORD_INDEX is disabled by default).
   * 2. Run async indexer for RECORD_INDEX.
   * 3. Upsert with metadata enabled with default configs (RECORD_INDEX is disabled by default).
   * 4. Validate RECORD_INDEX partition is not deleted.
   */
  @Test
  public void testIndexerWithDifferentIngestionWriterConfig() {
    String tableName = "indexer_test";
    // Step 1: upsert with metadata enabled with default configs (RECORD_INDEX is disabled by default)
    HoodieMetadataConfig.Builder metadataConfigBuilder = HoodieMetadataConfig.newBuilder().enable(true);
    upsertToTable(metadataConfigBuilder.build(), tableName);

    // Step 2: build indexer config which has only RECORD_INDEX enabled
    indexMetadataPartitionsAndAssert(RECORD_INDEX.getPartitionPath(), Arrays.asList(new MetadataPartitionType[] {FILES, RECORD_INDEX}), Arrays.asList(new MetadataPartitionType[] {BLOOM_FILTERS}),
        tableName, "streamer-config/indexer-record-index.properties");
    // validate table config and metadata partitions actually exist
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(RECORD_INDEX.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath(), context(), RECORD_INDEX.getPartitionPath()));

    // Step 3: writer upsert with metadata enabled with RECORD_INDEX enabled
    upsertToTable(metadataConfigBuilder.withEnableGlobalRecordLevelIndex(true).build(), tableName);

    // Step 4: validate RECORD_INDEX partition is not deleted
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(RECORD_INDEX.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath(), context(), RECORD_INDEX.getPartitionPath()));
  }

  /**
   * Test indexer for Expression Index.
   */
  @Test
  public void testIndexerForExpressionIndex() {
    String tableName = "indexer_test_expr_ei";
    // enable files only with the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withAsyncIndex(false).withMetadataIndexColumnStats(false);
    upsertToTable(metadataConfigBuilder.build(), tableName);

    // validate table config
    metaClient = reload(metaClient);
    assertTrue(metaClient.getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));

    // build RLI with the indexer
    indexMetadataPartitionsAndAssert(RECORD_INDEX.getPartitionPath(), Collections.singletonList(FILES), Arrays.asList(new MetadataPartitionType[] {COLUMN_STATS, BLOOM_FILTERS}), tableName,
        "streamer-config/indexer-record-index.properties");

    // rebuild metadata config with expression index name and indexed column
    String indexName = "idx_ts";
    // build expression index with the indexer
    indexMetadataPartitionsAndAssert(EXPRESSION_INDEX.getPartitionPath() + indexName, Arrays.asList(new MetadataPartitionType[] {FILES, RECORD_INDEX}),
        Arrays.asList(new MetadataPartitionType[] {COLUMN_STATS, BLOOM_FILTERS}), tableName, "streamer-config/indexer-expression-index.properties");
    // validate the expression index is built
    assertTrue(metadataPartitionExists(basePath(), context(), EXPRESSION_INDEX.getPartitionPath() + indexName));
  }

  @Test
  public void testIndexerWithWriterFinishingFirst() throws IOException {
    // Test the case where the indexer is running, i.e., the delta commit in the metadata table
    // is inflight, while the regular writer is updating metadata table.
    // The delta commit from the indexer should not be rolled back.
    String tableName = "indexer_with_writer_finishing_first";
    // Enable files and bloom_filters only with the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder =
        getMetadataConfigBuilder(true, false).withMetadataIndexBloomFilter(true).withMetadataIndexColumnStats(false);
    HoodieMetadataConfig metadataConfig = metadataConfigBuilder.build();
    upsertToTable(metadataConfig, tableName);

    // Validate table config
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));

    // Run async indexer, creating a new indexing instant in the data table and a new delta commit
    // in the metadata table, with the suffix "004"
    scheduleAndExecuteIndexing(COLUMN_STATS, tableName, "streamer-config/indexer.properties");

    HoodieInstant indexingInstant = metaClient.getActiveTimeline()
        .filter(i -> HoodieTimeline.INDEXING_ACTION.equals(i.getAction()))
        .getInstants().get(0);
    HoodieIndexPlan indexPlan = metaClient.getActiveTimeline().readIndexPlan(indexingInstant);
    HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(
        context(), metaClient.getStorage(), metadataConfig, metaClient.getBasePath().toString());
    HoodieTableMetaClient metadataMetaClient = metadata.getMetadataMetaClient();
    String mdtCommitTime = indexingInstant.requestedTime();
    assertTrue(metadataMetaClient.getActiveTimeline().containsInstant(mdtCommitTime));

    // Reverts both instants to inflight state, to simulate inflight indexing instants
    metaClient.getActiveTimeline().revertToInflight(indexingInstant);
    metaClient = reload(metaClient);

    HoodieInstant mdtIndexingCommit = metadataMetaClient.getActiveTimeline()
        .filter(i -> i.requestedTime().equals(mdtCommitTime))
        .getInstants().get(0);
    metadataMetaClient.getActiveTimeline().revertToInflight(mdtIndexingCommit);
    metadataMetaClient = reload(metadataMetaClient);
    // Simulate heartbeats for ongoing write from async indexer in the metadata table
    HoodieHeartbeatClient heartbeatClient = new HoodieHeartbeatClient(
        metadataMetaClient.getStorage(), metadataMetaClient.getBasePath().toString(),
        CLIENT_HEARTBEAT_INTERVAL_IN_MS.defaultValue().longValue(),
        CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES.defaultValue());
    heartbeatClient.start(mdtCommitTime);

    HoodieMetadataConfig metadataConfigColStats = getMetadataConfigBuilder(true, false).withMetadataIndexBloomFilter(true).withMetadataIndexColumnStats(true).build();
    upsertToTable(metadataConfigColStats, tableName);
    metaClient = reload(metaClient);
    metadataMetaClient = reload(metadataMetaClient);
    // The delta commit from async indexer in metadata table should not be rolled back
    assertTrue(metadataMetaClient.getActiveTimeline().containsInstant(mdtIndexingCommit.requestedTime()));
    assertTrue(metadataMetaClient.getActiveTimeline().getRollbackTimeline().empty());

    // Simulate heartbeat timeout
    heartbeatClient.stop(mdtCommitTime);
    upsertToTable(metadataConfigColStats, tableName);
    metaClient = reload(metaClient);
    metadataMetaClient = reload(metadataMetaClient);
    // The delta commit from async indexer in metadata table should be rolled back now
    assertFalse(metadataMetaClient.getActiveTimeline().containsInstant(mdtIndexingCommit.requestedTime()));
    assertEquals(1, metadataMetaClient.getActiveTimeline().getRollbackTimeline().countInstants());
    HoodieInstant rollbackInstant = metadataMetaClient.getActiveTimeline()
        .getRollbackTimeline().firstInstant().get();
    HoodieRollbackMetadata rollbackMetadata =
        metadataMetaClient.getActiveTimeline().readRollbackMetadata(rollbackInstant);
    assertEquals(mdtCommitTime, rollbackMetadata.getInstantsRollback()
        .stream().findFirst().get().getCommitTime());
  }

  @Test
  public void testIndexerWithWriterFinishingLast() throws IOException {
    // Test the case where a regular write updating the metadata table is in progress,
    // i.e., a delta commit in the metadata table is inflight, and the async indexer
    // finishes the original delta commit.  In this case, the async indexer should not
    // trigger the rollback on other inflight writes in the metadata table.
    String tableName = "indexer_with_writer_finishing_first";
    // Enable files and bloom_filters only with the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder =
        getMetadataConfigBuilder(true, false).withMetadataIndexBloomFilter(true).withMetadataIndexColumnStats(false);
    HoodieMetadataConfig metadataConfig = metadataConfigBuilder.build();
    upsertToTable(metadataConfig, tableName);
    upsertToTable(metadataConfig, tableName);

    // Transition the last commit to inflight
    HoodieInstant commit = metaClient.getActiveTimeline().lastInstant().get();
    String commitTime = commit.requestedTime();
    metaClient.getActiveTimeline().revertToInflight(commit);

    HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(
        context(), metaClient.getStorage(), metadataConfig, metaClient.getBasePath().toString());
    HoodieTableMetaClient metadataMetaClient = metadata.getMetadataMetaClient();
    HoodieInstant mdtCommit = metadataMetaClient.getActiveTimeline()
        .filter(i -> i.requestedTime().equals(commitTime))
        .getInstants().get(0);
    metadataMetaClient.getActiveTimeline().revertToInflight(mdtCommit);

    // Run async indexer, creating a new indexing instant in the data table and a new delta commit
    // in the metadata table, with the suffix "004"
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    String propsPath = Objects.requireNonNull(getClass().getClassLoader().getResource("streamer-config/indexer.properties")).getPath();
    config.basePath = basePath();
    config.tableName = tableName;
    config.indexTypes = COLUMN_STATS.name();
    config.runningMode = SCHEDULE_AND_EXECUTE;
    config.propsFilePath = propsPath;
    config.configs.add(HoodieMetadataConfig.METADATA_INDEX_COLUMN_STATS_FILE_GROUP_COUNT.key() + "=" + colStatsFileGroupCount);
    config.configs.add(HoodieMetadataConfig.METADATA_INDEX_CHECK_TIMEOUT_SECONDS + "=1");

    // start the indexer and validate files index is completely built out
    HoodieIndexer indexer = new HoodieIndexer(jsc(), config);
    // The catchup must finish even with inflight delta commit
    assertEquals(0, indexer.start(0));

    // Now, make sure that the inflight delta commit happened before the async indexer
    // is intact
    metaClient = reload(metaClient);
    metadataMetaClient = reload(metadataMetaClient);

    assertTrue(metaClient.getActiveTimeline().containsInstant(commitTime));
    assertTrue(metadataMetaClient.getActiveTimeline().containsInstant(commitTime));
    assertTrue(metaClient.getActiveTimeline()
        .filter(i -> i.requestedTime().equals(commitTime))
        .getInstants().get(0).isInflight());
    assertTrue(metadataMetaClient.getActiveTimeline()
        .filter(i -> i.requestedTime().equals(commitTime))
        .getInstants().get(0).isInflight());
    assertTrue(metaClient.getActiveTimeline().getRollbackTimeline().empty());
    assertTrue(metadataMetaClient.getActiveTimeline().getRollbackTimeline().empty());
  }

  private static Stream<Arguments> colStatsFileGroupCountParams() {
    return Stream.of(
        Arguments.of(1),
        Arguments.of(2),
        Arguments.of(4),
        Arguments.of(8)
    );
  }

  @ParameterizedTest
  @MethodSource("colStatsFileGroupCountParams")
  public void testColStatsFileGroupCount(int colStatsFileGroupCount) {
    TestHoodieIndexer.colStatsFileGroupCount = colStatsFileGroupCount;
    String tableName = "indexer_test";
    // enable files and bloom_filters only with the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = getMetadataConfigBuilder(false, false).withMetadataIndexBloomFilter(true);
    upsertToTable(metadataConfigBuilder.build(), tableName);

    // validate table config
    assertFalse(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));

    // build indexer config which has only files enabled
    indexMetadataPartitionsAndAssert(FILES.getPartitionPath(), Collections.emptyList(), Arrays.asList(new MetadataPartitionType[] {COLUMN_STATS, BLOOM_FILTERS}), tableName,
        "streamer-config/indexer.properties");

    // build indexer config which has only col stats enabled
    indexMetadataPartitionsAndAssert(COLUMN_STATS.getPartitionPath(), Collections.singletonList(FILES), Arrays.asList(new MetadataPartitionType[] {BLOOM_FILTERS}), tableName,
        "streamer-config/indexer.properties");

    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
        .setConf(metaClient.getStorageConf().newInstance()).setBasePath(metaClient.getMetaPath() + "/metadata").build();
    List<FileSlice> partitionFileSlices =
        HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(
            metadataMetaClient, getFileSystemViewForMetadataTable(metadataMetaClient), COLUMN_STATS.getPartitionPath());
    assertEquals(partitionFileSlices.size(), colStatsFileGroupCount);
  }

  /**
   * If first time indexing is done for any other partition other than FILES partition, exception will be thrown, given metadata table is not initialized in synchronous code path
   * with regular writers.
   */
  @Test
  public void testIndexerForExceptionWithNonFilesPartition() {
    String tableName = "indexer_test";
    // enable files and bloom_filters only with the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = getMetadataConfigBuilder(false, false);
    upsertToTable(metadataConfigBuilder.build(), tableName);
    // validate table config
    assertFalse(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));

    // build indexer config which has only column stats enabled. expected to throw exception.
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    String propsPath = Objects.requireNonNull(getClass().getClassLoader().getResource("streamer-config/indexer.properties")).getPath();
    config.basePath = basePath();
    config.tableName = tableName;
    config.indexTypes = COLUMN_STATS.name();
    config.runningMode = SCHEDULE_AND_EXECUTE;
    config.propsFilePath = propsPath;
    // start the indexer and validate index building fails
    HoodieIndexer indexer = new HoodieIndexer(jsc(), config);
    Throwable cause = assertThrows(RuntimeException.class, () -> indexer.start(0))
        .getCause();
    assertTrue(cause instanceof HoodieException);
    assertTrue(cause.getMessage().contains("Metadata table is not yet initialized"));

    // validate table config
    metaClient = reload(metaClient);
    assertFalse(metaClient.getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertFalse(metaClient.getTableConfig().getMetadataPartitions().contains(COLUMN_STATS.getPartitionPath()));
    assertFalse(metaClient.getTableConfig().getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));
    // validate metadata partitions actually exist
    assertFalse(metadataPartitionExists(basePath(), context(), FILES.getPartitionPath()));

    // trigger FILES partition and indexing should succeed.
    indexMetadataPartitionsAndAssert(FILES.getPartitionPath(), Collections.emptyList(), Arrays.asList(new MetadataPartitionType[] {COLUMN_STATS, BLOOM_FILTERS}), tableName,
        "streamer-config/indexer.properties");

    // build indexer config which has only col stats enabled
    indexMetadataPartitionsAndAssert(COLUMN_STATS.getPartitionPath(), Collections.singletonList(FILES), Arrays.asList(new MetadataPartitionType[] {BLOOM_FILTERS}), tableName,
        "streamer-config/indexer.properties");

    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
        .setConf(metaClient.getStorageConf().newInstance()).setBasePath(metaClient.getMetaPath() + "/metadata").build();
    List<FileSlice> partitionFileSlices =
        HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(
            metadataMetaClient, getFileSystemViewForMetadataTable(metadataMetaClient), COLUMN_STATS.getPartitionPath());
    assertEquals(partitionFileSlices.size(), HoodieMetadataConfig.METADATA_INDEX_COLUMN_STATS_FILE_GROUP_COUNT.defaultValue());
  }

  private void upsertToTable(HoodieMetadataConfig metadataConfig, String tableName) {
    HoodieWriteConfig.Builder writeConfigBuilder = getWriteConfigBuilder(basePath(), tableName);
    HoodieWriteConfig writeConfig = writeConfigBuilder.withMetadataConfig(metadataConfig).build();
    // do one upsert with synchronous metadata update
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context(), writeConfig)) {
      String instant = writeClient.startCommit();
      List<HoodieRecord> records = DATA_GENERATOR.generateInserts(instant, 100);
      List<WriteStatus> statusList = writeClient.upsert(jsc().parallelize(records, 1), instant).collect();
      writeClient.commit(instant, jsc().parallelize(statusList));
      assertNoWriteErrors(statusList);
    }
  }

  private void scheduleAndExecuteIndexing(MetadataPartitionType partitionTypeToIndex, String tableName, String propsFilePath) {
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    String propsPath = Objects.requireNonNull(getClass().getClassLoader().getResource(propsFilePath)).getPath();
    config.basePath = basePath();
    config.tableName = tableName;
    config.indexTypes = partitionTypeToIndex.name();
    config.runningMode = SCHEDULE_AND_EXECUTE;
    config.propsFilePath = propsPath;
    if (partitionTypeToIndex.getPartitionPath().equals(COLUMN_STATS.getPartitionPath())) {
      config.configs.add(HoodieMetadataConfig.METADATA_INDEX_COLUMN_STATS_FILE_GROUP_COUNT.key() + "=" + colStatsFileGroupCount);
    }
    // start the indexer and validate files index is completely built out
    HoodieIndexer indexer = new HoodieIndexer(jsc(), config);
    assertEquals(0, indexer.start(0));

    // validate table config
    metaClient = reload(metaClient);
  }

  private void indexMetadataPartitionsAndAssert(String indexPartitionPath, List<MetadataPartitionType> alreadyCompletedPartitions, List<MetadataPartitionType> nonExistentPartitions,
                                                String tableName, String propsFilePath) {
    scheduleAndExecuteIndexing(MetadataPartitionType.fromPartitionPath(indexPartitionPath), tableName, propsFilePath);

    // validate table config
    metaClient.reloadTableConfig();
    Set<String> completedPartitions = metaClient.getTableConfig().getMetadataPartitions();
    assertTrue(completedPartitions.contains(indexPartitionPath));
    alreadyCompletedPartitions.forEach(entry -> assertTrue(completedPartitions.contains(entry.getPartitionPath())));
    nonExistentPartitions.forEach(entry -> assertFalse(completedPartitions.contains(entry.getPartitionPath())));

    // validate metadata partitions actually exist
    assertTrue(metadataPartitionExists(basePath(), context(), indexPartitionPath));
    alreadyCompletedPartitions.forEach(entry -> assertTrue(metadataPartitionExists(basePath(), context(), entry.getPartitionPath())));
  }

  @Test
  public void testIndexerDropPartitionDeletesInstantFromTimeline() {
    String tableName = "indexer_test";
    HoodieWriteConfig.Builder writeConfigBuilder = getWriteConfigBuilder(basePath(), tableName);
    // enable files only with the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = getMetadataConfigBuilder(true, false).withMetadataIndexBloomFilter(true).withMetadataIndexColumnStats(false);
    HoodieWriteConfig writeConfig = writeConfigBuilder.withMetadataConfig(metadataConfigBuilder.build()).build();
    // do one upsert with synchronous metadata update
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context(), writeConfig)) {
      String instant = writeClient.startCommit();
      List<HoodieRecord> records = DATA_GENERATOR.generateInserts(instant, 100);
      List<WriteStatus> statusList = writeClient.upsert(jsc().parallelize(records, 1), instant).collect();
      writeClient.commit(instant, jsc().parallelize(statusList));
      assertNoWriteErrors(statusList);
    }

    // validate partitions built successfully
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath(), context(), FILES.getPartitionPath()));
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath(), context(), BLOOM_FILTERS.getPartitionPath()));

    // build indexer config which has only column_stats enabled (files is enabled by default)
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    String propsPath = Objects.requireNonNull(getClass().getClassLoader().getResource("streamer-config/indexer.properties")).getPath();
    config.basePath = basePath();
    config.tableName = tableName;
    config.indexTypes = COLUMN_STATS.name();
    config.runningMode = SCHEDULE;
    config.propsFilePath = propsPath;

    // schedule indexing and validate column_stats index is also initialized
    HoodieIndexer indexer = new HoodieIndexer(jsc(), config);
    assertEquals(0, indexer.start(0));
    Option<HoodieInstant> indexInstantInTimeline = metaClient.reloadActiveTimeline().filterPendingIndexTimeline().lastInstant();
    assertTrue(indexInstantInTimeline.isPresent());
    assertEquals(REQUESTED, indexInstantInTimeline.get().getState());

    // drop column_stats and validate indexing.requested is also removed from the timeline
    config.runningMode = DROP_INDEX;
    indexer = new HoodieIndexer(jsc(), config);
    assertEquals(0, indexer.start(0));
    indexInstantInTimeline = metaClient.reloadActiveTimeline().filterPendingIndexTimeline().lastInstant();
    assertFalse(indexInstantInTimeline.isPresent());
    assertFalse(metadataPartitionExists(basePath(), context(), COLUMN_STATS.getPartitionPath()));

    // check other partitions are intact
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath(), context(), FILES.getPartitionPath()));
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath(), context(), BLOOM_FILTERS.getPartitionPath()));
  }

  @Test
  public void testTwoIndexersOneCreateOneDropPartition() {
    String tableName = "indexer_test";
    HoodieWriteConfig.Builder writeConfigBuilder = getWriteConfigBuilder(basePath(), tableName);
    // enable files only with the regular write client
    HoodieMetadataConfig.Builder metadataConfigBuilder = getMetadataConfigBuilder(true, false).withMetadataIndexColumnStats(false);
    HoodieWriteConfig writeConfig = writeConfigBuilder.withMetadataConfig(metadataConfigBuilder.build()).build();
    // do one upsert with synchronous metadata update
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context(), writeConfig)) {
      String instant = writeClient.startCommit();
      List<HoodieRecord> records = DATA_GENERATOR.generateInserts(instant, 100);
      List<WriteStatus> statusList = writeClient.upsert(jsc().parallelize(records, 1), instant).collect();
      writeClient.commit(instant, jsc().parallelize(statusList));
      assertNoWriteErrors(statusList);
    }

    // validate files partition built successfully
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath(), context(), FILES.getPartitionPath()));

    // build indexer config which has only bloom_filters enabled
    HoodieIndexer.Config config = getHoodieIndexConfig(BLOOM_FILTERS.name(), SCHEDULE_AND_EXECUTE, "streamer-config/indexer-only-bloom.properties", tableName);
    // start the indexer and validate bloom_filters index is also complete
    HoodieIndexer indexer = new HoodieIndexer(jsc(), config);
    assertEquals(0, indexer.start(0));
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath(), context(), BLOOM_FILTERS.getPartitionPath()));

    // completed index timeline for later validation
    Option<HoodieInstant> bloomIndexInstant = metaClient.reloadActiveTimeline().filterCompletedIndexTimeline().lastInstant();
    assertTrue(bloomIndexInstant.isPresent());

    // build indexer config which has only column_stats enabled
    config = getHoodieIndexConfig(COLUMN_STATS.name(), SCHEDULE, "streamer-config/indexer.properties", tableName);

    // schedule indexing and validate column_stats index is also initialized
    // and indexing.requested instant is present
    indexer = new HoodieIndexer(jsc(), config);
    assertEquals(0, indexer.start(0));
    Option<HoodieInstant> columnStatsIndexInstant = metaClient.reloadActiveTimeline().filterPendingIndexTimeline().lastInstant();
    assertTrue(columnStatsIndexInstant.isPresent());
    assertEquals(REQUESTED, columnStatsIndexInstant.get().getState());

    // drop column_stats and validate indexing.requested is also removed from the timeline
    // and completed indexing instant corresponding to bloom_filters index is still present
    dropIndexAndAssert(COLUMN_STATS, "streamer-config/indexer.properties", Option.empty(), tableName);

    // check other partitions are intact
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath(), context(), FILES.getPartitionPath()));
    assertTrue(reload(metaClient).getTableConfig().getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));
    assertTrue(metadataPartitionExists(basePath(), context(), BLOOM_FILTERS.getPartitionPath()));

    // drop bloom filter partition. timeline files should not be deleted since the index building is complete.
    dropIndexAndAssert(BLOOM_FILTERS, "streamer-config/indexer-only-bloom.properties", bloomIndexInstant, tableName);
  }

  private void dropIndexAndAssert(MetadataPartitionType indexType, String resourceFilePath, Option<HoodieInstant> completedIndexInstant, String tableName) {
    HoodieIndexer.Config config = getHoodieIndexConfig(indexType.name(), DROP_INDEX, resourceFilePath, tableName);
    HoodieIndexer indexer = new HoodieIndexer(jsc(), config);
    assertEquals(0, indexer.start(0));
    Option<HoodieInstant> pendingFlights = metaClient.reloadActiveTimeline().filterPendingIndexTimeline().lastInstant();
    assertFalse(pendingFlights.isPresent());
    assertFalse(metadataPartitionExists(basePath(), context(), indexType.getPartitionPath()));
    if (completedIndexInstant.isPresent()) {
      assertEquals(completedIndexInstant, metaClient.reloadActiveTimeline().filterCompletedIndexTimeline().lastInstant());
    }
  }

  private HoodieIndexer.Config getHoodieIndexConfig(String indexType, String runMode, String resourceFilePath, String tableName) {
    HoodieIndexer.Config config = new HoodieIndexer.Config();
    String propsPath = Objects.requireNonNull(getClass().getClassLoader().getResource(resourceFilePath)).getPath();
    config.basePath = basePath();
    config.tableName = tableName;
    config.indexTypes = indexType;
    config.runningMode = runMode;
    config.propsFilePath = propsPath;
    return config;
  }

  private static HoodieWriteConfig.Builder getWriteConfigBuilder(String basePath, String tableName) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .forTable(tableName);
  }

  private static HoodieMetadataConfig.Builder getMetadataConfigBuilder(boolean enable, boolean asyncIndex) {
    return HoodieMetadataConfig.newBuilder()
        .enable(enable)
        .withAsyncIndex(asyncIndex);
  }

  /**
   * Test to reproduce the issue where dropping an index that failed mid-way does not clean
   * up the inflight MDT partition from table config.
   */
  @Test
  public void testDropIndexMidwayDoesNotCleanTableConfig() throws IOException {
    String tableName = "indexer_test_drop_midway";
    // Step 1: Write some data to initialize the table with metadata enabled
    HoodieMetadataConfig.Builder metadataConfigBuilder = getMetadataConfigBuilder(true, false)
        .withMetadataIndexColumnStats(false);
    upsertToTable(metadataConfigBuilder.build(), tableName);

    // Verify that record index is not yet built
    metaClient = reload(metaClient);
    Set<String> initialCompletedPartitions = metaClient.getTableConfig().getMetadataPartitions();
    assertFalse(initialCompletedPartitions.contains(RECORD_INDEX.getPartitionPath()),
        "Record index partition should not be completed initially");

    // Step 2: Start async index building for RLI - schedule it first
    HoodieIndexer.Config config = getHoodieIndexConfig(RECORD_INDEX.name(), SCHEDULE,
        "streamer-config/indexer-record-index.properties", tableName);
    HoodieIndexer indexer = new HoodieIndexer(jsc(), config);
    assertEquals(0, indexer.start(0));

    // Verify that the indexing instant is in requested state
    metaClient = reload(metaClient);
    Option<HoodieInstant> requestedIndexingInstant = metaClient.getActiveTimeline()
        .filterPendingIndexTimeline()
        .lastInstant();
    assertTrue(requestedIndexingInstant.isPresent(), "Indexing instant should be scheduled");
    assertEquals(REQUESTED, requestedIndexingInstant.get().getState());

    String indexingInstantTime = requestedIndexingInstant.get().requestedTime();

    // Step 3: Transition requested instant to inflight to simulate indexing started but crashed mid-way
    metaClient.getActiveTimeline().transitionIndexRequestedToInflight(requestedIndexingInstant.get());
    metaClient = reload(metaClient);
    
    // Manually update table config to reflect inflight state (simulating the state before crash)
    // When indexing is in progress, the partition is in inflight state in table config
    metaClient.getTableConfig().setMetadataPartitionsInflight(metaClient, RECORD_INDEX);
    // Remove from completed partitions since we're simulating it never completed
    Set<String> completedPartitions = new java.util.HashSet<>(metaClient.getTableConfig().getMetadataPartitions());
    completedPartitions.remove(RECORD_INDEX.getPartitionPath());
    metaClient.getTableConfig().setValue(
        org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS.key(),
        completedPartitions.isEmpty() ? "" : String.join(",", completedPartitions));
    org.apache.hudi.common.table.HoodieTableConfig.update(
        metaClient.getStorage(), metaClient.getMetaPath(), metaClient.getTableConfig().getProps());
    // Reload metaClient and table config to ensure changes are reflected
    metaClient = reload(metaClient);
    metaClient.reloadTableConfig();

    // Verify that inflight MDT partition is set in table config
    Set<String> inflightPartitionsBeforeDrop = metaClient.getTableConfig().getMetadataPartitionsInflight();
    assertTrue(inflightPartitionsBeforeDrop.contains(RECORD_INDEX.getPartitionPath()),
        "Record index partition should be in inflight state in table config (simulating crash mid-way)");
    
    // Verify instant is still in INFLIGHT state (dropIndex only works on REQUESTED instants)
    Option<HoodieInstant> pendingInstant = metaClient.getActiveTimeline()
        .filterPendingIndexTimeline()
        .filter(instant -> instant.requestedTime().equals(requestedIndexingInstant.get().requestedTime()))
        .lastInstant();
    assertTrue(pendingInstant.isPresent(), "Indexing instant should still be in timeline");
    assertEquals(INFLIGHT, pendingInstant.get().getState(), "Indexing instant should be in REQUESTED state for drop to work");

    // Step 4: Run HoodieIndexer to drop the RLI index which failed mid-way
    dropIndexAndAssert(RECORD_INDEX, "streamer-config/indexer-record-index.properties", Option.empty(), tableName);

    // Step 5: inflight MDT partition is still in table config?
    metaClient = reload(metaClient);
    metaClient.reloadTableConfig();
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
    Set<String> inflightPartitionsAfterDrop = metaClient.getTableConfig().getMetadataPartitionsInflight();
    assertFalse(inflightPartitionsAfterDrop.contains(RECORD_INDEX.getPartitionPath()),
        "BUG: Inflight MDT partition should be cleaned from table config after dropping index, but it's still present");
  }
}
