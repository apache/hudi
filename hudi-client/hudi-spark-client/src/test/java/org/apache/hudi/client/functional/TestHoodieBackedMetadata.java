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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.SparkSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.metadata.HoodieTableMetadata.getMetadataTableBasePath;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHoodieBackedMetadata extends BaseTestHoodieBackedMetadata<JavaRDD<HoodieRecord>, JavaRDD<WriteStatus>> {

  @Override
  protected BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> getHoodieWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig) {
    return new SparkRDDWriteClient(context, writeConfig);
  }

  @Override
  protected JavaRDD<HoodieRecord> convertInputRecords(List<HoodieRecord> inputRecords) {
    return jsc.parallelize(inputRecords, 1);
  }

  @Override
  protected List<WriteStatus> collectWriteStatuses(JavaRDD<WriteStatus> writeOutputs) {
    return writeOutputs.collect();
  }

  @Override
  protected String getClusteringPlanStrategyClass() {
    return SparkSizeBasedClusteringPlanStrategy.class.getName();
  }

  @Override
  protected String getClusteringExecutionStrategyClass() {
    return SparkSortAndSizeExecutionStrategy.class.getName();
  }

  @Override
  public void init(HoodieTableType tableType, Option<HoodieWriteConfig> writeConfig, boolean enableMetadataTable,
                   boolean enableMetrics, boolean validateMetadataPayloadStateConsistency) throws IOException {
    this.tableType = tableType;
    initPath();
    initSparkContexts("TestHoodieMetadata");
    initFileSystem();
    fs.mkdirs(new Path(basePath));
    initTimelineService();
    initMetaClient(tableType);
    initTestDataGenerator();
    metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
    this.writeConfig = writeConfig.isPresent()
        ? writeConfig.get() : getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true,
        enableMetadataTable, enableMetrics, true,
        validateMetadataPayloadStateConsistency)
        .build();
    initWriteConfigAndMetatableWriter(this.writeConfig, enableMetadataTable);
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataTableDeletePartition(HoodieTableType tableType) throws IOException {
    initPath();
    int maxCommits = 1;
    HoodieWriteConfig cfg = getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(maxCommits)
            .build())
        .withParallelism(1, 1).withBulkInsertParallelism(1).withFinalizeWriteParallelism(1).withDeleteParallelism(1)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .build();
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, cfg)) {
      // Write 1 (Bulk insert)
      String newCommitTime = "0000001";
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Write 2 (upserts)
      newCommitTime = "0000002";
      client.startCommitWithTime(newCommitTime);
      validateMetadata(client);

      records = dataGen.generateInserts(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      // metadata writer to delete column_stats partition
      HoodieBackedTableMetadataWriter metadataWriter = metadataWriter(client);
      assertNotNull(metadataWriter, "MetadataWriter should have been initialized");
      metadataWriter.deletePartitions("0000003", Arrays.asList(COLUMN_STATS));

      HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build();
      List<String> metadataTablePartitions = FSUtils.getAllPartitionPaths(engineContext, metadataMetaClient.getBasePath(), false, false);
      // partition should be physically deleted
      assertEquals(metadataWriter.getEnabledPartitionTypes().size(), metadataTablePartitions.size());
      assertFalse(metadataTablePartitions.contains(COLUMN_STATS.getPartitionPath()));

      Option<HoodieInstant> completedReplaceInstant = metadataMetaClient.reloadActiveTimeline().getCompletedReplaceTimeline().lastInstant();
      assertTrue(completedReplaceInstant.isPresent());
      assertEquals("0000003", completedReplaceInstant.get().getTimestamp());

      final Map<String, MetadataPartitionType> metadataEnabledPartitionTypes = new HashMap<>();
      metadataWriter.getEnabledPartitionTypes().forEach(e -> metadataEnabledPartitionTypes.put(e.getPartitionPath(), e));
      HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metadataMetaClient, metadataMetaClient.getActiveTimeline());
      metadataTablePartitions.forEach(partition -> {
        List<FileSlice> latestSlices = fsView.getLatestFileSlices(partition).collect(Collectors.toList());
        if (COLUMN_STATS.getPartitionPath().equals(partition)) {
          // there should not be any file slice in column_stats partition
          assertTrue(latestSlices.isEmpty());
        } else {
          assertFalse(latestSlices.isEmpty());
          assertTrue(latestSlices.stream().map(FileSlice::getBaseFile).count() <= latestSlices.size(), "Should have a single latest base file per file group");
        }
      });
    }
  }

  @Test
  public void testClusteringWithRecordIndex() throws Exception {
    initPath();

    // Config with 5 fileGroups for record index
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.RECORD_INDEX)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableRecordIndex(true)
            .withRecordIndexFileGroupCount(5, 5)
            .build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withInlineClustering(true)
            .withInlineClusteringNumCommits(2)
            .build())
        .build();

    init(COPY_ON_WRITE, writeConfig);
    testTableOperationsForMetaIndexImpl(writeConfig);
  }

  @Test
  public void testUpgradeDowngrade() throws IOException {
    init(HoodieTableType.COPY_ON_WRITE, false);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Perform a commit. This should bootstrap the metadata table with latest version.
    List<HoodieRecord> records;
    List<WriteStatus> writeStatuses;
    String commitTimestamp = HoodieActiveTimeline.createNewInstantTime();
    HoodieWriteConfig writeConfig = getWriteConfig(true, true);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      records = dataGen.generateInserts(commitTimestamp, 5);
      client.startCommitWithTime(commitTimestamp);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), commitTimestamp).collect();
      assertNoWriteErrors(writeStatuses);
    }

    // Metadata table should have been bootstrapped
    assertTrue(fs.exists(new Path(metadataTableBasePath)), "Metadata table should exist");
    FileStatus oldStatus = fs.getFileStatus(new Path(metadataTableBasePath));

    // set hoodie.table.version to 2 in hoodie.properties file
    changeTableVersion(HoodieTableVersion.TWO);

    // With next commit the table should be deleted (as part of upgrade) and then re-bootstrapped automatically
    commitTimestamp = HoodieActiveTimeline.createNewInstantTime();
    metaClient.reloadActiveTimeline();
    FileStatus prevStatus = fs.getFileStatus(new Path(metadataTableBasePath));
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      records = dataGen.generateInserts(commitTimestamp, 5);
      client.startCommitWithTime(commitTimestamp);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), commitTimestamp).collect();
      assertNoWriteErrors(writeStatuses);
    }
    assertTrue(fs.exists(new Path(metadataTableBasePath)), "Metadata table should exist");
    FileStatus currentStatus = fs.getFileStatus(new Path(metadataTableBasePath));
    assertTrue(currentStatus.getModificationTime() > prevStatus.getModificationTime());

    initMetaClient();
    assertEquals(metaClient.getTableConfig().getTableVersion().versionCode(), HoodieTableVersion.current().versionCode());
    assertTrue(fs.exists(new Path(metadataTableBasePath)), "Metadata table should exist");
    FileStatus newStatus = fs.getFileStatus(new Path(metadataTableBasePath));
    assertTrue(oldStatus.getModificationTime() < newStatus.getModificationTime());

    // Test downgrade by running the downgrader
    new UpgradeDowngrade(metaClient, writeConfig, context, SparkUpgradeDowngradeHelper.getInstance()).run(HoodieTableVersion.TWO, null);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(HoodieTableVersion.TWO.versionCode(), metaClient.getTableConfig().getTableVersion().versionCode());
    assertFalse(fs.exists(new Path(metadataTableBasePath)), "Metadata table should not exist");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRollbackPendingCommitWithRecordIndex(boolean performUpsert) throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    // Enable Record index and set index type to record index.
    Properties props = new Properties();
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true");
    props.setProperty(HoodieIndexConfig.INDEX_TYPE.key(), "RECORD_INDEX");
    HoodieWriteConfig cfg = getWriteConfigBuilder(true, true, false)
        .withProps(props).build();
    // Initialize write client.
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    // Insert first batch
    String commitTime = HoodieActiveTimeline.createNewInstantTime();
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 100);
    client.startCommitWithTime(commitTime);
    List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), commitTime).collect();
    assertNoWriteErrors(writeStatuses);

    // create pending commits scenario by disabling auto commit
    HoodieWriteConfig autoCommitDisabled = getWriteConfigBuilder(false, true, false)
        .withProps(props).build();
    // initialize second client, that will stop short of producing the commit.
    SparkRDDWriteClient client2 = getHoodieWriteClient(autoCommitDisabled);
    // Insert second batch
    commitTime = HoodieActiveTimeline.createNewInstantTime();
    records = dataGen.generateInserts(commitTime, 100);
    client.startCommitWithTime(commitTime);
    writeStatuses = client2.insert(jsc.parallelize(records, 1), commitTime).collect();
    assertNoWriteErrors(writeStatuses);

    // delete the metadata table partitions to check, whether rollback of pending commit succeeds and
    // metadata table partitions are rebootstrapped.
    metadataWriter.dropMetadataPartitions(Arrays.asList(MetadataPartitionType.RECORD_INDEX, FILES));
    assertFalse(fs.exists(new Path(getMetadataTableBasePath(basePath)
        + Path.SEPARATOR + FILES.getPartitionPath())));
    assertFalse(fs.exists(new Path(getMetadataTableBasePath(basePath)
        + Path.SEPARATOR + MetadataPartitionType.RECORD_INDEX.getPartitionPath())));

    metaClient = HoodieTableMetaClient.reload(metaClient);
    // Insert/upsert third batch of records
    client = getHoodieWriteClient(cfg);
    commitTime = HoodieActiveTimeline.createNewInstantTime();
    if (performUpsert) {
      records = dataGen.generateUpdates(commitTime, 100);
      records.addAll(dataGen.generateInserts(commitTime, 20));
      client.startCommitWithTime(commitTime);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), commitTime).collect();
    } else {
      records = dataGen.generateInserts(commitTime, 100);
      client.startCommitWithTime(commitTime);
      writeStatuses = client.insert(jsc.parallelize(records, 1), commitTime).collect();
    }
    assertNoWriteErrors(writeStatuses);
    assertTrue(fs.exists(new Path(basePath + Path.SEPARATOR + METAFOLDER_NAME)));
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertFalse(metaClient.getActiveTimeline().filterCompletedInstants().filterCompletedInstants().findInstantsAfterOrEquals(commitTime, 1).empty());

    assertTrue(fs.exists(new Path(getMetadataTableBasePath(basePath)
        + Path.SEPARATOR + FILES.getPartitionPath())));
    assertTrue(fs.exists(new Path(getMetadataTableBasePath(basePath)
        + Path.SEPARATOR + MetadataPartitionType.RECORD_INDEX.getPartitionPath())));
  }

  @Test
  public void testDeletePartitions() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);

    int maxCommits = 1;
    HoodieWriteConfig cfg = getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(maxCommits).build())
        .withParallelism(1, 1)
        .withBulkInsertParallelism(1)
        .withFinalizeWriteParallelism(1)
        .withDeleteParallelism(1)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .build();

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 10);
      List<HoodieRecord> upsertRecords = new ArrayList<>();
      for (HoodieRecord entry : records) {
        if (entry.getPartitionPath().equals(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
            || entry.getPartitionPath().equals(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)) {
          upsertRecords.add(entry);
        }
      }
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(upsertRecords, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // delete partitions
      newCommitTime = HoodieActiveTimeline.createNewInstantTime(5000);
      client.deletePartitions(singletonList(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH), newCommitTime);

      // add 1 more commit
      newCommitTime = HoodieActiveTimeline.createNewInstantTime(5000);
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 10);
      upsertRecords = new ArrayList<>();
      for (HoodieRecord entry : records) {
        if (entry.getPartitionPath().equals(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)) {
          upsertRecords.add(entry);
        }
      }
      writeStatuses = client.upsert(jsc.parallelize(upsertRecords, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      // above upsert would have triggered clean
      validateMetadata(client);
      assertEquals(1, metadata(client).getAllPartitionPaths().size());
    }
  }

  @Test
  public void testDuplicatesDuringRecordIndexBootstrap() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE, true);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    List<String> commitTimestamps = new ArrayList<>();
    HoodieWriteConfig customConfig = getWriteConfigBuilder(true, true, false)
        .build();

    List<HoodieRecord> recordsFirstBatch = new ArrayList<>();
    String firstCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, customConfig)) {
      // Create a commit, with record index disabled
      List<HoodieRecord> insertRecords = dataGen.generateInserts(firstCommitTime, 100);
      recordsFirstBatch.addAll(insertRecords);
      // To test duplicates during bootstrap, insert duplicates in the first batch.
      recordsFirstBatch.addAll(insertRecords);
      client.startCommitWithTime(firstCommitTime);
      List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(recordsFirstBatch, 1), firstCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      commitTimestamps.add(firstCommitTime);
    }
    assertEquals(false, fs.exists(new Path(metaClient.getMetaPath(), "metadata/record_index")));

    // bootstrap record index
    customConfig = getWriteConfigBuilder(false, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .enableMetrics(false)
            .ignoreSpuriousDeletes(false)
            .withEnableRecordIndex(true)
            .build())
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, customConfig)) {
      // Create a commit, with record index enabled
      String secondCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> recordsSecondBatch = dataGen.generateInserts(secondCommitTime, 100);
      client.startCommitWithTime(secondCommitTime);
      assertThrows(HoodieException.class, () -> client.insert(jsc.parallelize(recordsSecondBatch, 1), secondCommitTime));
    }
  }

  @Override
  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr, HoodieIndex.IndexType indexType,
                                                    HoodieFailedWritesCleaningPolicy cleaningPolicy) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(cleaningPolicy).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).orcMaxFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withEnableBackupForRemoteFileSystemView(false) // Fail test if problem connecting to timeline-server
            .withRemoteServerPort(timelineServicePort)
            .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
  }

  @Test
  public void testDeleteWithRecordIndex() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE, true);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withEnableRecordIndex(true).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.RECORD_INDEX).build())
        .build();

    String firstCommitTime = HoodieActiveTimeline.createNewInstantTime();
    String secondCommitTime;
    List<HoodieRecord> allRecords;
    List<String> keysToDelete;
    List<HoodieRecord> recordsToDelete;

    // Initialize the dataset and add some commits.
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      // First commit
      List<HoodieRecord> firstBatchOfrecords = dataGen.generateInserts(firstCommitTime, 10);
      client.startCommitWithTime(firstCommitTime);
      client.insert(jsc.parallelize(firstBatchOfrecords, 1), firstCommitTime).collect();

      // Records got inserted and RI is initialized
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(metaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX), "RI is disabled");
      assertEquals(firstBatchOfrecords.size(),
          HoodieClientTestUtils.readCommit(writeConfig.getBasePath(), engineContext.getSqlContext(), metaClient.reloadActiveTimeline(), firstCommitTime).count());

      // Another batch of records added
      secondCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> secondBatchOfrecords = dataGen.generateInserts(secondCommitTime, 5);
      client.startCommitWithTime(secondCommitTime);
      client.bulkInsert(jsc.parallelize(secondBatchOfrecords, 1), secondCommitTime).collect();

      assertEquals(secondBatchOfrecords.size(),
          HoodieClientTestUtils.readCommit(writeConfig.getBasePath(), engineContext.getSqlContext(), metaClient.reloadActiveTimeline(), secondCommitTime).count());

      allRecords = new ArrayList<>(firstBatchOfrecords);
      allRecords.addAll(secondBatchOfrecords);

      // RI should have created mappings for all the records inserted above
      HoodieTableMetadata metadataReader = HoodieTableMetadata.create(context, writeConfig.getMetadataConfig(), writeConfig.getBasePath());
      Map<String, HoodieRecordGlobalLocation> result = metadataReader
          .readRecordIndex(allRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()));
      assertEquals(allRecords.size(), result.size(), "RI should have mapping for all the records in firstCommit");

      // Delete some records from each commit. This should also remove the RI mapping.
      recordsToDelete = firstBatchOfrecords.subList(0, 3);
      recordsToDelete.addAll(secondBatchOfrecords.subList(0, 2));
      keysToDelete = recordsToDelete.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList());

      String deleteTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(deleteTime);
      client.delete(jsc.parallelize(recordsToDelete, 1).map(HoodieRecord::getKey), deleteTime);

      // RI should not return mappings for deleted records
      metadataReader = HoodieTableMetadata.create(context, writeConfig.getMetadataConfig(), writeConfig.getBasePath());
      result = metadataReader.readRecordIndex(allRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()));
      assertEquals(allRecords.size() - recordsToDelete.size(), result.size(), "RI should not have mapping for deleted records");
      result.keySet().forEach(mappingKey -> assertFalse(keysToDelete.contains(mappingKey), "RI should not have mapping for deleted records"));
    }

    // Compaction should work too by removing the deleted keys from the base files
    // To perform compaction, we need to create a new write client as compaction is attempted before any operations in write client
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      // An empty delete to trigger compaction
      String deleteTime = client.startCommit();
      client.delete(jsc.emptyRDD(), deleteTime);

      HoodieTableMetadata metadataReader = HoodieTableMetadata.create(context, writeConfig.getMetadataConfig(), writeConfig.getBasePath());
      assertTrue(metadataReader.getLatestCompactionTime().isPresent(), "Compaction should have taken place on MDT");

      // RI should not return mappings for deleted records
      metadataReader = HoodieTableMetadata.create(context, writeConfig.getMetadataConfig(), writeConfig.getBasePath());
      Map<String, HoodieRecordGlobalLocation> result = metadataReader.readRecordIndex(allRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()));
      assertEquals(allRecords.size() - keysToDelete.size(), result.size(), "RI should not have mapping for deleted records");
      result.keySet().forEach(mappingKey -> assertFalse(keysToDelete.contains(mappingKey), "RI should not have mapping for deleted records"));

      // Adding records with the same keys after delete should work
      String reinsertTime = client.startCommit();
      client.upsert(jsc.parallelize(recordsToDelete, 1), reinsertTime).collect();

      // New mappings should have been created for re-inserted records and should map to the new commit time
      metadataReader = HoodieTableMetadata.create(context, writeConfig.getMetadataConfig(), writeConfig.getBasePath());
      result = metadataReader.readRecordIndex(allRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()));
      assertEquals(allRecords.size(), result.size(), "RI should have mappings for re-inserted records");
      for (String reInsertedKey : keysToDelete) {
        assertEquals(reinsertTime, result.get(reInsertedKey).getInstantTime(), "RI mapping for re-inserted keys should have new commit time");
      }
    }
  }
}
