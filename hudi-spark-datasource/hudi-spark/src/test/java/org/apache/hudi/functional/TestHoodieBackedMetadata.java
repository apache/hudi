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

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.functional.TestHoodieMetadataBase;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.metadata.ColumnStatsIndexPrefixRawKey;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieMetadataMetrics;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.RawKey;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.hudi.avro.HoodieAvroWrapperUtils.unwrapAvroValueWrapper;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.model.WriteOperationType.DELETE;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.apache.hudi.common.table.HoodieTableMetaClient.TIMELINEFOLDER_NAME;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_EXTENSION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_EXTENSION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.INFLIGHT_EXTENSION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REQUESTED_EXTENSION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getNextCommitTime;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.config.HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS;
import static org.apache.hudi.io.storage.HoodieSparkIOFactory.getHoodieSparkIOFactory;
import static org.apache.hudi.metadata.HoodieIndexVersion.V1;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.apache.hudi.metadata.HoodieTableMetadata.getMetadataTableBasePath;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.deleteMetadataTable;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getFileIDForFileGroup;
import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.apache.hudi.metadata.MetadataPartitionType.PARTITION_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.SECONDARY_INDEX;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("functional")
public class TestHoodieBackedMetadata extends TestHoodieMetadataBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieBackedMetadata.class);

  public static List<Arguments> tableTypeAndEnableOperationArgs() {
    return asList(
        Arguments.of(COPY_ON_WRITE, true),
        Arguments.of(COPY_ON_WRITE, false),
        Arguments.of(MERGE_ON_READ, true),
        Arguments.of(MERGE_ON_READ, false)
    );
  }

  public static List<Arguments> tableOperationsTestArgs() {
    return asList(
        Arguments.of(COPY_ON_WRITE, true),
        Arguments.of(COPY_ON_WRITE, false),
        Arguments.of(MERGE_ON_READ, true),
        Arguments.of(MERGE_ON_READ, false)
    );
  }

  private final List<BaseHoodieWriteClient> clientsToClose = new ArrayList<>();

  @AfterEach
  public void closeClients() {
    clientsToClose.forEach(BaseHoodieWriteClient::close);
  }

  /**
   * Metadata Table bootstrap scenarios.
   */
  @ParameterizedTest
  @MethodSource("tableTypeAndEnableOperationArgs")
  public void testMetadataTableBootstrap(HoodieTableType tableType, boolean addRollback) throws Exception {
    init(tableType, false);
    // bootstrap with few commits
    doPreBootstrapOperations(testTable);

    writeConfig = getWriteConfig(true, true);
    initWriteConfigAndMetatableWriter(writeConfig, true);
    syncTableMetadata(writeConfig);
    validateMetadata(testTable);
    doWriteInsertAndUpsert(testTable, "0000003", "0000004", false);
    validateMetadata(testTable);
    if (addRollback) {
      // trigger an UPSERT that will be rolled back
      doWriteOperationAndValidate(testTable, "0000005");

      // rollback last commit
      doRollbackAndValidate(testTable, "0000005", "0000006");
    }

    // trigger couple of upserts
    doWriteOperation(testTable, "0000007");
    doWriteOperation(testTable, "0000008");
    doWriteOperation(testTable, "0000009");
    doCleanAndValidate(testTable, "0000010", Arrays.asList("0000009"));
    validateMetadata(testTable, true);
  }

  @Test
  public void testTurnOffMetadataIndexAfterEnable() throws Exception {
    initPath();
    HoodieWriteConfig cfg = getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER)
        .withParallelism(1, 1).withBulkInsertParallelism(1).withFinalizeWriteParallelism(1).withDeleteParallelism(1)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .build();
    init(COPY_ON_WRITE, Option.of(cfg), true, false, false);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    // metadata enabled with only FILES partition
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, cfg, true)) {
      // Insert
      String commitTime = "0000001";
      List<HoodieRecord> records = dataGen.generateInserts(commitTime, 20);
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      List<WriteStatus> statusList = client.insert(jsc.parallelize(records, 1), commitTime).collect();
      assertTrue(client.commit(commitTime, jsc.parallelize(statusList)));
      assertNoWriteErrors(statusList);

      // Upsert
      commitTime = "0000002";
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      records = dataGen.generateUniqueUpdates(commitTime, 10);
      statusList = client.upsert(jsc.parallelize(records, 1), commitTime).collect();
      assertTrue(client.commit(commitTime, jsc.parallelize(statusList)));
      assertNoWriteErrors(statusList);
      validateMetadata(client);
    }
    // check table config
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    assertFalse(tableConfig.getMetadataPartitions().isEmpty());
    assertTrue(tableConfig.getMetadataPartitions().contains(FILES.getPartitionPath()));
    // column_stats is enabled by default
    assertTrue(tableConfig.getMetadataPartitions().contains(COLUMN_STATS.getPartitionPath()));
    assertFalse(tableConfig.getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));

    // enable column stats and run 1 upserts
    HoodieWriteConfig cfgWithColStatsEnabled = HoodieWriteConfig.newBuilder()
        .withProperties(cfg.getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withProperties(cfg.getMetadataConfig().getProps())
            .withMetadataIndexColumnStats(true)
            .build())
        .build();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, cfgWithColStatsEnabled, true)) {
      // Upsert
      String commitTime = "0000003";
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      List<HoodieRecord> records = dataGen.generateUniqueUpdates(commitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), commitTime).collect();
      assertNoWriteErrors(writeStatuses);
      assertTrue(client.commit(commitTime, jsc.parallelize(writeStatuses)));
      validateMetadata(client);
    }
    // check table config
    metaClient = HoodieTableMetaClient.reload(metaClient);
    tableConfig = metaClient.getTableConfig();
    assertFalse(tableConfig.getMetadataPartitions().isEmpty());
    assertTrue(tableConfig.getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertTrue(tableConfig.getMetadataPartitions().contains(COLUMN_STATS.getPartitionPath()));
    assertFalse(tableConfig.getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));

    // disable column stats and run 1 upsert
    HoodieWriteConfig cfgWithColStatsDisabled = HoodieWriteConfig.newBuilder()
        .withProperties(cfg.getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withProperties(cfg.getMetadataConfig().getProps())
            .withMetadataIndexColumnStats(false)
            .build())
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, cfgWithColStatsDisabled, true)) {
      // Upsert
      String commitTime = "0000004";
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      List<HoodieRecord> records = dataGen.generateUniqueUpdates(commitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), commitTime).collect();
      assertTrue(client.commit(commitTime, jsc.parallelize(writeStatuses)));
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
    }
    // check table config
    metaClient = HoodieTableMetaClient.reload(metaClient);
    tableConfig = metaClient.getTableConfig();
    assertFalse(tableConfig.getMetadataPartitions().isEmpty());
    assertTrue(tableConfig.getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertFalse(tableConfig.getMetadataPartitions().contains(COLUMN_STATS.getPartitionPath()));
    assertFalse(tableConfig.getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));

    // enable bloom filter as well as column stats and run 1 upsert
    HoodieWriteConfig cfgWithBloomFilterEnabled = HoodieWriteConfig.newBuilder()
        .withProperties(cfgWithColStatsEnabled.getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withProperties(cfgWithColStatsEnabled.getMetadataConfig().getProps())
            .withMetadataIndexBloomFilter(true)
            .build())
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, cfgWithBloomFilterEnabled, true)) {
      // Upsert
      String commitTime = "0000005";
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      List<HoodieRecord> records = dataGen.generateUniqueUpdates(commitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), commitTime).collect();
      assertTrue(client.commit(commitTime, jsc.parallelize(writeStatuses)));
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
    }
    // check table config
    metaClient = HoodieTableMetaClient.reload(metaClient);
    tableConfig = metaClient.getTableConfig();
    assertFalse(tableConfig.getMetadataPartitions().isEmpty());
    assertTrue(tableConfig.getMetadataPartitions().contains(FILES.getPartitionPath()));
    assertTrue(tableConfig.getMetadataPartitions().contains(COLUMN_STATS.getPartitionPath()));
    assertTrue(tableConfig.getMetadataPartitions().contains(BLOOM_FILTERS.getPartitionPath()));

    // disable entire MDT and validate its deleted
    HoodieWriteConfig cfgWithMetadataDisabled = getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER)
        .withParallelism(1, 1).withBulkInsertParallelism(1).withFinalizeWriteParallelism(1).withDeleteParallelism(1)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, cfgWithMetadataDisabled, true)) {
      // Upsert
      String commitTime = "0000006";
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      List<HoodieRecord> records = dataGen.generateUniqueUpdates(commitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), commitTime).collect();
      assertTrue(client.commit(commitTime, jsc.parallelize(writeStatuses)));
      assertNoWriteErrors(writeStatuses);
    }

    // check table config
    tableConfig = HoodieTableMetaClient.reload(metaClient).getTableConfig();
    assertTrue(tableConfig.getMetadataPartitions().isEmpty());
  }

  @Test
  public void testTurnOffMetadataTableAfterEnable() throws Exception {
    init(COPY_ON_WRITE, true);
    String instant1 = "0000001";
    HoodieCommitMetadata hoodieCommitMetadata = doWriteOperationWithMeta(testTable, instant1, INSERT);

    // Simulate the complete data directory including ".hoodie_partition_metadata" file
    File metaForP1 = new File(metaClient.getBasePath() + "/p1", ".hoodie_partition_metadata");
    File metaForP2 = new File(metaClient.getBasePath() + "/p2", ".hoodie_partition_metadata");
    metaForP1.createNewFile();
    metaForP2.createNewFile();

    // Sync to metadata table
    metaClient.reloadActiveTimeline();
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    Option metadataWriter = table.getMetadataWriter(instant1);
    validateMetadata(testTable, true);

    assertTrue(metadataWriter.isPresent());
    HoodieTableConfig hoodieTableConfig = new HoodieTableConfig(this.storage, metaClient.getMetaPath());
    assertFalse(hoodieTableConfig.getMetadataPartitions().isEmpty());

    // Turn off metadata table
    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withProperties(this.writeConfig.getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    testTable = HoodieTestTable.of(metaClient);
    String instant2 = "0000002";
    HoodieCommitMetadata hoodieCommitMetadata2 =
        doWriteOperationWithMeta(testTable, instant2, INSERT);
    metaClient.reloadActiveTimeline();
    HoodieTable table2 = HoodieSparkTable.create(writeConfig2, context, metaClient);
    Option metadataWriter2 = table2.getMetadataWriter(instant2);
    assertFalse(metadataWriter2.isPresent());

    HoodieTableConfig hoodieTableConfig2 =
        new HoodieTableConfig(this.storage, metaClient.getMetaPath());
    assertEquals(Collections.emptySet(), hoodieTableConfig2.getMetadataPartitions());
    // Assert metadata table folder is deleted
    assertFalse(metaClient.getStorage().exists(
        new StoragePath(getMetadataTableBasePath(writeConfig2.getBasePath()))));

    // Enable metadata table again and initialize metadata table through
    // HoodieTable.getMetadataWriter() function
    HoodieWriteConfig writeConfig3 = HoodieWriteConfig.newBuilder()
        .withProperties(this.writeConfig.getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .build();
    testTable = HoodieTestTable.of(metaClient);
    metaClient.reloadActiveTimeline();
    String instant3 = "0000003";
    HoodieCommitMetadata hoodieCommitMetadata3 = doWriteOperationWithMeta(testTable, instant3, INSERT);
    metaClient.reloadActiveTimeline();
    HoodieTable table3 = HoodieSparkTable.create(writeConfig3, context, metaClient);
    Option metadataWriter3 = table3.getMetadataWriter(instant3);
    validateMetadata(testTable, true);
    assertTrue(metadataWriter3.isPresent());
    HoodieTableConfig hoodieTableConfig3 =
        new HoodieTableConfig(this.storage, metaClient.getMetaPath());
    assertFalse(hoodieTableConfig3.getMetadataPartitions().isEmpty());
  }

  /**
   * Only valid partition directories are added to the metadata.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testOnlyValidPartitionsAdded(HoodieTableType tableType) throws Exception {
    // This test requires local file system
    init(tableType, false);
    // Create an empty directory which is not a partition directory (lacks partition metadata)
    final String nonPartitionDirectory = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-nonpartition";
    Files.createDirectories(Paths.get(basePath, nonPartitionDirectory));
    // Write random file to assert it is not added to the view
    Files.createFile(Paths.get(basePath, nonPartitionDirectory, "randomFile.parquet"));

    // Three directories which are partitions but will be ignored due to filter
    final String filterDirRegex = ".*-filterDir\\d|\\..*";
    final String filteredDirectoryOne = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-filterDir1";
    final String filteredDirectoryTwo = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-filterDir2";
    final String filteredDirectoryThree = ".backups";

    // Create some commits
    testTable.withPartitionMetaFiles("p1", "p2", filteredDirectoryOne, filteredDirectoryTwo, filteredDirectoryThree)
        .addCommit("0000001").withBaseFilesInPartition("p1", 10).withBaseFilesInPartition("p2", 10, 10)
        .addCommit("0000002").withBaseFilesInPartition("p1", 10).withBaseFilesInPartition("p2", 10, 10, 10);

    writeConfig = getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.NEVER, true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withDirectoryFilterRegex(filterDirRegex).build()).build();
    testTable.doWriteOperation("0000003", UPSERT, emptyList(), asList("p1", "p2"), 1, true);
    syncTableMetadata(writeConfig);

    List<String> partitions = metadataWriter(writeConfig).getTableMetadata().getAllPartitionPaths();
    assertFalse(partitions.contains(nonPartitionDirectory),
        "Must not contain the non-partition " + nonPartitionDirectory);
    assertTrue(partitions.contains("p1"), "Must contain partition p1");
    assertTrue(partitions.contains("p2"), "Must contain partition p2");

    assertFalse(partitions.contains(filteredDirectoryOne),
        "Must not contain the filtered directory " + filteredDirectoryOne);
    assertFalse(partitions.contains(filteredDirectoryTwo),
        "Must not contain the filtered directory " + filteredDirectoryTwo);
    assertFalse(partitions.contains(filteredDirectoryThree),
        "Must not contain the filtered directory " + filteredDirectoryThree);

    List<StoragePathInfo> allFilesList = metadata(writeConfig, context)
        .getAllFilesInPartition(new StoragePath(basePath, "p1"));
    assertEquals(tableType == COPY_ON_WRITE ? 3 : 4, allFilesList.size());
    allFilesList = metadata(writeConfig, context)
        .getAllFilesInPartition(new StoragePath(basePath, "p2"));
    assertEquals(tableType == COPY_ON_WRITE ? 6 : 7, allFilesList.size());
    Map<String, List<StoragePathInfo>> partitionsToFilesMap = metadata(writeConfig, context)
        .getAllFilesInPartitions(asList(basePath + "/p1", basePath + "/p2"));
    assertEquals(2, partitionsToFilesMap.size());
    assertEquals(tableType == COPY_ON_WRITE ? 3 : 4,
        partitionsToFilesMap.get(basePath + "/p1").size());
    assertEquals(tableType == COPY_ON_WRITE ? 6 : 7,
        partitionsToFilesMap.get(basePath + "/p2").size());
  }

  /**
   * Test various table operations sync to Metadata Table correctly.
   */
  @ParameterizedTest
  @MethodSource("tableOperationsTestArgs")
  public void testTableOperations(HoodieTableType tableType, boolean enableFullScan) throws Exception {
    init(tableType, true, enableFullScan, false, false);
    List<Long> commitTimeList = new ArrayList<>();

    commitTimeList.add(Long.parseLong(WriteClientTestUtils.createNewInstantTime()));
    for (int i = 0; i < 8; i++) {
      long nextCommitTime = getNextCommitTime(commitTimeList.get(commitTimeList.size() - 1));
      commitTimeList.add(nextCommitTime);
    }
    doWriteInsertAndUpsert(testTable, commitTimeList.get(0).toString(), commitTimeList.get(1).toString(), false);

    // trigger an upsert
    doWriteOperationAndValidate(testTable, commitTimeList.get(2).toString());

    // trigger compaction
    if (MERGE_ON_READ.equals(tableType)) {
      doCompactionAndValidate(testTable, commitTimeList.get(3).toString());
    }

    // trigger an upsert
    doWriteOperation(testTable, commitTimeList.get(4).toString());

    // trigger clean
    doCleanAndValidate(testTable, commitTimeList.get(5).toString(), singletonList(commitTimeList.get(0).toString()));

    // trigger few upserts and validate
    doWriteOperation(testTable, commitTimeList.get(6).toString());
    doWriteOperation(testTable, commitTimeList.get(7).toString());
    validateMetadata(testTable, emptyList(), true);
  }

  @Test
  public void testMetadataTableArchival() throws Exception {
    init(COPY_ON_WRITE, false);
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .enableMetrics(false)
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .retainCommits(1)
            .build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(4, 5)
            .build())
        .build();
    initWriteConfigAndMetatableWriter(writeConfig, true);

    // Trigger 5 regular writes in data table.
    List<String> instants = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      String instant = WriteClientTestUtils.createNewInstantTime();
      instants.add(instant);
      doWriteOperation(testTable, instant, INSERT);
    }
    // The earliest deltacommit in the metadata table should be the 1st instant,
    // and the "00000000000000" init deltacommit should be archived.
    HoodieTableMetaClient metadataMetaClient = createMetaClient(metadataTableBasePath);
    HoodieActiveTimeline metadataTimeline = metadataMetaClient.reloadActiveTimeline();
    assertEquals(instants.get(0), metadataTimeline.getCommitsTimeline().firstInstant().get().requestedTime());

    // Trigger clustering in the data table, archival should not kick in, even though conditions are met.
    doCluster(testTable, WriteClientTestUtils.createNewInstantTime());
    metadataTimeline = metadataMetaClient.reloadActiveTimeline();
    assertEquals(instants.get(0), metadataTimeline.getCommitsTimeline().firstInstant().get().requestedTime());

    getHoodieWriteClient(writeConfig);
    // Trigger a regular write operation. data set timeline archival should kick in.
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);
    archiveDataTable(writeConfig, createMetaClient(basePath));
    assertEquals(instants.get(3),
        metaClient.reloadActiveTimeline().getCommitsTimeline().firstInstant().get().requestedTime());
    metadataTimeline = metadataMetaClient.reloadActiveTimeline();
    assertEquals(instants.get(0), metadataTimeline.getCommitsTimeline().firstInstant().get().requestedTime());

    // Trigger a regular write operation. metadata timeline archival should kick in.
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);
    metadataTimeline = metadataMetaClient.reloadActiveTimeline();
    assertEquals(instants.get(3), metadataTimeline.getCommitsTimeline().firstInstant().get().requestedTime());
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataArchivalCleanConfig(HoodieTableType tableType) throws Exception {
    init(tableType, false);
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .enableMetrics(false)
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .retainCommits(1)
            .build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(2, 3)
            .build())
        .build();
    initWriteConfigAndMetatableWriter(writeConfig, true);

    List<String> instants = new ArrayList<>();
    // Trigger 4 regular writes in data table.
    for (int i = 1; i <= 4; i++) {
      String instant = WriteClientTestUtils.createNewInstantTime();
      instants.add(instant);
      doWriteOperation(testTable, instant, INSERT);
    }

    // The earliest deltacommit in the metadata table should be the 1st instant,
    // and the "00000000000000" init deltacommit should be archived.
    HoodieTableMetaClient metadataMetaClient = createMetaClient(metadataTableBasePath);
    HoodieActiveTimeline metadataTimeline = metadataMetaClient.reloadActiveTimeline();
    assertEquals(instants.get(0), metadataTimeline.getCommitsTimeline().firstInstant().get().requestedTime());

    getHoodieWriteClient(writeConfig);
    // Trigger data table archive, should archive 1st, 2nd.
    archiveDataTable(writeConfig, createMetaClient(basePath));
    // Trigger a regular write operation. metadata timeline archival should kick in and catch up with data table.
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);
    metadataTimeline = metadataMetaClient.reloadActiveTimeline();
    assertEquals(instants.get(2), metadataTimeline.getCommitsTimeline().firstInstant().get().requestedTime());
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataInsertUpsertClean(HoodieTableType tableType) throws Exception {
    init(tableType);
    doWriteOperation(testTable, "0000001", INSERT);
    doWriteOperation(testTable, "0000002");
    doCleanAndValidate(testTable, "0000003", Arrays.asList("0000001"));
    if (tableType == MERGE_ON_READ) {
      doCompaction(testTable, "0000004");
    }
    doWriteOperation(testTable, "0000005");
    validateMetadata(testTable, emptyList(), true);
  }

  @Test
  public void testMetadataInsertUpsertCleanNonPartitioned() throws Exception {
    init(COPY_ON_WRITE);
    testTable.setNonPartitioned();
    doWriteOperationNonPartitioned(testTable, "0000001", INSERT);
    doWriteOperationNonPartitioned(testTable, "0000002", UPSERT);
    testTable.doCleanBasedOnCommits("0000003", Arrays.asList("0000001"));
    validateMetadata(testTable, emptyList(), true);
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testInsertUpsertCluster(HoodieTableType tableType) throws Exception {
    init(tableType);
    doWriteOperation(testTable, "0000001", INSERT);
    doWriteOperation(testTable, "0000002");
    doClusterAndValidate(testTable, "0000003");
    if (tableType == MERGE_ON_READ) {
      doCompaction(testTable, "0000004");
    }
    doCleanAndValidate(testTable, "0000005", Arrays.asList("0000001"));
    validateMetadata(testTable, emptyList(), true);
  }

  /**
   * Tests that table services in data table won't trigger table services in metadata table.
   */
  @Test
  public void testMetadataTableServices() throws Exception {
    init(COPY_ON_WRITE, false);
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .enableMetrics(false)
            // after 4 (3 due to files, column_Stats, partition_stats init and 1 more due to data commit) delta commits for regular writer operations, compaction should kick in.
            .withMaxNumDeltaCommitsBeforeCompaction(4)
            .build()).build();
    initWriteConfigAndMetatableWriter(writeConfig, true);

    String firstInstant = WriteClientTestUtils.createNewInstantTime();
    doWriteOperation(testTable, firstInstant, INSERT);
    doCleanAndValidate(testTable, WriteClientTestUtils.createNewInstantTime(), Collections.singletonList(firstInstant));

    HoodieTableMetadata tableMetadata = metadata(writeConfig, context);
    // since clean was the last commit, table services should not get triggered in metadata table.
    assertFalse(tableMetadata.getLatestCompactionTime().isPresent());

    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), UPSERT);
    // this should have triggered compaction in metadata table
    tableMetadata = metadata(writeConfig, context);
    assertTrue(tableMetadata.getLatestCompactionTime().isPresent());
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testTableOperationsWithMetadataIndex(HoodieTableType tableType) throws Exception {
    initPath();
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .bloomIndexBucketizedChecking(false)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMetadataIndexBloomFilter(true)
            .withMetadataIndexBloomFilterFileGroups(4)
            .withMetadataIndexColumnStats(true)
            .withMetadataIndexBloomFilterFileGroups(2)
            .withMaxNumDeltaCommitsBeforeCompaction(12) // cannot restore to before the oldest compaction on MDT as there are no base files before that time
            .build())
        .build();
    init(tableType, writeConfig);
    testTableOperationsForMetaIndexImpl(writeConfig);
  }

  private void testTableOperationsForMetaIndexImpl(final HoodieWriteConfig writeConfig) throws Exception {
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    testTableOperationsImpl(engineContext, writeConfig);
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataTableDeletePartition(HoodieTableType tableType) throws Exception {
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

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, cfg, true)) {
      // Write 1 (Bulk insert)
      String newCommitTime = "0000001";
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertTrue(client.commit(newCommitTime, jsc.parallelize(writeStatuses)));
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Write 2 (upserts)
      newCommitTime = "0000002";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      validateMetadata(client);

      records = dataGen.generateInserts(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertTrue(client.commit(newCommitTime, jsc.parallelize(writeStatuses)));
      assertNoWriteErrors(writeStatuses);

      // metadata writer to delete column_stats partition
      try (HoodieBackedTableMetadataWriter<JavaRDD<HoodieRecord>, JavaRDD<WriteStatus>> metadataWriter = metadataWriter(client)) {
        assertNotNull(metadataWriter, "MetadataWriter should have been initialized");
        metadataWriter.deletePartitions("0000003", Arrays.asList(COLUMN_STATS));

        HoodieTableMetaClient metadataMetaClient = createMetaClient(metadataTableBasePath);
        List<String> metadataTablePartitions = FSUtils.getAllPartitionPaths(
            engineContext, metadataMetaClient, false);

        Option<HoodieInstant> completedReplaceInstant = metadataMetaClient.reloadActiveTimeline().getCompletedReplaceTimeline().lastInstant();
        assertTrue(completedReplaceInstant.isPresent());
        assertEquals("0000003", completedReplaceInstant.get().requestedTime());

        final Map<String, MetadataPartitionType> metadataEnabledPartitionTypes = new HashMap<>();
        metadataWriter.getEnabledPartitionTypes().forEach(e -> metadataEnabledPartitionTypes.put(e.getPartitionPath(), e));
        HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(engineContext, metadataMetaClient, metadataMetaClient.getActiveTimeline());
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
  }

  /**
   * Tests that virtual key configs are honored in base files after compaction in metadata table.
   */
  @Test
  public void testVirtualKeysInBaseFiles() throws Exception {
    boolean populateMetaFields = false;
    init(MERGE_ON_READ, false);
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .enableMetrics(false)
            .withMaxNumDeltaCommitsBeforeCompaction(2)
            .build()).build();
    initWriteConfigAndMetatableWriter(writeConfig, true);

    String firstInstant = WriteClientTestUtils.createNewInstantTime();
    doWriteOperation(testTable, firstInstant, INSERT);
    doClean(testTable, WriteClientTestUtils.createNewInstantTime(), Collections.singletonList(firstInstant));
    // this should have triggered compaction in metadata table
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), UPSERT);

    HoodieTableMetadata tableMetadata = metadata(writeConfig, context);
    assertTrue(tableMetadata.getLatestCompactionTime().isPresent());

    HoodieTableMetaClient metadataMetaClient = createMetaClient(metadataTableBasePath);
    HoodieWriteConfig metadataTableWriteConfig = getMetadataWriteConfig(writeConfig);
    metadataMetaClient.reloadActiveTimeline();

    HoodieTable table = HoodieSparkTable.create(metadataTableWriteConfig, context, metadataMetaClient);
    table.getHoodieView().sync();
    List<FileSlice> fileSlices = table.getSliceView().getLatestFileSlices("files").collect(Collectors.toList());
    HoodieBaseFile baseFile = fileSlices.get(0).getBaseFile().get();
    HoodieAvroHFileReaderImplBase hoodieHFileReader = (HoodieAvroHFileReaderImplBase)
        getHoodieSparkIOFactory(storage).getReaderFactory(HoodieRecordType.AVRO).getFileReader(
            table.getConfig(), new StoragePath(baseFile.getPath()));
    List<IndexedRecord> records = HoodieAvroHFileReaderImplBase.readAllRecords(hoodieHFileReader);
    records.forEach(entry -> {
      if (populateMetaFields) {
        assertNotNull(((GenericRecord) entry).get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
      } else {
        assertNull(((GenericRecord) entry).get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
      }
    });
  }

  /**
   * Test MDT compaction with delayed pending instants on DT(induced by multi-writer or async table services).
   *
   * <p>A demo:
   * <pre>
   *   Time t1
   *
   *   Main                 Metadata
   *   c1.commit    ->   c1.deltacommit
   *   c2.commit.   ->   c2.deltacommit
   *   c3.inflight  ->
   *   c4.commit    ->   c4.deltacommit
   *
   *   c5.requested ->   c6.compaction
   *
   *
   *   MDT files:
   *   F1.c1 (base) -> log(c2) -> log(c4)
   *   F1.c6 (base)
   *
   *
   *   Time t2 (Now c3 is completed)
   *
   *   Main                 Metadata
   *   c1.commit    ->   c1.deltacommit
   *   c2.commit.   ->   c2.deltacommit
   *   c3.commit    ->   c3.deltacommit
   *   c4.commit    ->   c4.deltacommit
   *
   *   c5.requested -> c6.compaction
   *
   *
   *   MDT files:
   *   F1.c1 (base) -> log(c2) -> log(c4)
   *   F1.c6 (base) -> log(c3)
   * </pre>
   */
  @Test
  public void testMetadataTableCompactionWithPendingInstants() throws Exception {
    init(COPY_ON_WRITE, false);
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .enableMetrics(false)
            .withMaxNumDeltaCommitsBeforeCompaction(4)
            .withMetadataIndexColumnStats(false)
            .build()).build();
    initWriteConfigAndMetatableWriter(writeConfig, true);
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);

    // test multi-writer scenario. let's add 1,2,3,4 where 1,2,4 succeeded, but 3 is still inflight. so latest delta commit in MDT is 4, while 3 is still pending
    // in DT and not seen by MDT yet. compaction should not trigger until 3 goes to completion.

    // case 1: a pending commit on DT that finished later on.
    String inflightInstant1 = WriteClientTestUtils.createNewInstantTime();
    HoodieCommitMetadata inflightCommitMeta = testTable.doWriteOperation(inflightInstant1, UPSERT, emptyList(),
        asList("p1", "p2"), 2, false, true);
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime());
    HoodieTableMetadata tableMetadata = metadata(writeConfig, context);
    // verify that compaction of metadata table does not kick in.
    assertFalse(tableMetadata.getLatestCompactionTime().isPresent());

    // write some commits to trigger the MDT compaction
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);

    tableMetadata = metadata(writeConfig, context);
    assertTrue(tableMetadata.getLatestCompactionTime().isPresent(), "Compaction of metadata table should kick in");

    // move inflight to completed
    testTable.moveInflightCommitToComplete(inflightInstant1, inflightCommitMeta);

    // case2: a pending commit succeeds on MDT but fails on DT.
    String inflightInstant2 = WriteClientTestUtils.createNewInstantTime();
    doWriteOperation(testTable, inflightInstant2, INSERT);
    testTable.moveCompleteCommitToInflight(inflightInstant2);

    // write some commits to trigger the MDT compaction
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);

    tableMetadata = metadata(writeConfig, context);
    assertTrue(tableMetadata.getLatestCompactionTime().isPresent(), "Compaction of metadata table should kick in");
    assertEquals(HoodieInstantTimeGenerator.instantTimeMinusMillis(inflightInstant2, 1L), tableMetadata.getLatestCompactionTime().get());
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testInitializeMetadataTableWithPendingInstant(HoodieTableType tableType) throws Exception {
    init(tableType, false);
    initWriteConfigAndMetatableWriter(writeConfig, false);
    // 1. firstly we disable the metadata table, then create two completed commits and one inflight commit.
    // 1.1write 2 commits first.
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);

    // 1.2 create another inflight commit
    String inflightInstant = WriteClientTestUtils.createNewInstantTime();
    HoodieCommitMetadata inflightCommitMeta = testTable.doWriteOperation(inflightInstant, UPSERT, emptyList(),
        asList("p1", "p2"), 2, false, true);
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime());

    // 2. now enable the metadata table and triggers the initialization
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .enableMetrics(false)
            .withMaxNumDeltaCommitsBeforeCompaction(4)
            .build()).build();

    // 2.1 initializes the metadata table, it will exclude the files from the inflight instant.
    initWriteConfigAndMetatableWriter(writeConfig, true);

    // 2.2 move inflight to completed
    testTable.moveInflightCommitToComplete(inflightInstant, inflightCommitMeta);
    validateMetadata(testTable, true);
  }

  /**
   * Tests that virtual key configs are honored in base files after compaction in metadata table.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMetadataTableWithPendingCompaction(boolean simulateFailedCompaction) throws Exception {
    init(COPY_ON_WRITE, false);
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .enableMetrics(false)
            .withMaxNumDeltaCommitsBeforeCompaction(3)
            .build()).build();
    initWriteConfigAndMetatableWriter(writeConfig, true);

    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);
    // create an inflight compaction in metadata table.
    // not easy to create an inflight in metadata table directly, hence letting compaction succeed and then deleting the completed instant.
    // this new write is expected to trigger metadata table compaction
    String commitInstant = WriteClientTestUtils.createNewInstantTime();
    doWriteOperation(testTable, commitInstant, INSERT);
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);

    HoodieTableMetadata tableMetadata = metadata(writeConfig, context);
    Option<String> metadataCompactionInstant = tableMetadata.getLatestCompactionTime();
    assertTrue(metadataCompactionInstant.isPresent());

    // validateMetadata(testTable);
    // Fetch compaction Commit file and rename to some other file. completed compaction meta file should have some serialized info that table interprets
    // for future upserts. so, renaming the file here to some temp name and later renaming it back to same name.
    java.nio.file.Path metaFilePath = Paths.get(HoodieTestUtils.getCompleteInstantPath(
            metaClient.getStorage(),
            new StoragePath(new StoragePath(metadataTableBasePath, METAFOLDER_NAME), TIMELINEFOLDER_NAME),
            metadataCompactionInstant.get(), HoodieTimeline.COMMIT_ACTION)
        .toUri());
    java.nio.file.Path tempFilePath =
        FileCreateUtilsLegacy.renameFileToTemp(metaFilePath, metadataCompactionInstant.get());
    metaClient.reloadActiveTimeline();
    testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter, Option.of(context));
    // this validation will exercise the code path where a compaction is inflight in metadata table, but still metadata based file listing should match non
    // metadata based file listing.
    validateMetadata(testTable);

    if (simulateFailedCompaction) {
      // this should retry the compaction in metadata table.
      doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);
    } else {
      // let the compaction succeed in metadata and validation should succeed.
      FileCreateUtilsLegacy.renameTempToMetaFile(tempFilePath, metaFilePath);
    }

    validateMetadata(testTable);

    // add few more write and validate
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);
    doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), UPSERT);
    validateMetadata(testTable);

    if (simulateFailedCompaction) {
      //trigger another compaction failure.
      tableMetadata = metadata(writeConfig, context);
      metadataCompactionInstant = tableMetadata.getLatestCompactionTime();
      assertTrue(metadataCompactionInstant.isPresent());

      // Fetch compaction Commit file and rename to some other file. completed compaction meta file should have some serialized info that table interprets
      // for future upserts. so, renaming the file here to some temp name and later renaming it back to same name.
      metaFilePath = Paths.get(HoodieTestUtils.getCompleteInstantPath(
              metaClient.getStorage(),
              new StoragePath(new StoragePath(metadataTableBasePath, METAFOLDER_NAME), TIMELINEFOLDER_NAME),
              metadataCompactionInstant.get(), HoodieTimeline.COMMIT_ACTION)
          .toUri());
      FileCreateUtilsLegacy.renameFileToTemp(metaFilePath, metadataCompactionInstant.get());

      validateMetadata(testTable);

      // this should retry the failed compaction in metadata table.
      doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);

      validateMetadata(testTable);

      // add few more write and validate
      doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), INSERT);
      doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime(), UPSERT);
      validateMetadata(testTable);
    }
  }

  @Test
  public void testMetadataRollbackWithCompaction() throws Exception {
    HoodieTableType tableType = COPY_ON_WRITE;
    init(tableType, false);
    writeConfig = getWriteConfigBuilder(false, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .build())
        .build();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      // Write 1 (Bulk insert)
      String newCommitTime1 = "0000001";
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime1, 100);
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime1);
      JavaRDD writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime1);
      client.commit(newCommitTime1, writeStatuses);

      String newCommitTime2 = "0000002";
      records = dataGen.generateUniqueUpdates(newCommitTime2, 20);
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime2);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime2);
      client.commit(newCommitTime2, writeStatuses);

      String newCommitTime3 = "0000003";
      records = dataGen.generateUniqueUpdates(newCommitTime3, 20);
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime3);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime3);
      client.commit(newCommitTime3, writeStatuses);

      // rollback "3" so that there is no "3" in data table timeline, but there exists a DC "3" in metadata timeline.
      client.rollback(newCommitTime3);

      // mimicing crash or making an inflight in metadata table.
      StoragePath toDelete = HoodieTestUtils.getCompleteInstantPath(
          metaClient.getStorage(),
          new StoragePath(metaClient.getMetaPath() + "/metadata" + "/" + ".hoodie/timeline/"),
          newCommitTime2, HoodieTimeline.DELTA_COMMIT_ACTION);
      metaClient.getStorage().deleteDirectory(toDelete);

      // re-ingest w/ same commit time.
      records = dataGen.generateUniqueUpdates(newCommitTime3, 20);
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime3);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime3);
      client.commit(newCommitTime3, writeStatuses);

      // collect all commit meta files from metadata table.
      HoodieTableMetaClient metadataMetaClient = HoodieTestUtils.init(storageConf, HoodieTableMetadata.getMetadataTableBasePath(basePath), tableType, new Properties());
      String completionTimeForCommit3 = metadataMetaClient.getActiveTimeline().filter(instant -> instant.requestedTime().equals(newCommitTime3)).firstInstant()
          .map(HoodieInstant::getCompletionTime)
          .orElseThrow(() -> new IllegalStateException(newCommitTime3 + " should exist on the metadata"));
      String completionTimeForRollback = metadataMetaClient.getActiveTimeline().filter(instant -> instant.getAction().equals(ROLLBACK_ACTION)).firstInstant()
          .map(HoodieInstant::getCompletionTime)
          .orElseThrow(() -> new IllegalStateException("A rollback commit should exist on the metadata"));

      // ensure commit2's delta commit in MDT has completion time > the actual rollback for previous failed commit i.e. commit2.
      // if rollback wasn't eager, rollback's last completion time will be lower than the commit3'd delta commit completion time.
      assertTrue(compareTimestamps(completionTimeForCommit3, GREATER_THAN, completionTimeForRollback));
    }
  }

  @Test
  public void testMetadataRollbackDuringInit() throws Exception {
    HoodieTableType tableType = COPY_ON_WRITE;
    init(tableType, false);
    writeConfig = getWriteConfigBuilder(false, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableRecordIndex(true)
            // Disable the other two default index for this test because the test orchestrates
            // the rollback with the assumption of init commits being in certain order
            .withMetadataIndexColumnStats(false)
            .build())
        .build();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // First write that will be rolled back
    String newCommitTime1 = "20230809230000000";
    List<HoodieRecord> records1 = dataGen.generateInserts(newCommitTime1, 100);
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime1);
      JavaRDD writeStatuses = client.insert(jsc.parallelize(records1, 1), newCommitTime1);
      client.commit(newCommitTime1, writeStatuses);
    }

    // Revert the first commit to inflight, and move the table to a state where MDT fails
    // during the initialization of the second partition (record_index)
    revertTableToInflightState(writeConfig);

    // Second write
    String newCommitTime2 = "20230809232000000";
    List<HoodieRecord> records2 = dataGen.generateInserts(newCommitTime2, 20);
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime2);
      JavaRDD writeStatuses = client.insert(jsc.parallelize(records2, 1), newCommitTime2);
      client.commit(newCommitTime2, writeStatuses);
    }

    HoodieTableMetadata metadataReader = metaClient.getTableFormat().getMetadataFactory().create(
        context, storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath());
    HoodiePairData<String, HoodieRecordGlobalLocation> recordIndexData1 = metadataReader
        .readRecordIndexLocationsWithKeys(HoodieListData.eager(records1.stream().map(HoodieRecord::getRecordKey)
        .collect(Collectors.toList())));
    Map<String, HoodieRecordGlobalLocation> result;
    try {
      result = HoodieDataUtils.dedupeAndCollectAsMap(recordIndexData1);
      assertEquals(0, result.size(), "RI should not return entries that are rolled back.");
    } finally {
      recordIndexData1.unpersistWithDependencies();
    }
    
    HoodiePairData<String, HoodieRecordGlobalLocation> recordIndexData2 = metadataReader
        .readRecordIndexLocationsWithKeys(HoodieListData.eager(records2.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList())));
    try {
      result = HoodieDataUtils.dedupeAndCollectAsMap(recordIndexData2);
      assertEquals(records2.size(), result.size(), "RI should return entries in the commit.");
    } finally {
      recordIndexData2.unpersistWithDependencies();
    }
  }

  private void revertTableToInflightState(HoodieWriteConfig writeConfig) throws IOException {
    String basePath = writeConfig.getBasePath();
    String mdtBasePath = getMetadataTableBasePath(basePath);
    HoodieTableMetaClient metaClient = createMetaClient(basePath);
    HoodieTableMetaClient mdtMetaClient = createMetaClient(mdtBasePath);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieActiveTimeline mdtTimeline = mdtMetaClient.getActiveTimeline();
    assertEquals(1, timeline.countInstants());
    assertEquals(1, timeline.getCommitsTimeline().filterCompletedInstants().countInstants());
    assertEquals(3, mdtTimeline.countInstants());
    assertEquals(3, mdtTimeline.getCommitsTimeline().filterCompletedInstants().countInstants());
    String mdtInitCommit2 = mdtTimeline.getCommitsTimeline().filterCompletedInstants().getInstants().get(1).requestedTime();
    Pair<HoodieInstant, HoodieCommitMetadata> lastCommitMetadataWithValidData =
        mdtTimeline.getLastCommitMetadataWithValidData().get();
    String commit = lastCommitMetadataWithValidData.getLeft().requestedTime();
    assertTrue(timeline.getCommitsTimeline().containsInstant(commit));
    assertTrue(mdtTimeline.getCommitsTimeline().containsInstant(commit));

    // Transition the last commit to inflight in DT
    deleteMetaFile(metaClient.getStorage(), basePath, commit, COMMIT_EXTENSION);

    // Remove the last commit and written data files in MDT
    List<String> dataFiles =
        lastCommitMetadataWithValidData.getRight().getWriteStats().stream().map(
            HoodieWriteStat::getPath).collect(Collectors.toList());

    for (String relativeFilePath : dataFiles) {
      deleteFileFromStorage(metaClient.getStorage(), mdtBasePath + "/" + relativeFilePath);
    }

    deleteMetaFile(metaClient.getStorage(), mdtBasePath, commit, DELTA_COMMIT_EXTENSION);
    deleteMetaFile(metaClient.getStorage(), mdtBasePath, commit,
        DELTA_COMMIT_EXTENSION + INFLIGHT_EXTENSION);
    deleteMetaFile(metaClient.getStorage(), mdtBasePath, commit,
        DELTA_COMMIT_EXTENSION + REQUESTED_EXTENSION);

    // Transition the second init commit for record_index partition to inflight in MDT
    deleteMetaFile(
        metaClient.getStorage(), mdtBasePath, mdtInitCommit2, DELTA_COMMIT_EXTENSION);
    metaClient.getTableConfig().setMetadataPartitionState(
        metaClient, RECORD_INDEX.getPartitionPath(), false);
    metaClient.getTableConfig().setMetadataPartitionsInflight(
        metaClient, RECORD_INDEX);
    timeline = metaClient.getActiveTimeline().reload();
    mdtTimeline = mdtMetaClient.getActiveTimeline().reload();
    assertEquals(commit, timeline.lastInstant().get().requestedTime());
    assertTrue(timeline.lastInstant().get().isInflight());
    assertEquals(mdtInitCommit2, mdtTimeline.lastInstant().get().requestedTime());
    assertTrue(mdtTimeline.lastInstant().get().isInflight());
  }

  public static void deleteFileFromStorage(HoodieStorage storage, String targetPath)
      throws IOException {
    if (storage.exists(new StoragePath(targetPath))) {
      storage.deleteFile(new StoragePath(targetPath));
    }
  }

  public static void deleteMetaFile(HoodieStorage storage,
                                    String basePath,
                                    String instantTime,
                                    String suffix) throws IOException {
    if (suffix.contains(REQUESTED_EXTENSION) || suffix.contains(INFLIGHT_EXTENSION)) {
      String targetPath = basePath + "/" + METAFOLDER_NAME + "/" + TIMELINEFOLDER_NAME + "/" + instantTime + suffix;
      deleteFileFromStorage(storage, targetPath);
    } else {
      try {
        StoragePath completeInstantPath = HoodieTestUtils.getCompleteInstantPath(
            storage,
            new StoragePath(new StoragePath(basePath + "/" + METAFOLDER_NAME), TIMELINEFOLDER_NAME),
            instantTime, suffix.replaceFirst(".", ""));
        deleteFileFromStorage(storage, completeInstantPath.toString());
      } catch (Exception e) {
        // Ignore failure if file doesn't exist
      }
    }
  }

  /**
   * Test arguments - Table type, populate meta fields, exclude key from payload.
   */
  public static List<Arguments> testMetadataRecordKeyExcludeFromPayloadArgs() {
    return asList(
        Arguments.of(COPY_ON_WRITE, false),
        Arguments.of(MERGE_ON_READ, false)
    );
  }

  /**
   * 1. Verify metadata table records key deduplication feature. When record key
   * deduplication is enabled, verify the metadata record payload on disk has empty key.
   * Otherwise, verify the valid key.
   * 2. Verify populate meta fields work irrespective of record key deduplication config.
   * 3. Verify table services like compaction benefit from record key deduplication feature.
   */
  @ParameterizedTest
  @MethodSource("testMetadataRecordKeyExcludeFromPayloadArgs")
  public void testMetadataRecordKeyExcludeFromPayload(final HoodieTableType tableType, final boolean enableMetaFields) throws Exception {
    initPath();
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMaxNumDeltaCommitsBeforeCompaction(3)
            .build())
        .build();
    init(tableType, writeConfig);

    // 2nd commit
    doWriteOperation(testTable, "0000001", INSERT);

    final HoodieTableMetaClient metadataMetaClient = createMetaClient(metadataTableBasePath);
    HoodieWriteConfig metadataTableWriteConfig = getMetadataWriteConfig(writeConfig);
    metadataMetaClient.reloadActiveTimeline();
    final HoodieTable table = HoodieSparkTable.create(metadataTableWriteConfig, context, metadataMetaClient);

    // Compaction has not yet kicked in. Verify all the log files
    // for the metadata records persisted on disk as per the config.
    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadLogFiles(table, metadataMetaClient, "0000001",
          enableMetaFields);
    }, "Metadata table should have valid log files!");

    verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(table, enableMetaFields);

    // 2 more commits
    doWriteOperation(testTable, "0000002", UPSERT);
    doWriteOperation(testTable, "0000004", UPSERT);

    // Compaction should be triggered by now. Let's verify the log files
    // if any for the metadata records persisted on disk as per the config.
    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadLogFiles(table, metadataMetaClient, "0000002",
          enableMetaFields);
    }, "Metadata table should have valid log files!");

    // Verify the base file created by the just completed compaction.
    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(table, enableMetaFields);
    }, "Metadata table should have a valid base file!");

    // 2 more commits to trigger one more compaction, along with a clean
    doWriteOperation(testTable, "0000005", UPSERT);
    doClean(testTable, "0000006", Arrays.asList("0000004"));
    doWriteOperation(testTable, "0000007", UPSERT);

    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadLogFiles(table, metadataMetaClient, "7", enableMetaFields);
    }, "Metadata table should have valid log files!");

    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(table, enableMetaFields);
    }, "Metadata table should have a valid base file!");

    validateMetadata(testTable);
  }

  /**
   * Verify the metadata table log files for the record field correctness. On disk format
   * should be based on meta fields and key deduplication config. And the in-memory merged
   * records should all be materialized fully irrespective of the config.
   *
   * @param table                 - Hoodie metadata test table
   * @param metadataMetaClient    - Metadata meta client
   * @param latestCommitTimestamp - Latest commit timestamp
   * @param enableMetaFields      - Enable meta fields for the table records
   */
  private void verifyMetadataRecordKeyExcludeFromPayloadLogFiles(HoodieTable table, HoodieTableMetaClient metadataMetaClient,
                                                                 String latestCommitTimestamp,
                                                                 boolean enableMetaFields) throws IOException {
    table.getHoodieView().sync();

    // Compaction should not be triggered yet. Let's verify no base file
    // and few log files available.
    List<FileSlice> fileSlices = table.getSliceView()
        .getLatestFileSlices(FILES.getPartitionPath()).collect(Collectors.toList());
    if (fileSlices.isEmpty()) {
      throw new IllegalStateException("LogFile slices are not available!");
    }

    // Verify the log files honor the key deduplication and virtual keys config
    List<HoodieLogFile> logFiles = fileSlices.get(0).getLogFiles().collect(Collectors.toList());

    // Verify the on-disk raw records before they get materialized
    verifyMetadataRawRecords(table, logFiles, enableMetaFields);

    // Verify the in-memory materialized and merged records
    verifyMetadataMergedRecords(metadataMetaClient, logFiles, latestCommitTimestamp, enableMetaFields);
  }

  /**
   * Verify the metadata table on-disk raw records. When populate meta fields is enabled,
   * these records should have additional meta fields in the payload. When key deduplication
   * is enabled, these records on the disk should have key in the payload as empty string.
   *
   * @param table
   * @param logFiles         - Metadata table log files to be verified
   * @param enableMetaFields - Enable meta fields for records
   */
  private static void verifyMetadataRawRecords(HoodieTable table, List<HoodieLogFile> logFiles, boolean enableMetaFields) throws IOException {
    HoodieStorage storage = table.getStorage();
    for (HoodieLogFile logFile : logFiles) {
      List<StoragePathInfo> pathInfoList = storage.listDirectEntries(logFile.getPath());
      Schema writerSchema  =
          TableSchemaResolver.readSchemaFromLogFile(storage, logFile.getPath());
      if (writerSchema == null) {
        // not a data block
        continue;
      }

      try (HoodieLogFormat.Reader logFileReader = HoodieLogFormat.newReader(storage,
          new HoodieLogFile(pathInfoList.get(0).getPath()), writerSchema)) {
        while (logFileReader.hasNext()) {
          HoodieLogBlock logBlock = logFileReader.next();
          if (logBlock instanceof HoodieDataBlock) {
            try (
                ClosableIterator<HoodieRecord<IndexedRecord>> recordItr = ((HoodieDataBlock) logBlock).getRecordIterator(
                    HoodieRecordType.AVRO)) {
              recordItr.forEachRemaining(indexRecord -> {
                final GenericRecord record = (GenericRecord) indexRecord.getData();
                if (enableMetaFields) {
                  // Metadata table records should have meta fields!
                  assertNotNull(record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
                  assertNotNull(record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD));
                } else {
                  // Metadata table records should not have meta fields!
                  assertNull(record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
                  assertNull(record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD));
                }

                final String key = String.valueOf(record.get(HoodieMetadataPayload.KEY_FIELD_NAME));
                assertFalse(key.isEmpty());
                if (enableMetaFields) {
                  assertTrue(key.equals(String.valueOf(record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD))));
                }
              });
            }
          }
        }
      }
    }
  }

  /**
   * Verify the metadata table in-memory merged records. Irrespective of key deduplication
   * config, the in-memory merged records should always have the key field in the record
   * payload fully materialized.
   *
   * @param metadataMetaClient    - Metadata table meta client
   * @param logFiles              - Metadata table log files
   * @param latestCommitTimestamp
   * @param enableMetaFields      - Enable meta fields
   */
  private void verifyMetadataMergedRecords(HoodieTableMetaClient metadataMetaClient, List<HoodieLogFile> logFiles,
                                           String latestCommitTimestamp, boolean enableMetaFields) {
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
    if (enableMetaFields) {
      schema = HoodieAvroUtils.addMetadataFields(schema);
    }
    HoodieAvroReaderContext readerContext = new HoodieAvroReaderContext(metadataMetaClient.getStorageConf(), metadataMetaClient.getTableConfig(), Option.empty(), Option.empty());
    HoodieFileGroupReader<IndexedRecord> fileGroupReader = HoodieFileGroupReader.<IndexedRecord>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metadataMetaClient)
        .withLogFiles(logFiles.stream())
        .withBaseFileOption(Option.empty())
        .withPartitionPath(FILES.getPartitionPath())
        .withLatestCommitTime(latestCommitTimestamp)
        .withRequestedSchema(schema)
        .withDataSchema(schema)
        .withProps(new TypedProperties())
        .withEnableOptimizedLogBlockScan(writeConfig.getMetadataConfig().isOptimizedLogBlocksScanEnabled())
        .build();

    try (ClosableIterator<HoodieRecord<IndexedRecord>> iter = fileGroupReader.getClosableHoodieRecordIterator()) {
      iter.forEachRemaining(entry -> {
        assertFalse(entry.getRecordKey().isEmpty());
        assertEquals(entry.getKey().getRecordKey(), entry.getRecordKey());
      });
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  /**
   * Verify metadata table base files for the records persisted based on the config. When
   * the key deduplication is enabled, the records persisted on the disk in the base file
   * should have key field in the payload as empty string.
   *
   * @param table            - Metadata table
   * @param enableMetaFields - Enable meta fields
   */
  private void verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(HoodieTable table, boolean enableMetaFields) throws IOException {
    table.getHoodieView().sync();
    List<FileSlice> fileSlices = table.getSliceView()
        .getLatestFileSlices(FILES.getPartitionPath()).collect(Collectors.toList());
    if (!fileSlices.get(0).getBaseFile().isPresent()) {
      throw new IllegalStateException("Base file not available!");
    }
    final HoodieBaseFile baseFile = fileSlices.get(0).getBaseFile().get();

    HoodieAvroHFileReaderImplBase hoodieHFileReader = (HoodieAvroHFileReaderImplBase) getHoodieSparkIOFactory(storage)
        .getReaderFactory(HoodieRecordType.AVRO)
        .getFileReader(table.getConfig(), new StoragePath(baseFile.getPath()));
    List<IndexedRecord> records = HoodieAvroHFileReaderImplBase.readAllRecords(hoodieHFileReader);
    records.forEach(entry -> {
      if (enableMetaFields) {
        assertNotNull(((GenericRecord) entry).get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
      } else {
        assertNull(((GenericRecord) entry).get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
      }

      final String keyInPayload = (String) ((GenericRecord) entry)
          .get(HoodieMetadataPayload.KEY_FIELD_NAME);
      assertFalse(keyInPayload.isEmpty());
    });
  }

  /**
   * Test rollback of various table operations sync to Metadata Table correctly.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testRollbackOperations(HoodieTableType tableType) throws Exception {
    init(tableType);
    doWriteInsertAndUpsert(testTable);

    // trigger an upsert
    doWriteOperation(testTable, "0000003", UPSERT);

    // trigger a commit and rollback
    doWriteOperation(testTable, "0000004");
    doRollbackAndValidate(testTable, "0000004", "0000005");

    // trigger few upserts and validate
    for (int i = 6; i < 10; i++) {
      doWriteOperation(testTable, "000000" + i);
    }
    validateMetadata(testTable);

    doWriteOperation(testTable, "0000010");

    // rollback last commit. and validate.
    doRollbackAndValidate(testTable, "0000010", "0000011");

    // rollback of compaction
    if (MERGE_ON_READ.equals(tableType)) {
      doCompactionAndValidate(testTable, "0000012");
      doRollbackAndValidate(testTable, "0000012", "0000013");
    }

    // roll back of delete
    doWriteOperation(testTable, "0000014", DELETE);
    doRollbackAndValidate(testTable, "0000014", "0000015");

    // rollback partial commit
    writeConfig = getWriteConfigBuilder(true, true, false).withRollbackUsingMarkers(false).build();
    doWriteOperation(testTable, "0000016");
    testTable.doRollback("0000016", "0000017");
    validateMetadata(testTable);

    // marker-based rollback of partial commit
    writeConfig = getWriteConfigBuilder(true, true, false).withRollbackUsingMarkers(true).build();
    doWriteOperation(testTable, "0000018");
    testTable.doRollback("0000018", "0000019");
    validateMetadata(testTable, true);
  }

  @Test
  public void testRollbackOperationsNonPartitioned() throws Exception {
    init(COPY_ON_WRITE);
    testTable.setNonPartitioned();
    doWriteInsertAndUpsertNonPartitioned(testTable);

    // trigger an upsert
    doWriteOperationNonPartitioned(testTable, "0000003", UPSERT);

    // trigger a commit and rollback
    doWriteOperationNonPartitioned(testTable, "0000004", UPSERT);
    doRollback(testTable, "0000004", "0000005");
    validateMetadata(testTable);

    // trigger few upserts and validate
    for (int i = 6; i < 10; i++) {
      doWriteOperationNonPartitioned(testTable, "000000" + i, UPSERT);
    }
    validateMetadata(testTable);
  }

  /**
   * Test that manual rollbacks work correctly and enough timeline history is maintained on the metadata table
   * timeline.
   */
  @Test
  public void testManualRollbacks() throws Exception {
    init(COPY_ON_WRITE, false);
    // Setting to archive more aggressively on the Metadata Table than the Dataset
    final int maxDeltaCommitsBeforeCompaction = 4;
    final int minArchiveCommitsDataset = 4;
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsBeforeCompaction)
            .build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .retainCommits(1)
            .retainFileVersions(1)
            .withAutoClean(false)
            .withAsyncClean(true)
            .build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(minArchiveCommitsDataset, minArchiveCommitsDataset + 1)
            .build())
        .build();

    initWriteConfigAndMetatableWriter(writeConfig, true);
    String commit1 = WriteClientTestUtils.createNewInstantTime();
    String commit2 = WriteClientTestUtils.createNewInstantTime();
    doWriteInsertAndUpsert(testTable, commit1, commit2, false);

    for (int i = 3; i < 10; i++) {
      doWriteOperation(testTable, WriteClientTestUtils.createNewInstantTime());
      archiveDataTable(writeConfig, metaClient);
    }
    validateMetadata(testTable);

    // We can only roll back those commits whose deltacommit have not been archived yet.
    List<HoodieInstant> allInstants = metaClient.reloadActiveTimeline().getCommitsTimeline().getReverseOrderedInstants().collect(Collectors.toList());
    for (HoodieInstant instantToRollback : allInstants) {
      try {
        testTable.doRollback(instantToRollback.requestedTime(), WriteClientTestUtils.createNewInstantTime());
        validateMetadata(testTable);
      } catch (HoodieMetadataException e) {
        // This is expected since we are rolling back commits that are older than the latest compaction on the MDT
        break;
      }
    }
  }

  /**
   * Test sync of table operations.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testSync(HoodieTableType tableType) throws Exception {
    init(tableType, false);
    // Initial commits without metadata table enabled
    writeConfig = getWriteConfigBuilder(true, false, false).build();
    doPreBootstrapOperations(testTable, "00000001", "00000002");

    // Enable metadata table so it initialized by listing from file system
    writeConfig = getWriteConfigBuilder(true, true, false).build();

    initWriteConfigAndMetatableWriter(writeConfig, true);
    syncTableMetadata(writeConfig);
    validateMetadata(testTable);

    doWriteOperation(testTable, "00000003", INSERT);
    doWriteOperation(testTable, "00000004", UPSERT);
    doWriteOperation(testTable, "00000005", UPSERT);

    // trigger compaction
    if (MERGE_ON_READ.equals(tableType)) {
      doCompactionAndValidate(testTable, "00000006");
    }

    // trigger an upsert
    doWriteOperation(testTable, "00000008");
    // trigger delete
    doWriteOperation(testTable, "00000009", DELETE);
    // trigger clean
    doCleanAndValidate(testTable, "00000010", asList("00000003", "00000004"));
    // trigger another upsert
    doWriteOperation(testTable, "00000011");
    // trigger clustering
    doClusterAndValidate(testTable, "00000012");

    // If there is an inflight operation, the Metadata Table is not updated beyond that operations but the
    // in-memory merge should consider all the completed operations.
    HoodieCommitMetadata inflightCommitMeta = testTable.doWriteOperation("00000007", UPSERT, emptyList(),
        asList("p1", "p2"), 2, false, true);
    // trigger upsert
    doWriteOperation(testTable, "00000013");
    // testTable validation will fetch only files pertaining to completed commits. So, validateMetadata() will skip files for 007
    // while validating against actual metadata table.
    validateMetadata(testTable, singletonList("00000007"));

    // Remove the inflight instance holding back table sync
    testTable.moveInflightCommitToComplete("00000007", inflightCommitMeta);
    validateMetadata(testTable);
    // A regular commit should get synced
    doWriteOperation(testTable, "00000014");
    validateMetadata(testTable, emptyList(), true);
  }

  @ParameterizedTest
  @MethodSource("tableTypeAndEnableOperationArgs")
  public void testMetadataBootstrapLargeCommitList(HoodieTableType tableType, boolean nonPartitionedDataset) throws Exception {
    init(tableType, true, true, true, false);
    if (nonPartitionedDataset) {
      testTable.setNonPartitioned();
    }
    for (int i = 1; i < 25; i += 7) {
      String commitTime1 = InProcessTimeGenerator.createNewInstantTime();
      doWriteOperation(testTable, commitTime1, INSERT, nonPartitionedDataset);
      doWriteOperation(testTable, InProcessTimeGenerator.createNewInstantTime(), UPSERT, nonPartitionedDataset);
      doClean(testTable, InProcessTimeGenerator.createNewInstantTime(), Collections.singletonList(commitTime1));
      doWriteOperation(testTable, InProcessTimeGenerator.createNewInstantTime(), UPSERT, nonPartitionedDataset);
      if (tableType == MERGE_ON_READ) {
        doCompaction(testTable, InProcessTimeGenerator.createNewInstantTime(), nonPartitionedDataset);
      }
      // added 60s to commitTime6 to make sure it is greater than compaction instant triggered by previous commit
      String commitTime6 = HoodieInstantTimeGenerator.instantTimePlusMillis(InProcessTimeGenerator.createNewInstantTime(), 60000L);
      doWriteOperation(testTable, commitTime6, UPSERT, nonPartitionedDataset);
      doRollback(testTable, commitTime6, InProcessTimeGenerator.createNewInstantTime());
    }
    validateMetadata(testTable, emptyList(), nonPartitionedDataset);
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

  /**
   * First attempt at bootstrap failed but the file slices get created. The next bootstrap should continue successfully.
   */
  @Test
  public void testFailedBootstrap() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Config with 5 fileGroups for record index
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableRecordIndex(true)
            .withRecordIndexFileGroupCount(5, 5)
            .build())
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      // Write
      String newCommitTime = client.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      List<WriteStatus> writeStatuses =
          client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);

      // Metadata table should exist
      final StoragePath metadataTablePath =
          new StoragePath(getMetadataTableBasePath(writeConfig.getBasePath()));
      assertTrue(storage.exists(metadataTablePath));
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(metaClient.getTableConfig().isMetadataTableAvailable());

      // File groups should be created as in the config
      HoodieBackedTableMetadata metadataReader = (HoodieBackedTableMetadata) metadata(client, storage);
      assertEquals(HoodieTableMetadataUtil.getPartitionLatestFileSlices(
          metadataReader.getMetadataMetaClient(), Option.empty(),
          MetadataPartitionType.FILES.getPartitionPath()).size(), 1);
      assertEquals(HoodieTableMetadataUtil.getPartitionLatestFileSlices(
          metadataReader.getMetadataMetaClient(), Option.empty(),
          RECORD_INDEX.getPartitionPath()).size(), 5);
    }

    // remove the MDT partition from dataset to simulate failed bootstrap
    Properties updateProperties = new Properties();
    updateProperties.setProperty(HoodieTableConfig.TABLE_METADATA_PARTITIONS.key(), "");
    HoodieTableConfig.update(
        storage,
        new StoragePath(basePath + StoragePath.SEPARATOR + METAFOLDER_NAME),
        updateProperties);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertFalse(metaClient.getTableConfig().isMetadataTableAvailable());

    // Config with 3 fileGroups for record index
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableRecordIndex(true)
            .withRecordIndexFileGroupCount(3, 3)
            .build())
        .build();

    // Another commit which should bootstrap the MDT again
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      // Write
      String newCommitTime = client.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));

      // Metadata table is recreated, during bootstrapping of metadata table.
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(metaClient.getTableConfig().isMetadataTableAvailable());
      validateMetadata(client);

      // File groups should be created as in the config
      HoodieBackedTableMetadata metadataReader = (HoodieBackedTableMetadata) metadata(client, storage);
      assertEquals(HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataReader.getMetadataMetaClient(), Option.empty(),
          MetadataPartitionType.FILES.getPartitionPath()).size(), 1);
      assertEquals(HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataReader.getMetadataMetaClient(), Option.empty(),
          RECORD_INDEX.getPartitionPath()).size(), 3);
    }
  }

  // Some operations are not feasible with test table infra. hence using write client to test those cases.

  /**
   * Rollback of the first commit should not trigger bootstrap errors at the metadata table.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testFirstCommitRollback(HoodieTableType tableType) throws Exception {
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext,
        getWriteConfigBuilder(true, true, false).withRollbackUsingMarkers(false).build(), true)) {

      // Write 1
      String commitTime = "0000001";
      List<HoodieRecord> records = dataGen.generateInserts(commitTime, 20);
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), commitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(commitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);

      // Rollback the first commit
      client.rollback(commitTime);

      // Write 2
      commitTime = "0000002";
      records = dataGen.generateInserts(commitTime, 10);
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), commitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(commitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);
    }
  }

  /**
   * Tests the metadata payload spurious deletes.
   * Lets say a commit was applied to metadata table, and later was explicitly got rolled back. Due to spark task failures, there could be more files in rollback
   * metadata when compared to the original commit metadata. When payload consistency check is enabled, it will throw exception. If not, it will succeed.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMetadataPayloadSpuriousDeletes(boolean ignoreSpuriousDeletes) throws Exception {
    tableType = COPY_ON_WRITE;
    init(tableType, true, true, false, ignoreSpuriousDeletes);
    doWriteInsertAndUpsert(testTable);
    // trigger an upsert
    doWriteOperationAndValidate(testTable, "0000003");

    // trigger a commit and rollback
    doWriteOperation(testTable, "0000004");
    // add extra files in rollback to check for payload consistency
    Map<String, List<String>> extraFiles = new HashMap<>();
    extraFiles.put("p1", Collections.singletonList("f10"));
    extraFiles.put("p2", Collections.singletonList("f12"));
    testTable.doRollbackWithExtraFiles("0000004", "0000005", extraFiles);
    validateMetadata(testTable);
  }

  /**
   * Test several table operations with restore. This test uses SparkRDDWriteClient.
   * Once the restore support is ready in HoodieTestTable, then rewrite this test.
   */
  @Test
  public void testTableOperationsWithRestore() throws Exception {
    HoodieTableType tableType = COPY_ON_WRITE;
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withRollbackUsingMarkers(false).build();
    testTableOperationsImpl(engineContext, writeConfig);
  }

  /**
   * Test several table operations with restore. This test uses SparkRDDWriteClient.
   * Once the restore support is ready in HoodieTestTable, then rewrite this test.
   */
  @Test
  public void testTableOperationsWithRestoreforMOR() throws Exception {
    HoodieTableType tableType = MERGE_ON_READ;
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withRollbackUsingMarkers(false).build();
    testTableOperationsImpl(engineContext, writeConfig);
  }

  @Test
  public void testColStatsPrefixLookup() throws IOException {
    this.tableType = COPY_ON_WRITE;
    initPath();
    initSparkContexts("TestHoodieMetadata");
    initHoodieStorage();
    storage.createDirectory(new StoragePath(basePath));
    initTimelineService();
    initMetaClient(tableType);
    initTestDataGenerator();
    metadataTableBasePath = getMetadataTableBasePath(basePath);

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    // disable small file handling so that every insert goes to a new file group.
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withRollbackUsingMarkers(false)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER)
            .withAutoClean(false).retainCommits(1).retainFileVersions(1)
            .build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(0)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMetadataIndexColumnStats(true)
            .build())
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {

      String firstCommit = "0000001";
      List<HoodieRecord> records = dataGen.generateInserts(firstCommit, 20);

      AtomicInteger counter = new AtomicInteger();
      List<HoodieRecord> processedRecords = records.stream().map(entry ->
             entry.newInstance(new HoodieKey("key1_" + counter.getAndIncrement(), entry.getPartitionPath())))
          .collect(Collectors.toList());

      WriteClientTestUtils.startCommitWithTime(client, firstCommit);
      List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(processedRecords, 1), firstCommit).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(firstCommit, jsc.parallelize(writeStatuses));

      // Write 2 (inserts)
      String secondCommit = "0000002";
      WriteClientTestUtils.startCommitWithTime(client, secondCommit);
      records = dataGen.generateInserts(secondCommit, 20);
      AtomicInteger counter1 = new AtomicInteger();
      processedRecords = records.stream().map(entry ->
              entry.newInstance(new HoodieKey("key2_" + counter1.getAndIncrement(), entry.getPartitionPath())))
          .collect(Collectors.toList());
      writeStatuses = client.insert(jsc.parallelize(processedRecords, 1), secondCommit).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(secondCommit, jsc.parallelize(writeStatuses));

      Map<String, Map<String, List<String>>> commitToPartitionsToFiles = new HashMap<>();
      // populate commit -> partition -> file info to assist in validation and prefix search
      metaClient.getActiveTimeline().getInstants().forEach(entry -> {
        try {
          HoodieCommitMetadata commitMetadata =
              metaClient.getActiveTimeline().readCommitMetadata(entry);
          String commitTime = entry.requestedTime();
          if (!commitToPartitionsToFiles.containsKey(commitTime)) {
            commitToPartitionsToFiles.put(commitTime, new HashMap<>());
          }
          commitMetadata.getPartitionToWriteStats().entrySet()
              .stream()
              .forEach(partitionWriteStat -> {
                String partitionStatName = partitionWriteStat.getKey();
                List<HoodieWriteStat> writeStats = partitionWriteStat.getValue();
                String partition = HoodieTableMetadataUtil.getColumnStatsIndexPartitionIdentifier(partitionStatName);
                if (!commitToPartitionsToFiles.get(commitTime).containsKey(partition)) {
                  commitToPartitionsToFiles.get(commitTime).put(partition, new ArrayList<>());
                }
                writeStats.forEach(writeStat -> commitToPartitionsToFiles.get(commitTime).get(partition).add(writeStat.getPath()));
              });
        } catch (IOException e) {
          e.printStackTrace();
        }
      });

      HoodieTableMetadata tableMetadata = metadata(client, storage);
      // prefix search for column (_hoodie_record_key)
      ColumnStatsIndexPrefixRawKey columnKey = new ColumnStatsIndexPrefixRawKey(HoodieRecord.RECORD_KEY_METADATA_FIELD);
      List<HoodieRecord<HoodieMetadataPayload>> result = tableMetadata.getRecordsByKeyPrefixes(
          HoodieListData.lazy(Collections.singletonList(columnKey)),
          MetadataPartitionType.COLUMN_STATS.getPartitionPath(), true).collectAsList();

      // there are 3 partitions in total and 2 commits. total entries should be 6.
      assertEquals(result.size(), 6);

      // prefix search for col(_hoodie_record_key) and first partition. only 2 files should be matched
      ColumnStatsIndexPrefixRawKey columnWithPartitionKey = new ColumnStatsIndexPrefixRawKey(HoodieRecord.RECORD_KEY_METADATA_FIELD,
          HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
      result = tableMetadata.getRecordsByKeyPrefixes(
          HoodieListData.lazy(Collections.singletonList(columnWithPartitionKey)),
          MetadataPartitionType.COLUMN_STATS.getPartitionPath(), true).collectAsList();
      // 1 partition and 2 commits. total entries should be 2.
      assertEquals(result.size(), 2);
      result.forEach(entry -> {
        HoodieMetadataColumnStats metadataColumnStats = entry.getData().getColumnStatMetadata().get();
        String fileName = metadataColumnStats.getFileName();
        if (fileName.contains(firstCommit)) {
          assertTrue(commitToPartitionsToFiles.get(firstCommit).get(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
              .contains(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH + "/" + fileName));
        } else {
          assertTrue(commitToPartitionsToFiles.get(secondCommit).get(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
              .contains(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH + "/" + fileName));
        }
      });

      // prefix search for column {commit time} and first partition
      ColumnStatsIndexPrefixRawKey commitTimeKey = new ColumnStatsIndexPrefixRawKey(HoodieRecord.COMMIT_TIME_METADATA_FIELD,
          HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
      result = tableMetadata.getRecordsByKeyPrefixes(
          HoodieListData.lazy(Collections.singletonList(commitTimeKey)),
          MetadataPartitionType.COLUMN_STATS.getPartitionPath(), true).collectAsList();

      // 1 partition and 2 commits. total entries should be 2.
      assertEquals(result.size(), 2);
      result.forEach(entry -> {
        HoodieMetadataColumnStats metadataColumnStats = entry.getData().getColumnStatMetadata().get();
        // for commit time column, min max should be the same since we disable small files, every commit will create a new file
        assertEquals(metadataColumnStats.getMinValue(), metadataColumnStats.getMaxValue());
        String fileName = metadataColumnStats.getFileName();
        if (fileName.contains(firstCommit)) {
          assertTrue(commitToPartitionsToFiles.get(firstCommit).get(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
              .contains(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH + "/" + fileName));
        } else {
          assertTrue(commitToPartitionsToFiles.get(secondCommit).get(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
              .contains(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH + "/" + fileName));
        }
      });
    }
  }

  @Test
  public void testEagerRollbackinMDT() throws IOException {
    tableType = MERGE_ON_READ;
    initPath();
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true);
    // Write 1 (Bulk insert)
    String commit1 = client.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(commit1, 20);
    List<WriteStatus> writeStatuses =
        client.bulkInsert(jsc.parallelize(records, 1), commit1).collect();
    assertNoWriteErrors(writeStatuses);
    client.commit(commit1, jsc.parallelize(writeStatuses));

    // Write 2 (inserts)
    String commit2 = client.startCommit();
    records = dataGen.generateInserts(commit2, 20);
    writeStatuses = client.insert(jsc.parallelize(records, 1), commit2).collect();
    assertNoWriteErrors(writeStatuses);
    client.commit(commit2, jsc.parallelize(writeStatuses));

    // remove latest completed delta commit from MDT.
    StoragePath toDelete = HoodieTestUtils.getCompleteInstantPath(
        metaClient.getStorage(),
        new StoragePath(metaClient.getMetaPath() + "/metadata/.hoodie/timeline/"),
        commit2, HoodieTimeline.DELTA_COMMIT_ACTION);
    metaClient.getStorage().deleteDirectory(toDelete);

    // Write 3 (updates)
    client.close();
    client = new SparkRDDWriteClient(engineContext, writeConfig, true);
    String commit3 = client.startCommit();
    records = dataGen.generateUniqueUpdates(commit3, 10);
    writeStatuses = client.upsert(jsc.parallelize(records, 1), commit3).collect();
    assertNoWriteErrors(writeStatuses);
    client.commit(commit3, jsc.parallelize(writeStatuses));

    // ensure that 000003 is after rollback of the partially failed 2nd commit.
    HoodieTableMetaClient metadataMetaClient = HoodieTestUtils.createMetaClient(
        metaClient.getStorageConf(), metaClient.getMetaPath() + "/metadata/");
    HoodieInstant rollbackInstant =
        metadataMetaClient.getActiveTimeline().getRollbackTimeline().getInstants().get(0);

    // collect all commit meta files from metadata table.
    List<StoragePathInfo> metaFiles = metaClient.getStorage()
        .listDirectEntries(new StoragePath(metaClient.getMetaPath(), "metadata/.hoodie/timeline/"));
    List<StoragePathInfo> commit3Files = metaFiles.stream()
        .filter(pathInfo ->
            pathInfo.getPath().getName().contains(commit3)
                && pathInfo.getPath().getName().endsWith(HoodieTimeline.DELTA_COMMIT_ACTION))
        .collect(Collectors.toList());
    List<StoragePathInfo> rollbackFiles = metaFiles.stream()
        .filter(pathInfo ->
            pathInfo.getPath().getName().equals(INSTANT_FILE_NAME_GENERATOR.getFileName(rollbackInstant)))
        .collect(Collectors.toList());

    // ensure commit3's delta commit in MDT has last mod time > the actual rollback for previous failed commit i.e. commit2.
    // if rollback wasn't eager, rollback's last mod time will be lower than the commit3'd delta commit last mod time.
    assertTrue(commit3Files.get(0).getModificationTime() >= rollbackFiles.get(0).getModificationTime());
    client.close();
  }

  /**
   * Test all major table operations with the given table, config and context.
   *
   * @param engineContext - Engine context
   * @param writeConfig   - Write config
   */
  private void testTableOperationsImpl(HoodieSparkEngineContext engineContext, HoodieWriteConfig writeConfig) throws Exception {

    String newCommitTime = null;
    String instantToRestore = null;
    List<HoodieRecord> records = new ArrayList<>();
    List<WriteStatus> writeStatuses = null;

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      // Write 1 (Bulk insert)
      newCommitTime = client.startCommit();
      records = dataGen.generateInserts(newCommitTime, 20);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);

      // Write 2 (inserts)
      newCommitTime = client.startCommit();
      validateMetadata(client);

      records = dataGen.generateInserts(newCommitTime, 20);
      writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);

      // Write 3 (updates)
      newCommitTime = client.startCommit();
      records = dataGen.generateUniqueUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));

      // Write 4 (updates and inserts)
      newCommitTime = client.startCommit();
      records = dataGen.generateUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = (String) client.scheduleCompaction(Option.empty()).get();
        HoodieWriteMetadata result = client.compact(newCommitTime);
        client.commitCompaction(newCommitTime, result, Option.empty());
        assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(newCommitTime));
        validateMetadata(client);
      }

      // Write 5 (updates and inserts)
      newCommitTime = client.startCommit();
      instantToRestore = newCommitTime;
      records = dataGen.generateUpdates(newCommitTime, 5);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);

    }

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = (String) client.scheduleCompaction(Option.empty()).get();
        HoodieWriteMetadata result = client.compact(newCommitTime);
        client.commitCompaction(newCommitTime, result, Option.empty());
        assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(newCommitTime));
        validateMetadata(client);
      }

      // upserts
      newCommitTime = client.startCommit();
      records = dataGen.generateUpdates(newCommitTime, 5);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));

      // Clean
      client.clean();
      validateMetadata(client);

      // Restore
      client.restoreToInstant(instantToRestore, writeConfig.isMetadataTableEnabled());
      validateMetadata(client);
    }
  }

  /**
   * Test multi-writer on metadata table with optimistic concurrency.
   */
  @Test
  public void testMetadataMultiWriter() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "20");
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .withProperties(properties)
        .build();

    // Create commit1 with single writer.
    SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, writeConfig, true);
    String initialCommit = "0000000";
    List<HoodieRecord> initialRecords = dataGen.generateInserts(initialCommit, 100);
    WriteClientTestUtils.startCommitWithTime(writeClient, initialCommit);
    List<WriteStatus> initialWriteStatuses = writeClient.insert(jsc.parallelize(initialRecords, 1), initialCommit).collect();
    assertNoWriteErrors(initialWriteStatuses);
    writeClient.commit(initialCommit, jsc.parallelize(initialWriteStatuses));
    writeClient.close();

    ExecutorService executors = Executors.newFixedThreadPool(dataGen.getPartitionPaths().length);
    // Create clients in advance
    SparkRDDWriteClient[] writeClients = new SparkRDDWriteClient[dataGen.getPartitionPaths().length];
    for (int i = 0; i < dataGen.getPartitionPaths().length; i++) {
      writeClients[i] = new SparkRDDWriteClient(engineContext, writeConfig, true);
    }

    // Parallel commits for separate partitions
    List<Future> futures = new LinkedList<>();
    for (int i = 0; i < dataGen.getPartitionPaths().length; ++i) {
      final int index = i;
      String newCommitTime = "000000" + (index + 2);
      Future future = executors.submit(() -> {
        List<HoodieRecord> records = dataGen.generateInsertsForPartition(newCommitTime, 100, dataGen.getPartitionPaths()[index]);
        SparkRDDWriteClient localWriteClient = writeClients[index];
        WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);
        List<WriteStatus> writeStatuses = localWriteClient.insert(jsc.parallelize(records, 1), newCommitTime).collect();
        assertNoWriteErrors(writeStatuses);
        localWriteClient.commit(newCommitTime, jsc.parallelize(writeStatuses));
      });
      futures.add(future);
    }

    // Wait for all commits to complete
    for (Future future : futures) {
      future.get();
    }
    executors.shutdown();

    // Ensure all commits were synced to the Metadata Table
    HoodieTableMetaClient metadataMetaClient = createMetaClient(metadataTableBasePath);
    assertEquals(metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().countInstants(), 7);
    assertTrue(metadataMetaClient.getActiveTimeline().containsInstant(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "0000002")));
    assertTrue(metadataMetaClient.getActiveTimeline().containsInstant(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "0000003")));
    assertTrue(metadataMetaClient.getActiveTimeline().containsInstant(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "0000004")));

    // Compaction may occur if the commits completed in order
    assertTrue(metadataMetaClient.getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants().countInstants() <= 1);

    // Validation
    validateMetadata(writeClients[0]);
    Arrays.stream(writeClients).forEach(SparkRDDWriteClient::close);
  }

  /**
   * Tests that when inline cleaning is enabled and with auto commit set to true, there is no double locking.
   * bcoz, auto clean is triggered within post commit which is already happening within a lock.
   */
  @Test
  public void testMultiWriterForDoubleLocking() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");

    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).withAutoClean(true).retainCommits(4)
            .build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .withProperties(properties)
        .build();

    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      String partitionPath = dataGen.getPartitionPaths()[0];
      for (int j = 0; j < 6; j++) {
        String newCommitTime = writeClient.startCommit();
        List<HoodieRecord> records = dataGen.generateInsertsForPartition(newCommitTime, 100, partitionPath);
        JavaRDD writeStatuses = writeClient.insert(jsc.parallelize(records, 1), newCommitTime);
        writeClient.commit(newCommitTime, writeStatuses);
      }


      // Ensure all commits were synced to the Metadata Table
      HoodieTableMetaClient metadataMetaClient = createMetaClient(metadataTableBasePath);
      LOG.warn("total commits in metadata table " + metadataMetaClient.getActiveTimeline().getCommitsTimeline().countInstants());

      // 8 commits (3 init + 5 deltacommits) and 2 cleaner commits.
      assertEquals(metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().countInstants(), 10);
      assertTrue(metadataMetaClient.getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants().countInstants() <= 1);
      // Validation
      validateMetadata(writeClient);
    }
  }

  /**
   * Lets say clustering commit succeeded in metadata table, but failed before committing to datatable.
   * Next time, when clustering kicks in, hudi will rollback pending clustering (in data table) and re-attempt the clustering with same
   * instant time. So, this test ensures the 2nd attempt succeeds with metadata enabled.
   * This is applicable to any table service where instant time is fixed. So, how many ever times the operation fails, re attempt will
   * be made with same commit time.
   * Tests uses clustering to test out the scenario.
   */
  @Test
  public void testReattemptOfFailedClusteringCommit() throws Exception {
    tableType = HoodieTableType.COPY_ON_WRITE;
    init(tableType);
    context = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig config = getSmallInsertWriteConfigForMDT(2000, TRIP_EXAMPLE_SCHEMA, 10, false);
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Write 1 (Bulk insert)
    String newCommitTime = "0000001";
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
    WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
    List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
    assertTrue(client.commit(newCommitTime, jsc.parallelize(writeStatuses), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty()));
    assertNoWriteErrors(writeStatuses);
    validateMetadata(client);

    // Write 2 (inserts)
    newCommitTime = "0000002";
    WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
    records = dataGen.generateInserts(newCommitTime, 20);
    writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
    assertTrue(client.commit(newCommitTime, jsc.parallelize(writeStatuses), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty()));
    assertNoWriteErrors(writeStatuses);
    validateMetadata(client);

    // setup clustering config.
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringSortColumns("_row_key").withInlineClustering(true)
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(1).build();

    HoodieWriteConfig newWriteConfig = getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER)
        .withClusteringConfig(clusteringConfig)
        .withRollbackUsingMarkers(false)
        .build();

    // trigger clustering
    SparkRDDWriteClient newClient = getHoodieWriteClient(newWriteConfig);
    String clusteringCommitTime = newClient.scheduleClustering(Option.empty()).get().toString();
    HoodieWriteMetadata<JavaRDD<WriteStatus>> clusterMetadata = newClient.cluster(clusteringCommitTime, true);

    // collect replaceFileIds for validation later.
    Set<HoodieFileGroupId> replacedFileIds = new HashSet<>();
    clusterMetadata.getPartitionToReplaceFileIds().entrySet().forEach(partitionFiles ->
        partitionFiles.getValue().stream().forEach(file ->
            replacedFileIds.add(new HoodieFileGroupId(partitionFiles.getKey(), file))));

    // trigger new write to mimic other writes succeeding before re-attempt.
    newCommitTime = "0000003";
    WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
    records = dataGen.generateInserts(newCommitTime, 20);
    writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
    assertTrue(client.commit(newCommitTime, jsc.parallelize(writeStatuses), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty()));
    assertNoWriteErrors(writeStatuses);
    validateMetadata(client);

    // manually remove clustering completed instant from .hoodie folder and to mimic succeeded clustering in metadata table, but failed in data table.
    FileCreateUtilsLegacy.deleteReplaceCommit(basePath, clusteringCommitTime);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> updatedClusterMetadata = newClient.cluster(clusteringCommitTime, true);

    metaClient.reloadActiveTimeline();
    Set<HoodieFileGroupId> updatedReplacedFileIds = new HashSet<>();
    updatedClusterMetadata.getPartitionToReplaceFileIds().entrySet().forEach(partitionFiles ->
        partitionFiles.getValue().stream().forEach(file ->
            updatedReplacedFileIds.add(new HoodieFileGroupId(partitionFiles.getKey(), file))));
    assertEquals(replacedFileIds, updatedReplacedFileIds);
    validateMetadata(client);
  }

  /**
   * Validates that if an instant is completed in MDT, but crashed before commiting to DT, MDT compaction should not kick in based on the instant time
   * since its not complete in DT yet.
   */
  @Test
  public void testMDTCompactionWithFailedCommits() throws Exception {
    tableType = HoodieTableType.COPY_ON_WRITE;
    init(tableType);
    context = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig initialConfig = getSmallInsertWriteConfigForMDT(2000, TRIP_EXAMPLE_SCHEMA, 10, false);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProperties(initialConfig.getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(4).build()).build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Write 1 (Bulk insert)
    String newCommitTime = client.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
    List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
    client.commit(newCommitTime, jsc.parallelize(writeStatuses), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    assertNoWriteErrors(writeStatuses);
    validateMetadata(client);

    // Write 2 (inserts)
    newCommitTime = client.startCommit();
    records = dataGen.generateInserts(newCommitTime, 20);
    writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
    client.commit(newCommitTime, jsc.parallelize(writeStatuses), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    assertNoWriteErrors(writeStatuses);
    validateMetadata(client);

    // setup clustering config.
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringSortColumns("_row_key").withInlineClustering(true)
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(1).build();

    HoodieWriteConfig newWriteConfig = getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER)
        .withClusteringConfig(clusteringConfig).build();

    // trigger clustering
    SparkRDDWriteClient newClient = getHoodieWriteClient(newWriteConfig);
    String clusteringCommitTime = newClient.scheduleClustering(Option.empty()).get().toString();
    HoodieWriteMetadata<JavaRDD<WriteStatus>> clusterMetadata = newClient.cluster(clusteringCommitTime, true);

    // manually remove clustering completed instant from .hoodie folder and to mimic succeeded clustering in metadata table, but failed in data table.
    FileCreateUtilsLegacy.deleteReplaceCommit(basePath, clusteringCommitTime);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieWriteConfig updatedWriteConfig = HoodieWriteConfig.newBuilder().withProperties(initialConfig.getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(4).build())
        .withRollbackUsingMarkers(false).build();

    client = getHoodieWriteClient(updatedWriteConfig);

    newCommitTime = client.startCommit();
    records = dataGen.generateInserts(newCommitTime, 20);
    writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
    client.commit(newCommitTime, jsc.parallelize(writeStatuses), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    assertNoWriteErrors(writeStatuses);
    validateMetadata(client, Option.of(clusteringCommitTime));
  }

  @Test
  public void testMetadataReadWithNoCompletedCommits() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    List<HoodieRecord> records;
    List<WriteStatus> writeStatuses;

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true), true)) {
      String[] commitTimestamps = {WriteClientTestUtils.createNewInstantTime(), WriteClientTestUtils.createNewInstantTime()};
      records = dataGen.generateInserts(commitTimestamps[0], 5);
      WriteClientTestUtils.startCommitWithTime(client, commitTimestamps[0]);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), commitTimestamps[0]).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(commitTimestamps[0], jsc.parallelize(writeStatuses));

      // make all commits to inflight in metadata table. Still read should go through, just that it may not return any data.
      FileCreateUtilsLegacy.deleteDeltaCommit(basePath + "/.hoodie/metadata/", commitTimestamps[0]);
      FileCreateUtilsLegacy.deleteDeltaCommit(basePath + " /.hoodie/metadata/", SOLO_COMMIT_TIMESTAMP);
      assertEquals(getAllFiles(metadata(client, storage)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet()).size(), 0);
    }
  }


  /**
   * Ensure that the reader only reads completed instants.
   */
  @Test
  public void testReader() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    List<HoodieRecord> records;
    List<WriteStatus> writeStatuses;

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true), true)) {
      String[] commitTimestamps = {WriteClientTestUtils.createNewInstantTime(), WriteClientTestUtils.createNewInstantTime(),
          WriteClientTestUtils.createNewInstantTime(), WriteClientTestUtils.createNewInstantTime()};

      for (int i = 0; i < commitTimestamps.length; ++i) {
        records = dataGen.generateInserts(commitTimestamps[i], 5);
        WriteClientTestUtils.startCommitWithTime(client, commitTimestamps[i]);
        writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), commitTimestamps[i]).collect();
        assertNoWriteErrors(writeStatuses);
        client.commit(commitTimestamps[i], jsc.parallelize(writeStatuses));
      }

      // Ensure we can see files from each commit
      Set<String> timelineTimestamps = getAllFiles(metadata(client, storage)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet());
      assertEquals(timelineTimestamps.size(), commitTimestamps.length);
      for (int i = 0; i < commitTimestamps.length; ++i) {
        assertTrue(timelineTimestamps.contains(commitTimestamps[i]));
      }

      // mark each commit as incomplete and ensure files are not seen
      for (int i = 0; i < commitTimestamps.length; ++i) {
        FileCreateUtilsLegacy.deleteCommit(basePath, commitTimestamps[i]);
        timelineTimestamps = getAllFiles(metadata(client, storage)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet());
        assertEquals(timelineTimestamps.size(), commitTimestamps.length - 1);
        for (int j = 0; j < commitTimestamps.length; ++j) {
          assertTrue(j == i || timelineTimestamps.contains(commitTimestamps[j]));
        }
        FileCreateUtilsLegacy.createCommit(basePath, commitTimestamps[i]);
      }

      // Test multiple incomplete commits
      FileCreateUtilsLegacy.deleteCommit(basePath, commitTimestamps[0]);
      FileCreateUtilsLegacy.deleteCommit(basePath, commitTimestamps[2]);
      timelineTimestamps = getAllFiles(metadata(client, storage)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet());
      assertEquals(timelineTimestamps.size(), commitTimestamps.length - 2);
      for (int j = 0; j < commitTimestamps.length; ++j) {
        assertTrue(j == 0 || j == 2 || timelineTimestamps.contains(commitTimestamps[j]));
      }

      // Test no completed commits
      for (int i = 0; i < commitTimestamps.length; ++i) {
        FileCreateUtilsLegacy.deleteCommit(basePath, commitTimestamps[i]);
      }
      timelineTimestamps = getAllFiles(metadata(client, storage)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet());
      assertEquals(timelineTimestamps.size(), 0);
    }
  }

  /**
   * Instants on Metadata Table should be archived as per config but we always keep at least the number of instants
   * as on the dataset.
   * <p>
   * Metadata Table should be automatically compacted as per config.
   */
  @Disabled
  public void testCleaningArchivingAndCompaction() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE, false);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    final int maxDeltaCommitsBeforeCompaction = 3;
    HoodieWriteConfig config = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsBeforeCompaction).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.NEVER)
            .retainCommits(1).retainFileVersions(1).withAutoClean(true).withAsyncClean(false)
            .build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(4, 5).build())
        .build();

    List<HoodieRecord> records;
    String newCommitTime;
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, config, true)) {
      // Some initial commits so compaction is not triggered.
      // 1 deltacommit will be from bootstrap. So we can perform maxDeltaCommitsBeforeCompaction - 2 more commits before
      // compaction will be attempted.
      for (int i = 0; i < maxDeltaCommitsBeforeCompaction - 2; ++i) {
        newCommitTime = client.startCommit();
        records = dataGen.generateInserts(newCommitTime, 5);
        client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      }

      HoodieTableMetaClient metadataMetaClient = createMetaClient(metadataTableBasePath);
      HoodieTableMetaClient datasetMetaClient = createMetaClient(config.getBasePath());

      // There should not be any compaction yet and we have not performed more than maxDeltaCommitsBeforeCompaction
      // deltacommits (1 will be due to bootstrap)
      HoodieActiveTimeline metadataTimeline = metadataMetaClient.reloadActiveTimeline();
      assertEquals(metadataTimeline.getCommitAndReplaceTimeline().filterCompletedInstants().countInstants(), 0);
      assertEquals(metadataTimeline.getCommitsTimeline().filterCompletedInstants().countInstants(), maxDeltaCommitsBeforeCompaction - 1);
      assertEquals(datasetMetaClient.getArchivedTimeline().reload().countInstants(), 0);

      // Next commit will initiate a compaction
      newCommitTime = client.startCommit();
      records = dataGen.generateInserts(newCommitTime, 5);
      client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      metadataTimeline = metadataMetaClient.reloadActiveTimeline();
      assertEquals(metadataTimeline.getCommitAndReplaceTimeline().filterCompletedInstants().countInstants(), 1);
      assertEquals(metadataTimeline.getCommitsTimeline().filterCompletedInstants().countInstants(), maxDeltaCommitsBeforeCompaction + 1);
      assertEquals(datasetMetaClient.getArchivedTimeline().reload().countInstants(), 0);

      // More than maxDeltaCommitsBeforeCompaction commits
      String inflightCommitTime = newCommitTime;
      for (int i = 0; i < maxDeltaCommitsBeforeCompaction + 1; ++i) {
        newCommitTime = client.startCommit();
        records = dataGen.generateInserts(newCommitTime, 5);
        client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
        if (i == 0) {
          // Mark this commit inflight so compactions don't take place
          FileCreateUtilsLegacy.deleteCommit(basePath, newCommitTime);
          FileCreateUtilsLegacy.createInflightCommit(basePath, newCommitTime);
          inflightCommitTime = newCommitTime;
        }
      }

      // Ensure no more compactions took place due to the leftover inflight commit
      metadataTimeline = metadataMetaClient.reloadActiveTimeline();
      assertEquals(metadataTimeline.getCommitAndReplaceTimeline().filterCompletedInstants().countInstants(), 1);
      assertEquals(metadataTimeline.getDeltaCommitTimeline().filterCompletedInstants().countInstants(),
          ((2 * maxDeltaCommitsBeforeCompaction) + (maxDeltaCommitsBeforeCompaction /* clean from dataset */) + 1)/* clean in metadata table */);

      // Complete commit
      FileCreateUtilsLegacy.createCommit(basePath, inflightCommitTime);

      // Next commit should lead to compaction
      newCommitTime = client.startCommit();
      records = dataGen.generateInserts(newCommitTime, 5);
      client.insert(jsc.parallelize(records, 1), newCommitTime).collect();

      // Ensure compactions took place
      metadataTimeline = metadataMetaClient.reloadActiveTimeline();
      assertEquals(metadataTimeline.getCommitAndReplaceTimeline().filterCompletedInstants().countInstants(), 2);
      assertEquals(metadataTimeline.getDeltaCommitTimeline().filterCompletedInstants().countInstants(),
          ((2 * maxDeltaCommitsBeforeCompaction) + (maxDeltaCommitsBeforeCompaction + 1 /* clean from dataset */) + 2 /* clean in metadata table */));
      assertTrue(datasetMetaClient.getArchivedTimeline().reload().countInstants() > 0);

      validateMetadata(client);
    }
  }

  @Disabled("HUDI-9700")
  @Test
  public void testUpgradeDowngrade() throws IOException {
    init(HoodieTableType.COPY_ON_WRITE, false);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Perform a commit. This should bootstrap the metadata table with latest version.
    List<HoodieRecord> records;
    List<WriteStatus> writeStatuses;
    // Set Writer Version 6.
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    tableConfig.setTableVersion(HoodieTableVersion.SIX);
    initMetaClient(COPY_ON_WRITE, tableConfig.getProps());
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false).withWriteTableVersion(6).build();
    String commitTimestamp;
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      commitTimestamp = client.startCommit();
      records = dataGen.generateInserts(commitTimestamp, 5);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), commitTimestamp).collect();
      assertNoWriteErrors(writeStatuses);
    }

    // Metadata table should have been bootstrapped
    assertTrue(storage.exists(new StoragePath(metadataTableBasePath)),
        "Metadata table should exist");
    StoragePathInfo oldInfo = storage.getPathInfo(new StoragePath(metadataTableBasePath));

    // set hoodie.table.version to 2 in hoodie.properties file
    changeTableVersion(HoodieTableVersion.TWO);
    // Set Writer Version 6.
    HoodieTableConfig tableConfig2 = metaClient.getTableConfig();
    tableConfig2.setTableVersion(HoodieTableVersion.SIX);
    initMetaClient(COPY_ON_WRITE, tableConfig2.getProps());
    HoodieWriteConfig writeConfig2 = getWriteConfigBuilder(true, true, false)
        .withWriteTableVersion(HoodieTableVersion.SIX.versionCode()).build();

    // With next commit the table should be deleted (as part of upgrade) and then re-bootstrapped automatically
    metaClient.reloadActiveTimeline();
    StoragePathInfo prevInfo = storage.getPathInfo(new StoragePath(metadataTableBasePath));
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig2, true)) {
      commitTimestamp = client.startCommit();
      records = dataGen.generateInserts(commitTimestamp, 5);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), commitTimestamp).collect();
      assertNoWriteErrors(writeStatuses);
    }
    assertTrue(storage.exists(new StoragePath(metadataTableBasePath)),
        "Metadata table should exist");
    StoragePathInfo currentInfo =
        storage.getPathInfo(new StoragePath(metadataTableBasePath));
    assertTrue(currentInfo.getModificationTime() == prevInfo.getModificationTime());

    initMetaClient();
    assertEquals(metaClient.getTableConfig().getTableVersion().versionCode(),
        HoodieTableVersion.current().versionCode());
    assertTrue(storage.exists(new StoragePath(metadataTableBasePath)),
        "Metadata table should exist");
    StoragePathInfo newInfo = storage.getPathInfo(new StoragePath(metadataTableBasePath));
    assertTrue(oldInfo.getModificationTime() == newInfo.getModificationTime());

    // Test downgrade by running the downgrader
    new UpgradeDowngrade(metaClient, writeConfig, context,
        SparkUpgradeDowngradeHelper.getInstance()).run(HoodieTableVersion.TWO, null);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(HoodieTableVersion.TWO.versionCode(),
        metaClient.getTableConfig().getTableVersion().versionCode());
    assertFalse(storage.exists(new StoragePath(metadataTableBasePath)),
        "Metadata table should not exist");
  }

  /**
   * When table needs to be upgraded and when multi writer is enabled, hudi rolls back partial commits. Upgrade itself is happening
   * within a lock and hence rollback should not lock again.
   */
  @Disabled("HUDI-9700")
  @Test
  public void testRollbackDuringUpgradeForDoubleLocking() throws IOException {
    init(HoodieTableType.COPY_ON_WRITE, false);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Perform a commit. This should bootstrap the metadata table with latest version.
    List<HoodieRecord> records;
    JavaRDD<WriteStatus> writeStatuses;
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "3");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(false, true, false)
        .withWriteTableVersion(6)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .withProperties(properties)
        .build();
    String commitTimestamp;
    // Set Writer Version 6.
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    tableConfig.setTableVersion(HoodieTableVersion.SIX);
    initMetaClient(COPY_ON_WRITE, tableConfig.getProps());
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      commitTimestamp = client.startCommit();
      records = dataGen.generateInserts(commitTimestamp, 5);
      writeStatuses = client.insert(jsc.parallelize(records, 1), commitTimestamp);
      client.commit(commitTimestamp, writeStatuses);
    }

    // Metadata table should have been bootstrapped
    assertTrue(storage.exists(new StoragePath(metadataTableBasePath)),
        "Metadata table should exist");
    StoragePathInfo oldInfo = storage.getPathInfo(new StoragePath(metadataTableBasePath));

    // trigger partial commit
    metaClient.reloadActiveTimeline();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      commitTimestamp = client.startCommit();
      records = dataGen.generateInserts(commitTimestamp, 5);
      writeStatuses = client.insert(jsc.parallelize(records, 1), commitTimestamp);
    }

    // set hoodie.table.version to 2 in hoodie.properties file
    changeTableVersion(HoodieTableVersion.TWO);
    // Set Writer Version 6.
    HoodieTableConfig tableConfig2 = metaClient.getTableConfig();
    tableConfig2.setTableVersion(HoodieTableVersion.SIX);
    initMetaClient(COPY_ON_WRITE, tableConfig2.getProps());
    writeConfig = getWriteConfigBuilder(true, true, false).withRollbackUsingMarkers(false).withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .withProperties(properties)
        .withEmbeddedTimelineServerEnabled(false)
        .withWriteTableVersion(HoodieTableVersion.SIX.versionCode())
        .build();

    // With next commit the table should be re-bootstrapped and partial commit should be rolled back.
    metaClient.reloadActiveTimeline();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      commitTimestamp = client.startCommit();
      records = dataGen.generateInserts(commitTimestamp, 5);
      writeStatuses = client.insert(jsc.parallelize(records, 1), commitTimestamp);
      assertNoWriteErrors(writeStatuses.collect());
    }

    initMetaClient();
    assertEquals(metaClient.getTableConfig().getTableVersion().versionCode(),
        HoodieTableVersion.current().versionCode());
    assertTrue(storage.exists(new StoragePath(metadataTableBasePath)),
        "Metadata table should exist");
    StoragePathInfo newInfo = storage.getPathInfo(new StoragePath(metadataTableBasePath));
    assertTrue(oldInfo.getModificationTime() == newInfo.getModificationTime());
  }

  /**
   * Tests rollback of a commit which has new partitions which is not present in hudi table prior to the commit being rolled back.
   */
  @Test
  public void testRollbackOfPartiallyFailedCommitWithNewPartitions() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext,
        getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true, true, false, false, false).build(),
        Option.empty(), true)) {
      String newCommitTime = client.startCommit();
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
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);

      newCommitTime = client.startCommit();
      records = dataGen.generateInserts(newCommitTime, 20);
      writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);

      // There is no way to simulate failed commit on the main dataset, hence we simply delete the completed
      // instant so that only the inflight is left over.
      String commitInstantFileName =
          INSTANT_FILE_NAME_GENERATOR.getFileName(metaClient.getActiveTimeline().getReverseOrderedInstants().findFirst().get());
      assertTrue(storage.deleteFile(new StoragePath(
          basePath + StoragePath.SEPARATOR + METAFOLDER_NAME + StoragePath.SEPARATOR + TIMELINEFOLDER_NAME, commitInstantFileName)));
    }

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext,
        getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true, true, false, false, false).build(),
        Option.empty(), true)) {
      String newCommitTime = client.startCommit();
      // Next insert
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);
    }
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
    String commitTime = client.startCommit();
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 100);
    List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), commitTime).collect();
    assertNoWriteErrors(writeStatuses);

    // create pending commits scenario by disabling auto commit
    HoodieWriteConfig autoCommitDisabled = getWriteConfigBuilder(false, true, false)
        .withProps(props).build();
    // initialize second client, that will stop short of producing the commit.
    SparkRDDWriteClient client2 = getHoodieWriteClient(autoCommitDisabled);
    // Insert second batch
    commitTime = client2.startCommit();
    records = dataGen.generateInserts(commitTime, 100);
    writeStatuses = client2.insert(jsc.parallelize(records, 1), commitTime).collect();
    assertNoWriteErrors(writeStatuses);

    // delete the metadata table partitions to check, whether rollback of pending commit succeeds and
    // metadata table partitions are rebootstrapped.
    client.dropIndex(Arrays.asList(RECORD_INDEX.getPartitionPath(), FILES.getPartitionPath()));
    assertFalse(storage.exists(new StoragePath(
        getMetadataTableBasePath(basePath) + StoragePath.SEPARATOR + FILES.getPartitionPath())));
    assertFalse(storage.exists(new StoragePath(getMetadataTableBasePath(basePath)
        + StoragePath.SEPARATOR + RECORD_INDEX.getPartitionPath())));

    metaClient = HoodieTableMetaClient.reload(metaClient);
    // Insert/upsert third batch of records
    client = getHoodieWriteClient(cfg);
    commitTime = WriteClientTestUtils.createNewInstantTime();
    if (performUpsert) {
      records = dataGen.generateUpdates(commitTime, 100);
      records.addAll(dataGen.generateInserts(commitTime, 20));
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), commitTime).collect();
    } else {
      records = dataGen.generateInserts(commitTime, 100);
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      writeStatuses = client.insert(jsc.parallelize(records, 1), commitTime).collect();
    }
    assertNoWriteErrors(writeStatuses);
    assertTrue(storage.exists(new StoragePath(basePath + StoragePath.SEPARATOR + METAFOLDER_NAME)));
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertFalse(metaClient.getActiveTimeline().filterCompletedInstants().filterCompletedInstants()
        .findInstantsAfterOrEquals(commitTime, 1).empty());

    assertTrue(storage.exists(new StoragePath(
        getMetadataTableBasePath(basePath) + StoragePath.SEPARATOR + FILES.getPartitionPath())));
    assertTrue(storage.exists(new StoragePath(getMetadataTableBasePath(basePath)
        + StoragePath.SEPARATOR + RECORD_INDEX.getPartitionPath())));
  }

  /**
   * Hoodie.properties indicates metadata is enabled, however metadata folder is missing.
   */
  @Test
  public void testBootstrapWithTableNotFound() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // create initial commit
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false).build();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      // Write
      String newCommitTime = client.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 1);
      List<WriteStatus> writeStatuses =
          client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);
    }

    final StoragePath metadataTablePath = new StoragePath(
        getMetadataTableBasePath(writeConfig.getBasePath()));
    assertTrue(storage.exists(metadataTablePath), "metadata table should exist.");

    deleteMetadataTable(metaClient, context, false);
    assertFalse(storage.exists(metadataTablePath),
        "metadata table should not exist after being deleted.");

    writeConfig = getWriteConfigBuilder(true, true, false).build();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      // Write
      String newCommitTime = client.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 1);
      List<WriteStatus> writeStatuses =
          client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
    }

    // Metadata table is recreated, during bootstrapping of metadata table.
    assertTrue(storage.exists(metadataTablePath));
  }

  /**
   * Test bootstrap when the dataset is empty
   */
  @Test
  public void testbootstrapWithEmptyCommit() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);

    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false).build();
    initWriteConfigAndMetatableWriter(writeConfig, true);
    testTable.doWriteOperation(WriteClientTestUtils.createNewInstantTime(), INSERT, Collections.EMPTY_LIST, 0);
    syncTableMetadata(writeConfig);
    validateMetadata(testTable);
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
      String newCommitTime = client.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 10);
      List<HoodieRecord> upsertRecords = new ArrayList<>();
      for (HoodieRecord entry : records) {
        if (entry.getPartitionPath().equals(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
            || entry.getPartitionPath().equals(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)) {
          upsertRecords.add(entry);
        }
      }
      JavaRDD<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(upsertRecords, 1), newCommitTime);
      assertTrue(client.commit(newCommitTime, writeStatuses));
      validateMetadata(client);

      // delete partitions
      newCommitTime = client.startDeletePartitionCommit(metaClient);
      HoodieWriteResult writeResult = client.deletePartitions(singletonList(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH), newCommitTime);
      client.commit(newCommitTime, writeResult.getWriteStatuses(), Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION, writeResult.getPartitionToReplaceFileIds(),
          Option.empty());

      // add 1 more commit
      newCommitTime = client.startCommit();
      records = dataGen.generateInserts(newCommitTime, 10);
      upsertRecords = new ArrayList<>();
      for (HoodieRecord entry : records) {
        if (entry.getPartitionPath().equals(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)) {
          upsertRecords.add(entry);
        }
      }
      writeStatuses = client.upsert(jsc.parallelize(upsertRecords, 1), newCommitTime);
      assertTrue(client.commit(newCommitTime, writeStatuses));

      // assert entry is not present for deleted partition in metadata table
      HoodieTableMetadata tableMetadata = metadata(client, storage);
      // Create a simple RawKey implementation for the partition path
      RawKey partitionKey = new RawKey() {
        @Override
        public String encode() {
          return HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
        }
      };
      assertTrue(tableMetadata.getRecordsByKeyPrefixes(
          HoodieListData.lazy(Collections.singletonList(partitionKey)),
          FILES.getPartitionPath(), false).isEmpty());
      assertTrue(tableMetadata.getAllPartitionPaths().contains(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH));
      assertFalse(tableMetadata.getAllPartitionPaths().contains(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH));
      // above upsert would have triggered clean
      validateMetadata(client);
      assertEquals(1, metadata(client, storage).getAllPartitionPaths().size());
    }
  }

  /**
   * Test various error scenarios.
   */
  @Test
  public void testErrorCases() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // TESTCASE: If commit on the metadata table succeeds but fails on the dataset, then on next init the metadata table
    // should be rolled back to last valid commit.
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext,
        getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true, true, false, false, false).build(),
        Option.empty(), true)) {
      String newCommitTime = client.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);

      newCommitTime = client.startCommit();
      records = dataGen.generateInserts(newCommitTime, 5);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);

      // There is no way to simulate failed commit on the main dataset, hence we simply delete the completed
      // instant so that only the inflight is left over.
      String commitInstantFileName =
          INSTANT_FILE_NAME_GENERATOR.getFileName(metaClient.getActiveTimeline().getReverseOrderedInstants().findFirst().get());
      assertTrue(storage.deleteFile(new StoragePath(
          basePath + StoragePath.SEPARATOR + METAFOLDER_NAME + StoragePath.SEPARATOR + TIMELINEFOLDER_NAME, commitInstantFileName)));
    }

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext,
        getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true, true, false, false, false).build(),
        Option.empty(), true)) {
      String newCommitTime = client.startCommit();
      // Next insert
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 5);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));

      // Post rollback commit and metadata should be valid
      validateMetadata(client);
    }
  }

  @Test
  public void testMetadataTableWithLongLog() throws Exception {
    init(COPY_ON_WRITE, false);
    final int maxNumDeltaCommits = 3;
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .enableMetrics(false)
            .withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltaCommits + 100)
            .withMaxNumDeltacommitsWhenPending(maxNumDeltaCommits)
            .build()).build();
    initWriteConfigAndMetatableWriter(writeConfig, true);
    testTable.addRequestedCommit(String.format("%016d", 0));
    for (int i = 1; i <= maxNumDeltaCommits; i++) {
      doWriteOperation(testTable, String.format("%016d", i));
    }
    int instant = maxNumDeltaCommits + 1;
    assertDoesNotThrow(() -> doWriteOperation(testTable, String.format("%016d", instant)));
  }

  @Test
  public void testMORCheckNumDeltaCommits() throws Exception {
    init(MERGE_ON_READ, true);
    final int maxNumDeltaCommits = 3;
    writeConfig = getWriteConfigBuilder(true, true, false)
            .withMetadataConfig(HoodieMetadataConfig.newBuilder()
                    .enable(true)
                    .enableMetrics(false)
                    .withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltaCommits - 1)
                    .withMaxNumDeltacommitsWhenPending(maxNumDeltaCommits)
                    .build())
            .build();
    initWriteConfigAndMetatableWriter(writeConfig, true);
    // write deltacommits to data-table and do compaction in metadata-table (with commit-instant)
    doWriteOperation(testTable, InProcessTimeGenerator.createNewInstantTime(1));
    doWriteOperation(testTable, InProcessTimeGenerator.createNewInstantTime(1));
    // ensure the compaction is triggered and executed
    try (HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(
        context, storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath(), true)) {
      HoodieTableMetaClient metadataMetaClient = metadata.getMetadataMetaClient();
      final HoodieActiveTimeline activeTimeline = metadataMetaClient.reloadActiveTimeline();
      Option<HoodieInstant> lastCompaction = activeTimeline.filterCompletedInstants()
              .filter(s -> s.getAction().equals(COMMIT_ACTION)).lastInstant();
      assertTrue(lastCompaction.isPresent());
      // create pending instant in data table
      testTable.addRequestedCommit(InProcessTimeGenerator.createNewInstantTime(1));
      // continue writing
      for (int i = 0; i <= maxNumDeltaCommits; i++) {
        doWriteOperation(testTable, InProcessTimeGenerator.createNewInstantTime(1));
      }
      assertDoesNotThrow(() -> doWriteOperation(testTable, InProcessTimeGenerator.createNewInstantTime(1)));
    }
  }

  @Test
  public void testNonPartitioned() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE, false);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    HoodieTestDataGenerator nonPartitionedGenerator = new HoodieTestDataGenerator(new String[] {""});
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true), true)) {
      // Write 1 (Bulk insert)
      String newCommitTime = "0000001";
      List<HoodieRecord> records = nonPartitionedGenerator.generateInserts(newCommitTime, 10);
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);

      List<String> metadataPartitions = metadata(client, storage).getAllPartitionPaths();
      assertTrue(metadataPartitions.contains(""), "Must contain empty partition");
    }
  }

  @Test
  public void testNonPartitionedColStats() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE, false);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    HoodieTestDataGenerator nonPartitionedGenerator = new HoodieTestDataGenerator(new String[] {""});
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexColumnStats(true).build()).build();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      // Write 1 (Bulk insert)
      String newCommitTime = "0000001";
      List<HoodieRecord> records = nonPartitionedGenerator.generateInserts(newCommitTime, 10);
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);

      List<String> metadataPartitions = metadata(client, storage).getAllPartitionPaths();
      assertTrue(metadataPartitions.contains(""), "Must contain empty partition");
    }
  }

  @Test
  public void testColStatsMultipleColumns() throws Exception {
    initPath();
    Properties properties = new TypedProperties();
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .withMetadataIndexColumnStats(true)
            .withColumnStatsIndexForColumns("begin_lat,end_lat,distance_in_meters,weight")
            .withProperties(properties)
            .build()).build();
    init(HoodieTableType.COPY_ON_WRITE, writeConfig);
    initMetaClient(writeConfig.getProps());
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    HoodieTestDataGenerator partitionedGenerator = new HoodieTestDataGenerator();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      // Write 1 (Bulk insert)
      String newCommitTime = "0000001";
      List<HoodieRecord> records = partitionedGenerator.generateInserts(newCommitTime, 10);
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();

      HoodieTableMetadata tableMetadata = metadata(client, metaClient.getStorage());
      List<String> metadataPartitions = tableMetadata.getAllPartitionPaths();
      List<Pair<String, String>> partitionNameFileNameList = new ArrayList<>();
      for (String metadataPartition : metadataPartitions) {
        partitionNameFileNameList.addAll(
            tableMetadata.getAllFilesInPartitions(singletonList(String.format("%s/%s",basePath, metadataPartition))).entrySet().stream()
            .flatMap(entry -> entry.getValue().stream().map(storagePathInfo -> Pair.of(metadataPartition, storagePathInfo.getPath().getName())))
            .collect(Collectors.toList())
        );
      }
      List<String> columns = Arrays.asList("begin_lat", "end_lat", "distance_in_meters", "weight", "seconds_since_epoch", "nation");
      Map<Pair<String, String>, List<HoodieMetadataColumnStats>> colStatsFromMetadata = tableMetadata.getColumnStats(partitionNameFileNameList, columns);
      assertEquals(partitionNameFileNameList.size(), colStatsFromMetadata.size());
      // Assert stats from parquet footer same as metadata.
      colStatsFromMetadata.forEach(((partitionAndFileName, stats) -> {
        StoragePath fullFilePath = new StoragePath(basePath, String.format("%s/%s", partitionAndFileName.getLeft(), partitionAndFileName.getRight()));
        Map<String, HoodieColumnRangeMetadata<Comparable>> parquetStatsMap =
            HoodieIOFactory.getIOFactory(metaClient.getStorage())
                .getFileFormatUtils(HoodieFileFormat.PARQUET)
                .readColumnStatsFromMetadata(metaClient.getStorage(), fullFilePath, columns, V1)
                .stream()
                .collect(Collectors.toMap(HoodieColumnRangeMetadata::getColumnName, Function.identity()));
        Map<String, HoodieMetadataColumnStats> columnStatsMap = stats.stream().collect(Collectors.toMap(HoodieMetadataColumnStats::getColumnName, Function.identity()));
        List<String> columnsWithoutStats = Arrays.asList("seconds_since_epoch", "nation");
        Assertions.assertEquals(parquetStatsMap.size() - columnsWithoutStats.size(), columnStatsMap.size());
        for (String column : columns) {
          if (columnsWithoutStats.contains(column)) {
            // Assert columnsWithoutStats are not present in MDT result.
            Assertions.assertFalse(columnStatsMap.containsKey(column));
            continue;
          }
          // Assert getColumnStats returns same data.
          Assertions.assertEquals(columnStatsMap.get(column), tableMetadata.getColumnStats(Collections.singletonList(partitionAndFileName), column).get(partitionAndFileName));
          Assertions.assertEquals(parquetStatsMap.get(column).getNullCount(), columnStatsMap.get(column).getNullCount());
          Assertions.assertEquals(parquetStatsMap.get(column).getValueCount(),columnStatsMap.get(column).getValueCount());
          Assertions.assertEquals(parquetStatsMap.get(column).getMaxValue(), unwrapAvroValueWrapper(columnStatsMap.get(column).getMaxValue()));
          Assertions.assertEquals(parquetStatsMap.get(column).getMinValue(), unwrapAvroValueWrapper(columnStatsMap.get(column).getMinValue()));
        }
      }));
    }
  }

  /**
   * Test various metrics published by metadata table.
   */
  @Test
  public void testMetadataMetrics() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE, false);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfigBuilder(true, true, true).build(), true)) {
      // Write
      String newCommitTime = client.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(newCommitTime, jsc.parallelize(writeStatuses));
      validateMetadata(client);

      Metrics metrics = Metrics.getInstance(writeConfig.getMetricsConfig(), storage);
      assertTrue(metrics.getRegistry().getGauges().containsKey(HoodieMetadataMetrics.INITIALIZE_STR + ".count"));
      assertTrue(metrics.getRegistry().getGauges().containsKey(HoodieMetadataMetrics.INITIALIZE_STR + ".totalDuration"));
      assertTrue((Long) metrics.getRegistry().getGauges().get(HoodieMetadataMetrics.INITIALIZE_STR + ".count").getValue() >= 1L);
      final String prefix = FILES.getPartitionPath() + ".";
      assertTrue(metrics.getRegistry().getGauges().containsKey(prefix + HoodieMetadataMetrics.STAT_COUNT_BASE_FILES));
      assertTrue(metrics.getRegistry().getGauges().containsKey(prefix + HoodieMetadataMetrics.STAT_COUNT_LOG_FILES));
      assertTrue(metrics.getRegistry().getGauges().containsKey(prefix + HoodieMetadataMetrics.STAT_TOTAL_BASE_FILE_SIZE));
      assertTrue(metrics.getRegistry().getGauges().containsKey(prefix + HoodieMetadataMetrics.STAT_TOTAL_LOG_FILE_SIZE));
    }
  }

  @Test
  public void testGetFileGroupIndexFromFileId() {
    int index = new Random().nextInt(10000);
    String fileId = getFileIDForFileGroup(FILES, index, FILES.getPartitionPath(), Option.empty());
    assertEquals(fileId.substring(0, fileId.length() - 2), HoodieTableMetadataUtil.getFileGroupPrefix(fileId));
    assertEquals(index, HoodieTableMetadataUtil.getFileGroupIndexFromFileId(fileId));

    assertEquals(HoodieTableMetadataUtil.getFileGroupPrefix("some-file-id-0"), "some-file-id");
    assertEquals(HoodieTableMetadataUtil.getFileGroupPrefix("some-file-id"), "some-file-id");
    assertEquals(HoodieTableMetadataUtil.getFileGroupPrefix("some-file-id-2"), "some-file-id-2");
  }

  @Test
  public void testGetFileGroupIndexFromFileIdPartitionedRLI() {
    assertEquals("record-index-basicName-0005-0", getFileIDForFileGroup(RECORD_INDEX, 5, RECORD_INDEX.getPartitionPath(), Option.of("basicName")));
    assertEquals("basicName", getDataTablePartitionNameFromFileGroupName(getFileIDForFileGroup(RECORD_INDEX, 5, RECORD_INDEX.getPartitionPath(), Option.of("basicName"))));
    assertEquals("record-index-with%5Funderscore-0005-0", getFileIDForFileGroup(RECORD_INDEX, 5, RECORD_INDEX.getPartitionPath(), Option.of("with_underscore")));
    assertEquals("with_underscore", getDataTablePartitionNameFromFileGroupName(getFileIDForFileGroup(RECORD_INDEX, 5, RECORD_INDEX.getPartitionPath(), Option.of("with_underscore"))));
    assertEquals("record-index-with%2Ddash-0005-0", getFileIDForFileGroup(RECORD_INDEX, 5, RECORD_INDEX.getPartitionPath(), Option.of("with-dash")));
    assertEquals("with-dash", getDataTablePartitionNameFromFileGroupName(getFileIDForFileGroup(RECORD_INDEX, 5, RECORD_INDEX.getPartitionPath(), Option.of("with-dash"))));
    assertEquals("record-index-with%25percent-0005-0", getFileIDForFileGroup(RECORD_INDEX, 5, RECORD_INDEX.getPartitionPath(), Option.of("with%percent")));
    assertEquals("with%percent", getDataTablePartitionNameFromFileGroupName(getFileIDForFileGroup(RECORD_INDEX, 5, RECORD_INDEX.getPartitionPath(), Option.of("with%percent"))));
    assertEquals("record-index-2016%2F08%2F04-0005-0", getFileIDForFileGroup(RECORD_INDEX, 5, RECORD_INDEX.getPartitionPath(), Option.of("2016/08/04")));
    assertEquals("2016/08/04", getDataTablePartitionNameFromFileGroupName(getFileIDForFileGroup(RECORD_INDEX, 5, RECORD_INDEX.getPartitionPath(), Option.of("2016/08/04"))));

  }

  @Test
  public void testDuplicatesDuringRecordIndexBootstrap() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE, true);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    List<String> commitTimestamps = new ArrayList<>();
    HoodieWriteConfig customConfig = getWriteConfigBuilder(true, true, false)
        .build();

    List<HoodieRecord> recordsFirstBatch = new ArrayList<>();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, customConfig, true)) {
      String firstCommitTime = client.startCommit();
      // Create a commit, with record index disabled
      List<HoodieRecord> insertRecords = dataGen.generateInserts(firstCommitTime, 100);
      recordsFirstBatch.addAll(insertRecords);
      // To test duplicates during bootstrap, insert duplicates in the first batch.
      recordsFirstBatch.addAll(insertRecords);
      List<WriteStatus> writeStatuses =
          client.insert(jsc.parallelize(recordsFirstBatch, 1), firstCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.commit(firstCommitTime, jsc.parallelize(writeStatuses));
      commitTimestamps.add(firstCommitTime);
    }
    assertEquals(false,
        storage.exists(new StoragePath(metaClient.getMetaPath(), "metadata/record_index")));

    // bootstrap record index
    customConfig = getWriteConfigBuilder(false, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .enableMetrics(false)
            .ignoreSpuriousDeletes(false)
            .withEnableRecordIndex(true)
            .build())
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, customConfig, true)) {
      // Create a commit, with record index enabled
      String secondCommitTime = client.startCommit();
      List<HoodieRecord> recordsSecondBatch = dataGen.generateInserts(secondCommitTime, 100);
      assertThrows(HoodieException.class, () -> client.insert(jsc.parallelize(recordsSecondBatch, 1), secondCommitTime).collect());
    }
  }

  /**
   * Test duplicate operation with same instant timestamp.
   * <p>
   * This can happen if the commit on the MDT succeeds but fails on the dataset. For some table services like clean,
   * compaction, replace commit, the operation will be retried with the same timestamp (taken from inflight). Hence,
   * metadata table will see an additional commit with the same timestamp as a previously completed deltacommit.
   */
  @Test
  public void testRepeatedActionWithSameInstantTime() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    Properties props = new Properties();
    props.put(HoodieCleanConfig.ALLOW_MULTIPLE_CLEANS.key(), "false");
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false).withProps(props).build();

    // Perform three writes so we can initiate a clean
    int index = 0;
    final String partition = "2015/03/16";
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      for (; index < 3; ++index) {
        String newCommitTime = client.startCommit();
        List<HoodieRecord> records = index == 0 ? dataGen.generateInsertsForPartition(newCommitTime, 10, partition)
            : dataGen.generateUniqueUpdates(newCommitTime, 5);
        JavaRDD<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime);
        client.commit(newCommitTime, writeStatuses);
      }
    }
    assertEquals(3, metaClient.reloadActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants().countInstants());

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      // Perform a clean
      HoodieCleanMetadata cleanMetadata = client.clean();
      // 1 partition should be cleaned
      assertEquals(1, cleanMetadata.getPartitionMetadata().size());
      // 1 file cleaned
      assertEquals(1, cleanMetadata.getPartitionMetadata().get(partition).getSuccessDeleteFiles().size());
      assertEquals(0, cleanMetadata.getPartitionMetadata().get(partition).getFailedDeleteFiles().size());
      assertEquals(1, cleanMetadata.getPartitionMetadata().get(partition).getDeletePathPatterns().size());

      // To simulate failed clean on the main dataset, we will delete the completed clean instant
      String cleanInstantFileName =
          INSTANT_FILE_NAME_GENERATOR.getFileName(metaClient.reloadActiveTimeline().getCleanerTimeline().filterCompletedInstants()
              .getReverseOrderedInstants().findFirst().get());
      assertTrue(storage.deleteFile(new StoragePath(
          basePath + StoragePath.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME + StoragePath.SEPARATOR + TIMELINEFOLDER_NAME,
          cleanInstantFileName)));
      assertEquals(1, metaClient.reloadActiveTimeline().getCleanerTimeline().filterInflights().countInstants());
      assertEquals(0, metaClient.reloadActiveTimeline().getCleanerTimeline().filterCompletedInstants().countInstants());

      // Initiate another clean. The previous leftover clean will be attempted and no other clean will be scheduled.
      cleanMetadata = client.clean();

      // 1 partition should be cleaned
      assertEquals(1, cleanMetadata.getPartitionMetadata().size());
      // 1 file cleaned but was already deleted so will be a failed delete
      assertEquals(0, cleanMetadata.getPartitionMetadata().get(partition).getSuccessDeleteFiles().size());
      assertEquals(1, cleanMetadata.getPartitionMetadata().get(partition).getFailedDeleteFiles().size());
      assertEquals(1, cleanMetadata.getPartitionMetadata().get(partition).getDeletePathPatterns().size());

      validateMetadata(client);
    }
  }

  private void doPreBootstrapOperations(HoodieTestTable testTable) throws Exception {
    doPreBootstrapOperations(testTable, "0000001", "0000002");
  }

  private void doPreBootstrapOperations(HoodieTestTable testTable, String commit1, String commit2) throws Exception {
    testTable.doWriteOperation(commit1, INSERT, asList("p1", "p2"), asList("p1", "p2"),
        2, true);
    testTable.doWriteOperation(commit2, UPSERT, asList("p1", "p2"),
        2, true);
    validateMetadata(testTable);
  }

  private void doWriteInsertAndUpsertNonPartitioned(HoodieTestTable testTable) throws Exception {
    doWriteInsertAndUpsert(testTable, "0000001", "0000002", true);
  }

  private void doWriteInsertAndUpsert(HoodieTestTable testTable) throws Exception {
    doWriteInsertAndUpsert(testTable, "0000001", "0000002", false);
  }

  @Override
  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr, HoodieIndex.IndexType indexType,
                                                    HoodieFailedWritesCleaningPolicy cleaningPolicy) {
    Properties properties = getDisabledRowWriterProperties();
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
            .withRemoteServerPort(timelineServicePort).build())
        .withProperties(properties);
  }

  @Test
  public void testClusterOperationOnMainTable() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    // Disable small file handling that way multiple files are created for small batches.
    Properties props = new Properties();
    props.setProperty(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "0");
    HoodieWriteConfig cfg = getWriteConfigBuilder(true, true, false)
        .withProps(props).build();
    // Initialize metadata writers.
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    // Insert one batch 0000001
    String commitTime = "0000001";
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 100);
    WriteClientTestUtils.startCommitWithTime(client, commitTime);
    List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), commitTime).collect();
    assertNoWriteErrors(writeStatuses);
    client.commit(commitTime, jsc.parallelize(writeStatuses));

    // Insert second batch 0000002
    commitTime = "0000002";
    records = dataGen.generateInserts(commitTime, 100);
    WriteClientTestUtils.startCommitWithTime(client, commitTime);
    writeStatuses = client.insert(jsc.parallelize(records, 1), commitTime).collect();
    assertNoWriteErrors(writeStatuses);
    client.commit(commitTime, jsc.parallelize(writeStatuses));

    // Schedule clustering operation 0000003
    HoodieWriteConfig clusterWriteCfg = getWriteConfigBuilder(true, true, false)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withInlineClusteringNumCommits(0)
            .build())
        .withProperties(getDisabledRowWriterProperties())
        .build();
    SparkRDDWriteClient clusteringClient = getHoodieWriteClient(clusterWriteCfg);
    WriteClientTestUtils.scheduleTableService(clusteringClient, "0000003", Option.empty(), TableServiceType.CLUSTER);

    // Execute pending clustering operation
    clusteringClient.cluster("0000003", true);

    // verify metadata table
    validateMetadata(client);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTableMetadata tableMetadata = metadata(client, storage);
    HoodieTableFileSystemView metadataFileSystemView = new HoodieTableFileSystemView(tableMetadata,
        metaClient, metaClient.reloadActiveTimeline());
    HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(getEngineContext(), metaClient, metaClient.getActiveTimeline());
    tableMetadata.getAllPartitionPaths().forEach(partition -> {
      List<String> fileNamesFromMetadataFileListing = metadataFileSystemView.getLatestBaseFiles(partition)
          .map(baseFile -> baseFile.getFileName())
          .sorted()
          .collect(Collectors.toList());
      List<String> fileNamesFromBaseFileView = fsView.getLatestBaseFiles(partition)
          .map(baseFile -> baseFile.getFileName())
          .sorted()
          .collect(Collectors.toList());
      assertEquals(fileNamesFromBaseFileView, fileNamesFromMetadataFileListing);
    });
  }

  @Test
  public void testOutOfOrderCommits() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    // Disable small file handling that way multiple files are created for small batches.
    Properties props = new Properties();
    props.setProperty(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "0");
    HoodieWriteConfig cfg = getWriteConfigBuilder(true, true, false)
        .withProps(props).build();
    // Initialize metadata writers.
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    // Insert one batch 0000001
    String commitTime = "0000001";
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 100);
    WriteClientTestUtils.startCommitWithTime(client, commitTime);
    List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), commitTime).collect();
    assertNoWriteErrors(writeStatuses);
    client.commit(commitTime, jsc.parallelize(writeStatuses));

    // Insert second batch 0000002
    commitTime = "0000002";
    records = dataGen.generateInserts(commitTime, 100);
    WriteClientTestUtils.startCommitWithTime(client, commitTime);
    writeStatuses = client.insert(jsc.parallelize(records, 1), commitTime).collect();
    assertNoWriteErrors(writeStatuses);
    client.commit(commitTime, jsc.parallelize(writeStatuses));

    // Schedule clustering operation 0000003
    HoodieWriteConfig clusterWriteCfg = getWriteConfigBuilder(true, true, false)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withInlineClusteringNumCommits(0)
            .build())
        .withProperties(getDisabledRowWriterProperties())
        .build();
    SparkRDDWriteClient clusteringClient = getHoodieWriteClient(clusterWriteCfg);
    WriteClientTestUtils.scheduleTableService(clusteringClient, "0000003", Option.empty(), TableServiceType.CLUSTER);

    // Insert second batch 0000004
    commitTime = "0000004";
    records = dataGen.generateInserts(commitTime, 100);
    client = getHoodieWriteClient(cfg);
    WriteClientTestUtils.startCommitWithTime(client, commitTime);
    writeStatuses = client.insert(jsc.parallelize(records, 1), commitTime).collect();
    assertNoWriteErrors(writeStatuses);
    client.commit(commitTime, jsc.parallelize(writeStatuses));

    // verify metadata table
    validateMetadata(client);

    // Execute compaction on metadata table.
    metadataWriter = SparkHoodieBackedTableMetadataWriter.create(storageConf, client.getConfig(), context);
    Properties metadataProps = ((SparkHoodieBackedTableMetadataWriter) metadataWriter).getWriteConfig().getProps();
    metadataProps.setProperty(INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "3");
    HoodieWriteConfig metadataWriteConfig = HoodieWriteConfig.newBuilder()
        .withProperties(metadataProps).build();
    try (SparkRDDWriteClient metadataWriteClient = new SparkRDDWriteClient(context, metadataWriteConfig, Option.empty(), true)) {
      Option<String> compactionInstantTimeOpt = metadataWriteClient.scheduleCompaction(Option.empty());
      assertTrue(compactionInstantTimeOpt.isPresent());
      final String compactionInstantTime = compactionInstantTimeOpt.get();
      HoodieWriteMetadata result = metadataWriteClient.compact(compactionInstantTime);
      metadataWriteClient.commitCompaction(compactionInstantTime, result, Option.empty());
      // validate that compaction has been complete in metadata table.
      assertTrue(((SparkHoodieBackedTableMetadataWriter) metadataWriter).getTableMetadata().getMetadataMetaClient().reloadActiveTimeline().filterCompletedInstants()
          .containsInstant(compactionInstantTime));

      // verify metadata table
      validateMetadata(client);

      // Execute pending clustering operation
      clusteringClient = getHoodieWriteClient(clusterWriteCfg);
      clusteringClient.cluster("0000003", true);

      // verify metadata table
      validateMetadata(client);
    }
  }

  @Test
  public void testDeleteWithRecordIndex() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE, true);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withEnableRecordIndex(true).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.RECORD_INDEX).build())
        .build();

    String secondCommitTime;
    List<HoodieRecord> allRecords;
    List<String> keysToDelete;
    List<HoodieRecord> recordsToDelete;

    // Initialize the dataset and add some commits.
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      String firstCommitTime = client.startCommit();
      // First commit
      List<HoodieRecord> firstBatchOfrecords = dataGen.generateInserts(firstCommitTime, 10);
      List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(firstBatchOfrecords, 1), firstCommitTime).collect();
      client.commit(firstCommitTime, jsc.parallelize(writeStatuses));

      // Records got inserted and RI is initialized
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(metaClient.getTableConfig().isMetadataPartitionAvailable(RECORD_INDEX), "RI is disabled");
      assertEquals(firstBatchOfrecords.size(),
          HoodieClientTestUtils.readCommit(writeConfig.getBasePath(), engineContext.getSqlContext(), metaClient.reloadActiveTimeline(), firstCommitTime, true, INSTANT_GENERATOR).count());

      // Another batch of records added
      secondCommitTime = client.startCommit();
      List<HoodieRecord> secondBatchOfrecords = dataGen.generateInserts(secondCommitTime, 5);

      writeStatuses = client.bulkInsert(jsc.parallelize(secondBatchOfrecords, 1), secondCommitTime).collect();
      client.commit(secondCommitTime, jsc.parallelize(writeStatuses));

      assertEquals(secondBatchOfrecords.size(),
          HoodieClientTestUtils.readCommit(writeConfig.getBasePath(), engineContext.getSqlContext(), metaClient.reloadActiveTimeline(), secondCommitTime, true, INSTANT_GENERATOR).count());

      allRecords = new ArrayList<>(firstBatchOfrecords);
      allRecords.addAll(secondBatchOfrecords);

      // RI should have created mappings for all the records inserted above
      HoodieTableMetadata metadataReader = metaClient.getTableFormat().getMetadataFactory().create(
          context, storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath());
      Map<String, HoodieRecordGlobalLocation> result = HoodieDataUtils.dedupeAndCollectAsMap(
          metadataReader.readRecordIndexLocationsWithKeys(
              HoodieListData.eager(allRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()))));
      assertEquals(allRecords.size(), result.size(), "RI should have mapping for all the records in firstCommit");

      // Delete some records from each commit. This should also remove the RI mapping.
      recordsToDelete = firstBatchOfrecords.subList(0, 3);
      recordsToDelete.addAll(secondBatchOfrecords.subList(0, 2));
      keysToDelete = recordsToDelete.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList());

      String deleteTime = client.startCommit();
      writeStatuses = client.delete(jsc.parallelize(recordsToDelete, 1).map(HoodieRecord::getKey), deleteTime).collect();
      client.commit(deleteTime, jsc.parallelize(writeStatuses));

      // RI should not return mappings for deleted records
      metadataReader = metaClient.getTableFormat().getMetadataFactory().create(
          context, storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath());
      result = HoodieDataUtils.dedupeAndCollectAsMap(metadataReader.readRecordIndexLocationsWithKeys(
          HoodieListData.eager(allRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()))));
      assertEquals(allRecords.size() - recordsToDelete.size(), result.size(), "RI should not have mapping for deleted records");
      result.keySet().forEach(mappingKey -> assertFalse(keysToDelete.contains(mappingKey), "RI should not have mapping for deleted records"));
    }

    // Compaction should work too by removing the deleted keys from the base files
    // To perform compaction, we need to create a new write client as compaction is attempted before any operations in write client
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig, true)) {
      // An empty delete to trigger compaction
      String deleteTime = client.startCommit();
      List<WriteStatus> writeStatuses = client.delete(jsc.emptyRDD(), deleteTime).collect();
      client.commit(deleteTime, jsc.parallelize(writeStatuses));

      HoodieTableMetadata metadataReader = metaClient.getTableFormat().getMetadataFactory().create(
          context, storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath());
      assertTrue(metadataReader.getLatestCompactionTime().isPresent(), "Compaction should have taken place on MDT");

      // RI should not return mappings for deleted records
      metadataReader = metaClient.getTableFormat().getMetadataFactory().create(context, storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath());
      Map<String, HoodieRecordGlobalLocation> result =
          HoodieDataUtils.dedupeAndCollectAsMap(metadataReader.readRecordIndexLocationsWithKeys(
              HoodieListData.eager(allRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()))));
      assertEquals(allRecords.size() - keysToDelete.size(), result.size(), "RI should not have mapping for deleted records");

      // Adding records with the same keys after delete should work
      String reinsertTime = client.startCommit();
      writeStatuses = client.upsert(jsc.parallelize(recordsToDelete, 1), reinsertTime).collect();
      client.commit(reinsertTime, jsc.parallelize(writeStatuses));

      // New mappings should have been created for re-inserted records and should map to the new commit time
      metadataReader = metaClient.getTableFormat().getMetadataFactory().create(context, storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath());
      result = HoodieDataUtils.dedupeAndCollectAsMap(metadataReader.readRecordIndexLocationsWithKeys(
          HoodieListData.eager(allRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()))));
      assertEquals(allRecords.size(), result.size(), "RI should have mappings for re-inserted records");
      for (String reInsertedKey : keysToDelete) {
        assertEquals(reinsertTime, result.get(reInsertedKey).getInstantTime(), "RI mapping for re-inserted keys should have new commit time");
      }
    }
  }

  private void validateMetadata(SparkRDDWriteClient testClient) throws Exception {
    validateMetadata(testClient, Option.empty());
  }

  private void validateMetadata(SparkRDDWriteClient testClient, Option<String> ignoreFilesWithCommit) throws Exception {
    HoodieWriteConfig config = testClient.getConfig();

    SparkRDDWriteClient client;
    if (config.isEmbeddedTimelineServerEnabled()) {
      testClient.close();
      client = new SparkRDDWriteClient(testClient.getEngineContext(), testClient.getConfig(), true);
    } else {
      client = testClient;
    }
    clientsToClose.add(client);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTableMetadata tableMetadata = metadata(client, storage);
    assertNotNull(tableMetadata, "MetadataReader should have been initialized");
    if (!config.isMetadataTableEnabled()) {
      return;
    }

    HoodieTimer timer = HoodieTimer.start();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    validateMetadata(config, ignoreFilesWithCommit, storage, basePath, metaClient, storageConf, engineContext, tableMetadata, client, timer);

    HoodieBackedTableMetadataWriter<JavaRDD<HoodieRecord>, JavaRDD<WriteStatus>> metadataWriter = metadataWriter(client);
    assertNotNull(metadataWriter, "MetadataWriter should have been initialized");

    // Validate write config for metadata table
    HoodieWriteConfig metadataWriteConfig = metadataWriter.getWriteConfig();
    assertFalse(metadataWriteConfig.isMetadataTableEnabled(), "No metadata table for metadata table");

    // Metadata table should be in sync with the dataset
    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(storageConf).setBasePath(metadataTableBasePath).build();

    // Metadata table is MOR
    assertEquals(metadataMetaClient.getTableType(), HoodieTableType.MERGE_ON_READ, "Metadata Table should be MOR");

    // Metadata table is HFile format
    assertEquals(metadataMetaClient.getTableConfig().getBaseFileFormat(), HoodieFileFormat.HFILE,
        "Metadata Table base file format should be HFile");

    // Metadata table has a fixed number of partitions
    // Cannot use FSUtils.getAllFoldersWithPartitionMetaFile for this as that function filters all directory
    // in the .hoodie folder.
    List<String> metadataTablePartitions = FSUtils.getAllPartitionPaths(engineContext, metadataMetaClient, false);
    // Secondary index is enabled by default but no MDT partition corresponding to it is available
    final boolean isPartitionStatsEnabled;
    if (!metadataWriter.getEnabledPartitionTypes().contains(COLUMN_STATS)) {
      isPartitionStatsEnabled = false;
    } else {
      isPartitionStatsEnabled = true;
    }
    long enabledMDTPartitionsSize = metadataWriter.getEnabledPartitionTypes().stream()
        .filter(partition -> !partition.equals(SECONDARY_INDEX))
        // Filter out partition stats if column stats is disabled since it does not get initialized in such a case
        .filter(partition -> isPartitionStatsEnabled || !partition.equals(PARTITION_STATS))
        .count();
    assertEquals(enabledMDTPartitionsSize, metadataTablePartitions.size());

    final Map<String, MetadataPartitionType> metadataEnabledPartitionTypes = new HashMap<>();
    metadataWriter.getEnabledPartitionTypes().forEach(e -> metadataEnabledPartitionTypes.put(e.getPartitionPath(), e));

    // Metadata table should automatically compact and clean
    // versions are +1 as autoclean / compaction happens end of commits
    int numFileVersions = metadataWriteConfig.getCleanerFileVersionsRetained() + 1;
    FileSystemBackedTableMetadata mdtMetadata = new FileSystemBackedTableMetadata(engineContext, storage, metadataTableBasePath);
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(mdtMetadata, metadataMetaClient, metadataMetaClient.getActiveTimeline());
    metadataTablePartitions.forEach(partition -> {
      List<FileSlice> latestSlices = fsView.getLatestFileSlices(partition).collect(Collectors.toList());
      assertTrue(latestSlices.stream().map(FileSlice::getBaseFile).count() <= latestSlices.size(), "Should have a single latest base file per file group");
      List<HoodieLogFile> logFiles = latestSlices.get(0).getLogFiles().collect(Collectors.toList());
      try {
        if (FILES.getPartitionPath().equals(partition)) {
          HoodieTable table = HoodieSparkTable.create(config, engineContext);
          verifyMetadataRawRecords(table, logFiles, false);
        }
        if (COLUMN_STATS.getPartitionPath().equals(partition)) {
          verifyMetadataColumnStatsRecords(storage, logFiles);
        }
      } catch (IOException e) {
        LOG.error("Metadata record validation failed", e);
        fail("Metadata record validation failed");
      }
    });

    // TODO: include validation for record_index partition here.
  }

  public static void validateMetadata(HoodieWriteConfig config, Option<String> ignoreFilesWithCommit,
                               HoodieStorage storage, String basePath, HoodieTableMetaClient metaClient,
                               StorageConfiguration storageConf, HoodieSparkEngineContext engineContext, HoodieTableMetadata tableMetadata,
                               SparkRDDWriteClient client, HoodieTimer timer) throws Exception {

    // Partitions should match
    FileSystemBackedTableMetadata fsBackedTableMetadata = new FileSystemBackedTableMetadata(
        engineContext, metaClient.getTableConfig(), metaClient.getStorage(), config.getBasePath());
    List<String> fsPartitions = fsBackedTableMetadata.getAllPartitionPaths();
    List<String> metadataPartitions = tableMetadata.getAllPartitionPaths();

    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    assertEquals(fsPartitions.size(), metadataPartitions.size(), "Partitions should match");
    assertTrue(fsPartitions.equals(metadataPartitions), "Partitions should match");

    // Files within each partition should match
    HoodieTable table = HoodieSparkTable.create(config, engineContext);
    TableFileSystemView tableView = table.getHoodieView();
    List<String> fullPartitionPaths =
        fsPartitions.stream().map(partition -> basePath + "/" + partition)
            .collect(Collectors.toList());
    Map<String, List<StoragePathInfo>> partitionToFilesMap =
        tableMetadata.getAllFilesInPartitions(fullPartitionPaths);
    assertEquals(fsPartitions.size(), partitionToFilesMap.size());

    fsPartitions.forEach(partition -> {
      try {
        StoragePath partitionPath;
        if (partition.equals("")) {
          // Should be the non-partitioned case
          partitionPath = new StoragePath(basePath);
        } else {
          partitionPath = new StoragePath(basePath, partition);
        }

        List<StoragePathInfo> pathInfoList =
            FSUtils.getAllDataFilesInPartition(storage, partitionPath);
        if (ignoreFilesWithCommit.isPresent()) {
          pathInfoList = pathInfoList.stream()
              .filter(pathInfo ->
                  !pathInfo.getPath().getName().contains(ignoreFilesWithCommit.get()))
              .collect(Collectors.toList());
        }
        List<StoragePathInfo> metaFilesList = tableMetadata.getAllFilesInPartition(partitionPath);
        List<String> fsFileNames = pathInfoList.stream()
            .map(s -> s.getPath().getName()).collect(Collectors.toList());
        List<String> metadataFilenames = metaFilesList.stream()
            .map(s -> s.getPath().getName()).collect(Collectors.toList());
        Collections.sort(fsFileNames);
        Collections.sort(metadataFilenames);

        assertEquals(pathInfoList.size(), partitionToFilesMap.get(partitionPath.toString()).size(),
            "Files within partition " + partition + " should match");

        // File sizes should be valid
        metaFilesList.stream().forEach(s -> assertTrue(s.getLength() > 0));

        if ((fsFileNames.size() != metadataFilenames.size())
            || (!fsFileNames.equals(metadataFilenames))) {
          LOG.info("*** File system listing = " + Arrays.toString(fsFileNames.toArray()));
          LOG.info("*** Metadata listing = " + Arrays.toString(metadataFilenames.toArray()));

          for (String fileName : fsFileNames) {
            if (!metadataFilenames.contains(fileName)) {
              LOG.error(partition + "FsFilename " + fileName + " not found in Meta data");
            }
          }
          for (String fileName : metadataFilenames) {
            if (!fsFileNames.contains(fileName)) {
              LOG.error(partition + "Metadata file " + fileName + " not found in original FS");
            }
          }
        }

        // Block sizes should be valid
        metaFilesList.forEach(s -> assertTrue(s.getBlockSize() > 0));
        List<Long> fsBlockSizes = pathInfoList.stream().map(StoragePathInfo::getBlockSize).collect(Collectors.toList());
        Collections.sort(fsBlockSizes);
        List<Long> metadataBlockSizes = metaFilesList.stream().map(StoragePathInfo::getBlockSize).collect(Collectors.toList());
        Collections.sort(metadataBlockSizes);
        assertEquals(fsBlockSizes, metadataBlockSizes);

        assertEquals(fsFileNames.size(), metadataFilenames.size(),
            "Files within partition " + partition + " should match");
        assertTrue(fsFileNames.equals(metadataFilenames),
            "Files within partition " + partition + " should match");

        // FileSystemView should expose the same data
        List<HoodieFileGroup> fileGroups =
            tableView.getAllFileGroups(partition).collect(Collectors.toList());
        fileGroups.addAll(
            tableView.getAllReplacedFileGroups(partition).collect(Collectors.toList()));

        fileGroups.forEach(
            g -> LoggerFactory.getLogger(TestHoodieBackedMetadata.class).info(g.toString()));
        fileGroups.forEach(g -> g.getAllBaseFiles().forEach(
            b -> LoggerFactory.getLogger(TestHoodieBackedMetadata.class).info(b.toString())));
        fileGroups.forEach(g -> g.getAllFileSlices().forEach(
            s -> LoggerFactory.getLogger(TestHoodieBackedMetadata.class).info(s.toString())));

        long numFiles = fileGroups.stream()
            .mapToLong(g -> g.getAllBaseFiles().count()
                + g.getAllFileSlices().mapToLong(s -> s.getLogFiles().count()).sum())
            .sum();
        assertEquals(metadataFilenames.size(), numFiles);
      } catch (IOException e) {
        e.printStackTrace();
        assertTrue(false, "Exception should not be raised: " + e);
      }
    });

    try (HoodieBackedTableMetadataWriter<JavaRDD<HoodieRecord>, JavaRDD<WriteStatus>> metadataWriter = metadataWriter(client, storageConf, engineContext)) {
      assertNotNull(metadataWriter, "MetadataWriter should have been initialized");

      // Validate write config for metadata table
      HoodieWriteConfig metadataWriteConfig = metadataWriter.getWriteConfig();
      assertFalse(metadataWriteConfig.isMetadataTableEnabled(), "No metadata table for metadata table");

      // Metadata table should be in sync with the dataset
      HoodieTableMetaClient metadataMetaClient = HoodieTestUtils.createMetaClient(storageConf, metaClient.getMetaPath().toString() + "/metadata/");

      // Metadata table is MOR
      assertEquals(metadataMetaClient.getTableType(), HoodieTableType.MERGE_ON_READ, "Metadata Table should be MOR");

      // Metadata table is HFile format
      assertEquals(metadataMetaClient.getTableConfig().getBaseFileFormat(), HoodieFileFormat.HFILE,
          "Metadata Table base file format should be HFile");

      // Metadata table has a fixed number of partitions
      // Cannot use FSUtils.getAllFoldersWithPartitionMetaFile for this as that function filters all directory
      // in the .hoodie folder.
      List<String> metadataTablePartitions = FSUtils.getAllPartitionPaths(engineContext, metadataMetaClient, false);
      // check if the last instant is restore, then the metadata table should have only the partitions that are not deleted
      metaClient.reloadActiveTimeline().getReverseOrderedInstants().findFirst().ifPresent(instant -> {
        if (instant.getAction().equals(HoodieActiveTimeline.RESTORE_ACTION)) {
          metadataWriter.getEnabledPartitionTypes().stream().filter(partitionType -> !MetadataPartitionType.shouldDeletePartitionOnRestore(partitionType.getPartitionPath()))
              .forEach(partitionType -> assertTrue(metadataTablePartitions.contains(partitionType.getPartitionPath())));
        }
      });

      // Metadata table should automatically compact and clean
      // versions are +1 as autoclean / compaction happens end of commits
      int numFileVersions = metadataWriteConfig.getCleanerFileVersionsRetained() + 1;
      HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(engineContext, metadataMetaClient, metadataMetaClient.getActiveTimeline());
      metadataTablePartitions.forEach(partition -> {
        List<FileSlice> latestSlices = fsView.getLatestFileSlices(partition).collect(Collectors.toList());
        assertTrue(latestSlices.stream().map(FileSlice::getBaseFile).count() <= latestSlices.size(), "Should have a single latest base file per file group");
        List<HoodieLogFile> logFiles = latestSlices.get(0).getLogFiles().collect(Collectors.toList());
        try {
          if (FILES.getPartitionPath().equals(partition)) {
            verifyMetadataRawRecords(table, logFiles, false);
          }
          if (COLUMN_STATS.getPartitionPath().equals(partition)) {
            verifyMetadataColumnStatsRecords(storage, logFiles);
          }
        } catch (Exception e) {
          LOG.error("Metadata record validation failed", e);
          fail("Metadata record validation failed");
        }
      });

      // TODO: include validation for record_index partition here.
      LOG.info("Validation time=" + timer.endTimer());
    }
  }

  private static void verifyMetadataColumnStatsRecords(HoodieStorage storage, List<HoodieLogFile> logFiles) throws IOException {
    for (HoodieLogFile logFile : logFiles) {
      List<StoragePathInfo> pathInfoList = storage.listDirectEntries(logFile.getPath());
      Schema writerSchema =
          TableSchemaResolver.readSchemaFromLogFile(storage, logFile.getPath());
      if (writerSchema == null) {
        // not a data block
        continue;
      }

      try (HoodieLogFormat.Reader logFileReader = HoodieLogFormat.newReader(storage,
          new HoodieLogFile(pathInfoList.get(0).getPath()), writerSchema)) {
        while (logFileReader.hasNext()) {
          HoodieLogBlock logBlock = logFileReader.next();
          if (logBlock instanceof HoodieDataBlock) {
            try (
                ClosableIterator<HoodieRecord<IndexedRecord>> recordItr = ((HoodieDataBlock) logBlock).getRecordIterator(
                    HoodieRecordType.AVRO)) {
              recordItr.forEachRemaining(indexRecord -> {
                final GenericRecord record = (GenericRecord) indexRecord.getData();
                final GenericRecord colStatsRecord =
                    (GenericRecord) record.get(HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS);
                assertNotNull(colStatsRecord);
                assertNotNull(
                    colStatsRecord.get(HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME));
                assertNotNull(
                    colStatsRecord.get(HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT));
                /**
                 * TODO: some types of field may have null min/max as these statistics are only supported for primitive types
                 * assertNotNull(colStatsRecord.get(HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE));
                 * assertNotNull(colStatsRecord.get(HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE));
                 */
              });
            }
          }
        }
      }
    }
  }

  /**
   * Returns the list of all files in the dataset by iterating over the metadata table.
   */
  private List<StoragePath> getAllFiles(HoodieTableMetadata metadata) throws Exception {
    List<StoragePath> allfiles = new LinkedList<>();
    for (String partition : metadata.getAllPartitionPaths()) {
      for (StoragePathInfo pathInfo :
          metadata.getAllFilesInPartition(new StoragePath(basePath, partition))) {
        allfiles.add(pathInfo.getPath());
      }
    }

    return allfiles;
  }

  private HoodieBackedTableMetadataWriter<JavaRDD<HoodieRecord>, JavaRDD<WriteStatus>> metadataWriter(SparkRDDWriteClient client) {
    return metadataWriter(client, storageConf, new HoodieSparkEngineContext(jsc));
  }

  private static HoodieBackedTableMetadataWriter<JavaRDD<HoodieRecord>, JavaRDD<WriteStatus>> metadataWriter(SparkRDDWriteClient client, StorageConfiguration storageConf,
                                                                                                      HoodieEngineContext engineContext) {
    return (HoodieBackedTableMetadataWriter<JavaRDD<HoodieRecord>, JavaRDD<WriteStatus>>) SparkHoodieBackedTableMetadataWriter
        .create(storageConf, client.getConfig(), engineContext);
  }

  public static HoodieTableMetadata metadata(SparkRDDWriteClient client, HoodieStorage storage) {
    HoodieWriteConfig clientConfig = client.getConfig();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(clientConfig.getBasePath()).setConf(storage.getConf()).build();
    return metaClient.getTableFormat().getMetadataFactory().create(client.getEngineContext(), storage, clientConfig.getMetadataConfig(), clientConfig.getBasePath());
  }

  private void changeTableVersion(HoodieTableVersion version) throws IOException {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    metaClient.getTableConfig().setTableVersion(version);
    StoragePath propertyFile = new StoragePath(
        metaClient.getMetaPath(), HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    try (OutputStream os = metaClient.getStorage().create(propertyFile)) {
      metaClient.getTableConfig().getProps().store(os, "");
    }
  }

  @Override
  protected HoodieTableType getTableType() {
    return tableType;
  }

  /**
   * Disabling row writer here as clustering tests will throw the error below if it is used.
   * java.util.concurrent.CompletionException: java.lang.ClassNotFoundException
   * TODO: Fix this and increase test coverage to include clustering via row writers
   * @return
   */
  private static Properties getDisabledRowWriterProperties() {
    Properties properties = new Properties();
    properties.setProperty("hoodie.datasource.write.row.writer.enable", String.valueOf(false));
    return properties;
  }
}
