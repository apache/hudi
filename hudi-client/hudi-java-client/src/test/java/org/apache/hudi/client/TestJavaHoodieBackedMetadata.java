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

package org.apache.hudi.client;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordPayload;
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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;
import org.apache.hudi.metadata.HoodieMetadataLogRecordReader;
import org.apache.hudi.metadata.HoodieMetadataMetrics;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.JavaHoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;
import org.apache.hudi.testutils.TestHoodieMetadataBase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.model.WriteOperationType.DELETE;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getNextCommitTime;
import static org.apache.hudi.config.HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS;
import static org.apache.hudi.metadata.HoodieBackedTableMetadataWriter.METADATA_COMPACTION_TIME_SUFFIX;
import static org.apache.hudi.metadata.HoodieTableMetadata.getMetadataTableBasePath;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.deleteMetadataTable;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestJavaHoodieBackedMetadata extends TestHoodieMetadataBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestJavaHoodieBackedMetadata.class);

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
    doWriteInsertAndUpsert(testTable);
    validateMetadata(testTable);
    if (addRollback) {
      // trigger an UPSERT that will be rolled back
      doWriteOperationAndValidate(testTable, "0000003");

      // rollback last commit
      doRollbackAndValidate(testTable, "0000003", "0000004");
    }

    // trigger couple of upserts
    doWriteOperation(testTable, "0000005");
    doWriteOperation(testTable, "0000006");
    doWriteOperation(testTable, "0000007");
    doCleanAndValidate(testTable, "0000008", Arrays.asList("0000007"));
    validateMetadata(testTable, true);
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


    List<String> partitions;
    try (HoodieBackedTableMetadataWriter metadataWriter = metadataWriter(writeConfig)) {
      partitions = metadataWriter.getTableMetadata().getAllPartitionPaths();
    }
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

    List<StoragePathInfo> pathInfoList =
        metadata(writeConfig, context).getAllFilesInPartition(new StoragePath(basePath,
            "p1"));
    assertEquals(tableType == COPY_ON_WRITE ? 3 : 4, pathInfoList.size());
    pathInfoList =
        metadata(writeConfig, context).getAllFilesInPartition(new StoragePath(basePath, "p2"));
    assertEquals(tableType == COPY_ON_WRITE ? 6 : 7, pathInfoList.size());
    Map<String, List<StoragePathInfo>> partitionsToFilesMap =
        metadata(writeConfig, context).getAllFilesInPartitions(
            asList(basePath + "/p1", basePath + "/p2"));
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
    List<Long> commitTimeList = new ArrayList<>();
    commitTimeList.add(Long.parseLong(HoodieActiveTimeline.createNewInstantTime()));
    for (int i = 0; i < 8; i++) {
      long nextCommitTime = getNextCommitTime(commitTimeList.get(commitTimeList.size() - 1));
      commitTimeList.add(nextCommitTime);
    }
    init(tableType, true, enableFullScan, false, false);
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

    AtomicInteger commitTime = new AtomicInteger(1);
    // Trigger 5 regular writes in data table.
    for (int i = 1; i <= 5; i++) {
      doWriteOperation(testTable, "000000" + (commitTime.getAndIncrement()), INSERT);
    }
    // The earliest deltacommit in the metadata table should be "0000001",
    // and the "00000000000000" init deltacommit should be archived.
    HoodieTableMetaClient metadataMetaClient = createMetaClientForMetadataTable();
    HoodieActiveTimeline metadataTimeline = metadataMetaClient.reloadActiveTimeline();
    assertEquals("0000001", metadataTimeline.getCommitsTimeline().firstInstant().get().getTimestamp());

    // Trigger clustering in the data table, archival should not kick in, even though conditions are met.
    doCluster(testTable, "000000" + commitTime.getAndIncrement());
    metadataTimeline = metadataMetaClient.reloadActiveTimeline();
    assertEquals("0000001", metadataTimeline.getCommitsTimeline().firstInstant().get().getTimestamp());

    getHoodieWriteClient(writeConfig);
    // Trigger a regular write operation. data set timeline archival should kick in.
    doWriteOperation(testTable, "000000" + (commitTime.getAndIncrement()), INSERT);
    archiveDataTable(writeConfig, createMetaClient());
    assertEquals("0000004",
        metaClient.reloadActiveTimeline().getCommitsTimeline().firstInstant().get().getTimestamp());
    metadataTimeline = metadataMetaClient.reloadActiveTimeline();
    assertEquals("0000001", metadataTimeline.getCommitsTimeline().firstInstant().get().getTimestamp());

    // Trigger a regular write operation. metadata timeline archival should kick in.
    doWriteOperation(testTable, "000000" + (commitTime.getAndIncrement()), INSERT);
    metadataTimeline = metadataMetaClient.reloadActiveTimeline();
    assertEquals("0000004", metadataTimeline.getCommitsTimeline().firstInstant().get().getTimestamp());
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

    AtomicInteger commitTime = new AtomicInteger(1);
    // Trigger 4 regular writes in data table.
    for (int i = 1; i <= 4; i++) {
      doWriteOperation(testTable, "000000" + (commitTime.getAndIncrement()), INSERT);
    }

    // The earliest deltacommit in the metadata table should be "0000001",
    // and the "00000000000000" init deltacommit should be archived.
    HoodieTableMetaClient metadataMetaClient = createMetaClientForMetadataTable();
    HoodieActiveTimeline metadataTimeline = metadataMetaClient.reloadActiveTimeline();
    assertEquals("0000001", metadataTimeline.getCommitsTimeline().firstInstant().get().getTimestamp());

    getHoodieWriteClient(writeConfig);
    // Trigger data table archive, should archive "0000001", "0000002"
    archiveDataTable(writeConfig, createMetaClient());
    // Trigger a regular write operation. metadata timeline archival should kick in and catch up with data table.
    doWriteOperation(testTable, "000000" + (commitTime.getAndIncrement()), INSERT);
    metadataTimeline = metadataMetaClient.reloadActiveTimeline();
    assertEquals("0000003", metadataTimeline.getCommitsTimeline().firstInstant().get().getTimestamp());
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
            .withMaxNumDeltaCommitsBeforeCompaction(3) // after 3 delta commits for regular writer operations, compaction should kick in.
            .build()).build();
    initWriteConfigAndMetatableWriter(writeConfig, true);

    doWriteOperation(testTable, "0000001", INSERT);
    doCleanAndValidate(testTable, "0000003", Arrays.asList("0000001"));

    HoodieTableMetadata tableMetadata = metadata(writeConfig, context);
    // since clean was the last commit, table services should not get triggered in metadata table.
    assertFalse(tableMetadata.getLatestCompactionTime().isPresent());

    doWriteOperation(testTable, "0000004", UPSERT);
    // this should have triggered compaction in metadata table
    tableMetadata = metadata(writeConfig, context);
    assertTrue(tableMetadata.getLatestCompactionTime().isPresent());
    assertEquals(tableMetadata.getLatestCompactionTime().get(), "0000003001");
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
    // module com.fasterxml.jackson.datatype:jackson-datatype-jsr310 is needed for proper column stats processing for Jackson >= 2.11 (Spark >= 3.3)
    // Java 8 date/time type `java.time.LocalDate` is not supported by default
    JsonUtils.registerModules();
    init(tableType, writeConfig);
    testTableOperationsForMetaIndexImpl(writeConfig);
  }

  private void testTableOperationsForMetaIndexImpl(final HoodieWriteConfig writeConfig) throws Exception {
    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);
    testTableOperationsImpl(engineContext, writeConfig);
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

    doWriteOperation(testTable, "0000001", INSERT);
    doClean(testTable, "0000003", Arrays.asList("0000001"));
    // this should have triggered compaction in metadata table
    doWriteOperation(testTable, "0000004", UPSERT);

    HoodieTableMetadata tableMetadata = metadata(writeConfig, context);
    assertTrue(tableMetadata.getLatestCompactionTime().isPresent());
    assertEquals(tableMetadata.getLatestCompactionTime().get(), "0000003001");

    HoodieTableMetaClient metadataMetaClient = createMetaClientForMetadataTable();
    HoodieWriteConfig metadataTableWriteConfig = getMetadataWriteConfig(writeConfig);
    metadataMetaClient.reloadActiveTimeline();

    HoodieTable table = HoodieJavaTable.create(metadataTableWriteConfig, context, metadataMetaClient);
    table.getHoodieView().sync();
    List<FileSlice> fileSlices = table.getSliceView().getLatestFileSlices("files").collect(Collectors.toList());
    HoodieBaseFile baseFile = fileSlices.get(0).getBaseFile().get();
    HoodieAvroHFileReaderImplBase hoodieHFileReader = (HoodieAvroHFileReaderImplBase)
        HoodieIOFactory.getIOFactory(storage).getReaderFactory(HoodieRecordType.AVRO)
            .getFileReader(writeConfig, new StoragePath(baseFile.getPath()));
    List<IndexedRecord> records = HoodieAvroHFileReaderImplBase.readAllRecords(hoodieHFileReader);
    records.forEach(entry -> {
      if (populateMetaFields) {
        assertNotNull(((GenericRecord) entry).get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
      } else {
        assertNull(((GenericRecord) entry).get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
      }
    });
  }

  @Test
  public void testMetadataTableCompactionWithPendingInstants() throws Exception {
    init(COPY_ON_WRITE, false);
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .enableMetrics(false)
            .withMaxNumDeltaCommitsBeforeCompaction(4)
            .build()).build();
    initWriteConfigAndMetatableWriter(writeConfig, true);
    doWriteOperation(testTable, "0000001", INSERT);
    String commitInstant = "0000002";
    doWriteOperation(testTable, commitInstant, INSERT);

    // test multi-writer scenario. lets add 1,2,3,4 where 1,2,4 succeeded, but 3 is still inflight. so latest delta commit in MDT is 4, while 3 is still pending
    // in DT and not seen by MDT yet. compaction should not trigger until 3 goes to completion.

    // create an inflight commit for 3
    HoodieCommitMetadata inflightCommitMeta = testTable.doWriteOperation("0000003", UPSERT, emptyList(),
        asList("p1", "p2"), 2, false, true);
    doWriteOperation(testTable, "0000004");
    HoodieTableMetadata tableMetadata = metadata(writeConfig, context);
    // verify that compaction of metadata table does not kick in.
    assertFalse(tableMetadata.getLatestCompactionTime().isPresent());
    doWriteOperation(testTable, "0000005", INSERT);
    doWriteOperation(testTable, "0000006", INSERT);
    doWriteOperation(testTable, "0000007", INSERT);

    tableMetadata = metadata(writeConfig, context);
    // verify that compaction of metadata table does not kick in.
    assertFalse(tableMetadata.getLatestCompactionTime().isPresent());

    // move inflight to completed
    testTable.moveInflightCommitToComplete("0000003", inflightCommitMeta);

    // we have to add another commit for compaction to trigger. if not, latest delta commit in MDT is 7, but the new incoming i.e 3 is still inflight in DT while "3"
    // is getting applied to MDT.
    doWriteOperation(testTable, "0000008", INSERT);
    // verify compaction kicked in now
    String metadataCompactionInstant = "0000007" + METADATA_COMPACTION_TIME_SUFFIX;
    tableMetadata = metadata(writeConfig, context);
    assertTrue(tableMetadata.getLatestCompactionTime().isPresent());
    assertEquals(tableMetadata.getLatestCompactionTime().get(), metadataCompactionInstant);
    // do full metadata validation
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

    doWriteOperation(testTable, "0000001", INSERT);
    // create an inflight compaction in metadata table.
    // not easy to create an inflight in metadata table directly, hence letting compaction succeed and then deleting the completed instant.
    // this new write is expected to trigger metadata table compaction
    String commitInstant = "0000002";
    doWriteOperation(testTable, commitInstant, INSERT);
    doWriteOperation(testTable, "0000003", INSERT);

    HoodieTableMetadata tableMetadata = metadata(writeConfig, context);
    String metadataCompactionInstant = commitInstant + METADATA_COMPACTION_TIME_SUFFIX;
    assertTrue(tableMetadata.getLatestCompactionTime().isPresent());
    assertEquals(tableMetadata.getLatestCompactionTime().get(), metadataCompactionInstant);

    validateMetadata(testTable);
    // Fetch compaction Commit file and rename to some other file. completed compaction meta file should have some serialized info that table interprets
    // for future upserts. so, renaming the file here to some temp name and later renaming it back to same name.
    java.nio.file.Path parentPath = Paths.get(metadataTableBasePath, METAFOLDER_NAME);
    java.nio.file.Path metaFilePath = parentPath.resolve(metadataCompactionInstant + HoodieTimeline.COMMIT_EXTENSION);
    java.nio.file.Path tempFilePath = FileCreateUtils.renameFileToTemp(metaFilePath, metadataCompactionInstant);
    metaClient.reloadActiveTimeline();
    testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter, Option.of(context));
    // this validation will exercise the code path where a compaction is inflight in metadata table, but still metadata based file listing should match non
    // metadata based file listing.
    validateMetadata(testTable);

    if (simulateFailedCompaction) {
      // this should retry the compaction in metadata table.
      doWriteOperation(testTable, "0000004", INSERT);
    } else {
      // let the compaction succeed in metadata and validation should succeed.
      FileCreateUtils.renameTempToMetaFile(tempFilePath, metaFilePath);
    }

    validateMetadata(testTable);

    // add few more write and validate
    doWriteOperation(testTable, "0000005", INSERT);
    doWriteOperation(testTable, "0000006", UPSERT);
    validateMetadata(testTable);

    if (simulateFailedCompaction) {
      //trigger another compaction failure.
      metadataCompactionInstant = "0000005001";
      tableMetadata = metadata(writeConfig, context);
      assertTrue(tableMetadata.getLatestCompactionTime().isPresent());
      assertEquals(tableMetadata.getLatestCompactionTime().get(), metadataCompactionInstant);

      // Fetch compaction Commit file and rename to some other file. completed compaction meta file should have some serialized info that table interprets
      // for future upserts. so, renaming the file here to some temp name and later renaming it back to same name.
      parentPath = Paths.get(metadataTableBasePath, METAFOLDER_NAME);
      metaFilePath = parentPath.resolve(metadataCompactionInstant + HoodieTimeline.COMMIT_EXTENSION);
      tempFilePath = FileCreateUtils.renameFileToTemp(metaFilePath, metadataCompactionInstant);

      validateMetadata(testTable);

      // this should retry the failed compaction in metadata table.
      doWriteOperation(testTable, "0000007", INSERT);

      validateMetadata(testTable);

      // add few more write and validate
      doWriteOperation(testTable, "0000008", INSERT);
      doWriteOperation(testTable, "0000009", UPSERT);
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

    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {
      // Write 1 (Bulk insert)
      String newCommitTime1 = "0000001";
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime1, 100);
      client.startCommitWithTime(newCommitTime1);
      List<WriteStatus> writeStatuses = client.insert(records, newCommitTime1);
      client.commit(newCommitTime1, writeStatuses);

      String newCommitTime2 = "0000002";
      records = dataGen.generateUniqueUpdates(newCommitTime2, 20);
      client.startCommitWithTime(newCommitTime2);
      writeStatuses = client.upsert(records, newCommitTime2);
      client.commit(newCommitTime2, writeStatuses);

      String newCommitTime3 = "0000003";
      records = dataGen.generateUniqueUpdates(newCommitTime3, 20);
      client.startCommitWithTime(newCommitTime3);
      writeStatuses = client.upsert(records, newCommitTime3);
      client.commit(newCommitTime3, writeStatuses);

      // rollback "3" so that there is no "3" in data table timeline, but there exists a DC "3" in metadata timeline.
      client.rollback(newCommitTime3);

      // mimicing crash or making an inflight in metadata table.
      StoragePath toDelete = new StoragePath(metaClient.getMetaPath() + "/metadata/.hoodie/" + newCommitTime2 + "." + HoodieTimeline.DELTA_COMMIT_ACTION);
      metaClient.getStorage().deleteFile(toDelete);

      // re-ingest w/ same commit time.
      records = dataGen.generateUniqueUpdates(newCommitTime3, 20);
      client.startCommitWithTime(newCommitTime3);
      writeStatuses = client.upsert(records, newCommitTime3);
      client.commit(newCommitTime3, writeStatuses);

      // collect all commit meta files from metadata table.
      List<StoragePathInfo> metaFiles = metaClient.getStorage().listDirectEntries(
          new StoragePath(metaClient.getMetaPath() + "/metadata/.hoodie"));
      List<StoragePathInfo> commit3Files = metaFiles.stream()
          .filter(fileInfo ->
              fileInfo.getPath().getName().equals(newCommitTime3 + "." + HoodieTimeline.DELTA_COMMIT_ACTION))
          .collect(Collectors.toList());
      List<StoragePathInfo> rollbackFiles = metaFiles.stream()
          .filter(fileStatus ->
              fileStatus.getPath().getName().endsWith("." + HoodieTimeline.ROLLBACK_ACTION))
          .collect(Collectors.toList());

      // ensure commit2's delta commit in MDT has last mod time > the actual rollback for previous failed commit i.e. commit2.
      // if rollback wasn't eager, rollback's last mod time will be lower than the commit3'd delta commit last mod time.
      assertTrue(
          commit3Files.get(0).getModificationTime() > rollbackFiles.get(0).getModificationTime());
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

    final HoodieTableMetaClient metadataMetaClient = createMetaClientForMetadataTable();
    HoodieWriteConfig metadataTableWriteConfig = getMetadataWriteConfig(writeConfig);
    metadataMetaClient.reloadActiveTimeline();
    final HoodieTable table = HoodieJavaTable.create(metadataTableWriteConfig, context, metadataMetaClient);

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
    List<HoodieLogFile> logFiles = fileSlices.get(0).getLogFiles().map(logFile -> {
      return logFile;
    }).collect(Collectors.toList());

    List<String> logFilePaths = logFiles.stream().map(logFile -> {
      return logFile.getPath().toString();
    }).collect(Collectors.toList());

    // Verify the on-disk raw records before they get materialized
    verifyMetadataRawRecords(table, logFiles, enableMetaFields);

    // Verify the in-memory materialized and merged records
    verifyMetadataMergedRecords(metadataMetaClient, logFilePaths, latestCommitTimestamp, enableMetaFields);
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
  private void verifyMetadataRawRecords(HoodieTable table, List<HoodieLogFile> logFiles, boolean enableMetaFields) throws IOException {
    for (HoodieLogFile logFile : logFiles) {
      List<StoragePathInfo> pathInfoList = storage.listDirectEntries(logFile.getPath());
      Schema writerSchema = TableSchemaResolver.readSchemaFromLogFile(storage,
          logFile.getPath());
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
   * @param logFilePaths          - Metadata table log file paths
   * @param latestCommitTimestamp
   * @param enableMetaFields      - Enable meta fields
   */
  private void verifyMetadataMergedRecords(HoodieTableMetaClient metadataMetaClient, List<String> logFilePaths,
                                           String latestCommitTimestamp, boolean enableMetaFields) {
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
    if (enableMetaFields) {
      schema = HoodieAvroUtils.addMetadataFields(schema);
    }
    HoodieMetadataLogRecordReader logRecordReader = HoodieMetadataLogRecordReader.newBuilder()
        .withStorage(metadataMetaClient.getStorage())
        .withBasePath(metadataMetaClient.getBasePath())
        .withLogFilePaths(logFilePaths)
        .withLatestInstantTime(latestCommitTimestamp)
        .withPartition(FILES.getPartitionPath())
        .withReaderSchema(schema)
        .withMaxMemorySizeInBytes(100000L)
        .withBufferSize(4096)
        .withSpillableMapBasePath(tempDir.toString())
        .withDiskMapType(ExternalSpillableMap.DiskMapType.BITCASK)
        .build();

    for (HoodieRecord<? extends HoodieRecordPayload> entry : logRecordReader.getRecords()) {
      assertFalse(entry.getRecordKey().isEmpty());
      assertEquals(entry.getKey().getRecordKey(), entry.getRecordKey());
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

    HoodieAvroHFileReaderImplBase hoodieHFileReader = (HoodieAvroHFileReaderImplBase)
        HoodieIOFactory.getIOFactory(storage).getReaderFactory(HoodieRecordType.AVRO)
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
    boolean populateMateFields = false;
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
    doWriteInsertAndUpsert(testTable, "000001", "000002", false);

    for (int i = 3; i < 10; i++) {
      doWriteOperation(testTable, "00000" + i);
      archiveDataTable(writeConfig, metaClient);
    }
    validateMetadata(testTable);

    // We can only rollback those commits whose deltacommit have not been archived yet.
    int numRollbacks = 0;
    boolean exceptionRaised = false;
    List<HoodieInstant> allInstants = metaClient.reloadActiveTimeline().getCommitsTimeline().getReverseOrderedInstants().collect(Collectors.toList());
    for (HoodieInstant instantToRollback : allInstants) {
      try {
        testTable.doRollback(instantToRollback.getTimestamp(), String.valueOf(Time.now()));
        validateMetadata(testTable);
        ++numRollbacks;
      } catch (HoodieMetadataException e) {
        // This is expected since we are rolling back commits that are older than the latest compaction on the MDT
        break;
      }
    }
    // Since each rollback also creates a deltacommit, we can only support rolling back of half of the original
    // instants present before rollback started.
    assertTrue(numRollbacks >= minArchiveCommitsDataset / 2, "Rollbacks of non archived instants should work");
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
    long baseCommitTime = Long.parseLong(HoodieActiveTimeline.createNewInstantTime());
    for (int i = 1; i < 25; i += 7) {
      long commitTime1 = getNextCommitTime(baseCommitTime);
      long commitTime2 = getNextCommitTime(commitTime1);
      long commitTime3 = getNextCommitTime(commitTime2);
      long commitTime4 = getNextCommitTime(commitTime3);
      long commitTime5 = getNextCommitTime(commitTime4);
      long commitTime6 = getNextCommitTime(commitTime5);
      long commitTime7 = getNextCommitTime(commitTime6);
      baseCommitTime = commitTime7;
      doWriteOperation(testTable, Long.toString(commitTime1), INSERT, nonPartitionedDataset);
      doWriteOperation(testTable, Long.toString(commitTime2), UPSERT, nonPartitionedDataset);
      doClean(testTable, Long.toString(commitTime3), Arrays.asList(Long.toString(commitTime1)));
      doWriteOperation(testTable, Long.toString(commitTime4), UPSERT, nonPartitionedDataset);
      if (tableType == MERGE_ON_READ) {
        doCompaction(testTable, Long.toString(commitTime5), nonPartitionedDataset);
      }
      doWriteOperation(testTable, Long.toString(commitTime6), UPSERT, nonPartitionedDataset);
      doRollback(testTable, Long.toString(commitTime6), Long.toString(commitTime7));
    }
    validateMetadata(testTable, emptyList(), nonPartitionedDataset);
  }

  /**
   * First attempt at bootstrap failed but the file slices get created. The next bootstrap should continue successfully.
   */
  @Test
  public void testFailedBootstrap() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    // Config with 5 fileGroups for record index
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableRecordIndex(true)
            .withRecordIndexFileGroupCount(5, 5)
            .build())
        .build();

    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {
      // Write
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.insert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Metadata table should exist
      final StoragePath metadataTablePath =
          new StoragePath(getMetadataTableBasePath(writeConfig.getBasePath()));
      assertTrue(storage.exists(metadataTablePath));
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(metaClient.getTableConfig().isMetadataTableAvailable());

      // File groups should be created as in the config
      HoodieBackedTableMetadata metadataReader = (HoodieBackedTableMetadata) metadata(client);
      assertEquals(HoodieTableMetadataUtil.getPartitionLatestFileSlices(
          metadataReader.getMetadataMetaClient(), Option.empty(),
          MetadataPartitionType.FILES.getPartitionPath()).size(), 1);
      assertEquals(HoodieTableMetadataUtil.getPartitionLatestFileSlices(
          metadataReader.getMetadataMetaClient(), Option.empty(),
          MetadataPartitionType.RECORD_INDEX.getPartitionPath()).size(), 5);
    }

    // remove the MDT partition from dataset to simulate failed bootstrap
    Properties updateProperties = new Properties();
    updateProperties.setProperty(HoodieTableConfig.TABLE_METADATA_PARTITIONS.key(), "");
    HoodieTableConfig.update(storage,
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
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {
      // Write
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.insert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);

      // Metadata table is recreated, during bootstrapping of metadata table.
      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(metaClient.getTableConfig().isMetadataTableAvailable());
      validateMetadata(client);

      // File groups should be created as in the config
      HoodieBackedTableMetadata metadataReader = (HoodieBackedTableMetadata) metadata(client);
      assertEquals(HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataReader.getMetadataMetaClient(), Option.empty(),
          MetadataPartitionType.FILES.getPartitionPath()).size(), 1);
      assertEquals(HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataReader.getMetadataMetaClient(), Option.empty(),
          MetadataPartitionType.RECORD_INDEX.getPartitionPath()).size(), 3);
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
    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext,
        getWriteConfigBuilder(true, true, false).withRollbackUsingMarkers(false).build())) {

      // Write 1
      String commitTime = "0000001";
      List<HoodieRecord> records = dataGen.generateInserts(commitTime, 20);
      client.startCommitWithTime(commitTime);
      List<WriteStatus> writeStatuses = client.insert(records, commitTime);
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Rollback the first commit
      client.rollback(commitTime);

      // Write 2
      commitTime = "0000002";
      records = dataGen.generateInserts(commitTime, 10);
      client.startCommitWithTime(commitTime);
      writeStatuses = client.upsert(records, commitTime);
      assertNoWriteErrors(writeStatuses);
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
    this.tableType = COPY_ON_WRITE;
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
   * Test several table operations with restore. This test uses HoodieJavaWriteClient.
   * Once the restore support is ready in HoodieTestTable, then rewrite this test.
   */
  @Test
  public void testTableOperationsWithRestore() throws Exception {
    this.tableType = COPY_ON_WRITE;
    init(tableType);
    HoodieJavaEngineContext engineContext = new HoodieJavaEngineContext(storageConf);
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withRollbackUsingMarkers(false).build();
    testTableOperationsImpl(engineContext, writeConfig);
  }

  /**
   * Test several table operations with restore. This test uses HoodieJavaWriteClient.
   * Once the restore support is ready in HoodieTestTable, then rewrite this test.
   */
  @Test
  public void testTableOperationsWithRestoreforMOR() throws Exception {
    this.tableType = MERGE_ON_READ;
    init(tableType);
    HoodieJavaEngineContext engineContext = new HoodieJavaEngineContext(storageConf);
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withRollbackUsingMarkers(false).build();
    testTableOperationsImpl(engineContext, writeConfig);
  }

  @Test
  public void testColStatsPrefixLookup() throws IOException {
    this.tableType = COPY_ON_WRITE;
    initPath();
    initFileSystem(basePath, storageConf);
    storage.createDirectory(new StoragePath(basePath));
    initMetaClient(tableType);
    initTestDataGenerator();
    metadataTableBasePath = getMetadataTableBasePath(basePath);

    HoodieJavaEngineContext engineContext = new HoodieJavaEngineContext(storageConf);
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

    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {

      String firstCommit = "0000001";
      List<HoodieRecord> records = dataGen.generateInserts(firstCommit, 20);

      AtomicInteger counter = new AtomicInteger();
      List<HoodieRecord> processedRecords = records.stream().map(entry ->
              new HoodieAvroRecord(new HoodieKey("key1_" + counter.getAndIncrement(), entry.getPartitionPath()), (HoodieRecordPayload) entry.getData()))
          .collect(Collectors.toList());

      client.startCommitWithTime(firstCommit);
      List<WriteStatus> writeStatuses = client.insert(processedRecords, firstCommit);
      assertNoWriteErrors(writeStatuses);

      // Write 2 (inserts)
      String secondCommit = "0000002";
      client.startCommitWithTime(secondCommit);
      records = dataGen.generateInserts(secondCommit, 20);
      AtomicInteger counter1 = new AtomicInteger();
      processedRecords = records.stream().map(entry ->
              new HoodieAvroRecord(new HoodieKey("key2_" + counter1.getAndIncrement(), entry.getPartitionPath()), (HoodieRecordPayload) entry.getData()))
          .collect(Collectors.toList());
      writeStatuses = client.insert(processedRecords, secondCommit);
      assertNoWriteErrors(writeStatuses);

      Map<String, Map<String, List<String>>> commitToPartitionsToFiles = new HashMap<>();
      // populate commit -> partition -> file info to assist in validation and prefix search
      metaClient.getActiveTimeline().getInstants().forEach(entry -> {
        try {
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
              .fromBytes(metaClient.getActiveTimeline().getInstantDetails(entry).get(), HoodieCommitMetadata.class);
          String commitTime = entry.getTimestamp();
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

      HoodieTableMetadata tableMetadata = metadata(client);
      // prefix search for column (_hoodie_record_key)
      ColumnIndexID columnIndexID = new ColumnIndexID(HoodieRecord.RECORD_KEY_METADATA_FIELD);
      List<HoodieRecord<HoodieMetadataPayload>> result = tableMetadata.getRecordsByKeyPrefixes(Collections.singletonList(columnIndexID.asBase64EncodedString()),
          MetadataPartitionType.COLUMN_STATS.getPartitionPath(), true).collectAsList();

      // there are 3 partitions in total and 2 commits. total entries should be 6.
      assertEquals(result.size(), 6);
      result.forEach(entry -> {
        //LOG.warn("Prefix search entries just for record key col : " + entry.getRecordKey().toString() + " :: " + entry.getData().getColumnStatMetadata().get().toString());
      });

      // prefix search for col(_hoodie_record_key) and first partition. only 2 files should be matched
      PartitionIndexID partitionIndexID = new PartitionIndexID(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
      result = tableMetadata.getRecordsByKeyPrefixes(Collections.singletonList(columnIndexID.asBase64EncodedString().concat(partitionIndexID.asBase64EncodedString())),
          MetadataPartitionType.COLUMN_STATS.getPartitionPath(), true).collectAsList();
      // 1 partition and 2 commits. total entries should be 2.
      assertEquals(result.size(), 2);
      result.forEach(entry -> {
        // LOG.warn("Prefix search entries for record key col and first partition : " + entry.getRecordKey().toString() + " :: " + entry.getData().getColumnStatMetadata().get().toString());
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
      columnIndexID = new ColumnIndexID(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
      result = tableMetadata.getRecordsByKeyPrefixes(Collections.singletonList(columnIndexID.asBase64EncodedString().concat(partitionIndexID.asBase64EncodedString())),
          MetadataPartitionType.COLUMN_STATS.getPartitionPath(), true).collectAsList();

      // 1 partition and 2 commits. total entries should be 2.
      assertEquals(result.size(), 2);
      result.forEach(entry -> {
        // LOG.warn("Prefix search entries for record key col and first partition : " + entry.getRecordKey().toString() + " :: " + entry.getData().getColumnStatMetadata().get().toString());
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
    HoodieJavaEngineContext engineContext = new HoodieJavaEngineContext(storageConf);
    HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig);
    // Write 1 (Bulk insert)
    String commit1 = HoodieActiveTimeline.createNewInstantTime();
    List<HoodieRecord> records = dataGen.generateInserts(commit1, 20);
    client.startCommitWithTime(commit1);
    List<WriteStatus> writeStatuses = client.bulkInsert(records, commit1);
    assertNoWriteErrors(writeStatuses);

    // Write 2 (inserts)
    String commit2 = HoodieActiveTimeline.createNewInstantTime();
    client.startCommitWithTime(commit2);
    records = dataGen.generateInserts(commit2, 20);
    writeStatuses = client.insert(records, commit2);
    assertNoWriteErrors(writeStatuses);
    // remove latest completed delta commit from MDT.
    StoragePath toDelete = new StoragePath(metaClient.getMetaPath() + "/metadata/.hoodie/" + commit2 + "." + HoodieTimeline.DELTA_COMMIT_ACTION);
    metaClient.getStorage().deleteFile(toDelete);

    // Write 3 (updates)
    client.close();
    client = new HoodieJavaWriteClient(engineContext, writeConfig);
    String commit3 = HoodieActiveTimeline.createNewInstantTime();
    client.startCommitWithTime(commit3);
    records = dataGen.generateUniqueUpdates(commit3, 10);
    writeStatuses = client.upsert(records, commit3);
    assertNoWriteErrors(writeStatuses);

    // ensure that 000003 is after rollback of the partially failed 2nd commit.
    HoodieTableMetaClient metadataMetaClient = HoodieTestUtils.createMetaClient(
        metaClient.getStorageConf(), metaClient.getMetaPath() + "/metadata/");
    HoodieInstant rollbackInstant =
        metadataMetaClient.getActiveTimeline().getRollbackTimeline().getInstants().get(0);

    // collect all commit meta files from metadata table.
    List<StoragePathInfo> metaFiles = metaClient.getStorage().listDirectEntries(
        new StoragePath(metaClient.getMetaPath() + "/metadata/.hoodie"));
    List<StoragePathInfo> commit3Files = metaFiles.stream()
        .filter(fileInfo ->
            fileInfo.getPath().getName().equals(commit3 + "." + HoodieTimeline.DELTA_COMMIT_ACTION))
        .collect(Collectors.toList());
    List<StoragePathInfo> rollbackFiles = metaFiles.stream()
        .filter(fileStatus ->
            fileStatus.getPath().getName().endsWith("." + HoodieTimeline.ROLLBACK_ACTION))
        .collect(Collectors.toList());

    // ensure commit3's delta commit in MDT has last mod time > the actual rollback for previous failed commit i.e. commit2.
    // if rollback wasn't eager, rollback's last mod time will be not larger than the commit3'd delta commit last mod time.
    assertTrue(commit3Files.get(0).getModificationTime() >= rollbackFiles.get(0).getModificationTime());
    client.close();
  }

  /**
   * Test all major table operations with the given table, config and context.
   *
   * @param engineContext - Engine context
   * @param writeConfig   - Write config
   */
  private void testTableOperationsImpl(HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) throws Exception {

    String newCommitTime = null;
    List<HoodieRecord> records = new ArrayList<>();
    List<WriteStatus> writeStatuses = null;

    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {
      // Write 1 (Bulk insert)
      newCommitTime = "20210101000100000";
      records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      writeStatuses = client.bulkInsert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Write 2 (inserts)
      newCommitTime = "20210101000200000";
      client.startCommitWithTime(newCommitTime);
      validateMetadata(client);

      records = dataGen.generateInserts(newCommitTime, 20);
      writeStatuses = client.insert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Write 3 (updates)
      newCommitTime = "20210101000300000";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);

      // Write 4 (updates and inserts)
      newCommitTime = "20210101000400000";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = "20210101000500000";
        boolean tmp = client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        validateMetadata(client);
      }

      // Write 5 (updates and inserts)
      newCommitTime = "20210101000600000";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 5);
      writeStatuses = client.upsert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

    }

    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = "20210101000700000";
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        validateMetadata(client);
      }

      // upserts
      newCommitTime = "20210101000900000";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 5);
      writeStatuses = client.upsert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);

      // Clean
      newCommitTime = "20210101001000000";
      client.clean(newCommitTime);
      validateMetadata(client);

      // Restore
      client.restoreToInstant("20210101000600000", writeConfig.isMetadataTableEnabled());
      validateMetadata(client);
    }
  }

  /**
   * Test multi-writer on metadata table with optimistic concurrency.
   */
  @Test
  public void testMetadataMultiWriter() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieJavaEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

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
    HoodieJavaWriteClient writeClient = new HoodieJavaWriteClient(engineContext, writeConfig);
    String initialCommit = "0000000";
    List<HoodieRecord> initialRecords = dataGen.generateInserts(initialCommit, 100);
    writeClient.startCommitWithTime(initialCommit);
    List<WriteStatus> initialWriteStatuses = writeClient.insert(initialRecords, initialCommit);
    assertNoWriteErrors(initialWriteStatuses);
    writeClient.close();

    ExecutorService executors = Executors.newFixedThreadPool(dataGen.getPartitionPaths().length);
    // Create clients in advance
    HoodieJavaWriteClient[] writeClients = new HoodieJavaWriteClient[dataGen.getPartitionPaths().length];
    for (int i = 0; i < dataGen.getPartitionPaths().length; i++) {
      writeClients[i] = new HoodieJavaWriteClient(engineContext, writeConfig);
    }

    // Parallel commits for separate partitions
    List<Future> futures = new ArrayList<>(dataGen.getPartitionPaths().length);
    for (int i = 0; i < dataGen.getPartitionPaths().length; ++i) {
      final int index = i;
      String newCommitTime = "000000" + (index + 2);
      Future future = executors.submit(() -> {
        List<HoodieRecord> records = dataGen.generateInsertsForPartition(newCommitTime, 100, dataGen.getPartitionPaths()[index]);
        HoodieJavaWriteClient localWriteClient = writeClients[index];
        writeClient.startCommitWithTime(newCommitTime);
        List<WriteStatus> writeStatuses = localWriteClient.insert(records, newCommitTime);
        assertNoWriteErrors(writeStatuses);
      });
      futures.add(future);
    }

    // Wait for all commits to complete
    for (Future future : futures) {
      future.get();
    }

    // Ensure all commits were synced to the Metadata Table
    HoodieTableMetaClient metadataMetaClient = createMetaClientForMetadataTable();
    assertEquals(metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().countInstants(), 5);
    assertTrue(metadataMetaClient.getActiveTimeline().containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "0000002")));
    assertTrue(metadataMetaClient.getActiveTimeline().containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "0000003")));
    assertTrue(metadataMetaClient.getActiveTimeline().containsInstant(new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "0000004")));

    // Compaction may occur if the commits completed in order
    assertTrue(metadataMetaClient.getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants().countInstants() <= 1);

    // Validation
    validateMetadata(writeClients[0]);
    Arrays.stream(writeClients).forEach(HoodieJavaWriteClient::close);
  }

  /**
   * Tests that when inline cleaning is enabled and with auto commit set to true, there is no double locking.
   * bcoz, auto clean is triggered within post commit which is already happening within a lock.
   */
  @Test
  public void testMultiWriterForDoubleLocking() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieJavaEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");

    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).withAutoClean(true).retainCommits(4)
            .build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024).build())
        .withAutoCommit(false)
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .withProperties(properties)
        .build();

    try (HoodieJavaWriteClient writeClient = new HoodieJavaWriteClient(engineContext, writeConfig)) {
      String partitionPath = dataGen.getPartitionPaths()[0];
      for (int j = 0; j < 6; j++) {
        String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        List<HoodieRecord> records = dataGen.generateInsertsForPartition(newCommitTime, 100, partitionPath);
        writeClient.startCommitWithTime(newCommitTime);
        List<WriteStatus> writeStatuses = writeClient.insert(records, newCommitTime);
        writeClient.commit(newCommitTime, writeStatuses);
      }

      // Ensure all commits were synced to the Metadata Table
      HoodieTableMetaClient metadataMetaClient = createMetaClientForMetadataTable();
      LOG.warn("total commits in metadata table " + metadataMetaClient.getActiveTimeline().getCommitsTimeline().countInstants());

      // 6 commits and 2 cleaner commits.
      assertEquals(metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().countInstants(), 8);
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
    context = new HoodieJavaEngineContext(storageConf);
    HoodieWriteConfig config = getSmallInsertWriteConfig(2000, TRIP_EXAMPLE_SCHEMA, 10, false);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    // Write 1 (Bulk insert)
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
    client.startCommitWithTime(newCommitTime);
    List<WriteStatus> writeStatuses = client.insert(records, newCommitTime);
    assertNoWriteErrors(writeStatuses);
    validateMetadata(client);

    // Write 2 (inserts)
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    client.startCommitWithTime(newCommitTime);
    records = dataGen.generateInserts(newCommitTime, 20);
    writeStatuses = client.insert(records, newCommitTime);
    assertNoWriteErrors(writeStatuses);
    validateMetadata(client);

    // setup clustering config.
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringSortColumns("_row_key").withInlineClustering(true)
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(1)
        .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
        .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName())
        .build();

    HoodieWriteConfig newWriteConfig = getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER)
        .withAutoCommit(false)
        .withClusteringConfig(clusteringConfig)
        .withRollbackUsingMarkers(false)
        .build();

    // trigger clustering
    HoodieJavaWriteClient newClient = getHoodieWriteClient(newWriteConfig);
    String clusteringCommitTime = newClient.scheduleClustering(Option.empty()).get().toString();
    HoodieWriteMetadata<List<WriteStatus>> clusterMetadata = newClient.cluster(clusteringCommitTime, true);

    // collect replaceFileIds for validation later.
    Set<HoodieFileGroupId> replacedFileIds = new HashSet<>();
    clusterMetadata.getPartitionToReplaceFileIds().entrySet().forEach(partitionFiles ->
        partitionFiles.getValue().stream().forEach(file ->
            replacedFileIds.add(new HoodieFileGroupId(partitionFiles.getKey(), file))));

    // trigger new write to mimic other writes succeeding before re-attempt.
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    client.startCommitWithTime(newCommitTime);
    records = dataGen.generateInserts(newCommitTime, 20);
    writeStatuses = client.insert(records, newCommitTime);
    assertNoWriteErrors(writeStatuses);
    validateMetadata(client);

    // manually remove clustering completed instant from .hoodie folder and to mimic succeeded clustering in metadata table, but failed in data table.
    FileCreateUtils.deleteReplaceCommit(basePath, clusteringCommitTime);
    HoodieWriteMetadata<List<WriteStatus>> updatedClusterMetadata = newClient.cluster(clusteringCommitTime, true);

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
    context = new HoodieJavaEngineContext(storageConf);
    HoodieWriteConfig initialConfig = getSmallInsertWriteConfig(2000, TRIP_EXAMPLE_SCHEMA, 10, false);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProperties(initialConfig.getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(4).build()).build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    // Write 1 (Bulk insert)
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
    client.startCommitWithTime(newCommitTime);
    List<WriteStatus> writeStatuses = client.insert(records, newCommitTime);
    assertNoWriteErrors(writeStatuses);
    validateMetadata(client);

    // Write 2 (inserts)
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    client.startCommitWithTime(newCommitTime);
    records = dataGen.generateInserts(newCommitTime, 20);
    writeStatuses = client.insert(records, newCommitTime);
    assertNoWriteErrors(writeStatuses);
    validateMetadata(client);

    // setup clustering config.
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringSortColumns("_row_key").withInlineClustering(true)
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(1)
        .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
        .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName())
        .build();

    HoodieWriteConfig newWriteConfig = getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER)
        .withAutoCommit(false)
        .withClusteringConfig(clusteringConfig).build();

    // trigger clustering
    HoodieJavaWriteClient newClient = getHoodieWriteClient(newWriteConfig);
    String clusteringCommitTime = newClient.scheduleClustering(Option.empty()).get().toString();
    HoodieWriteMetadata<List<WriteStatus>> clusterMetadata = newClient.cluster(clusteringCommitTime, true);

    // manually remove clustering completed instant from .hoodie folder and to mimic succeeded clustering in metadata table, but failed in data table.
    FileCreateUtils.deleteReplaceCommit(basePath, clusteringCommitTime);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieWriteConfig updatedWriteConfig = HoodieWriteConfig.newBuilder().withProperties(initialConfig.getProps())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(4).build())
        .withRollbackUsingMarkers(false).build();

    client = getHoodieWriteClient(updatedWriteConfig);

    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    client.startCommitWithTime(newCommitTime);
    records = dataGen.generateInserts(newCommitTime, 20);
    writeStatuses = client.insert(records, newCommitTime);
    assertNoWriteErrors(writeStatuses);
    validateMetadata(client, Option.of(clusteringCommitTime));
  }

  @Test
  public void testMetadataReadWithNoCompletedCommits() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    List<HoodieRecord> records;
    List<WriteStatus> writeStatuses;
    String[] commitTimestamps = {HoodieActiveTimeline.createNewInstantTime(), HoodieActiveTimeline.createNewInstantTime()};

    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, getWriteConfig(true, true))) {
      records = dataGen.generateInserts(commitTimestamps[0], 5);
      client.startCommitWithTime(commitTimestamps[0]);
      writeStatuses = client.bulkInsert(records, commitTimestamps[0]);
      assertNoWriteErrors(writeStatuses);

      // make all commits to inflight in metadata table. Still read should go through, just that it may not return any data.
      FileCreateUtils.deleteDeltaCommit(basePath + "/.hoodie/metadata/", commitTimestamps[0]);
      FileCreateUtils.deleteDeltaCommit(basePath + " /.hoodie/metadata/", HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP);
      assertEquals(getAllFiles(metadata(client)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet()).size(), 0);
    }
  }


  /**
   * Ensure that the reader only reads completed instants.
   */
  @Test
  public void testReader() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    List<HoodieRecord> records;
    List<WriteStatus> writeStatuses;
    String[] commitTimestamps = {HoodieActiveTimeline.createNewInstantTime(), HoodieActiveTimeline.createNewInstantTime(),
        HoodieActiveTimeline.createNewInstantTime(), HoodieActiveTimeline.createNewInstantTime()};

    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, getWriteConfig(true, true))) {
      for (int i = 0; i < commitTimestamps.length; ++i) {
        records = dataGen.generateInserts(commitTimestamps[i], 5);
        client.startCommitWithTime(commitTimestamps[i]);
        writeStatuses = client.bulkInsert(records, commitTimestamps[i]);
        assertNoWriteErrors(writeStatuses);
      }

      // Ensure we can see files from each commit
      Set<String> timelineTimestamps = getAllFiles(metadata(client)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet());
      assertEquals(timelineTimestamps.size(), commitTimestamps.length);
      for (int i = 0; i < commitTimestamps.length; ++i) {
        assertTrue(timelineTimestamps.contains(commitTimestamps[i]));
      }

      // mark each commit as incomplete and ensure files are not seen
      for (int i = 0; i < commitTimestamps.length; ++i) {
        FileCreateUtils.deleteCommit(basePath, commitTimestamps[i]);
        timelineTimestamps = getAllFiles(metadata(client)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet());
        assertEquals(timelineTimestamps.size(), commitTimestamps.length - 1);
        for (int j = 0; j < commitTimestamps.length; ++j) {
          assertTrue(j == i || timelineTimestamps.contains(commitTimestamps[j]));
        }
        FileCreateUtils.createCommit(basePath, commitTimestamps[i]);
      }

      // Test multiple incomplete commits
      FileCreateUtils.deleteCommit(basePath, commitTimestamps[0]);
      FileCreateUtils.deleteCommit(basePath, commitTimestamps[2]);
      timelineTimestamps = getAllFiles(metadata(client)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet());
      assertEquals(timelineTimestamps.size(), commitTimestamps.length - 2);
      for (int j = 0; j < commitTimestamps.length; ++j) {
        assertTrue(j == 0 || j == 2 || timelineTimestamps.contains(commitTimestamps[j]));
      }

      // Test no completed commits
      for (int i = 0; i < commitTimestamps.length; ++i) {
        FileCreateUtils.deleteCommit(basePath, commitTimestamps[i]);
      }
      timelineTimestamps = getAllFiles(metadata(client)).stream().map(p -> p.getName()).map(n -> FSUtils.getCommitTime(n)).collect(Collectors.toSet());
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
    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

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
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, config)) {
      // Some initial commits so compaction is not triggered.
      // 1 deltacommit will be from bootstrap. So we can perform maxDeltaCommitsBeforeCompaction - 2 more commits before
      // compaction will be attempted.
      for (int i = 0; i < maxDeltaCommitsBeforeCompaction - 2; ++i) {
        newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        records = dataGen.generateInserts(newCommitTime, 5);
        client.startCommitWithTime(newCommitTime);
        client.insert(records, newCommitTime);
      }

      HoodieTableMetaClient metadataMetaClient = createMetaClientForMetadataTable();
      HoodieTableMetaClient datasetMetaClient = createMetaClient();

      // There should not be any compaction yet and we have not performed more than maxDeltaCommitsBeforeCompaction
      // deltacommits (1 will be due to bootstrap)
      HoodieActiveTimeline metadataTimeline = metadataMetaClient.reloadActiveTimeline();
      assertEquals(metadataTimeline.getCommitAndReplaceTimeline().filterCompletedInstants().countInstants(), 0);
      assertEquals(metadataTimeline.getCommitsTimeline().filterCompletedInstants().countInstants(), maxDeltaCommitsBeforeCompaction - 1);
      assertEquals(datasetMetaClient.getArchivedTimeline().reload().countInstants(), 0);

      // Next commit will initiate a compaction
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateInserts(newCommitTime, 5);
      client.startCommitWithTime(newCommitTime);
      client.insert(records, newCommitTime);
      metadataTimeline = metadataMetaClient.reloadActiveTimeline();
      assertEquals(metadataTimeline.getCommitAndReplaceTimeline().filterCompletedInstants().countInstants(), 1);
      assertEquals(metadataTimeline.getCommitsTimeline().filterCompletedInstants().countInstants(), maxDeltaCommitsBeforeCompaction + 1);
      assertEquals(datasetMetaClient.getArchivedTimeline().reload().countInstants(), 0);

      // More than maxDeltaCommitsBeforeCompaction commits
      String inflightCommitTime = newCommitTime;
      for (int i = 0; i < maxDeltaCommitsBeforeCompaction + 1; ++i) {
        newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        records = dataGen.generateInserts(newCommitTime, 5);
        client.startCommitWithTime(newCommitTime);
        client.insert(records, newCommitTime);
        if (i == 0) {
          // Mark this commit inflight so compactions don't take place
          FileCreateUtils.deleteCommit(basePath, newCommitTime);
          FileCreateUtils.createInflightCommit(basePath, newCommitTime);
          inflightCommitTime = newCommitTime;
        }
      }

      // Ensure no more compactions took place due to the leftover inflight commit
      metadataTimeline = metadataMetaClient.reloadActiveTimeline();
      assertEquals(metadataTimeline.getCommitAndReplaceTimeline().filterCompletedInstants().countInstants(), 1);
      assertEquals(metadataTimeline.getDeltaCommitTimeline().filterCompletedInstants().countInstants(),
          ((2 * maxDeltaCommitsBeforeCompaction) + (maxDeltaCommitsBeforeCompaction /* clean from dataset */) + 1)/* clean in metadata table */);

      // Complete commit
      FileCreateUtils.createCommit(basePath, inflightCommitTime);

      // Next commit should lead to compaction
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateInserts(newCommitTime, 5);
      client.startCommitWithTime(newCommitTime);
      client.insert(records, newCommitTime);

      // Ensure compactions took place
      metadataTimeline = metadataMetaClient.reloadActiveTimeline();
      assertEquals(metadataTimeline.getCommitAndReplaceTimeline().filterCompletedInstants().countInstants(), 2);
      assertEquals(metadataTimeline.getDeltaCommitTimeline().filterCompletedInstants().countInstants(),
          ((2 * maxDeltaCommitsBeforeCompaction) + (maxDeltaCommitsBeforeCompaction + 1 /* clean from dataset */) + 2 /* clean in metadata table */));
      assertTrue(datasetMetaClient.getArchivedTimeline().reload().countInstants() > 0);

      validateMetadata(client);
    }
  }

  /**
   * When table needs to be upgraded and when multi writer is enabled, hudi rolls back partial commits. Upgrade itself is happening
   * within a lock and hence rollback should not lock again.
   */
  @Test
  public void testRollbackDuringUpgradeForDoubleLocking() throws IOException {
    init(HoodieTableType.COPY_ON_WRITE, false);
    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    // Perform a commit. This should bootstrap the metadata table with latest version.
    List<HoodieRecord> records;
    List<WriteStatus> writeStatuses;
    String commitTimestamp = HoodieActiveTimeline.createNewInstantTime();
    Properties properties = new Properties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "3");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(false, true, false)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .withProperties(properties)
        .build();
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {
      records = dataGen.generateInserts(commitTimestamp, 5);
      client.startCommitWithTime(commitTimestamp);
      writeStatuses = client.insert(records, commitTimestamp);
      client.commit(commitTimestamp, writeStatuses);
    }

    // Metadata table should have been bootstrapped
    assertTrue(storage.exists(new StoragePath(metadataTableBasePath)),
        "Metadata table should exist");
    StoragePathInfo oldInfo = storage.getPathInfo(new StoragePath(metadataTableBasePath));

    // trigger partial commit
    metaClient.reloadActiveTimeline();
    commitTimestamp = HoodieActiveTimeline.createNewInstantTime();
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {
      records = dataGen.generateInserts(commitTimestamp, 5);
      client.startCommitWithTime(commitTimestamp);
      writeStatuses = client.insert(records, commitTimestamp);
    }

    // set hoodie.table.version to 2 in hoodie.properties file
    changeTableVersion(HoodieTableVersion.TWO);
    writeConfig = getWriteConfigBuilder(true, true, false).withRollbackUsingMarkers(false).withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).withAutoClean(false).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .withProperties(properties)
        .withEmbeddedTimelineServerEnabled(false)
        .build();

    // With next commit the table should be re-bootstrapped and partial commit should be rolled back.
    metaClient.reloadActiveTimeline();
    commitTimestamp = HoodieActiveTimeline.createNewInstantTime();
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {
      records = dataGen.generateInserts(commitTimestamp, 5);
      client.startCommitWithTime(commitTimestamp);
      writeStatuses = client.insert(records, commitTimestamp);
      assertNoWriteErrors(writeStatuses);
    }

    initMetaClient();
    assertEquals(metaClient.getTableConfig().getTableVersion().versionCode(),
        HoodieTableVersion.current().versionCode());
    assertTrue(storage.exists(new StoragePath(metadataTableBasePath)),
        "Metadata table should exist");
    StoragePathInfo newInfo = storage.getPathInfo(new StoragePath(metadataTableBasePath));
    assertTrue(oldInfo.getModificationTime() < newInfo.getModificationTime());
  }

  /**
   * Tests rollback of a commit which has new partitions which is not present in hudi table prior to the commit being rolled back.
   */
  @Test
  public void testRollbackOfPartiallyFailedCommitWithNewPartitions() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext,
        getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true, true, false, false, false).build(),
        true, Option.empty())) {
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
      List<WriteStatus> writeStatuses = client.upsert(upsertRecords, newCommitTime);
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 20);
      writeStatuses = client.insert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // There is no way to simulate failed commit on the main dataset, hence we simply delete the completed
      // instant so that only the inflight is left over.
      String commitInstantFileName = HoodieTimeline.makeCommitFileName(newCommitTime);
      assertTrue(storage.deleteFile(new StoragePath(basePath + StoragePath.SEPARATOR + METAFOLDER_NAME,
          commitInstantFileName)));
    }

    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext,
        getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true, true, false, false, false).build(),
        true, Option.empty())) {
      String newCommitTime = client.startCommit();
      // Next insert
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      List<WriteStatus> writeStatuses = client.upsert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
    }
  }

  /**
   * Hoodie.properties indicates metadata is enabled, however metadata folder is missing.
   */
  @Test
  public void testBootstrapWithTableNotFound() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieJavaEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    // create initial commit
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false).build();
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {
      // Write
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 1);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.insert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
    }

    final StoragePath metadataTablePath =
        new StoragePath(getMetadataTableBasePath(writeConfig.getBasePath()));
    assertTrue(storage.exists(metadataTablePath), "metadata table should exist.");

    deleteMetadataTable(metaClient, context, false);
    assertFalse(storage.exists(metadataTablePath),
        "metadata table should not exist after being deleted.");

    writeConfig = getWriteConfigBuilder(true, true, false).build();
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {
      // Write
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 1);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.insert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);
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

    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, true).build();
    initWriteConfigAndMetatableWriter(writeConfig, true);
    testTable.doWriteOperation(HoodieActiveTimeline.createNewInstantTime(), INSERT, Collections.EMPTY_LIST, 0);
    syncTableMetadata(writeConfig);
    validateMetadata(testTable);
  }

  /**
   * Test various error scenarios.
   */
  @Test
  public void testErrorCases() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    // TESTCASE: If commit on the metadata table succeeds but fails on the dataset, then on next init the metadata table
    // should be rolled back to last valid commit.
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext,
        getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true, true, false, false, false).build(),
        true, Option.empty())) {
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 5);
      writeStatuses = client.bulkInsert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // There is no way to simulate failed commit on the main dataset, hence we simply delete the completed
      // instant so that only the inflight is left over.
      String commitInstantFileName = HoodieTimeline.makeCommitFileName(newCommitTime);
      assertTrue(storage.deleteFile(new StoragePath(basePath + StoragePath.SEPARATOR + METAFOLDER_NAME,
          commitInstantFileName)));
    }

    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext,
        getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true, true, false, false, false).build(),
        true, Option.empty())) {
      String newCommitTime = client.startCommit();
      // Next insert
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 5);
      List<WriteStatus> writeStatuses = client.upsert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);

      // Post rollback commit and metadata should be valid
      validateMetadata(client);
    }
  }

  @Test
  public void testMetadataTableWithLongLog() throws Exception {
    init(COPY_ON_WRITE, false);
    final int maxNumDeltacommits = 3;
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .enableMetrics(false)
            .withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltacommits + 100)
            .withMaxNumDeltacommitsWhenPending(maxNumDeltacommits)
            .build()).build();
    initWriteConfigAndMetatableWriter(writeConfig, true);
    testTable.addRequestedCommit(String.format("%016d", 0));
    for (int i = 1; i <= maxNumDeltacommits; i++) {
      doWriteOperation(testTable, String.format("%016d", i));
    }
    int instant = maxNumDeltacommits + 1;
    Throwable t = assertThrows(HoodieMetadataException.class, () -> doWriteOperation(testTable, String.format("%016d", instant)));
    assertTrue(t.getMessage().startsWith(String.format("Metadata table's deltacommits exceeded %d: ", maxNumDeltacommits)));
  }

  @Test
  public void testNonPartitioned() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE, false);
    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    HoodieTestDataGenerator nonPartitionedGenerator = new HoodieTestDataGenerator(new String[] {""});
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, getWriteConfig(true, true))) {
      // Write 1 (Bulk insert)
      String newCommitTime = "0000001";
      List<HoodieRecord> records = nonPartitionedGenerator.generateInserts(newCommitTime, 10);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(records, newCommitTime);
      validateMetadata(client);

      List<String> metadataPartitions = metadata(client).getAllPartitionPaths();
      assertTrue(metadataPartitions.contains(""), "Must contain empty partition");
    }
  }

  /**
   * Test various metrics published by metadata table.
   */
  @Test
  public void testMetadataMetrics() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE, false);
    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, true).build();
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {
      // Write
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.insert(records, newCommitTime);
      assertNoWriteErrors(writeStatuses);
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
    String fileId = HoodieTableMetadataUtil.getFileIDForFileGroup(FILES, index);
    assertEquals(fileId.substring(0, fileId.length() - 2), HoodieTableMetadataUtil.getFileGroupPrefix(fileId));
    assertEquals(index, HoodieTableMetadataUtil.getFileGroupIndexFromFileId(fileId));

    assertEquals(HoodieTableMetadataUtil.getFileGroupPrefix("some-file-id-0"), "some-file-id");
    assertEquals(HoodieTableMetadataUtil.getFileGroupPrefix("some-file-id"), "some-file-id");
    assertEquals(HoodieTableMetadataUtil.getFileGroupPrefix("some-file-id-2"), "some-file-id-2");
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
    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    Properties props = new Properties();
    props.put(HoodieCleanConfig.ALLOW_MULTIPLE_CLEANS.key(), "false");
    HoodieWriteConfig writeConfig = getWriteConfigBuilder(true, true, false).withProps(props).build();

    // Perform three writes so we can initiate a clean
    int index = 0;
    final String partition = "2015/03/16";
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {
      for (; index < 3; ++index) {
        String newCommitTime = "00" + index;
        List<HoodieRecord> records = index == 0 ? dataGen.generateInsertsForPartition(newCommitTime, 10, partition)
            : dataGen.generateUniqueUpdates(newCommitTime, 5);
        client.startCommitWithTime(newCommitTime);
        client.upsert(records, newCommitTime);
      }
    }
    assertEquals(metaClient.reloadActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants().countInstants(), 3);

    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(engineContext, writeConfig)) {
      // Perform a clean
      String cleanInstantTime = "00" + index++;
      HoodieCleanMetadata cleanMetadata = client.clean(cleanInstantTime);
      // 1 partition should be cleaned
      assertEquals(cleanMetadata.getPartitionMetadata().size(), 1);
      // 1 file cleaned
      assertEquals(
          cleanMetadata.getPartitionMetadata().get(partition).getSuccessDeleteFiles().size(), 1);
      assertEquals(
          cleanMetadata.getPartitionMetadata().get(partition).getFailedDeleteFiles().size(), 0);
      assertEquals(
          cleanMetadata.getPartitionMetadata().get(partition).getDeletePathPatterns().size(), 1);

      // To simulate failed clean on the main dataset, we will delete the completed clean instant
      String cleanInstantFileName = HoodieTimeline.makeCleanerFileName(cleanInstantTime);
      assertTrue(storage.deleteFile(new StoragePath(basePath + StoragePath.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME,
          cleanInstantFileName)));
      assertEquals(metaClient.reloadActiveTimeline().getCleanerTimeline().filterInflights().countInstants(), 1);
      assertEquals(metaClient.reloadActiveTimeline().getCleanerTimeline().filterCompletedInstants().countInstants(), 0);

      // Initiate another clean. The previous leftover clean will be attempted and no other clean will be scheduled.
      String newCleanInstantTime = "00" + index++;
      cleanMetadata = client.clean(newCleanInstantTime);

      // 1 partition should be cleaned
      assertEquals(cleanMetadata.getPartitionMetadata().size(), 1);
      // 1 file cleaned but was already deleted so will be a failed delete
      assertEquals(
          cleanMetadata.getPartitionMetadata().get(partition).getSuccessDeleteFiles().size(), 0);
      assertEquals(
          cleanMetadata.getPartitionMetadata().get(partition).getFailedDeleteFiles().size(), 1);
      assertEquals(
          cleanMetadata.getPartitionMetadata().get(partition).getDeletePathPatterns().size(), 1);

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

  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, String schemaStr, long smallFileSize, boolean mergeAllowDuplicateInserts) {
    HoodieWriteConfig.Builder builder = getConfigBuilder(schemaStr, HoodieIndex.IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER);
    return builder.withCompactionConfig(
            HoodieCompactionConfig.newBuilder()
                .compactionSmallFileSize(smallFileSize)
                // Set rollback to LAZY so no inflights are deleted
                .insertSplitSize(insertSplitSize).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withStorageConfig(
            HoodieStorageConfig.newBuilder()
                .hfileMaxFileSize(dataGen.getEstimatedFileSizeInBytes(200))
                .parquetMaxFileSize(dataGen.getEstimatedFileSizeInBytes(200)).build())
        .withMergeAllowDuplicateOnInserts(mergeAllowDuplicateInserts)
        .build();
  }

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
        .withEmbeddedTimelineServerEnabled(false).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withEnableBackupForRemoteFileSystemView(false) // Fail test if problem connecting to timeline-server
            .build());
  }

  @Test
  public void testClusterOperationOnMainTable() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    // Disable small file handling that way multiple files are created for small batches.
    Properties props = new Properties();
    props.setProperty(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "0");
    props.setProperty("hoodie.clustering.plan.strategy.class", JavaSizeBasedClusteringPlanStrategy.class.getName());
    HoodieWriteConfig cfg = getWriteConfigBuilder(true, true, false)
        .withProps(props).build();
    // Initialize metadata writers.
    HoodieJavaWriteClient client = getHoodieWriteClient(cfg);

    // Insert one batch 0000001
    String commitTime = "0000001";
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 100);
    client.startCommitWithTime(commitTime);
    List<WriteStatus> writeStatuses = client.insert(records, commitTime);
    assertNoWriteErrors(writeStatuses);

    // Insert second batch 0000002
    commitTime = "0000002";
    records = dataGen.generateInserts(commitTime, 100);
    client.startCommitWithTime(commitTime);
    writeStatuses = client.insert(records, commitTime);
    assertNoWriteErrors(writeStatuses);

    // Schedule clustering operation 0000003
    HoodieWriteConfig clusterWriteCfg = getWriteConfigBuilder(true, true, false)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withInlineClusteringNumCommits(0)
            .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName())
            .build())
        .build();
    HoodieJavaWriteClient clusteringClient = getHoodieWriteClient(clusterWriteCfg);
    clusteringClient.scheduleTableService("0000003", Option.empty(), TableServiceType.CLUSTER);

    // Execute pending clustering operation
    clusteringClient.cluster("0000003", true);

    // verify metadata table
    validateMetadata(client);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTableMetadata tableMetadata = metadata(client);
    HoodieMetadataFileSystemView metadataFileSystemView = new HoodieMetadataFileSystemView(
        metaClient, metaClient.reloadActiveTimeline(), tableMetadata);
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
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
    HoodieJavaWriteClient client = getHoodieWriteClient(cfg);

    // Insert one batch 0000001
    String commitTime = "0000001";
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 100);
    client.startCommitWithTime(commitTime);
    List<WriteStatus> writeStatuses = client.insert(records, commitTime);
    assertNoWriteErrors(writeStatuses);

    // Insert second batch 0000002
    commitTime = "0000002";
    records = dataGen.generateInserts(commitTime, 100);
    client.startCommitWithTime(commitTime);
    writeStatuses = client.insert(records, commitTime);
    assertNoWriteErrors(writeStatuses);

    // Schedule clustering operation 0000003
    HoodieWriteConfig clusterWriteCfg = getWriteConfigBuilder(true, true, false)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withInlineClusteringNumCommits(0)
            .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName())
            .build())
        .build();
    HoodieJavaWriteClient clusteringClient = getHoodieWriteClient(clusterWriteCfg);
    clusteringClient.scheduleTableService("0000003", Option.empty(), TableServiceType.CLUSTER);

    // Insert second batch 0000004
    commitTime = "0000004";
    records = dataGen.generateInserts(commitTime, 100);
    client = getHoodieWriteClient(cfg);
    client.startCommitWithTime(commitTime);
    writeStatuses = client.insert(records, commitTime);
    assertNoWriteErrors(writeStatuses);

    // verify metadata table
    validateMetadata(client);

    // Execute compaction on metadata table.
    try (JavaHoodieBackedTableMetadataWriter metadataWriter =
             (JavaHoodieBackedTableMetadataWriter) JavaHoodieBackedTableMetadataWriter.create(storageConf, client.getConfig(), context, Option.empty())) {
      Properties metadataProps = metadataWriter.getWriteConfig().getProps();
      metadataProps.setProperty(INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "3");
      HoodieWriteConfig metadataWriteConfig = HoodieWriteConfig.newBuilder()
          .withProperties(metadataProps).build();
      try (HoodieJavaWriteClient metadataWriteClient = new HoodieJavaWriteClient(context, metadataWriteConfig)) {
        final String compactionInstantTime = HoodieTableMetadataUtil.createCompactionTimestamp(commitTime);
        assertTrue(metadataWriteClient.scheduleCompactionAtInstant(compactionInstantTime, Option.empty()));
        metadataWriteClient.compact(compactionInstantTime);

        // verify metadata table
        validateMetadata(client);

        // Execute pending clustering operation
        clusteringClient = getHoodieWriteClient(clusterWriteCfg);
        clusteringClient.cluster("0000003", true);

        // verify metadata table
        validateMetadata(client);
      }
    }
  }

  private void validateMetadata(HoodieJavaWriteClient testClient) throws Exception {
    validateMetadata(testClient, Option.empty());
  }

  private void validateMetadata(HoodieJavaWriteClient testClient, Option<String> ignoreFilesWithCommit) throws Exception {
    HoodieWriteConfig config = testClient.getConfig();

    HoodieJavaWriteClient client;
    if (config.isEmbeddedTimelineServerEnabled()) {
      testClient.close();
      client = new HoodieJavaWriteClient(testClient.getEngineContext(), testClient.getConfig());
    } else {
      client = testClient;
    }
    clientsToClose.add(client);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTableMetadata tableMetadata = metadata(client);
    assertNotNull(tableMetadata, "MetadataReader should have been initialized");
    if (!config.isMetadataTableEnabled()) {
      return;
    }

    HoodieTimer timer = HoodieTimer.start();
    HoodieEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    // Partitions should match
    FileSystemBackedTableMetadata fsBackedTableMetadata = new FileSystemBackedTableMetadata(engineContext, metaClient.getTableConfig(),
        metaClient.getStorage(), config.getBasePath(), config.shouldAssumeDatePartitioning());
    List<String> fsPartitions = fsBackedTableMetadata.getAllPartitionPaths();
    List<String> metadataPartitions = tableMetadata.getAllPartitionPaths();

    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    assertEquals(fsPartitions.size(), metadataPartitions.size(), "Partitions should match");
    assertTrue(fsPartitions.equals(metadataPartitions), "Partitions should match");

    // Files within each partition should match
    HoodieTable table = HoodieJavaTable.create(config, engineContext);
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

        List<StoragePathInfo> allFilesList =
            FSUtils.getAllDataFilesInPartition(storage, partitionPath);
        if (ignoreFilesWithCommit.isPresent()) {
          allFilesList = allFilesList.stream()
              .filter(fileStatus -> !fileStatus.getPath().getName()
                  .contains(ignoreFilesWithCommit.get()))
              .collect(Collectors.toList());
        }
        List<StoragePathInfo> metaFilesList = tableMetadata.getAllFilesInPartition(partitionPath);
        List<String> fsFileNames = allFilesList.stream()
            .map(s -> s.getPath().getName()).collect(Collectors.toList());
        List<String> metadataFilenames = metaFilesList.stream()
            .map(s -> s.getPath().getName()).collect(Collectors.toList());
        Collections.sort(fsFileNames);
        Collections.sort(metadataFilenames);

        assertEquals(allFilesList.size(), partitionToFilesMap.get(partitionPath.toString()).size());

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
            g -> LoggerFactory.getLogger(TestJavaHoodieBackedMetadata.class).info(g.toString()));
        fileGroups.forEach(g -> g.getAllBaseFiles().forEach(
            b -> LoggerFactory.getLogger(TestJavaHoodieBackedMetadata.class).info(b.toString())));
        fileGroups.forEach(g -> g.getAllFileSlices().forEach(
            s -> LoggerFactory.getLogger(TestJavaHoodieBackedMetadata.class).info(s.toString())));

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

    try (HoodieBackedTableMetadataWriter<List<HoodieRecord>> metadataWriter = metadataWriter(client)) {
      assertNotNull(metadataWriter, "MetadataWriter should have been initialized");

      // Validate write config for metadata table
      HoodieWriteConfig metadataWriteConfig = metadataWriter.getWriteConfig();
      assertFalse(metadataWriteConfig.isMetadataTableEnabled(), "No metadata table for metadata table");

      // Metadata table should be in sync with the dataset
      HoodieTableMetaClient metadataMetaClient = createMetaClientForMetadataTable();

      // Metadata table is MOR
      assertEquals(metadataMetaClient.getTableType(), HoodieTableType.MERGE_ON_READ, "Metadata Table should be MOR");

      // Metadata table is HFile format
      assertEquals(metadataMetaClient.getTableConfig().getBaseFileFormat(), HoodieFileFormat.HFILE,
          "Metadata Table base file format should be HFile");

      // Metadata table has a fixed number of partitions
      // Cannot use FSUtils.getAllFoldersWithPartitionMetaFile for this as that function filters all directory
      // in the .hoodie folder.
      List<String> metadataTablePartitions = FSUtils.getAllPartitionPaths(engineContext, storage, getMetadataTableBasePath(basePath), false, false);
      assertEquals(metadataWriter.getEnabledPartitionTypes().size(), metadataTablePartitions.size());

      final Map<String, MetadataPartitionType> metadataEnabledPartitionTypes = new HashMap<>();
      metadataWriter.getEnabledPartitionTypes().forEach(e -> metadataEnabledPartitionTypes.put(e.getPartitionPath(), e));

      // Metadata table should automatically compact and clean
      // versions are +1 as autoclean / compaction happens end of commits
      int numFileVersions = metadataWriteConfig.getCleanerFileVersionsRetained() + 1;
      HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metadataMetaClient, metadataMetaClient.getActiveTimeline());
      metadataTablePartitions.forEach(partition -> {
        List<FileSlice> latestSlices = fsView.getLatestFileSlices(partition).collect(Collectors.toList());
        assertTrue(latestSlices.stream().map(FileSlice::getBaseFile).count() <= latestSlices.size(), "Should have a single latest base file per file group");
        List<HoodieLogFile> logFiles = latestSlices.get(0).getLogFiles().collect(Collectors.toList());
        try {
          if (FILES.getPartitionPath().equals(partition)) {
            verifyMetadataRawRecords(table, logFiles, false);
          }
          if (COLUMN_STATS.getPartitionPath().equals(partition)) {
            verifyMetadataColumnStatsRecords(logFiles);
          }
        } catch (IOException e) {
          LOG.error("Metadata record validation failed", e);
          fail("Metadata record validation failed");
        }
      });

      // TODO: include validation for record_index partition here.
      LOG.info("Validation time=" + timer.endTimer());
    }
  }

  private void verifyMetadataColumnStatsRecords(List<HoodieLogFile> logFiles) throws IOException {
    for (HoodieLogFile logFile : logFiles) {
      List<StoragePathInfo> pathInfoList = storage.listDirectEntries(logFile.getPath());
      Schema writerSchema = TableSchemaResolver.readSchemaFromLogFile(storage,
          logFile.getPath());
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
    List<StoragePath> allfiles = new ArrayList<>();
    for (String partition : metadata.getAllPartitionPaths()) {
      for (StoragePathInfo pathInfo : metadata.getAllFilesInPartition(
          new StoragePath(basePath, partition))) {
        allfiles.add(pathInfo.getPath());
      }
    }

    return allfiles;
  }

  private HoodieBackedTableMetadataWriter<List<HoodieRecord>> metadataWriter(HoodieJavaWriteClient client) {
    return metadataWriter(client.getConfig());
  }

  private HoodieTableMetadata metadata(HoodieJavaWriteClient client) {
    return metadata(client.getConfig(), client.getEngineContext());
  }

  private void changeTableVersion(HoodieTableVersion version) throws IOException {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    metaClient.getTableConfig().setTableVersion(version);
    StoragePath propertyFile =
        new StoragePath(
            metaClient.getMetaPath() + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    try (OutputStream os = metaClient.getStorage().create(propertyFile)) {
      metaClient.getTableConfig().getProps().store(os, "");
    }
  }

  @Override
  protected HoodieTableType getTableType() {
    return tableType;
  }
}
