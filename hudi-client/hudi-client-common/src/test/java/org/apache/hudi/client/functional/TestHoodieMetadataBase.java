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

import org.apache.hudi.client.HoodieTimelineArchiver;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.metrics.HoodieMetricsGraphiteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

abstract class TestHoodieMetadataBase extends HoodieCommonTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieMetadataBase.class);

  protected static HoodieTestTable testTable;
  protected String metadataTableBasePath;
  protected HoodieTableType tableType;
  protected HoodieWriteConfig writeConfig;
  protected HoodieTableMetadataWriter metadataWriter;
  protected HoodieEngineContext engineContext;
  protected FileSystem fs;

  public void init(HoodieTableType tableType) throws IOException {
    init(tableType, true);
  }

  public void init(HoodieTableType tableType, HoodieWriteConfig writeConfig) throws IOException {
    init(tableType, Option.of(writeConfig), true, false, false);
  }

  public void init(HoodieTableType tableType, boolean enableMetadataTable) throws IOException {
    init(tableType, enableMetadataTable, true, false, false);
  }

  public void init(HoodieTableType tableType, boolean enableMetadataTable, boolean enableColumnStats) throws IOException {
    init(tableType, enableMetadataTable, true, false, false);
  }

  public void init(HoodieTableType tableType, boolean enableMetadataTable, boolean enableFullScan, boolean enableMetrics, boolean
      validateMetadataPayloadStateConsistency) throws IOException {
    init(tableType, Option.empty(), enableMetadataTable, enableMetrics,
        validateMetadataPayloadStateConsistency);
  }

  abstract void init(HoodieTableType tableType, Option<HoodieWriteConfig> writeConfig, boolean enableMetadataTable,
                   boolean enableMetrics, boolean validateMetadataPayloadStateConsistency) throws IOException;

  protected void initWriteConfigAndMetatableWriter(HoodieWriteConfig writeConfig, boolean enableMetadataTable) throws IOException {
    this.writeConfig = writeConfig;
    if (enableMetadataTable) {
      metadataWriter = createTableMetadataWriter(writeConfig, engineContext);
      // reload because table configs could have been updated
      metaClient = HoodieTableMetaClient.reload(metaClient);
      testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter, Option.of(engineContext));
    } else {
      testTable = HoodieTestTable.of(metaClient);
    }
  }

  abstract HoodieBackedTableMetadataWriter createTableMetadataWriter(HoodieWriteConfig writeConfig, HoodieEngineContext context);

  @AfterEach
  public void clean() throws Exception {
    cleanupResources();
  }

  abstract void cleanupResources() throws Exception;

  protected void doWriteInsertAndUpsert(HoodieTestTable testTable, String commit1, String commit2, boolean nonPartitioned) throws Exception {
    testTable.doWriteOperation(commit1, INSERT, nonPartitioned ? asList("") : asList("p1", "p2"), nonPartitioned ? asList("") : asList("p1", "p2"),
        4, false);
    testTable.doWriteOperation(commit2, UPSERT, nonPartitioned ? asList("") : asList("p1", "p2"),
        4, false);
    validateMetadata(testTable);
  }

  protected void doWriteOperationAndValidateMetadata(HoodieTestTable testTable, String commitTime) throws Exception {
    doWriteOperation(testTable, commitTime);
    validateMetadata(testTable);
  }

  protected void doWriteOperation(HoodieTestTable testTable, String commitTime) throws Exception {
    doWriteOperation(testTable, commitTime, UPSERT);
  }

  protected void doWriteOperationAndValidate(HoodieTestTable testTable, String commitTime) throws Exception {
    doWriteOperationAndValidate(testTable, commitTime, UPSERT);
  }

  protected void doWriteOperationAndValidate(HoodieTestTable testTable, String commitTime, WriteOperationType operationType) throws Exception {
    doWriteOperation(testTable, commitTime, operationType);
    validateMetadata(testTable);
  }

  protected void doWriteOperationNonPartitioned(HoodieTestTable testTable, String commitTime, WriteOperationType operationType) throws Exception {
    testTable.doWriteOperation(commitTime, operationType, emptyList(), asList(""), 3);
  }

  protected void doWriteOperation(HoodieTestTable testTable, String commitTime, WriteOperationType operationType, boolean nonPartitioned) throws Exception {
    if (nonPartitioned) {
      doWriteOperationNonPartitioned(testTable, commitTime, operationType);
    } else {
      doWriteOperation(testTable, commitTime, operationType);
    }
  }

  protected void doWriteOperation(HoodieTestTable testTable, String commitTime, WriteOperationType operationType) throws Exception {
    testTable.doWriteOperation(commitTime, operationType, emptyList(), asList("p1", "p2"), 3);
  }

  protected HoodieCommitMetadata doWriteOperationWithMeta(HoodieTestTable testTable, String commitTime, WriteOperationType operationType) throws Exception {
    return testTable.doWriteOperation(commitTime, operationType, emptyList(), asList("p1", "p2"), 3);
  }

  protected void doClean(HoodieTestTable testTable, String commitTime, List<String> commitsToClean) throws IOException {
    doCleanInternal(testTable, commitTime, commitsToClean, false);
  }

  protected void doCleanAndValidate(HoodieTestTable testTable, String commitTime, List<String> commitsToClean) throws IOException {
    doCleanInternal(testTable, commitTime, commitsToClean, true);
  }

  private void doCleanInternal(HoodieTestTable testTable, String commitTime, List<String> commitsToClean, boolean validate) throws IOException {
    testTable.doCleanBasedOnCommits(commitTime, commitsToClean);
    if (validate) {
      validateMetadata(testTable);
    }
  }

  protected void doCompactionNonPartitioned(HoodieTestTable testTable, String commitTime) throws Exception {
    doCompactionInternal(testTable, commitTime, false, true);
  }

  protected void doCompaction(HoodieTestTable testTable, String commitTime, boolean nonPartitioned) throws Exception {
    doCompactionInternal(testTable, commitTime, false, nonPartitioned);
  }

  protected void doCompaction(HoodieTestTable testTable, String commitTime) throws Exception {
    doCompactionInternal(testTable, commitTime, false, false);
  }

  protected void doCompactionNonPartitionedAndValidate(HoodieTestTable testTable, String commitTime) throws Exception {
    doCompactionInternal(testTable, commitTime, true, true);
  }

  protected void doCompactionAndValidate(HoodieTestTable testTable, String commitTime) throws Exception {
    doCompactionInternal(testTable, commitTime, true, false);
  }

  private void doCompactionInternal(HoodieTestTable testTable, String commitTime, boolean validate, boolean nonPartitioned) throws Exception {
    testTable.doCompaction(commitTime, nonPartitioned ? asList("") : asList("p1", "p2"));
    if (validate) {
      validateMetadata(testTable);
    }
  }

  protected void doCluster(HoodieTestTable testTable, String commitTime) throws Exception {
    doClusterInternal(testTable, commitTime, false);
  }

  protected void doClusterAndValidate(HoodieTestTable testTable, String commitTime) throws Exception {
    doClusterInternal(testTable, commitTime, true);
  }

  protected void doClusterInternal(HoodieTestTable testTable, String commitTime, boolean validate) throws Exception {
    testTable.doCluster(commitTime, new HashMap<>(), Arrays.asList("p1", "p2"), 2);
    if (validate) {
      validateMetadata(testTable);
    }
  }

  protected void doRollback(HoodieTestTable testTable, String commitToRollback, String rollbackTime) throws Exception {
    doRollbackInternal(testTable, commitToRollback, rollbackTime, false);
  }

  protected void doRollbackAndValidate(HoodieTestTable testTable, String commitToRollback, String rollbackTime) throws Exception {
    doRollbackInternal(testTable, commitToRollback, rollbackTime, true);
  }

  private void doRollbackInternal(HoodieTestTable testTable, String commitToRollback, String rollbackTime, boolean validate) throws Exception {
    testTable.doRollback(commitToRollback, rollbackTime);
    if (validate) {
      validateMetadata(testTable);
    }
  }

  protected void doPreBootstrapWriteOperation(HoodieTestTable testTable, String commitTime) throws Exception {
    doPreBootstrapWriteOperation(testTable, UPSERT, commitTime);
  }

  protected void doPreBootstrapWriteOperation(HoodieTestTable testTable, WriteOperationType writeOperationType, String commitTime) throws Exception {
    doPreBootstrapWriteOperation(testTable, writeOperationType, commitTime, 2);
  }

  protected void doPreBootstrapWriteOperation(HoodieTestTable testTable, WriteOperationType writeOperationType, String commitTime, int filesPerPartition) throws Exception {
    testTable.doWriteOperation(commitTime, writeOperationType, asList("p1", "p2"), asList("p1", "p2"),
        filesPerPartition, true);
  }

  protected void doPreBootstrapClean(HoodieTestTable testTable, String commitTime, List<String> commitsToClean) throws Exception {
    testTable.doCleanBasedOnCommits(commitTime, commitsToClean);
  }

  protected void doPreBootstrapRollback(HoodieTestTable testTable, String rollbackTime, String commitToRollback) throws Exception {
    testTable.doRollback(commitToRollback, rollbackTime);
  }

  protected void doPrebootstrapCompaction(HoodieTestTable testTable, String commitTime) throws Exception {
    doPrebootstrapCompaction(testTable, commitTime, Arrays.asList("p1", "p2"));
  }

  protected void doPrebootstrapCompaction(HoodieTestTable testTable, String commitTime, List<String> partitions) throws Exception {
    testTable.doCompaction(commitTime, partitions);
  }

  protected void doPreBootstrapCluster(HoodieTestTable testTable, String commitTime) throws Exception {
    testTable.doCluster(commitTime, new HashMap<>(), Arrays.asList("p1", "p2"), 2);
  }

  protected void doPreBootstrapRestore(HoodieTestTable testTable, String restoreTime, String commitToRestore) throws Exception {
    testTable.doRestore(commitToRestore, restoreTime);
  }

  protected void archiveDataTable(HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient) throws IOException {
    HoodieTable table = createTable(writeConfig, engineContext, metaClient);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(writeConfig, table);
    archiver.archiveIfRequired(engineContext);
  }

  abstract HoodieTable createTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient);
  abstract HoodieTable createTable(HoodieWriteConfig config, HoodieEngineContext context);

  protected void validateMetadata(HoodieTestTable testTable) throws IOException {
    validateMetadata(testTable, emptyList());
  }

  protected void validateMetadata(HoodieTestTable testTable, boolean doFullValidation) throws IOException {
    validateMetadata(testTable, emptyList(), doFullValidation);
  }

  protected void validateMetadata(HoodieTestTable testTable, List<String> inflightCommits) throws IOException {
    validateMetadata(testTable, inflightCommits, false);
  }

  protected void validateMetadata(HoodieTestTable testTable, List<String> inflightCommits, boolean doFullValidation) throws IOException {
    validateMetadata(testTable, inflightCommits, writeConfig, metadataTableBasePath, doFullValidation);
  }

  protected HoodieWriteConfig getWriteConfig(boolean autoCommit, boolean useFileListingMetadata) {
    return getWriteConfigBuilder(autoCommit, useFileListingMetadata, false).build();
  }

  protected HoodieWriteConfig.Builder getWriteConfigBuilder(boolean autoCommit, boolean useFileListingMetadata, boolean enableMetrics) {
    return getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, autoCommit, useFileListingMetadata, enableMetrics);
  }

  protected HoodieWriteConfig.Builder getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy policy, boolean autoCommit, boolean useFileListingMetadata,
                                                            boolean enableMetrics) {
    return getWriteConfigBuilder(policy, autoCommit, useFileListingMetadata, enableMetrics, true, false);
  }

  protected HoodieWriteConfig.Builder getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy policy, boolean autoCommit, boolean useFileListingMetadata,
                                                            boolean enableMetrics, boolean useRollbackUsingMarkers,
                                                            boolean validateMetadataPayloadConsistency) {
    Properties properties = new Properties();
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withDeleteParallelism(2).withRollbackParallelism(2).withFinalizeWriteParallelism(2)
        .withAutoCommit(autoCommit)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(0)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(policy)
            .withAutoClean(false).retainCommits(1).retainFileVersions(1)
            .build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(true).forTable("test-trip-table")
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(useFileListingMetadata)
            .enableMetrics(enableMetrics)
            .ignoreSpuriousDeletes(validateMetadataPayloadConsistency)
            .build())
        .withMetricsConfig(HoodieMetricsConfig.newBuilder().on(enableMetrics)
            .withExecutorMetrics(true).build())
        .withMetricsGraphiteConfig(HoodieMetricsGraphiteConfig.newBuilder()
            .usePrefix("unit-test").build())
        .withRollbackUsingMarkers(useRollbackUsingMarkers)
        .withProperties(properties);
  }

  protected HoodieWriteConfig getMetadataWriteConfig(HoodieWriteConfig writeConfig) {
    return HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig, HoodieFailedWritesCleaningPolicy.LAZY);
  }

  public HoodieTableMetadata metadata(HoodieWriteConfig clientConfig, HoodieEngineContext hoodieEngineContext) {
    return HoodieTableMetadata.create(hoodieEngineContext, clientConfig.getMetadataConfig(), clientConfig.getBasePath());
  }

  public void validateMetadata(HoodieTestTable testTable, List<String> inflightCommits, HoodieWriteConfig writeConfig,
                               String metadataTableBasePath, boolean doFullValidation) throws IOException {
    HoodieTableMetadata tableMetadata = metadata(writeConfig, engineContext);
    assertNotNull(tableMetadata, "MetadataReader should have been initialized");
    if (!writeConfig.isMetadataTableEnabled()) {
      return;
    }

    if (tableMetadata instanceof FileSystemBackedTableMetadata || !tableMetadata.getSyncedInstantTime().isPresent()) {
      throw new IllegalStateException("Metadata should have synced some commits or tableMetadata should not be an instance "
          + "of FileSystemBackedTableMetadata");
    }
    assertEquals(inflightCommits, testTable.inflightCommits());

    HoodieTimer timer = HoodieTimer.start();

    // Partitions should match
    List<java.nio.file.Path> fsPartitionPaths = testTable.getAllPartitionPaths();
    List<String> fsPartitions = new ArrayList<>();
    fsPartitionPaths.forEach(entry -> fsPartitions.add(entry.getFileName().toString()));
    if (fsPartitions.isEmpty() && testTable.isNonPartitioned()) {
      fsPartitions.add("");
    }
    List<String> metadataPartitions = tableMetadata.getAllPartitionPaths();

    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    assertEquals(fsPartitions.size(), metadataPartitions.size(), "Partitions should match");
    assertEquals(fsPartitions, metadataPartitions, "Partitions should match");

    // Files within each partition should match
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = createTable(writeConfig, engineContext, metaClient);
    TableFileSystemView tableView = table.getHoodieView();
    List<String> fullPartitionPaths = fsPartitions.stream().map(partition -> basePath + "/" + partition).collect(Collectors.toList());
    Map<String, FileStatus[]> partitionToFilesMap = tableMetadata.getAllFilesInPartitions(fullPartitionPaths);
    assertEquals(fsPartitions.size(), partitionToFilesMap.size());

    fsPartitions.forEach(partition -> {
      try {
        validateFilesPerPartition(testTable, tableMetadata, tableView, partitionToFilesMap, partition);
      } catch (IOException e) {
        fail("Exception should not be raised: " + e);
      }
    });
    if (doFullValidation) {
      runFullValidation(table.getConfig().getMetadataConfig(), writeConfig, metadataTableBasePath, engineContext);
    }

    LOG.info("Validation time=" + timer.endTimer());
  }

  protected void validateFilesPerPartition(HoodieTestTable testTable, HoodieTableMetadata tableMetadata, TableFileSystemView tableView,
                                           Map<String, FileStatus[]> partitionToFilesMap, String partition) throws IOException {
    Path partitionPath;
    if (partition.equals("")) {
      // Should be the non-partitioned case
      partitionPath = new Path(basePath);
    } else {
      partitionPath = new Path(basePath, partition);
    }

    FileStatus[] fsStatuses = testTable.listAllFilesInPartition(partition);
    FileStatus[] metaStatuses = tableMetadata.getAllFilesInPartition(partitionPath);
    List<String> fsFileNames = Arrays.stream(fsStatuses)
        .map(s -> s.getPath().getName()).collect(Collectors.toList());
    List<String> metadataFilenames = Arrays.stream(metaStatuses)
        .map(s -> s.getPath().getName()).collect(Collectors.toList());
    Collections.sort(fsFileNames);
    Collections.sort(metadataFilenames);

    assertLinesMatch(fsFileNames, metadataFilenames);
    assertEquals(fsStatuses.length, partitionToFilesMap.get(partitionPath.toString()).length);

    // Block sizes should be valid
    Arrays.stream(metaStatuses).forEach(s -> assertTrue(s.getBlockSize() > 0));
    List<Long> fsBlockSizes = Arrays.stream(fsStatuses).map(FileStatus::getBlockSize).sorted().collect(Collectors.toList());
    List<Long> metadataBlockSizes = Arrays.stream(metaStatuses).map(FileStatus::getBlockSize).sorted().collect(Collectors.toList());
    assertEquals(fsBlockSizes, metadataBlockSizes);

    assertEquals(fsFileNames.size(), metadataFilenames.size(), "Files within partition " + partition + " should match");
    assertEquals(fsFileNames, metadataFilenames, "Files within partition " + partition + " should match");

    // FileSystemView should expose the same data
    List<HoodieFileGroup> fileGroups = tableView.getAllFileGroups(partition).collect(Collectors.toList());
    fileGroups.addAll(tableView.getAllReplacedFileGroups(partition).collect(Collectors.toList()));

    fileGroups.forEach(g -> LoggerFactory.getLogger(getClass()).info(g.toString()));
    fileGroups.forEach(g -> g.getAllBaseFiles().forEach(b -> LoggerFactory.getLogger(getClass()).info(b.toString())));
    fileGroups.forEach(g -> g.getAllFileSlices().forEach(s -> LoggerFactory.getLogger(getClass()).info(s.toString())));

    long numFiles = fileGroups.stream()
        .mapToLong(g -> g.getAllBaseFiles().count() + g.getAllFileSlices().mapToLong(s -> s.getLogFiles().count()).sum())
        .sum();
    assertEquals(metadataFilenames.size(), numFiles);
  }

  private void runFullValidation(HoodieMetadataConfig metadataConfig,
                                 HoodieWriteConfig writeConfig,
                                 String metadataTableBasePath,
                                 HoodieEngineContext engineContext) {
    HoodieBackedTableMetadataWriter metadataWriter = createTableMetadataWriter(writeConfig, this.engineContext);
    assertNotNull(metadataWriter, "MetadataWriter should have been initialized");

    // Validate write config for metadata table
    HoodieWriteConfig metadataWriteConfig = metadataWriter.getWriteConfig();
    assertFalse(metadataWriteConfig.isMetadataTableEnabled(), "No metadata table for metadata table");

    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(this.engineContext.getHadoopConf().get()).setBasePath(metadataTableBasePath).build();

    // Metadata table is MOR
    assertEquals(metadataMetaClient.getTableType(), HoodieTableType.MERGE_ON_READ, "Metadata Table should be MOR");

    // Metadata table is HFile format
    assertEquals(metadataMetaClient.getTableConfig().getBaseFileFormat(), HoodieFileFormat.HFILE,
        "Metadata Table base file format should be HFile");

    // Metadata table has a fixed number of partitions
    // Cannot use FSUtils.getAllFoldersWithPartitionMetaFile for this as that function filters all directory
    // in the .hoodie folder.
    List<String> metadataTablePartitions = FSUtils.getAllPartitionPaths(engineContext, HoodieTableMetadata.getMetadataTableBasePath(basePath),
        false, false);

    List<MetadataPartitionType> enabledPartitionTypes = metadataWriter.getEnabledPartitionTypes();

    assertEquals(enabledPartitionTypes.size(), metadataTablePartitions.size());

    Map<String, MetadataPartitionType> partitionTypeMap = enabledPartitionTypes.stream()
        .collect(Collectors.toMap(MetadataPartitionType::getPartitionPath, Function.identity()));

    // Metadata table should automatically compact and clean
    // versions are +1 as autoClean / compaction happens end of commits
    int numFileVersions = metadataWriteConfig.getCleanerFileVersionsRetained() + 1;
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metadataMetaClient, metadataMetaClient.getActiveTimeline());
    metadataTablePartitions.forEach(partition -> {
      MetadataPartitionType partitionType = partitionTypeMap.get(partition);

      List<FileSlice> latestSlices = fsView.getLatestFileSlices(partition).collect(Collectors.toList());

      assertTrue(latestSlices.stream().map(FileSlice::getBaseFile).filter(Objects::nonNull).count() > 0, "Should have a single latest base file");
      assertTrue(latestSlices.size() > 0, "Should have a single latest file slice");
      assertTrue(latestSlices.size() <= numFileVersions, "Should limit file slice to "
          + numFileVersions + " but was " + latestSlices.size());
    });
  }
}
