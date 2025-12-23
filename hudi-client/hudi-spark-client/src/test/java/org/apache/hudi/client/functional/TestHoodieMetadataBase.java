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

import org.apache.hudi.client.timeline.HoodieTimelineArchiver;
import org.apache.hudi.client.timeline.versioning.v2.TimelineArchiverV2;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;

@Slf4j
public class TestHoodieMetadataBase extends HoodieSparkClientTestHarness {

  protected static HoodieTestTable testTable;
  protected String metadataTableBasePath;
  protected HoodieTableType tableType;
  protected HoodieWriteConfig writeConfig;
  protected HoodieTableMetadataWriter metadataWriter;

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

  public void init(HoodieTableType tableType, Option<HoodieWriteConfig> writeConfig, boolean enableMetadataTable,
                   boolean enableMetrics, boolean validateMetadataPayloadStateConsistency) throws IOException {
    this.tableType = tableType;
    initPath();
    initSparkContexts("TestHoodieMetadata");
    initHoodieStorage();
    storage.createDirectory(new StoragePath(basePath));
    initTimelineService();
    initMetaClient(tableType, writeConfig.map(conf -> conf.getProps()).orElse(new TypedProperties()));
    initTestDataGenerator();
    metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
    this.writeConfig = writeConfig.isPresent()
        ? writeConfig.get() : getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true,
        enableMetadataTable, enableMetrics, true,
        validateMetadataPayloadStateConsistency)
        .build();
    initWriteConfigAndMetatableWriter(this.writeConfig, enableMetadataTable);
  }

  protected void initWriteConfigAndMetatableWriter(HoodieWriteConfig writeConfig, boolean enableMetadataTable) throws IOException {
    this.writeConfig = writeConfig;
    if (enableMetadataTable) {
      metadataWriter = SparkHoodieBackedTableMetadataWriter.create(storageConf, writeConfig, context);
      // reload because table configs could have been updated
      metaClient = HoodieTableMetaClient.reload(metaClient);
      testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter, Option.of(context));
    } else {
      testTable = HoodieTestTable.of(metaClient);
    }
  }

  @AfterEach
  public void clean() throws Exception {
    cleanupResources();
    if (metadataWriter != null) {
      metadataWriter.close();
    }
  }

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
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    HoodieTimelineArchiver archiver = new TimelineArchiverV2(writeConfig, table);
    archiver.archiveIfRequired(context);
  }

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
            .withExecutorMetrics(enableMetrics).withReporterType(MetricsReporterType.INMEMORY.name()).build())
        .withRollbackUsingMarkers(useRollbackUsingMarkers)
        .withProperties(properties);
  }

  protected HoodieWriteConfig getMetadataWriteConfig(HoodieWriteConfig writeConfig) {
    return HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig,
        HoodieFailedWritesCleaningPolicy.LAZY, HoodieTableVersion.EIGHT);
  }
}
