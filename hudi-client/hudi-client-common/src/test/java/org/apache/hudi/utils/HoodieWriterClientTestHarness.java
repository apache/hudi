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

package org.apache.hudi.utils;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodiePreCombineAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.HoodieWriteHelper;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.table.upgrade.SupportsUpgradeDowngrade;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.generic.GenericRecord;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.INFLIGHT;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION;
import static org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion.VERSION_0;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.NULL_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.apache.hudi.common.testutils.Transformations.randomSelectAsHoodieKeys;
import static org.apache.hudi.common.testutils.Transformations.recordsToRecordKeySet;
import static org.apache.hudi.config.HoodieClusteringConfig.SCHEDULE_INLINE_CLUSTERING;
import static org.apache.hudi.testutils.Assertions.assertNoDupesWithinPartition;
import static org.apache.hudi.testutils.Assertions.assertNoDuplicatesInPartition;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class HoodieWriterClientTestHarness extends HoodieCommonTestHarness {
  protected static int timelineServicePort = FileSystemViewStorageConfig.REMOTE_PORT_NUM.defaultValue();
  protected static final String CLUSTERING_FAILURE = "CLUSTERING FAILURE";
  protected static final String CLEANING_FAILURE = "CLEANING FAILURE";

  protected HoodieTestTable testTable;

  protected abstract BaseHoodieWriteClient getHoodieWriteClient(HoodieWriteConfig cfg);

  protected abstract BaseHoodieWriteClient getHoodieWriteClient(HoodieWriteConfig cfg, boolean shouldCloseOlderClient);

  protected void addConfigsForPopulateMetaFields(HoodieWriteConfig.Builder configBuilder, boolean populateMetaFields,
                                                 boolean isMetadataTable) {
    if (!populateMetaFields) {
      configBuilder.withProperties((isMetadataTable ? getPropertiesForMetadataTable() : getPropertiesForKeyGen(populateMetaFields)))
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build());
    }
  }

  protected void addConfigsForPopulateMetaFields(HoodieWriteConfig.Builder configBuilder, boolean populateMetaFields) {
    addConfigsForPopulateMetaFields(configBuilder, populateMetaFields, false);
  }

  public static Properties getPropertiesForKeyGen() {
    return getPropertiesForKeyGen(false);
  }

  public static Properties getPropertiesForKeyGen(boolean populateMetaFields) {
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), String.valueOf(populateMetaFields));
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    return properties;
  }

  protected Properties getPropertiesForMetadataTable() {
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "key");
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "key");
    return properties;
  }

  /**
   * Get Default HoodieWriteConfig for tests.
   *
   * @return Default Hoodie Write Config for tests
   */
  public HoodieWriteConfig getConfig() {
    return getConfigBuilder().build();
  }

  public HoodieWriteConfig getConfig(HoodieIndex.IndexType indexType) {
    return getConfigBuilder(indexType).build();
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  protected HoodieWriteConfig.Builder getConfigBuilder() {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
  }

  protected EngineType getEngineType() {
    return EngineType.SPARK;
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder(HoodieFailedWritesCleaningPolicy cleaningPolicy) {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.BLOOM, cleaningPolicy);
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder(HoodieIndex.IndexType indexType) {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, indexType, HoodieFailedWritesCleaningPolicy.EAGER);
  }

  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr) {
    return getConfigBuilder(schemaStr, HoodieIndex.IndexType.BLOOM, HoodieFailedWritesCleaningPolicy.EAGER);
  }

  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr, HoodieIndex.IndexType indexType) {
    return getConfigBuilder(schemaStr, indexType, HoodieFailedWritesCleaningPolicy.EAGER);
  }

  /**
   * Get Config builder with default configs set.
   *
   * @return Config Builder
   */
  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr, HoodieIndex.IndexType indexType,
                                                    HoodieFailedWritesCleaningPolicy cleaningPolicy) {
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(cleaningPolicy).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).orcMaxFileSize(1024 * 1024).build())
        .forTable(RAW_TRIPS_TEST_NAME)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withEnableBackupForRemoteFileSystemView(false) // Fail test if problem connecting to timeline-server
            .withRemoteServerPort(timelineServicePort).build());
    if (StringUtils.nonEmpty(schemaStr)) {
      builder.withSchema(schemaStr);
    }
    builder.withEngineType(getEngineType());
    return builder;
  }

  // Functional Interfaces for passing lambda and Hoodie Write API contexts

  @FunctionalInterface
  public interface Function2<R, T1, T2> {

    R apply(T1 v1, T2 v2) throws IOException;
  }

  @FunctionalInterface
  public interface Function3<R, T1, T2, T3> {

    R apply(T1 v1, T2 v2, T3 v3) throws IOException;
  }

  /* Auxiliary methods for testing CopyOnWriteStorage with Spark and Java clients
  to avoid code duplication in TestHoodieClientOnCopyOnWriteStorage and TestHoodieJavaClientOnCopyOnWriteStorage */

  protected abstract List<WriteStatus> writeAndVerifyBatch(BaseHoodieWriteClient client, List<HoodieRecord> inserts, String commitTime, boolean populateMetaFields, boolean autoCommitOff)
      throws IOException;

  protected Object castInsertFirstBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime,
                                        String initCommitTime, int numRecordsInThisCommit,
                                        Function3<Object, BaseHoodieWriteClient, Object, String> writeFn, boolean isPreppedAPI,
                                        boolean assertForCommit, int expRecordsInThisCommit, InstantGenerator instantGenerator) throws Exception {
    return castInsertFirstBatch(writeConfig, client, newCommitTime, initCommitTime, numRecordsInThisCommit, writeFn,
        isPreppedAPI, assertForCommit, expRecordsInThisCommit, true, instantGenerator);
  }

  protected Object castInsertFirstBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime,
                                        String initCommitTime, int numRecordsInThisCommit,
                                        Function3<Object, BaseHoodieWriteClient, Object, String> writeFn, boolean isPreppedAPI,
                                        boolean assertForCommit, int expRecordsInThisCommit,
                                        boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator) throws Exception {
    return null; // override in subclasses if needed
  }

  protected Object castWriteBatch(BaseHoodieWriteClient client, String newCommitTime, String prevCommitTime,
                                  Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                  Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                  Function3<Object, BaseHoodieWriteClient, Object, String> writeFn,
                                  boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords,
                                  int expTotalCommits, InstantGenerator instantGenerator) throws Exception {
    return castWriteBatch(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, true, instantGenerator);
  }

  protected Object castWriteBatch(BaseHoodieWriteClient client, String newCommitTime, String prevCommitTime,
                                  Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                  Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                  Function3<Object, BaseHoodieWriteClient, Object, String> writeFn,
                                  boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                  boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator) throws Exception {
    return castWriteBatch(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, filterForCommitTimeWithAssert, instantGenerator, false);
  }

  protected Object castWriteBatch(BaseHoodieWriteClient client, String newCommitTime, String prevCommitTime,
                                  Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                  Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                  Function3<Object, BaseHoodieWriteClient, Object, String> writeFn,
                                  boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                  boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator, boolean skipCommit) throws Exception {
    return null; // override in subclasses if needed
  }

  protected Object castUpdateBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime, String prevCommitTime,
                                   Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                   Function3<Object, BaseHoodieWriteClient, Object, String> writeFn, boolean isPreppedAPI,
                                   boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                   boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator) throws Exception {
    return null; // override in subclasses if needed
  }

  protected Object castDeleteBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime, String prevCommitTime,
                                   String initCommitTime, int numRecordsInThisCommit, boolean isPreppedAPI, boolean assertForCommit,
                                   int expRecordsInThisCommit, int expTotalRecords, TimelineFactory timelineFactory, InstantGenerator instantGenerator) throws Exception {
    return castDeleteBatch(writeConfig, client, newCommitTime, prevCommitTime, initCommitTime, numRecordsInThisCommit, isPreppedAPI,
        assertForCommit, expRecordsInThisCommit, expTotalRecords, true, timelineFactory, instantGenerator);
  }

  protected Object castDeleteBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime,
                                   String prevCommitTime, String initCommitTime, int numRecordsInThisCommit, boolean isPreppedAPI,
                                   boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, boolean filterForCommitTimeWithAssert,
                                   TimelineFactory timelineFactory, InstantGenerator instantGenerator) throws Exception {
    return null; // override in subclasses if needed
  }

  protected Function2<List<HoodieRecord>, String, Integer> generateWrapRecordsFn(boolean isPreppedAPI, HoodieWriteConfig writeConfig,
                                                                                 Function2<List<HoodieRecord>, String, Integer> wrapped) {
    return null; // override in subclasses if needed
  }

  protected String[] assertTheEntireDatasetHasAllRecordsStill(int expectedRecords) {
    return new String[0]; // override in subclasses if needed
  }

  protected void testMergeHandle(HoodieWriteConfig config) throws IOException {
    // override in subclasses if needed
  }

  protected HoodieWriteConfig getRollbackMarkersAndConsistencyGuardWriteConfig(
          boolean rollbackUsingMarkers, boolean enableOptimisticConsistencyGuard, boolean populateMetaFields) {
    Properties properties = new Properties();
    if (!populateMetaFields) {
      properties = getPropertiesForKeyGen();
    }

    return !enableOptimisticConsistencyGuard ? getConfigBuilder().withRollbackUsingMarkers(rollbackUsingMarkers)
            .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true)
                    .withMaxConsistencyCheckIntervalMs(1).withInitialConsistencyCheckIntervalMs(1)
                    .withEnableOptimisticConsistencyGuard(enableOptimisticConsistencyGuard).build())
            .build() :
            getConfigBuilder().withRollbackUsingMarkers(rollbackUsingMarkers)
                    .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder()
                            .withConsistencyCheckEnabled(true)
                            .withEnableOptimisticConsistencyGuard(enableOptimisticConsistencyGuard)
                            .withOptimisticConsistencyGuardSleepTimeMs(1).build())
                    .withProperties(properties).build();
  }

  protected HoodieWriteConfig getConsistencyCheckWriteConfig(boolean enableOptimisticConsistencyGuard) {
    return !enableOptimisticConsistencyGuard ? (getConfigBuilder()
            .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true)
                    .withMaxConsistencyCheckIntervalMs(1).withInitialConsistencyCheckIntervalMs(1).withEnableOptimisticConsistencyGuard(enableOptimisticConsistencyGuard).build())
            .build()) : (getConfigBuilder()
            .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true)
                    .withEnableOptimisticConsistencyGuard(enableOptimisticConsistencyGuard)
                    .withOptimisticConsistencyGuardSleepTimeMs(1).build())
            .build());
  }

  protected HoodieWriteConfig getParallelWritingWriteConfig(HoodieFailedWritesCleaningPolicy cleaningPolicy, boolean populateMetaFields) {
    Properties properties = new Properties();
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "3000");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "20");
    if (!populateMetaFields) {
      properties.putAll(getPropertiesForKeyGen(populateMetaFields));
    }
    return getConfigBuilder()
            .withEmbeddedTimelineServerEnabled(false)
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                    .withFailedWritesCleaningPolicy(cleaningPolicy)
                    .withAutoClean(false).build())
            .withHeartbeatIntervalInMs(3 * 1000)
            .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
                    .withRemoteServerPort(timelineServicePort).build())
            .withLockConfig(HoodieLockConfig.newBuilder()
                    .withLockProvider(InProcessLockProvider.class)
                    .build())
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
            .withProperties(properties).build();
  }

  @NotNull
  protected Set<String> verifyRecordKeys(List<HoodieRecord> expectedRecords, List<WriteStatus> allStatus, List<GenericRecord> records) {
    for (WriteStatus status : allStatus) {
      StoragePath filePath = new StoragePath(basePath, status.getStat().getPath());
      records.addAll(getFileUtilsInstance(createMetaClient()).readAvroRecords(storage, filePath));
    }
    Set<String> expectedKeys = recordsToRecordKeySet(expectedRecords);
    assertEquals(records.size(), expectedKeys.size());
    return expectedKeys;
  }

  protected void verifyRecordsWrittenWithPreservedMetadata(Set<String> commitTimes, List<HoodieRecord> expectedRecords, List<WriteStatus> allStatus) {
    List<GenericRecord> records = new ArrayList<>();
    Set<String> expectedKeys = verifyRecordKeys(expectedRecords, allStatus, records);
    Map<String, List<GenericRecord>> recordsByCommitTime = records.stream()
            .collect(Collectors.groupingBy(r -> r.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString()));
    assertTrue(commitTimes.containsAll(recordsByCommitTime.keySet()));
    Set<String> expectedFileIds = allStatus.stream().map(WriteStatus::getFileId).collect(Collectors.toSet());
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      assertTrue(expectedKeys.contains(recordKey));
      String fileName = record.get(HoodieRecord.FILENAME_METADATA_FIELD).toString();
      assertTrue(expectedFileIds.contains(FSUtils.getFileId(fileName)));
    }
  }

  protected Pair<String, String> getPartitionAndBaseFilePathsFromLatestCommitMetadata(HoodieTableMetaClient metaClient) throws IOException {
    String extension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    HoodieInstant instant = metaClient.getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieCommitMetadata commitMetadata =
        metaClient.getActiveTimeline().readCommitMetadata(instant);
    String filePath = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream).filter(s -> s.getPath().endsWith(extension)).findAny()
        .map(HoodieWriteStat::getPath).orElse(null);
    String partitionPath = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream).filter(s -> s.getPath().endsWith(extension)).findAny()
        .map(HoodieWriteStat::getPartitionPath).orElse(null);
    return Pair.of(partitionPath, metaClient.getBasePath() + StoragePath.SEPARATOR + filePath);
  }

  public static FileFormatUtils getFileUtilsInstance(HoodieTableMetaClient metaClient) {
    return HoodieIOFactory.getIOFactory(metaClient.getStorage())
            .getFileFormatUtils(metaClient.getTableConfig().getBaseFileFormat());
  }

  /**
   * Build Hoodie Write Config for small data file sizes.
   */
  protected HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, boolean useNullSchema, boolean mergeAllowDuplicateInserts) {
    return getSmallInsertWriteConfig(insertSplitSize, useNullSchema, dataGen.getEstimatedFileSizeInBytes(150), mergeAllowDuplicateInserts);
  }

  /**
   * Build Hoodie Write Config for specified small file sizes.
   */
  protected HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, boolean useNullSchema, long smallFileSize, boolean mergeAllowDuplicateInserts) {
    String schemaStr = useNullSchema ? NULL_SCHEMA : TRIP_EXAMPLE_SCHEMA;
    return getSmallInsertWriteConfig(insertSplitSize, schemaStr, smallFileSize, mergeAllowDuplicateInserts);
  }

  protected HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, String schemaStr, long smallFileSize) {
    return getSmallInsertWriteConfig(insertSplitSize, schemaStr, smallFileSize, false);
  }

  protected HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, String schemaStr, long smallFileSize, boolean mergeAllowDuplicateInserts) {
    return getSmallInsertWriteConfig(insertSplitSize, schemaStr, smallFileSize, mergeAllowDuplicateInserts, true, new Properties());
  }

  protected HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, String schemaStr, long smallFileSize, boolean populateMetaFields, Properties props) {
    return getSmallInsertWriteConfig(insertSplitSize, schemaStr, smallFileSize, false, populateMetaFields, props);
  }

  protected HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, String schemaStr, long smallFileSize, boolean mergeAllowDuplicateInserts,
                                                      boolean populateMetaFields, Properties props) {
    HoodieWriteConfig.Builder builder = getConfigBuilder(schemaStr);
    if (!populateMetaFields) {
      builder.withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE).build());
    }
    return builder.withCompactionConfig(HoodieCompactionConfig.newBuilder()
                    .compactionSmallFileSize(smallFileSize)
                    // Set rollback to LAZY so no inflights are deleted
                    .insertSplitSize(insertSplitSize).build())
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                    .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
                    .build())
            .withStorageConfig(HoodieStorageConfig.newBuilder()
                    .hfileMaxFileSize(dataGen.getEstimatedFileSizeInBytes(200))
                    .parquetMaxFileSize(dataGen.getEstimatedFileSizeInBytes(200)).build())
            .withMergeAllowDuplicateOnInserts(mergeAllowDuplicateInserts)
            .withPopulateMetaFields(populateMetaFields)
            .withProps(props)
            .build();
  }

  protected HoodieWriteConfig getSmallInsertWriteConfigForMDT(int insertSplitSize, String schemaStr, long smallFileSize, boolean mergeAllowDuplicateInserts) {
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

  protected HoodieWriteConfig getWriteConfigWithPopulateMetaFieldsAndAllowOperationMetaField(boolean populateMetaFields, boolean allowOperationMetadataField) {
    HoodieWriteConfig.Builder configBuilder = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
            .withAllowOperationMetadataField(allowOperationMetadataField).combineInput(true, true);
    addConfigsForPopulateMetaFields(configBuilder, populateMetaFields);
    return configBuilder.build();
  }

  protected List<HoodieRecord<RawTripTestPayload>> dedupForCopyOnWriteStorage(HoodieWriteConfig writeConfig, HoodieData<HoodieRecord> records,
                                                                              boolean isGlobal, int additionalParallelism, int expectedNumPartitions) {
    HoodieIndex index = mock(HoodieIndex.class);
    when(index.isGlobal()).thenReturn(isGlobal);
    int dedupParallelism = records.getNumPartitions() + additionalParallelism;
    HoodieData<HoodieRecord<RawTripTestPayload>> dedupedRecsRdd = (HoodieData<HoodieRecord<RawTripTestPayload>>) HoodieWriteHelper.newInstance()
            .deduplicateRecords(records, index, dedupParallelism, writeConfig.getSchema(), writeConfig.getProps(), HoodiePreCombineAvroRecordMerger.INSTANCE);
    assertEquals(expectedNumPartitions, dedupedRecsRdd.getNumPartitions());
    List<HoodieRecord<RawTripTestPayload>> dedupedRecs = dedupedRecsRdd.collectAsList();
    assertEquals(isGlobal ? 1 : 2, dedupedRecs.size());
    assertNoDupesWithinPartition(dedupedRecs);
    return dedupedRecs;
  }

  protected HoodieTableMetaClient createMetaClient() {
    return HoodieTestUtils.createMetaClient(storageConf, basePath);
  }

  protected HoodieTableMetaClient createMetaClient(String basePath) {
    return HoodieTestUtils.createMetaClient(storageConf, basePath);
  }

  protected Set<HoodieFileGroupId> getFileGroupIdsFromWriteStatus(List<WriteStatus> statuses) {
    return statuses.stream().map(s -> new HoodieFileGroupId(s.getPartitionPath(), s.getFileId())).collect(Collectors.toSet());
  }

  /**
   * Find all parquet files created as part of clustering. Verify it matches w/ what is found in replace commit metadata.
   *
   * @param partitionPath where parquet files created as part of clustering
   * @throws IOException
   */
  protected void verifyClusteredFilesWithReplaceCommitMetadata(String partitionPath) throws IOException {
    metaClient = HoodieTableMetaClient.reload(createMetaClient());
    HoodieInstant replaceCommitInstant =
            metaClient.getActiveTimeline().getCompletedReplaceTimeline().firstInstant().get();
    HoodieReplaceCommitMetadata replaceCommitMetadata =
        metaClient.getActiveTimeline().readReplaceCommitMetadata(replaceCommitInstant);

    List<String> filesFromReplaceCommit = new ArrayList<>();
    replaceCommitMetadata.getPartitionToWriteStats()
            .forEach((k, v) -> v.forEach(entry -> filesFromReplaceCommit.add(entry.getPath())));

    // find all parquet files created as part of clustering. Verify it matches w/ what is found in replace commit metadata.
    List<StoragePathInfo> pathInfoList =
            storage.listDirectEntries(new StoragePath(basePath, partitionPath));
    List<String> clusteredFiles = pathInfoList.stream()
            .filter(entry -> entry.getPath().getName().contains(replaceCommitInstant.requestedTime()))
            .map(pathInfo -> partitionPath + StoragePath.SEPARATOR + pathInfo.getPath().getName())
            .collect(Collectors.toList());
    assertEquals(clusteredFiles, filesFromReplaceCommit);
  }

  protected static List<Pair<HoodieInstant, HoodieClusteringPlan>> getAndAssertPendingClusteringPlans(boolean scheduleInlineClustering, HoodieTableMetaClient metaClient) {
    List<Pair<HoodieInstant, HoodieClusteringPlan>> pendingClusteringPlans =
            ClusteringUtils.getAllPendingClusteringPlans(metaClient).collect(Collectors.toList());
    if (scheduleInlineClustering) {
      assertEquals(1, pendingClusteringPlans.size());
    } else {
      assertEquals(0, pendingClusteringPlans.size());
    }
    return  pendingClusteringPlans;
  }

  protected void checkTimelineForUpsertsInternal(HoodieTableMetaClient metaClient) {
    HoodieActiveTimeline activeTimeline = TIMELINE_FACTORY.createActiveTimeline(metaClient, false);
    List<HoodieInstant> instants = activeTimeline.getCommitAndReplaceTimeline().getInstants();
    assertEquals(9, instants.size());
    // Timeline should be as per new format corresponding to table version 8
    assertEquals(INSTANT_GENERATOR.createNewInstant(REQUESTED, COMMIT_ACTION, "001"), instants.get(0));
    assertEquals(INSTANT_GENERATOR.createNewInstant(INFLIGHT, COMMIT_ACTION, "001"), instants.get(1));
    assertEquals(INSTANT_GENERATOR.createNewInstant(COMPLETED, COMMIT_ACTION, "001"), instants.get(2));
    assertFalse(instants.get(2).isLegacy());
    assertEquals(INSTANT_GENERATOR.createNewInstant(REQUESTED, COMMIT_ACTION, "004"), instants.get(3));
    assertEquals(INSTANT_GENERATOR.createNewInstant(INFLIGHT, COMMIT_ACTION, "004"), instants.get(4));
    assertEquals(INSTANT_GENERATOR.createNewInstant(COMPLETED, COMMIT_ACTION, "004"), instants.get(5));
    assertFalse(instants.get(5).isLegacy());
    // New Format should have all states of instants
    // Should be corresponding to table version 8
    assertFalse(instants.get(8).isLegacy());
    assertEquals(INSTANT_GENERATOR.createNewInstant(REQUESTED, COMMIT_ACTION, "006"), instants.get(6));
    assertEquals(INSTANT_GENERATOR.createNewInstant(INFLIGHT, COMMIT_ACTION, "006"), instants.get(7));
    assertEquals(INSTANT_GENERATOR.createNewInstant(COMPLETED, COMMIT_ACTION, "006"), instants.get(8));
  }

  /**
   * Verify data in base files matches expected records and commit time.
   */
  protected void verifyRecordsWritten(String commitTime, boolean populateMetadataField, List<HoodieRecord> expectedRecords,
                                    List<WriteStatus> allStatus, HoodieWriteConfig config, KeyGenerator keyGenerator) {
    List<GenericRecord> records = new ArrayList<>();
    Set<String> expectedKeys = verifyRecordKeys(expectedRecords, allStatus, records);
    if (config.populateMetaFields()) {
      for (GenericRecord record : records) {
        String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
        assertEquals(commitTime,
            record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());
        assertTrue(expectedKeys.contains(recordKey));
      }
    } else {
      for (GenericRecord record : records) {
        String recordKey = keyGenerator.getKey(record).getRecordKey();
        if (!populateMetadataField) {
          assertNull(record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD));
        }
        assertTrue(expectedKeys.contains(recordKey));
      }
    }
  }

  protected List<WriteStatus> writeAndVerifyBatch(BaseHoodieWriteClient client, List<HoodieRecord> inserts, String commitTime, boolean populateMetaFields) throws IOException {
    return writeAndVerifyBatch(client, inserts, commitTime, populateMetaFields, true);
  }

  /**
   * This method returns following three items:
   * 1. List of all HoodieRecord written in the two batches of insert.
   * 2. Commit instants of the two batches.
   * 3. List of new file group ids that were written in the two batches.
   */
  protected Pair<Pair<List<HoodieRecord>, List<String>>, Set<HoodieFileGroupId>> insertTwoBatches(BaseHoodieWriteClient writeClient, BaseHoodieWriteClient brokenClusteringClient,
                                                                                                  boolean populateMetaFields, String partitionPath, boolean failInlineClustering) throws IOException {
    BaseHoodieWriteClient client;
    if (failInlineClustering) {
      if (null != writeClient) {
        writeClient.close();
      }
      client = brokenClusteringClient;
    } else {
      client = writeClient;
    }

    dataGen = new HoodieTestDataGenerator(new String[] {partitionPath});
    String commitTime1 = client.createNewInstantTime();
    List<HoodieRecord> records1 = dataGen.generateInserts(commitTime1, 200);
    List<WriteStatus> statuses1 = writeAndVerifyBatch(client, records1, commitTime1, populateMetaFields, true);
    Set<HoodieFileGroupId> fileIds1 = getFileGroupIdsFromWriteStatus(statuses1);

    String commitTime2 = client.createNewInstantTime();
    List<HoodieRecord> records2 = dataGen.generateInserts(commitTime2, 200);
    List<WriteStatus> statuses2 = writeAndVerifyBatch(client, records2, commitTime2, populateMetaFields, true);
    client.close();
    Set<HoodieFileGroupId> fileIds2 = getFileGroupIdsFromWriteStatus(statuses2);
    Set<HoodieFileGroupId> fileIdsUnion = new HashSet<>(fileIds1);
    fileIdsUnion.addAll(fileIds2);
    //verify new files are created for 2nd write
    Set<HoodieFileGroupId> fileIdIntersection = new HashSet<>(fileIds1);
    fileIdIntersection.retainAll(fileIds2);
    assertEquals(0, fileIdIntersection.size());
    return Pair.of(Pair.of(Stream.concat(records1.stream(), records2.stream()).collect(Collectors.toList()), Arrays.asList(commitTime1, commitTime2)), fileIdsUnion);
  }

  protected void rollbackAndAssert(boolean enableOptimisticConsistencyGuard, String instantTime, HoodieTableMetaClient metaClient, BaseHoodieWriteClient client) throws IOException {
    if (!enableOptimisticConsistencyGuard) {
      // Rollback of this commit should succeed with FailSafeCG
      client.rollback(instantTime);
      assertFalse(testTable.commitExists(instantTime), "After explicit rollback, commit file should not be present");
      // Marker directory must be removed after rollback
      assertFalse(metaClient.getStorage().exists(new StoragePath(metaClient.getMarkerFolderPath(instantTime))));
    } else {
      // if optimistic CG is enabled, commit should have succeeded.
      assertTrue(testTable.commitExists(instantTime),
              "With optimistic CG, first commit should succeed. commit file should be present");
      // Marker directory must be removed after rollback
      assertFalse(metaClient.getStorage().exists(new StoragePath(metaClient.getMarkerFolderPath(instantTime))));
      client.rollback(instantTime);
      assertFalse(testTable.commitExists(instantTime), "After explicit rollback, commit file should not be present");
    }
  }

  protected Pair<StoragePath, List<WriteStatus>> testConsistencyCheck(HoodieEngineContext context, HoodieTableMetaClient metaClient,
                                                                    String instantTime, boolean enableOptimisticConsistencyGuard,
                                                                    Function2<HoodieTable, HoodieTableMetaClient, HoodieWriteConfig> getHoodieTableFn,
                                                                    Function transformInputFn, Function transformOutputFn)
          throws Exception {
    HoodieWriteConfig cfg = getConsistencyCheckWriteConfig(enableOptimisticConsistencyGuard);
    BaseHoodieWriteClient client = getHoodieWriteClient(cfg);

    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    List<HoodieRecord> writeRecords = dataGen.generateInserts(instantTime, 200);
    List<WriteStatus> result = (List<WriteStatus>) transformOutputFn.apply(client.bulkInsert(transformInputFn.apply(writeRecords), instantTime));

    // Create a dummy marker file to simulate the case that a marker file was created without data file.
    // This should fail the commit
    String partitionPath;
    String markerFolderPath = metaClient.getMarkerFolderPath(instantTime);
    if (cfg.getMarkersType() == MarkerType.TIMELINE_SERVER_BASED) {
      String markerName = MarkerUtils.readTimelineServerBasedMarkersFromFileSystem(
                      markerFolderPath, storage, context, 1).values().stream()
              .flatMap(Collection::stream).findFirst().get();
      partitionPath = new StoragePath(markerFolderPath, markerName).getParent().toString();
    } else {
      partitionPath = storage.globEntries(
                      new StoragePath(String.format("%s/*/*/*/*", markerFolderPath)), path ->
                              path.toString().contains(HoodieTableMetaClient.MARKER_EXTN))
              .stream()
              .limit(1).map(status -> status.getPath().getParent().toString())
              .collect(Collectors.toList()).get(0);
    }

    Option<StoragePath> markerFilePath = WriteMarkersFactory.get(
                    cfg.getMarkersType(), getHoodieTableFn.apply(metaClient, cfg), instantTime)
            .create(partitionPath,
                    FSUtils.makeBaseFileName(instantTime, "1-0-1", UUID.randomUUID().toString(),
                            HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension()),
                    IOType.MERGE);
    if (!enableOptimisticConsistencyGuard) {
      Exception e = assertThrows(HoodieCommitException.class, () -> client.commit(instantTime, transformInputFn.apply(result)),
              "Commit should fail due to consistency check");
      assertTrue(e.getCause() instanceof HoodieIOException);
    } else {
      // with optimistic CG, commit should succeed
      client.commit(instantTime, transformInputFn.apply(result));
    }
    return Pair.of(markerFilePath.get(), result);
  }

  protected void testConsistencyCheckDuringFinalize(HoodieEngineContext context, boolean enableOptimisticConsistencyGuard,
                                                    Function2<HoodieTable, HoodieTableMetaClient, HoodieWriteConfig> getHoodieTableFn,
                                                    Function transformInputFn, Function transformOutputFn) throws Exception {
    HoodieTableMetaClient metaClient = createMetaClient();
    String instantTime = "000";
    HoodieWriteConfig cfg = getConfigBuilder().withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder()
            .withEnableOptimisticConsistencyGuard(enableOptimisticConsistencyGuard).build()).build();
    BaseHoodieWriteClient client = getHoodieWriteClient(cfg);
    client.setOperationType(WriteOperationType.UPSERT);
    Pair<StoragePath, List<WriteStatus>> result = testConsistencyCheck(context, metaClient, instantTime,
              enableOptimisticConsistencyGuard, getHoodieTableFn, transformInputFn, transformOutputFn);

    // Delete orphan marker and commit should succeed
    metaClient.getStorage().deleteFile(result.getKey());
    if (!enableOptimisticConsistencyGuard) {
      assertTrue(client.commit(instantTime, transformInputFn.apply(result.getRight())), "Commit should succeed");
      assertTrue(testTable.commitExists(instantTime), "After explicit commit, commit file should be created");
      // Marker directory must be removed
      assertFalse(metaClient.getStorage().exists(new StoragePath(metaClient.getMarkerFolderPath(instantTime))));
    } else {
      // with optimistic, first client.commit should have succeeded.
      assertTrue(testTable.commitExists(instantTime), "After explicit commit, commit file should be created");
      // Marker directory must be removed
      assertFalse(metaClient.getStorage().exists(new StoragePath(metaClient.getMarkerFolderPath(instantTime))));
    }
  }

  protected void testRollbackAfterConsistencyCheckFailureUsingFileList(HoodieEngineContext context, boolean rollbackUsingMarkers,
                                                                       boolean enableOptimisticConsistencyGuard, boolean populateMetaFields,
                                                                       Function2<HoodieTable, HoodieTableMetaClient, HoodieWriteConfig> getHoodieTableFn,
                                                                       Function transformInputFn, Function transformOutputFn) throws Exception {
    String instantTime = "00000000000010";
    HoodieTableMetaClient metaClient = createMetaClient();
    HoodieWriteConfig cfg = getRollbackMarkersAndConsistencyGuardWriteConfig(rollbackUsingMarkers, enableOptimisticConsistencyGuard, populateMetaFields);
    BaseHoodieWriteClient client = getHoodieWriteClient(cfg);
    testConsistencyCheck(context, metaClient, instantTime, enableOptimisticConsistencyGuard, getHoodieTableFn, transformInputFn, transformOutputFn);
    rollbackAndAssert(enableOptimisticConsistencyGuard, instantTime, metaClient, client);
  }

  protected List<HoodieRecord> generate3AvroRecords(String commitTime) throws IOException {
    String recordKey = UUID.randomUUID().toString();
    HoodieKey keyOne = new HoodieKey(recordKey, "2018-01-01");
    HoodieRecord<RawTripTestPayload> recordOne =
            new HoodieAvroRecord(keyOne, dataGen.generateRandomValue(keyOne, commitTime), HoodieOperation.INSERT);
    HoodieKey keyTwo = new HoodieKey(recordKey, "2018-02-01");
    HoodieRecord recordTwo =
            new HoodieAvroRecord(keyTwo, dataGen.generateRandomValue(keyTwo, commitTime), HoodieOperation.INSERT);
    // Same key and partition as keyTwo
    HoodieRecord recordThree =
            new HoodieAvroRecord(keyTwo, dataGen.generateRandomValue(keyTwo, commitTime), HoodieOperation.UPDATE_AFTER);
    return Arrays.asList(recordOne, recordTwo, recordThree);
  }

  private HoodieWriteMetadata<List<WriteStatus>> performClustering(HoodieClusteringConfig clusteringConfig,
                                                                   boolean populateMetaFields,
                                                                   boolean completeClustering,
                                                                   String validatorClasses,
                                                                   String sqlQueryForEqualityValidation, String sqlQueryForSingleResultValidation,
                                                                   Pair<List<HoodieRecord>, List<String>> allRecords,
                                                                   Function<HoodieWriteMetadata, HoodieWriteMetadata<List<WriteStatus>>> transformWriteMetadataFn,
                                                                   Function<HoodieWriteConfig, KeyGenerator> createKeyGeneratorFn) {
    HoodiePreCommitValidatorConfig validatorConfig = HoodiePreCommitValidatorConfig.newBuilder()
            .withPreCommitValidator(StringUtils.nullToEmpty(validatorClasses))
            .withPrecommitValidatorEqualitySqlQueries(sqlQueryForEqualityValidation)
            .withPrecommitValidatorSingleResultSqlQueries(sqlQueryForSingleResultValidation)
            .build();

    HoodieWriteConfig config = getConfigBuilder()
            .withPreCommitValidatorConfig(validatorConfig)
            .withProps(populateMetaFields ? new Properties() : getPropertiesForKeyGen())
            .withClusteringConfig(clusteringConfig).build();

    // create client with new config.
    BaseHoodieWriteClient client = getHoodieWriteClient(config);
    String clusteringCommitTime = client.scheduleClustering(Option.empty()).get().toString();
    HoodieWriteMetadata<List<WriteStatus>> clusterMetadata = transformWriteMetadataFn.apply(client.cluster(clusteringCommitTime, completeClustering));
    if (config.populateMetaFields()) {
      verifyRecordsWrittenWithPreservedMetadata(new HashSet<>(allRecords.getRight()), allRecords.getLeft(), clusterMetadata.getWriteStatuses());
    } else {
      verifyRecordsWritten(clusteringCommitTime, populateMetaFields, allRecords.getLeft(), clusterMetadata.getWriteStatuses(),
              config, createKeyGeneratorFn.apply(config));
    }
    return clusterMetadata;
  }

  protected void testClustering(HoodieClusteringConfig clusteringConfig, boolean populateMetaFields, boolean completeClustering, boolean assertSameFileIds,
                                String validatorClasses, String sqlQueryForEqualityValidation, String sqlQueryForSingleResultValidation,
                                Pair<Pair<List<HoodieRecord>, List<String>>, Set<HoodieFileGroupId>> allRecords,
                                Function<HoodieWriteMetadata, HoodieWriteMetadata<List<WriteStatus>>> transformWriteMetadataFn,
                                Function<HoodieWriteConfig, KeyGenerator> createKeyGeneratorFn) {

    HoodieWriteConfig config = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
            .withClusteringConfig(clusteringConfig)
            .withProps(getPropertiesForKeyGen()).build();
    HoodieWriteMetadata<List<WriteStatus>> clusterMetadata =
            performClustering(clusteringConfig, populateMetaFields, completeClustering, validatorClasses, sqlQueryForEqualityValidation,
                    sqlQueryForSingleResultValidation, allRecords.getLeft(), transformWriteMetadataFn, createKeyGeneratorFn);
    if (assertSameFileIds) {
      Set<HoodieFileGroupId> replacedFileIds = clusterMetadata.getWriteStats().get().stream()
              .map(s -> new HoodieFileGroupId(s.getPartitionPath(), s.getFileId())).collect(Collectors.toSet());
      Set<HoodieFileGroupId> insertedFileIds = allRecords.getRight();
      assertEquals(insertedFileIds, replacedFileIds);
    }
    if (completeClustering) {
      String clusteringCommitTime = createMetaClient().reloadActiveTimeline().getCompletedReplaceTimeline()
              .getReverseOrderedInstants().findFirst().get().requestedTime();
      verifyRecordsWritten(clusteringCommitTime, populateMetaFields, allRecords.getLeft().getLeft(),
              clusterMetadata.getWriteStatuses(), config, createKeyGeneratorFn.apply(config));
    }
  }

  protected Pair<Pair<List<HoodieRecord>, List<String>>, Set<HoodieFileGroupId>> testInsertTwoBatches(boolean populateMetaFields, Function createBrokenClusteringClientFn) throws IOException {
    return testInsertTwoBatches(populateMetaFields, "2015/03/16", createBrokenClusteringClientFn);
  }

  protected Pair<Pair<List<HoodieRecord>, List<String>>, Set<HoodieFileGroupId>> testInsertTwoBatches(boolean populateMetaFields, String partitionPath,
                                                                                                      Function createBrokenClusteringClientFn) throws IOException {
    return testInsertTwoBatches(populateMetaFields, partitionPath, getPropertiesForKeyGen(populateMetaFields), false, createBrokenClusteringClientFn);
  }

  protected Pair<Pair<List<HoodieRecord>, List<String>>, Set<HoodieFileGroupId>> testInsertTwoBatches(boolean populateMetaFields, String partitionPath, Properties props, boolean failInlineClustering,
                                                                                                      Function createBrokenClusteringClientFn) throws IOException {
    // create config to not update small files.
    HoodieWriteConfig config = getSmallInsertWriteConfig(2000, TRIP_EXAMPLE_SCHEMA, 10, false, populateMetaFields,
            populateMetaFields ? props : getPropertiesForKeyGen());
    return insertTwoBatches(getHoodieWriteClient(config), (BaseHoodieWriteClient) createBrokenClusteringClientFn.apply(config), populateMetaFields, partitionPath, failInlineClustering);
  }

  protected void testCommitWritesRelativePaths(Function transformInputFn) throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder();
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    try (BaseHoodieWriteClient client = getHoodieWriteClient(cfgBuilder.build())) {
      HoodieTableMetaClient metaClient = createMetaClient();
      String instantTime = "000";
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 200);

      assertTrue(client.commit(instantTime, client.bulkInsert(transformInputFn.apply(records), instantTime)), "Commit should succeed");
      assertTrue(testTable.commitExists(instantTime), "After explicit commit, commit file should be created");

      // Get base file paths from commit metadata
      String actionType = metaClient.getCommitActionType();
      HoodieInstant commitInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, actionType, instantTime);
      HoodieTimeline commitTimeline = metaClient.getCommitTimeline().filterCompletedInstants();
      HoodieCommitMetadata commitMetadata = commitTimeline.readCommitMetadata(commitInstant);
      StoragePath basePath = metaClient.getBasePath();
      Collection<String> commitPathNames = commitMetadata.getFileIdAndFullPaths(basePath).values();

      // Read from commit file
      HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(COMPLETED, COMMIT_ACTION, instantTime);
      HoodieCommitMetadata metadata =
          metaClient.reloadActiveTimeline().readCommitMetadata(instant);
      HashMap<String, String> paths = metadata.getFileIdAndFullPaths(basePath);
      // Compare values in both to make sure they are equal.
      for (String pathName : paths.values()) {
        assertTrue(commitPathNames.contains(pathName));
      }
    }
  }

  protected void testMetadataStatsOnCommit(boolean populateMetaFields, Function transformInputFn) throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder();
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig cfg = cfgBuilder.build();
    BaseHoodieWriteClient client = getHoodieWriteClient(cfg);

    String instantTime0 = "000";
    WriteClientTestUtils.startCommitWithTime(client, instantTime0);
    List<HoodieRecord> records0 = dataGen.generateInserts(instantTime0, 200);
    assertTrue(client.commit(instantTime0, client.bulkInsert(transformInputFn.apply(records0), instantTime0)), "Commit should succeed");
    assertTrue(testTable.commitExists(instantTime0), "After explicit commit, commit file should be created");

    // Read from commit file
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(COMPLETED, COMMIT_ACTION, instantTime0);
    HoodieCommitMetadata metadata =
        createMetaClient().reloadActiveTimeline().readCommitMetadata(instant);
    int inserts = 0;
    for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
      for (HoodieWriteStat stat : pstat.getValue()) {
        inserts += stat.getNumInserts();
      }
    }
    assertEquals(200, inserts);

    // Update + Inserts such that they just expand file1
    String instantTime1 = "001";
    WriteClientTestUtils.startCommitWithTime(client, instantTime1);
    List<HoodieRecord> records1 = dataGen.generateUpdates(instantTime1, records0);
    assertTrue(client.commit(instantTime1, client.upsert(transformInputFn.apply(records1), instantTime1)), "Commit should succeed");
    assertTrue(testTable.commitExists(instantTime1), "After explicit commit, commit file should be created");

    metaClient = createMetaClient();
    instant = INSTANT_GENERATOR.createNewInstant(COMPLETED, COMMIT_ACTION, instantTime1);
    // Read from commit file
    metadata = metaClient.reloadActiveTimeline().readCommitMetadata(instant);
    inserts = 0;
    int upserts = 0;
    for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
      for (HoodieWriteStat stat : pstat.getValue()) {
        inserts += stat.getNumInserts();
        upserts += stat.getNumUpdateWrites();
      }
    }
    assertEquals(0, inserts);
    assertEquals(200, upserts);
  }

  protected void testFailWritesOnInlineTableServiceThrowable(
      boolean shouldFailOnException, boolean actuallyFailed, Function createBrokenClusteringClientFn, String error) throws IOException {
    try {
      Properties properties = new Properties();
      properties.setProperty("hoodie.fail.writes.on.inline.table.service.exception", String.valueOf(shouldFailOnException));
      properties.setProperty("hoodie.auto.commit", "false");
      properties.setProperty("hoodie.clustering.inline.max.commits", "1");
      properties.setProperty("hoodie.clustering.inline", "true");
      properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
      testInsertTwoBatches(true, "2015/03/16", properties, true, createBrokenClusteringClientFn);
      assertFalse(actuallyFailed);
    } catch (HoodieException | Error e) {
      assertEquals(error, e.getMessage());
      assertTrue(actuallyFailed);
    }
  }

  protected void testDeletesWithoutInserts(boolean populateMetaFields, Function transformInputFn, Function transformOutputFn) {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit,
            TRIP_EXAMPLE_SCHEMA, dataGen.getEstimatedFileSizeInBytes(150), populateMetaFields, populateMetaFields
                    ? new Properties() : getPropertiesForKeyGen());
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    BaseHoodieWriteClient client = getHoodieWriteClient(config);
    // delete non existent keys
    String commitTime1 = "001";
    WriteClientTestUtils.startCommitWithTime(client, commitTime1);

    List<HoodieRecord> dummyInserts = dataGen.generateInserts(commitTime1, 20);
    List<HoodieKey> hoodieKeysToDelete = randomSelectAsHoodieKeys(dummyInserts, 20);
    transformOutputFn.apply(client.delete(transformInputFn.apply(hoodieKeysToDelete), commitTime1));
  }

  protected void testInlineScheduleClustering(Function createBrokenClusteringClientFn, HoodieClusteringConfig clusteringConfig,
                                              Function transformInputFn, Function transformOutputFn) throws IOException {
    testInsertTwoBatches(true, createBrokenClusteringClientFn);
    HoodieWriteConfig config = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withClusteringConfig(clusteringConfig)
        .withProps(getPropertiesForKeyGen()).build();
    dataGen = new HoodieTestDataGenerator(new String[] {"2015/03/16"});
    generateInsertsAndCommit(config, transformInputFn, transformOutputFn);
    HoodieTableMetaClient metaClient = createMetaClient();
    getAndAssertPendingClusteringPlans(clusteringConfig.getBoolean(SCHEDULE_INLINE_CLUSTERING), metaClient);
  }

  protected void testAndValidateClusteringOutputFiles(Function createBrokenClusteringClientFn, HoodieClusteringConfig clusteringConfig,
                                              Function transformInputFn, Function transformOutputFn) throws IOException {
    String partitionPath = "2015/03/16";
    testInsertTwoBatches(true, partitionPath, createBrokenClusteringClientFn);
    // Trigger clustering
    HoodieWriteConfig config = getConfigBuilder().withEmbeddedTimelineServerEnabled(false)
        .withClusteringConfig(clusteringConfig).build();
    generateInsertsAndCommit(config, transformInputFn, transformOutputFn);
    verifyClusteredFilesWithReplaceCommitMetadata(partitionPath);
  }

  private void generateInsertsAndCommit(HoodieWriteConfig config, Function transformInputFn, Function transformOutputFn) {
    try (BaseHoodieWriteClient client = getHoodieWriteClient(config)) {
      String commitTime = client.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(commitTime, 200);
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      List<WriteStatus> statusList = (List<WriteStatus>) transformOutputFn.apply(client.insert(transformInputFn.apply(records), commitTime));
      assertNoWriteErrors(statusList);
      client.commit(commitTime, transformInputFn.apply(statusList));
    }
  }

  /**
   * Test Deduplication Logic for write function.
   *
   * @param writeFn One of HoodieWriteClient non-prepped write APIs
   * @throws Exception in case of failure
   */
  protected void testDeduplication(Function3<List<WriteStatus>, BaseHoodieWriteClient, List<HoodieRecord>, String> writeFn,
                                   boolean populateMetaFields, boolean allowOperationMetadataField) throws Exception {
    String newCommitTime = "001";
    List<HoodieRecord> recordList = generate3AvroRecords(newCommitTime);
    HoodieData<HoodieRecord> records = HoodieListData.eager(recordList);
    HoodieWriteConfig writeConfig = getWriteConfigWithPopulateMetaFieldsAndAllowOperationMetaField(populateMetaFields, allowOperationMetadataField);
    // Global dedup should be done based on recordKey only
    List<HoodieRecord<RawTripTestPayload>> dedupedRecs = dedupForCopyOnWriteStorage(writeConfig, records, true, allowOperationMetadataField ? 100 : 2, records.getNumPartitions());
    if (allowOperationMetadataField) {
      assertEquals(dedupedRecs.get(0).getOperation(), recordList.get(2).getOperation());
    } else {
      assertEquals(dedupedRecs.get(0).getPartitionPath(), recordList.get(2).getPartitionPath());
      // non-Global dedup should be done based on both recordKey and partitionPath
      dedupForCopyOnWriteStorage(writeConfig, records, false, 2, records.getNumPartitions());
    }
    // Perform write-action and check
    try (BaseHoodieWriteClient client = getHoodieWriteClient(writeConfig)) {
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      List<WriteStatus> statuses = writeFn.apply(client, recordList, newCommitTime);
      assertNoWriteErrors(statuses);
      assertEquals(2, statuses.size());
      assertNoDuplicatesInPartition(statuses.stream().map(WriteStatus::getWrittenRecordDelegates).flatMap(Collection::stream)
              .collect(Collectors.toList()));
    }
  }

  /**
   * Test auto-commit by applying the write function.
   *
   * @param writeFn One of HoodieWriteClient Write API
   * @throws Exception in case of failure
   */
  protected void testAutoCommit(Function3<Object, BaseHoodieWriteClient, Object, String> writeFn,
                                boolean isPrepped, boolean populateMetaFields, InstantGenerator instantGenerator) throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder();
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    try (BaseHoodieWriteClient client = getHoodieWriteClient(cfgBuilder.build())) {
      String prevCommitTime = "000";
      String newCommitTime = "001";
      int numRecords = 200;
      Object result = castInsertFirstBatch(cfgBuilder.build(), client, newCommitTime, prevCommitTime, numRecords, writeFn, isPrepped, false, numRecords, instantGenerator);
      assertTrue(testTable.commitExists(newCommitTime), "After explicit commit, commit file should be created");
    }
  }

  /**
   * When records getting inserted are deleted in the same write batch, hudi should have deleted those records and
   * not be available in read path.
   *
   * @throws Exception
   */
  protected void testDeletesForInsertsInSameBatch(InstantGenerator instantGenerator) throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY);
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    BaseHoodieWriteClient client = getHoodieWriteClient(cfgBuilder.build());
    /**
     * Write 200 inserts and issue deletes to a subset(50) of inserts.
     */
    String initCommitTime = "000";
    String newCommitTime = "001";

    final List<HoodieRecord> recordsInFirstBatch = new ArrayList<>();
    Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        (String instantTime, Integer numRecordsInThisCommit) -> {
          List<HoodieRecord> fewRecordsForInsert = dataGen.generateInserts(instantTime, 200);
          List<HoodieRecord> fewRecordsForDelete = fewRecordsForInsert.subList(40, 90);

          recordsInFirstBatch.addAll(fewRecordsForInsert);
          recordsInFirstBatch.addAll(dataGen.generateDeletesFromExistingRecords(fewRecordsForDelete));
          return recordsInFirstBatch;
        };

    castWriteBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime, -1, recordGenFunction,
        BaseHoodieWriteClient::upsert, true, 150, 150, 1, true, instantGenerator);
  }

  /**
   * Test one of HoodieConcatHandle w/ {@link BaseHoodieWriteClient#insert(Object, String)} API.
   *
   * @throws Exception in case of error
   */
  protected void testHoodieConcatHandle(boolean populateMetaFields, boolean isPrepped, InstantGenerator instantGenerator) throws Exception {
    metaClient = createMetaClient(basePath);
    // Force using older timeline layout
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder();
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig hoodieWriteConfig = cfgBuilder.withMergeAllowDuplicateOnInserts(true).withTimelineLayoutVersion(VERSION_0).build();

    HoodieTableMetaClient.newTableBuilder()
        .fromMetaClient(metaClient)
        .setTimelineLayoutVersion(VERSION_0)
        .initTable(metaClient.getStorageConf().newInstance(), metaClient.getBasePath());

    BaseHoodieWriteClient client = getHoodieWriteClient(hoodieWriteConfig);

    // Write 1 (only inserts)
    String newCommitTime = "001";
    String initCommitTime = "000";
    int numRecords = 200;
    castInsertFirstBatch(hoodieWriteConfig, client, newCommitTime, initCommitTime, numRecords, BaseHoodieWriteClient::insert,
        isPrepped, true, numRecords, populateMetaFields, instantGenerator);

    // Write 2 (updates)
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    numRecords = 100;
    String commitTimeBetweenPrevAndNew = "002";

    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPrepped, hoodieWriteConfig, dataGen::generateUniqueUpdates);

    castWriteBatch(client, newCommitTime, prevCommitTime, Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), initCommitTime, numRecords, recordGenFunction,
        BaseHoodieWriteClient::insert, true, numRecords, 300, 2, populateMetaFields, instantGenerator);
  }

  protected void testHoodieConcatHandleOnDupInserts(boolean isPrepped, InstantGenerator instantGenerator) throws Exception {
    HoodieWriteConfig config = getConfigBuilder()
        .withMergeAllowDuplicateOnInserts(true)
        .build();

    BaseHoodieWriteClient client = getHoodieWriteClient(config);

    // Write 1 (only inserts)
    String initCommitTime = "000";
    String newCommitTime = "001";
    int firstInsertRecords = 50;
    castInsertFirstBatch(config, client, newCommitTime, initCommitTime, firstInsertRecords, BaseHoodieWriteClient::insert,
        isPrepped, true, firstInsertRecords, config.populateMetaFields(), instantGenerator);

    // Write 2 (updates with duplicates)
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    int secondInsertRecords = 100; // needs to be larger than firstInsertRecords to guarantee duplicate keys
    List<String> commitTimesBetweenPrevAndNew = Arrays.asList("002", "003");

    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPrepped, config, dataGen::generateUpdates);

    castWriteBatch(client, newCommitTime, prevCommitTime, Option.of(commitTimesBetweenPrevAndNew), initCommitTime,
        secondInsertRecords, recordGenFunction, BaseHoodieWriteClient::insert, true, secondInsertRecords,
        firstInsertRecords + secondInsertRecords, 2, config.populateMetaFields(), instantGenerator);
  }

  /**
   * Test one of HoodieWriteClient upsert(Prepped) APIs.
   *
   * @param writeFn One of Hoodie Write Function API
   * @throws Exception in case of error
   */
  protected void testUpsertsInternal(Function3<Object, BaseHoodieWriteClient, Object, String> writeFn, boolean populateMetaFields, boolean isPrepped,
                                     SupportsUpgradeDowngrade upgradeDowngrade) throws Exception {

    metaClient.getStorage().deleteDirectory(new StoragePath(basePath));

    metaClient = HoodieTableMetaClient.newTableBuilder()
        .fromMetaClient(metaClient)
        .setTableVersion(6)
        .setPopulateMetaFields(populateMetaFields)
        .initTable(metaClient.getStorageConf().newInstance(), metaClient.getBasePath());

    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY).withRollbackUsingMarkers(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexColumnStats(true).withColumnStatsIndexForColumns("driver,rider")
            .withMetadataIndexColumnStatsFileGroupCount(1).withEngineType(getEngineType()).build())
        .withWriteTableVersion(6);

    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    metaClient = HoodieTestUtils.createMetaClient(storageConf, new StoragePath(basePath), HoodieTableVersion.SIX);

    HoodieWriteConfig config = cfgBuilder.build();
    BaseHoodieWriteClient client = getHoodieWriteClient(config);

    // Write 1 (only inserts)
    String newCommitTime = "001";
    String initCommitTime = "000";
    int numRecords = 200;
    castInsertFirstBatch(config, client, newCommitTime, initCommitTime, numRecords, BaseHoodieWriteClient::insert,
        isPrepped, true, numRecords, populateMetaFields, metaClient.getInstantGenerator());

    // Write 2 (updates)
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    numRecords = 100;
    String commitTimeBetweenPrevAndNew = "002";
    castUpdateBatch(config, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), initCommitTime, numRecords, writeFn, isPrepped, true,
        numRecords, 200, 2, populateMetaFields, metaClient.getInstantGenerator());

    // Delete 1
    prevCommitTime = newCommitTime;
    newCommitTime = "005";
    numRecords = 50;

    castDeleteBatch(config, client, newCommitTime, prevCommitTime, initCommitTime, numRecords, isPrepped, true,
        0, 150, config.populateMetaFields(), metaClient.getTimelineLayout().getTimelineFactory(),
        metaClient.getInstantGenerator());

    // Now perform an upgrade and perform a restore operation
    HoodieWriteConfig newConfig = getConfigBuilder().withProps(config.getProps()).withWriteTableVersion(HoodieTableVersion.EIGHT.versionCode()).build();
    client = getHoodieWriteClient(newConfig);
    new UpgradeDowngrade(metaClient, newConfig, client.getEngineContext(), upgradeDowngrade)
        .run(HoodieTableVersion.EIGHT, null);

    client = getHoodieWriteClient(newConfig);
    client.savepoint("004", "user1", "comment1");
    client.restoreToInstant("004", config.isMetadataTableEnabled());
    metaClient = HoodieTestUtils.createMetaClient(storageConf, new StoragePath(basePath), HoodieTableVersion.EIGHT);
    assertFalse(metaClient.reloadActiveTimeline().getRollbackTimeline().lastInstant().isPresent());

    client.deleteSavepoint("004");
    assertFalse(metaClient.reloadActiveTimeline().getSavePointTimeline().containsInstant("004"));

    assertTheEntireDatasetHasAllRecordsStill(200);

    // Perform Delete again on upgraded dataset.
    prevCommitTime = newCommitTime;
    newCommitTime = "006";
    numRecords = 50;

    castDeleteBatch(newConfig, client, newCommitTime, prevCommitTime, initCommitTime, numRecords, isPrepped, true, 0, 150,
        metaClient.getTimelineLayout().getTimelineFactory(), metaClient.getInstantGenerator());

    checkTimelineForUpsertsInternal(metaClient);

    testMergeHandle(config);
  }

  /**
   * Tests deletion of records.
   */
  protected void testDeletes(Function3<Function2<List<HoodieRecord>, String, Integer>, String, Integer, List<HoodieRecord>> secondBatchGenFn,
                             int numRecordsInSecondBatch, int expRecordsInSecondBatch, int expTotalRecords) throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY);
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    BaseHoodieWriteClient client = getHoodieWriteClient(cfgBuilder.build());
    /**
     * Write 1 (inserts and deletes) Write actual 200 insert records and ignore 100 delete records
     */
    String initCommitTime = "000";
    String newCommitTime = "001";

    final List<HoodieRecord> recordsInFirstBatch = new ArrayList<>();
    Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        (String instantTime, Integer numRecordsInThisCommit) -> {
          List<HoodieRecord> fewRecordsForInsert = dataGen.generateInserts(instantTime, 200);
          List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletes(instantTime, 100);

          recordsInFirstBatch.addAll(fewRecordsForInsert);
          recordsInFirstBatch.addAll(fewRecordsForDelete);
          return recordsInFirstBatch;
        };

    castWriteBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime,
        // unused as genFn uses hard-coded number of inserts/updates/deletes
        -1, recordGenFunction, BaseHoodieWriteClient::upsert, true, 200, 200, 1, true, INSTANT_GENERATOR);

    /**
     * Write 2 (deletes+writes).
     */
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    recordGenFunction = secondBatchGenFn.apply(newCommitTime, numRecordsInSecondBatch, recordsInFirstBatch);

    castWriteBatch(client, newCommitTime, prevCommitTime, Option.empty(), initCommitTime, numRecordsInSecondBatch, recordGenFunction,
        BaseHoodieWriteClient::upsert, true, expRecordsInSecondBatch, expTotalRecords, 2, true, INSTANT_GENERATOR);
  }

  protected void testRollbackFailedCommits(boolean populateMetaFields) throws Exception {
    // HoodieFailedWritesCleaningPolicy cleaningPolicy, boolean populateMetaFields
    HoodieFailedWritesCleaningPolicy cleaningPolicy = HoodieFailedWritesCleaningPolicy.NEVER;
    HoodieTestUtils.init(storageConf, basePath);
    BaseHoodieWriteClient client = getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));

    // perform 1 successful commit
    castWriteBatch(client, "100", "100", Option.of(Arrays.asList("100")), "100",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 300,
        0, INSTANT_GENERATOR);

    // Perform 2 failed writes to table
    castWriteBatch(client, "200", "100", Option.of(Arrays.asList("200")), "100",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 300,
        0, true, INSTANT_GENERATOR, true);
    client.close();
    client = getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    castWriteBatch(client, "300", "200", Option.of(Arrays.asList("300")), "300",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 300,
        0, true, INSTANT_GENERATOR, true);
    client.close();
    // refresh data generator to delete records generated from failed commits
    dataGen = new HoodieTestDataGenerator();
    // Perform 1 successful write
    client = getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    castWriteBatch(client, "400", "300", Option.of(Arrays.asList("400")), "400",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 300,
        0, INSTANT_GENERATOR);
    HoodieTableMetaClient metaClient = createMetaClient();

    assertEquals(0, metaClient.getActiveTimeline().getTimelineOfActions(CollectionUtils.createSet(ROLLBACK_ACTION)).countInstants());
    assertEquals(2, metaClient.getActiveTimeline().filterInflights().countInstants());
    assertEquals(2, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants());
    // Wait till enough time passes such that the first 2 failed commits heartbeats are expired
    boolean conditionMet = false;
    while (!conditionMet) {
      conditionMet = client.getHeartbeatClient().isHeartbeatExpired("300");
      Thread.sleep(2000);
    }
    client.close();
    client = getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    // Perform 1 successful write
    castWriteBatch(client, "500", "400", Option.of(Arrays.asList("500")), "500",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 300,
        0, INSTANT_GENERATOR);
    client.clean();
    client.close();
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline().reload();
    if (cleaningPolicy.isLazy()) {
      assertEquals(2, timeline.getTimelineOfActions(CollectionUtils.createSet(ROLLBACK_ACTION)).countInstants());
      // Since we write rollbacks not clean, there should be no clean action on the timeline
      assertEquals(0, timeline.getTimelineOfActions(CollectionUtils.createSet(CLEAN_ACTION)).countInstants());
      assertEquals(3, timeline.getCommitsTimeline().filterCompletedInstants().countInstants());
    } else if (cleaningPolicy.isNever()) {
      // never will get translated to Lazy if OCC is enabled.
      assertEquals(2, timeline.getTimelineOfActions(CollectionUtils.createSet(ROLLBACK_ACTION)).countInstants());
      // There should be no clean or rollback action on the timeline
      assertEquals(0, timeline.getTimelineOfActions(CollectionUtils.createSet(CLEAN_ACTION)).countInstants());
      assertEquals(3, timeline.getCommitsTimeline().filterCompletedInstants().countInstants());
    }
  }

  protected void testRollbackFailedCommitsToggleCleaningPolicy(boolean populateMetaFields) throws Exception {
    HoodieTestUtils.init(storageConf, basePath);
    metaClient = createMetaClient();
    HoodieFailedWritesCleaningPolicy cleaningPolicy = EAGER;
    BaseHoodieWriteClient client = getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    // Perform 1 successful writes to table
    castWriteBatch(client, "100", "100", Option.of(Arrays.asList("100")), "100",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 300,
        0, INSTANT_GENERATOR);

    // Perform 1 failed writes to table
    castWriteBatch(client, "200", "100", Option.of(Arrays.asList("200")), "200",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 300,
        0, true, INSTANT_GENERATOR, true);
    client.close();
    // Toggle cleaning policy to LAZY
    cleaningPolicy = HoodieFailedWritesCleaningPolicy.LAZY;
    // Perform 2 failed writes to table
    client = getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    castWriteBatch(client, "300", "200", Option.of(Arrays.asList("300")), "300",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 300,
        0, true, INSTANT_GENERATOR, true);
    client.close();
    client = getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    castWriteBatch(client, "400", "300", Option.of(Arrays.asList("400")), "400",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 300,
        0, true, INSTANT_GENERATOR, true);
    client.close();
    // Wait till enough time passes such that the 2 failed commits heartbeats are expired
    boolean conditionMet = false;
    while (!conditionMet) {
      conditionMet = client.getHeartbeatClient().isHeartbeatExpired("400");
      Thread.sleep(2000);
    }
    client.clean();
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline().reload();
    assertEquals(3, timeline.getTimelineOfActions(CollectionUtils.createSet(ROLLBACK_ACTION)).countInstants());
    // Perform 2 failed commits
    client = getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    castWriteBatch(client, "500", "400", Option.of(Arrays.asList("300")), "300",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 300,
        0, true, INSTANT_GENERATOR, true);
    client.close();
    client = getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    castWriteBatch(client, "600", "500", Option.of(Arrays.asList("400")), "400",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 300,
        0, true, INSTANT_GENERATOR, true);
    client.close();
    // Toggle cleaning policy to EAGER
    cleaningPolicy = EAGER;
    client = getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    client.startCommit();
    client.close();
    timeline = metaClient.getActiveTimeline().reload();
    // since OCC is enabled, hudi auto flips the cleaningPolicy to Lazy.
    assertEquals(3, timeline.getTimelineOfActions(CollectionUtils.createSet(ROLLBACK_ACTION)).countInstants());
    assertEquals(1, timeline.getCommitsTimeline().filterCompletedInstants().countInstants());
  }

  protected void testParallelInsertAndCleanPreviousFailedCommits(boolean populateMetaFields) throws Exception {
    HoodieFailedWritesCleaningPolicy cleaningPolicy = HoodieFailedWritesCleaningPolicy.LAZY;
    ExecutorService service = Executors.newFixedThreadPool(2);
    HoodieTestUtils.init(storageConf, basePath);
    BaseHoodieWriteClient client = getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    // perform 1 successful write
    castWriteBatch(client, "100", "100", Option.of(Arrays.asList("100")), "100",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 100,
        0, INSTANT_GENERATOR);

    // Perform 2 failed writes to table
    castWriteBatch(client, "200", "100", Option.of(Arrays.asList("200")), "200",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 100,
        0, true, INSTANT_GENERATOR, true);
    client.close();
    client = getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    castWriteBatch(client, "300", "200", Option.of(Arrays.asList("300")), "300",
        100, dataGen::generateInserts, BaseHoodieWriteClient::bulkInsert, false, 100, 100,
        0, true, INSTANT_GENERATOR, true);
    client.close();
    // refresh data generator to delete records generated from failed commits
    dataGen = new HoodieTestDataGenerator();
    // Create a successful commit
    Future<Object> commit3 = service.submit(() -> castWriteBatch(getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields)),
        "400", "300", Option.of(Arrays.asList("400")), "300", 100, dataGen::generateInserts,
        BaseHoodieWriteClient::bulkInsert, false, 100, 100, 0, INSTANT_GENERATOR));
    commit3.get();
    HoodieTableMetaClient metaClient = createMetaClient();

    assertEquals(0, metaClient.getActiveTimeline().getTimelineOfActions(CollectionUtils.createSet(ROLLBACK_ACTION)).countInstants());
    assertEquals(2, metaClient.getActiveTimeline().filterInflights().countInstants());
    assertEquals(2, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants());
    client = getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    // Wait till enough time passes such that the first 2 failed commits heartbeats are expired
    boolean conditionMet = false;
    while (!conditionMet) {
      conditionMet = client.getHeartbeatClient().isHeartbeatExpired("300");
      Thread.sleep(2000);
    }
    Future<Object> commit4 = service.submit(() -> castWriteBatch(getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields)),
        "500", "400", Option.of(Arrays.asList("500")), "500", 100, dataGen::generateInserts,
        BaseHoodieWriteClient::bulkInsert, false, 100, 100, 0, INSTANT_GENERATOR));
    Future<HoodieCleanMetadata> clean1 = service.submit(() -> getHoodieWriteClient(getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields), false).clean());
    commit4.get();
    clean1.get();
    client.close();
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline().reload();
    assertEquals(2, timeline.getTimelineOfActions(CollectionUtils.createSet(ROLLBACK_ACTION)).countInstants());
    // Since we write rollbacks not clean, there should be no clean action on the timeline
    assertEquals(0, timeline.getTimelineOfActions(CollectionUtils.createSet(CLEAN_ACTION)).countInstants());
    assertEquals(3, timeline.getCommitsTimeline().filterCompletedInstants().countInstants());
    service.shutdown();
  }
}
