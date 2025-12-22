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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy;
import org.apache.hudi.client.common.JavaTaskContextSupplier;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodiePreCombineAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileIOUtils;
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
import org.apache.hudi.exception.HoodieCorruptedDataException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.HoodieWriteHelper;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
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
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.Transformations.randomSelectAsHoodieKeys;
import static org.apache.hudi.common.testutils.Transformations.recordsToRecordKeySet;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TestHoodieJavaClientOnCopyOnWriteStorage extends HoodieJavaClientTestHarness {

  private static final String CLUSTERING_FAILURE = "CLUSTERING FAILURE";

  private static Stream<Arguments> rollbackAfterConsistencyCheckFailureParams() {
    return Stream.of(
        Arguments.of(true),
        Arguments.of(false)
    );
  }

  private HoodieTestTable testTable;

  @BeforeEach
  public void setUpTestTable() {
    testTable = HoodieMetadataTestTable.of(metaClient);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient insert API.
   */
  @Test
  public void testAutoCommitOnInsert() throws Exception {
    testAutoCommit(HoodieJavaWriteClient::insert, false, true);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient insertPrepped API.
   */
  @Test
  public void testAutoCommitOnInsertPrepped() throws Exception {
    testAutoCommit(HoodieJavaWriteClient::insertPreppedRecords, true, true);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient upsert API.
   */
  @Test
  public void testAutoCommitOnUpsert() throws Exception {
    testAutoCommit(HoodieJavaWriteClient::upsert, false, true);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient upsert Prepped API.
   */
  @Test
  public void testAutoCommitOnUpsertPrepped() throws Exception {
    testAutoCommit(HoodieJavaWriteClient::upsertPreppedRecords, true, true);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient bulk-insert API.
   */
  @Test
  public void testAutoCommitOnBulkInsert() throws Exception {
    testAutoCommit(HoodieJavaWriteClient::bulkInsert, false, true);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient bulk-insert prepped API.
   */
  @Test
  public void testAutoCommitOnBulkInsertPrepped() throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.bulkInsertPreppedRecords(recordRDD, instantTime,
        Option.empty()), true, true);
  }

  /**
   * Test auto-commit by applying the write function.
   *
   * @param writeFn One of HoodieWriteClient Write API
   * @throws Exception in case of failure
   */
  private void testAutoCommit(Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn,
                              boolean isPrepped, boolean populateMetaFields) throws Exception {
    // Set autoCommit false
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder().withAutoCommit(false);
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    try (HoodieJavaWriteClient client = getHoodieWriteClient(cfgBuilder.build());) {

      String prevCommitTime = "000";
      String newCommitTime = "001";
      int numRecords = 200;
      List<WriteStatus> result = insertFirstBatch(cfgBuilder.build(), client, newCommitTime, prevCommitTime, numRecords, writeFn,
          isPrepped, false, numRecords);

      assertFalse(testTable.commitExists(newCommitTime),
          "If Autocommit is false, then commit should not be made automatically");
      assertTrue(client.commit(newCommitTime, result), "Commit should succeed");
      assertTrue(testTable.commitExists(newCommitTime),
          "After explicit commit, commit file should be created");
    }
  }

  private void insertWithConfig(HoodieWriteConfig config, int numRecords, String instant) throws Exception {
    try (HoodieJavaWriteClient client = getHoodieWriteClient(config)) {
      Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn = (writeClient, recordRDD, instantTime) ->
          writeClient.bulkInsert(recordRDD, instantTime, Option.empty());
      List<WriteStatus> result = insertFirstBatch(config, client, instant,
          "000", numRecords, writeFn, false, false, numRecords);
    }
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient insert API.
   */
  @Test
  public void testDeduplicationOnInsert() throws Exception {
    testDeduplication(HoodieJavaWriteClient::insert, true);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient insert API.
   */
  @Test
  public void testDeduplicationKeepOperationFieldOnInsert() throws Exception {
    testDeduplicationKeepOperation(HoodieJavaWriteClient::insert, true);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient bulk-insert API.
   */
  @Test
  public void testDeduplicationOnBulkInsert() throws Exception {
    testDeduplication(HoodieJavaWriteClient::bulkInsert, true);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient upsert API.
   */
  @Test
  public void testDeduplicationOnUpsert() throws Exception {
    testDeduplication(HoodieJavaWriteClient::upsert, true);
  }

  /**
   * Test Deduplication Logic for write function.
   *
   * @param writeFn One of HoodieWriteClient non-prepped write APIs
   * @throws Exception in case of failure
   */
  private void testDeduplication(
      Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn, boolean populateMetaFields) throws Exception {
    String newCommitTime = "001";

    String recordKey = UUID.randomUUID().toString();
    HoodieKey keyOne = new HoodieKey(recordKey, "2018-01-01");
    HoodieRecord<RawTripTestPayload> recordOne =
        new HoodieAvroRecord(keyOne, dataGen.generateRandomValue(keyOne, newCommitTime));

    HoodieKey keyTwo = new HoodieKey(recordKey, "2018-02-01");
    HoodieRecord recordTwo =
        new HoodieAvroRecord(keyTwo, dataGen.generateRandomValue(keyTwo, newCommitTime));

    // Same key and partition as keyTwo
    HoodieRecord recordThree =
        new HoodieAvroRecord(keyTwo, dataGen.generateRandomValue(keyTwo, newCommitTime));

    HoodieData<HoodieRecord<RawTripTestPayload>> records = HoodieListData.eager(Arrays.asList(recordOne, recordTwo, recordThree));
    HoodieWriteConfig.Builder configBuilder = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .combineInput(true, true);
    addConfigsForPopulateMetaFields(configBuilder, populateMetaFields);
    HoodieWriteConfig writeConfig = configBuilder.build();

    // Global dedup should be done based on recordKey only
    HoodieIndex index = mock(HoodieIndex.class);
    when(index.isGlobal()).thenReturn(true);
    int dedupParallelism = records.getNumPartitions() + 100;
    HoodieData<HoodieRecord<RawTripTestPayload>> dedupedRecsRdd =
        (HoodieData<HoodieRecord<RawTripTestPayload>>) HoodieWriteHelper.newInstance()
            .deduplicateRecords(records, index, dedupParallelism, writeConfig.getSchema(), writeConfig.getProps(), HoodiePreCombineAvroRecordMerger.INSTANCE);
    List<HoodieRecord<RawTripTestPayload>> dedupedRecs = dedupedRecsRdd.collectAsList();
    assertEquals(records.getNumPartitions(), dedupedRecsRdd.getNumPartitions());
    assertEquals(1, dedupedRecs.size());
    assertEquals(dedupedRecs.get(0).getPartitionPath(), recordThree.getPartitionPath());
    assertNodupesWithinPartition(dedupedRecs);

    // non-Global dedup should be done based on both recordKey and partitionPath
    index = mock(HoodieIndex.class);
    when(index.isGlobal()).thenReturn(false);
    dedupedRecsRdd =
        (HoodieData<HoodieRecord<RawTripTestPayload>>) HoodieWriteHelper.newInstance()
            .deduplicateRecords(records, index, dedupParallelism, writeConfig.getSchema(), writeConfig.getProps(), HoodiePreCombineAvroRecordMerger.INSTANCE);
    dedupedRecs = dedupedRecsRdd.collectAsList();
    assertEquals(records.getNumPartitions(), dedupedRecsRdd.getNumPartitions());
    assertEquals(2, dedupedRecs.size());
    assertNodupesWithinPartition(dedupedRecs);

    // Perform write-action and check
    List<HoodieRecord> recordList = Arrays.asList(recordOne, recordTwo, recordThree);

    try (HoodieJavaWriteClient client = getHoodieWriteClient(writeConfig)) {
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> statuses = writeFn.apply(client, recordList, newCommitTime);
      assertNoWriteErrors(statuses);
      assertEquals(2, statuses.size());
      assertNoDuplicatesInPartition(statuses.stream().map(WriteStatus::getWrittenRecordDelegates).flatMap(Collection::stream)
          .collect(Collectors.toList()));
    }
  }

  /**
   * Test Deduplication Logic for write function.
   *
   * @param writeFn One of HoodieWriteClient non-prepped write APIs
   * @throws Exception in case of failure
   */
  private void testDeduplicationKeepOperation(
      Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn, boolean populateMetaFields) throws Exception {
    String newCommitTime = "001";

    String recordKey = UUID.randomUUID().toString();
    HoodieKey keyOne = new HoodieKey(recordKey, "2018-01-01");
    HoodieRecord<RawTripTestPayload> recordOne =
        new HoodieAvroRecord(keyOne, dataGen.generateRandomValue(keyOne, newCommitTime), HoodieOperation.INSERT);

    HoodieKey keyTwo = new HoodieKey(recordKey, "2018-02-01");
    HoodieRecord recordTwo =
        new HoodieAvroRecord(keyTwo, dataGen.generateRandomValue(keyTwo, newCommitTime), HoodieOperation.INSERT);

    // Same key and partition as keyTwo
    HoodieRecord recordThree =
        new HoodieAvroRecord(keyTwo, dataGen.generateRandomValue(keyTwo, newCommitTime), HoodieOperation.UPDATE_AFTER);

    HoodieData<HoodieRecord<RawTripTestPayload>> records = HoodieListData.eager(Arrays.asList(recordOne, recordTwo, recordThree));
    HoodieWriteConfig.Builder configBuilder = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY).withAllowOperationMetadataField(true)
        .combineInput(true, true);
    addConfigsForPopulateMetaFields(configBuilder, populateMetaFields);
    HoodieWriteConfig writeConfig = configBuilder.build();

    // Global dedup should be done based on recordKey only
    HoodieIndex index = mock(HoodieIndex.class);
    when(index.isGlobal()).thenReturn(true);
    int dedupParallelism = records.getNumPartitions() + 100;
    HoodieData<HoodieRecord<RawTripTestPayload>> dedupedRecsRdd =
        (HoodieData<HoodieRecord<RawTripTestPayload>>) HoodieWriteHelper.newInstance()
            .deduplicateRecords(records, index, dedupParallelism, writeConfig.getSchema(), writeConfig.getProps(), HoodiePreCombineAvroRecordMerger.INSTANCE);
    List<HoodieRecord<RawTripTestPayload>> dedupedRecs = dedupedRecsRdd.collectAsList();
    assertEquals(dedupedRecs.get(0).getOperation(), recordThree.getOperation());

    // Perform write-action and check
    List<HoodieRecord> recordList = Arrays.asList(recordOne, recordTwo, recordThree);

    try (HoodieJavaWriteClient client = getHoodieWriteClient(writeConfig)) {
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> statuses = writeFn.apply(client, recordList, newCommitTime);
      assertNoWriteErrors(statuses);
      assertEquals(2, statuses.size());
      assertNoDuplicatesInPartition(statuses.stream().map(WriteStatus::getWrittenRecordDelegates).flatMap(Collection::stream)
          .collect(Collectors.toList()));
    }
  }

  /**
   * Assert that there is no duplicate key at the partition level.
   *
   * @param recordDelegates List of Hoodie record delegates
   */
  void assertNoDuplicatesInPartition(List<HoodieRecordDelegate> recordDelegates) {
    Map<String, Set<String>> partitionToKeys = new HashMap<>();
    for (HoodieRecordDelegate r : recordDelegates) {
      String recordKey = r.getRecordKey();
      String partitionPath = r.getPartitionPath();
      if (!partitionToKeys.containsKey(partitionPath)) {
        partitionToKeys.put(partitionPath, new HashSet<>());
      }
      assertFalse(partitionToKeys.get(partitionPath).contains(recordKey), "key " + recordKey + " is duplicate within partition " + partitionPath);
      partitionToKeys.get(partitionPath).add(recordKey);
    }
  }

  /**
   * Test Upsert API.
   */
  @Test
  public void testUpserts() throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder().withRollbackUsingMarkers(true);
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    testUpsertsInternal(cfgBuilder.build(), HoodieJavaWriteClient::upsert, false);
  }

  /**
   * Test UpsertPrepped API.
   */
  @Test
  public void testUpsertsPrepped() throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder().withRollbackUsingMarkers(true);
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    testUpsertsInternal(cfgBuilder.build(), HoodieJavaWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test one of HoodieWriteClient upsert(Prepped) APIs.
   *
   * @param config  Write Config
   * @param writeFn One of Hoodie Write Function API
   * @throws Exception in case of error
   */
  private void testUpsertsInternal(HoodieWriteConfig config,
                                   Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn, boolean isPrepped)
      throws Exception {
    // Force using older timeline layout
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withProps(config.getProps()).withTimelineLayoutVersion(
            VERSION_0).build();

    HoodieTableMetaClient.withPropertyBuilder()
        .fromMetaClient(metaClient)
        .setTimelineLayoutVersion(VERSION_0)
        .setPopulateMetaFields(config.populateMetaFields())
        .initTable(metaClient.getStorageConf().newInstance(), metaClient.getBasePath().toString());

    HoodieJavaWriteClient client = getHoodieWriteClient(hoodieWriteConfig);

    // Write 1 (only inserts)
    String newCommitTime = "001";
    String initCommitTime = "000";
    int numRecords = 200;
    insertFirstBatch(hoodieWriteConfig, client, newCommitTime, initCommitTime, numRecords, HoodieJavaWriteClient::insert,
        isPrepped, true, numRecords, config.populateMetaFields());

    // Write 2 (updates)
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    numRecords = 100;
    String commitTimeBetweenPrevAndNew = "002";
    updateBatch(hoodieWriteConfig, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), initCommitTime, numRecords, writeFn, isPrepped, true,
        numRecords, 200, 2, config.populateMetaFields());

    // Delete 1
    prevCommitTime = newCommitTime;
    newCommitTime = "005";
    numRecords = 50;

    deleteBatch(hoodieWriteConfig, client, newCommitTime, prevCommitTime, initCommitTime, numRecords, isPrepped, true,
        0, 150, config.populateMetaFields());

    // Now simulate an upgrade and perform a restore operation
    HoodieWriteConfig newConfig = getConfigBuilder().withProps(config.getProps()).withTimelineLayoutVersion(
        TimelineLayoutVersion.CURR_VERSION).build();
    client = getHoodieWriteClient(newConfig);

    client.savepoint("004", "user1", "comment1");

    client.restoreToInstant("004", config.isMetadataTableEnabled());

    assertFalse(metaClient.reloadActiveTimeline().getRollbackTimeline().lastInstant().isPresent());

    client.deleteSavepoint("004");
    assertFalse(metaClient.reloadActiveTimeline().getSavePointTimeline().containsInstant("004"));

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(200, countRowsInPaths(basePath, storage, fullPartitionPaths),
        "Must contain " + 200 + " records");

    // Perform Delete again on upgraded dataset.
    prevCommitTime = newCommitTime;
    newCommitTime = "006";
    numRecords = 50;

    deleteBatch(newConfig, client, newCommitTime, prevCommitTime,
        initCommitTime, numRecords, isPrepped, true,
        0, 150);

    HoodieActiveTimeline activeTimeline = new HoodieActiveTimeline(metaClient, false);
    List<HoodieInstant> instants = activeTimeline.getCommitAndReplaceTimeline().getInstants();
    assertEquals(5, instants.size());
    assertEquals(new HoodieInstant(COMPLETED, COMMIT_ACTION, "001"),
        instants.get(0));
    assertEquals(new HoodieInstant(COMPLETED, COMMIT_ACTION, "004"),
        instants.get(1));
    // New Format should have all states of instants
    assertEquals(new HoodieInstant(REQUESTED, COMMIT_ACTION, "006"),
        instants.get(2));
    assertEquals(new HoodieInstant(INFLIGHT, COMMIT_ACTION, "006"),
        instants.get(3));
    assertEquals(new HoodieInstant(COMPLETED, COMMIT_ACTION, "006"),
        instants.get(4));

    final HoodieWriteConfig cfg = hoodieWriteConfig;
    final String instantTime = "007";
    HoodieTableMetaClient metaClient = createMetaClient();
    String basePathStr = basePath;
    HoodieTable table = getHoodieTable(metaClient, cfg);
    String extension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
        .fromBytes(metaClient.getActiveTimeline().getInstantDetails(
                metaClient.getCommitsTimeline().filterCompletedInstants().lastInstant().get()).get(),
            HoodieCommitMetadata.class);
    String filePath = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(w -> w.stream()).filter(s -> s.getPath().endsWith(extension)).findAny()
        .map(ee -> ee.getPath()).orElse(null);
    String partitionPath = commitMetadata.getPartitionToWriteStats().values().stream()
        .flatMap(w -> w.stream()).filter(s -> s.getPath().endsWith(extension)).findAny()
        .map(ee -> ee.getPartitionPath()).orElse(null);
    Path baseFilePath = new Path(basePathStr, filePath);
    HoodieBaseFile baseFile = new HoodieBaseFile(baseFilePath.toString());

    HoodieMergeHandle handle = null;
    try {
      handle = new HoodieMergeHandle(cfg, instantTime, table, new HashMap<>(),
          partitionPath, FSUtils.getFileId(baseFilePath.getName()), baseFile, new JavaTaskContextSupplier(),
          config.populateMetaFields() ? Option.empty() :
              Option.of((BaseKeyGenerator) HoodieAvroKeyGeneratorFactory.createKeyGenerator(new TypedProperties(config.getProps()))));
      WriteStatus writeStatus = new WriteStatus(false, 0.0);
      writeStatus.setStat(new HoodieWriteStat());
      writeStatus.getStat().setNumWrites(0);
      handle.performMergeDataValidationCheck(writeStatus);
    } catch (HoodieCorruptedDataException e1) {
      fail("Exception not expected because merge validation check is disabled");
    } finally {
      if (handle != null) {
        handle.close();
      }
    }

    handle = null;
    try {
      final String newInstantTime = "006";
      cfg.getProps().setProperty("hoodie.merge.data.validation.enabled", "true");
      HoodieWriteConfig cfg2 = HoodieWriteConfig.newBuilder().withProps(cfg.getProps()).build();
      // does the handle need to be closed to clean up the writer it contains?
      handle = new HoodieMergeHandle(cfg2, newInstantTime, table, new HashMap<>(),
          partitionPath, FSUtils.getFileId(baseFilePath.getName()), baseFile, new JavaTaskContextSupplier(),
          config.populateMetaFields() ? Option.empty() :
              Option.of((BaseKeyGenerator) HoodieAvroKeyGeneratorFactory.createKeyGenerator(new TypedProperties(config.getProps()))));
      fail("The above line should have thrown an exception");
    } catch (HoodieUpsertException e2) {
      // expected
    } finally {
      if (handle != null) {
        handle.close();
      }
    }
  }

  /**
   * Test Insert API for HoodieConcatHandle.
   */
  @Test
  public void testInsertsWithHoodieConcatHandle() throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder();
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    testHoodieConcatHandle(cfgBuilder.build(), false);
  }

  /**
   * Test InsertPrepped API for HoodieConcatHandle.
   */
  public void testInsertsPreppedWithHoodieConcatHandle() throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder();
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    testHoodieConcatHandle(cfgBuilder.build(), true);
  }

  /**
   * Test one of HoodieConcatHandle w/ {@link BaseHoodieWriteClient#insert(Object, String)} API.
   *
   * @param config Write Config
   * @throws Exception in case of error
   */
  private void testHoodieConcatHandle(HoodieWriteConfig config, boolean isPrepped)
      throws Exception {
    // Force using older timeline layout
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder()
        .withProps(config.getProps()).withMergeAllowDuplicateOnInserts(true).withTimelineLayoutVersion(
            VERSION_0).build();
    HoodieTableMetaClient.withPropertyBuilder()
        .fromMetaClient(metaClient)
        .setTimelineLayoutVersion(VERSION_0)
        .initTable(metaClient.getStorageConf().newInstance(), metaClient.getBasePath().toString());

    HoodieJavaWriteClient client = getHoodieWriteClient(hoodieWriteConfig);

    // Write 1 (only inserts)
    String newCommitTime = "001";
    String initCommitTime = "000";
    int numRecords = 200;
    insertFirstBatch(hoodieWriteConfig, client, newCommitTime, initCommitTime, numRecords, HoodieJavaWriteClient::insert,
        isPrepped, true, numRecords, config.populateMetaFields());

    // Write 2 (updates)
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    numRecords = 100;
    String commitTimeBetweenPrevAndNew = "002";

    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPrepped, hoodieWriteConfig, dataGen::generateUniqueUpdates);

    writeBatch(client, newCommitTime, prevCommitTime, Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), initCommitTime,
        numRecords, recordGenFunction, HoodieJavaWriteClient::insert, true, numRecords, 300,
        2, false, config.populateMetaFields());
  }

  /**
   * Test Insert API for HoodieConcatHandle when incoming entries contain duplicate keys.
   */
  @Test
  public void testInsertsWithHoodieConcatHandleOnDuplicateIncomingKeys() throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder();
    testHoodieConcatHandleOnDupInserts(cfgBuilder.build(), false);
  }

  /**
   * Test InsertPrepped API for HoodieConcatHandle when incoming entries contain duplicate keys.
   */
  @Test
  public void testInsertsPreppedWithHoodieConcatHandleOnDuplicateIncomingKeys() throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder();
    testHoodieConcatHandleOnDupInserts(cfgBuilder.build(), true);
  }

  private void testHoodieConcatHandleOnDupInserts(HoodieWriteConfig config, boolean isPrepped) throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder()
        .withProps(config.getProps())
        .withMergeAllowDuplicateOnInserts(true)
        .build();

    HoodieJavaWriteClient<RawTripTestPayload> client = getHoodieWriteClient(hoodieWriteConfig);

    // Write 1 (only inserts)
    String initCommitTime = "000";
    String newCommitTime = "001";
    int firstInsertRecords = 50;
    insertFirstBatch(hoodieWriteConfig, client, newCommitTime, initCommitTime, firstInsertRecords, HoodieJavaWriteClient::insert,
        isPrepped, true, firstInsertRecords, config.populateMetaFields());

    // Write 2 (updates with duplicates)
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    int secondInsertRecords = 100; // needs to be larger than firstInsertRecords to guarantee duplicate keys
    List<String> commitTimesBetweenPrevAndNew = Arrays.asList("002", "003");

    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPrepped, hoodieWriteConfig, dataGen::generateUpdates);

    writeBatch(client, newCommitTime, prevCommitTime, Option.of(commitTimesBetweenPrevAndNew), initCommitTime,
        secondInsertRecords, recordGenFunction, HoodieJavaWriteClient::insert, true, secondInsertRecords,
        firstInsertRecords + secondInsertRecords, 2, false, config.populateMetaFields());
  }

  /**
   * Tests deletion of records.
   */
  public void testDeletes() throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY);
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    HoodieJavaWriteClient client = getHoodieWriteClient(cfgBuilder.build());
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
    writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime,
        // unused as genFn uses hard-coded number of inserts/updates/deletes
        -1, recordGenFunction, HoodieJavaWriteClient::upsert, true, 200, 200, 1, false,
        true);

    /**
     * Write 2 (deletes+writes).
     */
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    final List<HoodieRecord> recordsInSecondBatch = new ArrayList<>();

    recordGenFunction = (String instantTime, Integer numRecordsInThisCommit) -> {
      List<HoodieRecord> fewRecordsForDelete = recordsInFirstBatch.subList(0, 50);
      recordsInSecondBatch.addAll(dataGen.generateDeletesFromExistingRecords(fewRecordsForDelete));
      recordsInSecondBatch.addAll(dataGen.generateInserts(instantTime, 25));
      return recordsInSecondBatch;
    };
    writeBatch(client, newCommitTime, prevCommitTime, Option.empty(), initCommitTime, 75, recordGenFunction,
        HoodieJavaWriteClient::upsert, true, 25, 175, 2, false,
        true);
  }

  /**
   * When records getting inserted are deleted in the same write batch, hudi should have deleted those records and
   * not be available in read path.
   *
   * @throws Exception
   */
  @Test
  public void testDeletesForInsertsInSameBatch() throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY);
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    HoodieJavaWriteClient client = getHoodieWriteClient(cfgBuilder.build());
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

    writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime,
        -1, recordGenFunction, HoodieJavaWriteClient::upsert, true, 150, 150, 1, false,
        true);
  }

  @Test
  public void testAndValidateClusteringOutputFiles() throws IOException {
    String partitionPath = "2015/03/16";
    testInsertTwoBatches(true, partitionPath);

    // Trigger clustering
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder().withEmbeddedTimelineServerEnabled(false).withAutoCommit(false)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName())
            .withInlineClustering(true).withInlineClusteringNumCommits(2).build());
    try (HoodieJavaWriteClient client = getHoodieWriteClient(cfgBuilder.build())) {
      int numRecords = 200;
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records1 = dataGen.generateInserts(newCommitTime, numRecords);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> statuses = client.insert(records1, newCommitTime);
      client.commit(newCommitTime, statuses);
      assertNoWriteErrors(statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieInstant replaceCommitInstant =
          metaClient.getActiveTimeline().getCompletedReplaceTimeline().firstInstant().get();
      HoodieReplaceCommitMetadata replaceCommitMetadata = HoodieReplaceCommitMetadata
          .fromBytes(metaClient.getActiveTimeline().getInstantDetails(replaceCommitInstant).get(),
              HoodieReplaceCommitMetadata.class);

      List<String> filesFromReplaceCommit = new ArrayList<>();
      replaceCommitMetadata.getPartitionToWriteStats()
          .forEach((k, v) -> v.forEach(entry -> filesFromReplaceCommit.add(entry.getPath())));

      // find all parquet files created as part of clustering. Verify it matches w/ what is found in replace commit metadata.
      List<StoragePathInfo> pathInfoList =
          storage.listDirectEntries(new StoragePath(basePath + "/" + partitionPath));
      List<String> clusteredFiles = pathInfoList.stream()
          .filter(
              entry -> entry.getPath().getName().contains(replaceCommitInstant.getTimestamp()))
          .map(fileStatus -> partitionPath + "/" + fileStatus.getPath().getName())
          .collect(Collectors.toList());
      assertEquals(clusteredFiles, filesFromReplaceCommit);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInlineScheduleClustering(boolean scheduleInlineClustering) throws IOException {
    testInsertTwoBatches(true);

    // setup clustering config.
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(1).withInlineClustering(false).withScheduleInlineClustering(scheduleInlineClustering)
        .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName())
        .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
        .build();

    HoodieWriteConfig config = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY).withAutoCommit(false)
        .withClusteringConfig(clusteringConfig)
        .withProps(getPropertiesForKeyGen()).build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);
    dataGen = new HoodieTestDataGenerator(new String[] {"2015/03/16"});
    String commitTime1 = HoodieActiveTimeline.createNewInstantTime();
    List<HoodieRecord> records1 = dataGen.generateInserts(commitTime1, 200);
    client.startCommitWithTime(commitTime1);
    List<WriteStatus> statuses = client.upsert(records1, commitTime1);
    assertNoWriteErrors(statuses);
    client.commit(commitTime1, statuses);

    HoodieTableMetaClient metaClient = createMetaClient();
    List<Pair<HoodieInstant, HoodieClusteringPlan>> pendingClusteringPlans =
        ClusteringUtils.getAllPendingClusteringPlans(metaClient).collect(Collectors.toList());
    if (scheduleInlineClustering) {
      assertEquals(1, pendingClusteringPlans.size());
    } else {
      assertEquals(0, pendingClusteringPlans.size());
    }
  }

  private List<HoodieRecord> testInsertAndClustering(HoodieClusteringConfig clusteringConfig, boolean populateMetaFields,
                                                     boolean completeClustering, boolean assertSameFileIds, String validatorClasses,
                                                     String sqlQueryForEqualityValidation, String sqlQueryForSingleResultValidation) throws Exception {
    Pair<Pair<List<HoodieRecord>, List<String>>, Set<HoodieFileGroupId>> allRecords = testInsertTwoBatches(populateMetaFields);
    testClustering(clusteringConfig, populateMetaFields, completeClustering, assertSameFileIds, validatorClasses, sqlQueryForEqualityValidation, sqlQueryForSingleResultValidation, allRecords);
    return allRecords.getLeft().getLeft();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testFailWritesOnInlineTableServiceExceptions(boolean shouldFail) throws IOException {
    try {
      Properties properties = new Properties();
      properties.setProperty("hoodie.fail.writes.on.inline.table.service.exception", String.valueOf(shouldFail));
      properties.setProperty("hoodie.auto.commit", "false");
      properties.setProperty("hoodie.clustering.inline.max.commits", "1");
      properties.setProperty("hoodie.clustering.inline", "true");
      testInsertTwoBatches(true, "2015/03/16", properties, true);
      assertFalse(shouldFail);
    } catch (HoodieException e) {
      assertEquals(CLUSTERING_FAILURE, e.getMessage());
      assertTrue(shouldFail);
    }
  }

  private Pair<Pair<List<HoodieRecord>, List<String>>, Set<HoodieFileGroupId>> testInsertTwoBatches(boolean populateMetaFields) throws IOException {
    return testInsertTwoBatches(populateMetaFields, "2015/03/16");
  }

  private Pair<Pair<List<HoodieRecord>, List<String>>, Set<HoodieFileGroupId>> testInsertTwoBatches(boolean populateMetaFields, String partitionPath) throws IOException {
    return testInsertTwoBatches(populateMetaFields, partitionPath, new Properties(), false);
  }

  /**
   * This method returns following three items:
   * 1. List of all HoodieRecord written in the two batches of insert.
   * 2. Commit instants of the two batches.
   * 3. List of new file group ids that were written in the two batches.
   */
  private Pair<Pair<List<HoodieRecord>, List<String>>, Set<HoodieFileGroupId>> testInsertTwoBatches(boolean populateMetaFields, String partitionPath, Properties props,
                                                                                                    boolean failInlineClustering) throws IOException {
    // create config to not update small files.
    HoodieWriteConfig config = getSmallInsertWriteConfig(2000, TRIP_EXAMPLE_SCHEMA, 10, false, populateMetaFields,
        populateMetaFields ? props : getPropertiesForKeyGen());
    HoodieJavaWriteClient client;
    if (failInlineClustering) {
      if (null != writeClient) {
        writeClient.close();
        writeClient = null;
      }
      client = new WriteClientBrokenClustering(context, config);
    } else {
      client = getHoodieWriteClient(config);
    }

    dataGen = new HoodieTestDataGenerator(new String[] {partitionPath});
    String commitTime1 = HoodieActiveTimeline.createNewInstantTime();
    List<HoodieRecord> records1 = dataGen.generateInserts(commitTime1, 200);
    List<WriteStatus> statuses1 = writeAndVerifyBatch(client, records1, commitTime1, populateMetaFields, failInlineClustering);
    Set<HoodieFileGroupId> fileIds1 = getFileGroupIdsFromWriteStatus(statuses1);

    String commitTime2 = HoodieActiveTimeline.createNewInstantTime();
    List<HoodieRecord> records2 = dataGen.generateInserts(commitTime2, 200);
    List<WriteStatus> statuses2 = writeAndVerifyBatch(client, records2, commitTime2, populateMetaFields, failInlineClustering);
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

  private void testClustering(HoodieClusteringConfig clusteringConfig, boolean populateMetaFields, boolean completeClustering, boolean assertSameFileIds,
                              String validatorClasses, String sqlQueryForEqualityValidation, String sqlQueryForSingleResultValidation,
                              Pair<Pair<List<HoodieRecord>, List<String>>, Set<HoodieFileGroupId>> allRecords) throws IOException {

    HoodieWriteConfig config = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY).withAutoCommit(false)
        .withClusteringConfig(clusteringConfig)
        .withProps(getPropertiesForKeyGen()).build();
    HoodieWriteMetadata<List<WriteStatus>> clusterMetadata =
        performClustering(clusteringConfig, populateMetaFields, completeClustering, validatorClasses, sqlQueryForEqualityValidation, sqlQueryForSingleResultValidation, allRecords.getLeft());
    if (assertSameFileIds) {
      Set<HoodieFileGroupId> replacedFileIds = clusterMetadata.getWriteStats().get().stream()
          .map(s -> new HoodieFileGroupId(s.getPartitionPath(), s.getFileId())).collect(Collectors.toSet());
      Set<HoodieFileGroupId> insertedFileIds = allRecords.getRight();
      assertEquals(insertedFileIds, replacedFileIds);
    }
    if (completeClustering) {
      String clusteringCommitTime = metaClient.reloadActiveTimeline().getCompletedReplaceTimeline()
          .getReverseOrderedInstants().findFirst().get().getTimestamp();
      verifyRecordsWritten(clusteringCommitTime, populateMetaFields, allRecords.getLeft().getLeft(), clusterMetadata.getWriteStatuses(), config);
    }
  }

  private HoodieWriteMetadata<List<WriteStatus>> performClustering(HoodieClusteringConfig clusteringConfig,
                                                                   boolean populateMetaFields,
                                                                   boolean completeClustering,
                                                                   String validatorClasses,
                                                                   String sqlQueryForEqualityValidation, String sqlQueryForSingleResultValidation,
                                                                   Pair<List<HoodieRecord>, List<String>> allRecords) throws IOException {
    HoodiePreCommitValidatorConfig validatorConfig = HoodiePreCommitValidatorConfig.newBuilder()
        .withPreCommitValidator(StringUtils.nullToEmpty(validatorClasses))
        .withPrecommitValidatorEqualitySqlQueries(sqlQueryForEqualityValidation)
        .withPrecommitValidatorSingleResultSqlQueries(sqlQueryForSingleResultValidation)
        .build();

    HoodieWriteConfig config = getConfigBuilder().withAutoCommit(false)
        .withPreCommitValidatorConfig(validatorConfig)
        .withProps(populateMetaFields ? new Properties() : getPropertiesForKeyGen())
        .withClusteringConfig(clusteringConfig).build();

    // create client with new config.
    HoodieJavaWriteClient client = getHoodieWriteClient(config);
    String clusteringCommitTime = client.scheduleClustering(Option.empty()).get().toString();
    HoodieWriteMetadata<List<WriteStatus>> clusterMetadata = client.cluster(clusteringCommitTime, completeClustering);
    if (config.populateMetaFields()) {
      verifyRecordsWrittenWithPreservedMetadata(new HashSet<>(allRecords.getRight()), allRecords.getLeft(), clusterMetadata.getWriteStatuses());
    } else {
      verifyRecordsWritten(clusteringCommitTime, populateMetaFields, allRecords.getLeft(), clusterMetadata.getWriteStatuses(), config);
    }

    Set<HoodieFileGroupId> replacedFileIds = new HashSet<>();
    clusterMetadata.getPartitionToReplaceFileIds().entrySet().forEach(partitionFiles ->
        partitionFiles.getValue().stream().forEach(file ->
            replacedFileIds.add(new HoodieFileGroupId(partitionFiles.getKey(), file))));
    return clusterMetadata;
  }

  private Set<HoodieFileGroupId> getFileGroupIdsFromWriteStatus(List<WriteStatus> statuses) {
    return statuses.stream().map(s -> new HoodieFileGroupId(s.getPartitionPath(), s.getFileId())).collect(Collectors.toSet());
  }

  private Set<String> insertPartitionRecordsWithCommit(HoodieJavaWriteClient client, int recordsCount, String commitTime1, String partitionPath) throws IOException {
    client.startCommitWithTime(commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInsertsForPartition(commitTime1, recordsCount, partitionPath);
    List<WriteStatus> statuses = client.upsert(inserts1, commitTime1);
    assertNoWriteErrors(statuses);
    Set<String> batchBuckets = statuses.stream().map(s -> s.getFileId()).collect(Collectors.toSet());
    verifyRecordsWritten(commitTime1, true, inserts1, statuses, client.getConfig());
    return batchBuckets;
  }

  /**
   * Verify data in base files matches expected records and commit time.
   */
  private void verifyRecordsWritten(String commitTime, boolean populateMetadataField,
                                    List<HoodieRecord> expectedRecords, List<WriteStatus> allStatus, HoodieWriteConfig config) throws IOException {
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
      KeyGenerator keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(new TypedProperties(config.getProps()));
      for (GenericRecord record : records) {
        String recordKey = keyGenerator.getKey(record).getRecordKey();
        if (!populateMetadataField) {
          assertNull(record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD));
        }
        assertTrue(expectedKeys.contains(recordKey));
      }
    }
  }

  @NotNull
  private Set<String> verifyRecordKeys(List<HoodieRecord> expectedRecords, List<WriteStatus> allStatus, List<GenericRecord> records) {
    for (WriteStatus status : allStatus) {
      StoragePath filePath = new StoragePath(basePath, status.getStat().getPath());
      records.addAll(HoodieIOFactory.getIOFactory(metaClient.getStorage())
          .getFileFormatUtils(metaClient.getTableConfig().getBaseFileFormat())
          .readAvroRecords(storage, filePath));
    }
    Set<String> expectedKeys = recordsToRecordKeySet(expectedRecords);
    assertEquals(records.size(), expectedKeys.size());
    return expectedKeys;
  }

  private void verifyRecordsWrittenWithPreservedMetadata(Set<String> commitTimes, List<HoodieRecord> expectedRecords, List<WriteStatus> allStatus) {
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

  private List<WriteStatus> writeAndVerifyBatch(HoodieJavaWriteClient client, List<HoodieRecord> inserts, String commitTime, boolean populateMetaFields) throws IOException {
    return writeAndVerifyBatch(client, inserts, commitTime, populateMetaFields, false);
  }

  private List<WriteStatus> writeAndVerifyBatch(HoodieJavaWriteClient client, List<HoodieRecord> inserts, String commitTime, boolean populateMetaFields, boolean autoCommitOff) throws IOException {
    client.startCommitWithTime(commitTime);
    List<WriteStatus> statusRDD = client.upsert(inserts, commitTime);
    if (autoCommitOff) {
      client.commit(commitTime, statusRDD);
    }
    List<WriteStatus> statuses = statusRDD;
    assertNoWriteErrors(statuses);
    verifyRecordsWritten(commitTime, populateMetaFields, inserts, statuses, client.getConfig());

    return statuses;
  }

  private Pair<Set<String>, List<HoodieRecord>> testUpdates(String instantTime, HoodieJavaWriteClient client,
                                                            int sizeToInsertAndUpdate, int expectedTotalRecords)
      throws IOException {
    client.startCommitWithTime(instantTime);
    List<HoodieRecord> inserts = dataGen.generateInserts(instantTime, sizeToInsertAndUpdate);
    Set<String> keys = recordsToRecordKeySet(inserts);
    List<HoodieRecord> insertsAndUpdates = new ArrayList<>();
    insertsAndUpdates.addAll(inserts);
    insertsAndUpdates.addAll(dataGen.generateUpdates(instantTime, inserts));

    List<WriteStatus> statuses = client.upsert(insertsAndUpdates, instantTime);
    assertNoWriteErrors(statuses);

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(expectedTotalRecords,
        countRowsInPaths(basePath, storage, fullPartitionPaths),
        "Must contain " + expectedTotalRecords + " records");
    return Pair.of(keys, inserts);
  }

  /**
   * Test delete with delete api.
   */
  @Test
  public void testDeletesWithoutInserts() {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit,
        TRIP_EXAMPLE_SCHEMA, dataGen.getEstimatedFileSizeInBytes(150), true, true
            ? new Properties() : getPropertiesForKeyGen());
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    // delete non existent keys
    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);

    List<HoodieRecord> dummyInserts = dataGen.generateInserts(commitTime1, 20);
    List<HoodieKey> hoodieKeysToDelete = randomSelectAsHoodieKeys(dummyInserts, 20);
    client.delete(hoodieKeysToDelete, commitTime1);
  }

  /**
   * Test to ensure commit metadata points to valid files.
   */
  @Test
  public void testCommitWritesRelativePaths() throws Exception {

    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder().withAutoCommit(false);
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    try (HoodieJavaWriteClient client = getHoodieWriteClient(cfgBuilder.build())) {
      HoodieTableMetaClient metaClient = createMetaClient();
      HoodieJavaTable table = HoodieJavaTable.create(cfgBuilder.build(), context, metaClient);

      String instantTime = "000";
      client.startCommitWithTime(instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 200);

      List<WriteStatus> result = client.bulkInsert(records, instantTime);

      assertTrue(client.commit(instantTime, result), "Commit should succeed");
      assertTrue(testTable.commitExists(instantTime),
          "After explicit commit, commit file should be created");

      // Get base file paths from commit metadata
      String actionType = metaClient.getCommitActionType();
      HoodieInstant commitInstant = new HoodieInstant(false, actionType, instantTime);
      HoodieTimeline commitTimeline = metaClient.getCommitTimeline().filterCompletedInstants();
      HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
          .fromBytes(commitTimeline.getInstantDetails(commitInstant).get(),
              HoodieCommitMetadata.class);
      StoragePath basePath = table.getMetaClient().getBasePath();
      Collection<String> commitPathNames =
          commitMetadata.getFileIdAndFullPaths(basePath).values();

      // Read from commit file
      try (InputStream inputStream = storage.open(testTable.getCommitFilePath(instantTime))) {
        String everything = FileIOUtils.readAsUTFString(inputStream);
        HoodieCommitMetadata metadata = HoodieCommitMetadata.fromJsonString(everything, HoodieCommitMetadata.class);
        HashMap<String, String> paths = metadata.getFileIdAndFullPaths(basePath);
        // Compare values in both to make sure they are equal.
        for (String pathName : paths.values()) {
          assertTrue(commitPathNames.contains(pathName));
        }
      }
    }
  }

  /**
   * Test to ensure commit metadata points to valid files.10.
   */
  @Test
  public void testMetadataStatsOnCommit() throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder().withAutoCommit(false);
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    HoodieWriteConfig cfg = cfgBuilder.build();
    HoodieJavaWriteClient client = getHoodieWriteClient(cfg);

    String instantTime0 = "000";
    client.startCommitWithTime(instantTime0);

    List<HoodieRecord> records0 = dataGen.generateInserts(instantTime0, 200);
    List<WriteStatus> result0 = client.bulkInsert(records0, instantTime0);

    assertTrue(client.commit(instantTime0, result0), "Commit should succeed");
    assertTrue(testTable.commitExists(instantTime0),
        "After explicit commit, commit file should be created");

    // Read from commit file
    try (InputStream inputStream = storage.open(testTable.getCommitFilePath(instantTime0))) {
      String everything = FileIOUtils.readAsUTFString(inputStream);
      HoodieCommitMetadata metadata =
          HoodieCommitMetadata.fromJsonString(everything, HoodieCommitMetadata.class);
      int inserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          inserts += stat.getNumInserts();
        }
      }
      assertEquals(200, inserts);
    }

    // Update + Inserts such that they just expand file1
    String instantTime1 = "001";
    client.startCommitWithTime(instantTime1);

    List<HoodieRecord> records1 = dataGen.generateUpdates(instantTime1, records0);
    List<WriteStatus> result1 = client.upsert(records1, instantTime1);

    assertTrue(client.commit(instantTime1, result1), "Commit should succeed");
    assertTrue(testTable.commitExists(instantTime1),
        "After explicit commit, commit file should be created");

    // Read from commit file
    try (InputStream inputStream = storage.open(testTable.getCommitFilePath(instantTime1))) {
      String everything = FileIOUtils.readAsUTFString(inputStream);
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromJsonString(everything, HoodieCommitMetadata.class);
      int inserts = 0;
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
  }

  /**
   * Tests behavior of committing only when consistency is verified.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testConsistencyCheckDuringFinalize(boolean enableOptimisticConsistencyGuard) throws Exception {
    HoodieTableMetaClient metaClient = createMetaClient();
    String instantTime = "000";
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder()
        .withEnableOptimisticConsistencyGuard(enableOptimisticConsistencyGuard).build()).build();
    HoodieJavaWriteClient client = getHoodieWriteClient(cfg);
    Pair<StoragePath, List<WriteStatus>> result = testConsistencyCheck(
        metaClient, instantTime, enableOptimisticConsistencyGuard);

    // Delete orphan marker and commit should succeed
    metaClient.getStorage().deleteFile(result.getKey());
    if (!enableOptimisticConsistencyGuard) {
      assertTrue(client.commit(instantTime, result.getRight()), "Commit should succeed");
      assertTrue(testTable.commitExists(instantTime),
          "After explicit commit, commit file should be created");
      // Marker directory must be removed
      assertFalse(metaClient.getStorage()
          .exists(new StoragePath(metaClient.getMarkerFolderPath(instantTime))));
    } else {
      // with optimistic, first client.commit should have succeeded.
      assertTrue(testTable.commitExists(instantTime),
          "After explicit commit, commit file should be created");
      // Marker directory must be removed
      assertFalse(metaClient.getStorage().exists(new StoragePath(metaClient.getMarkerFolderPath(instantTime))));
    }
  }

  private void testRollbackAfterConsistencyCheckFailureUsingFileList(boolean rollbackUsingMarkers, boolean enableOptimisticConsistencyGuard,
                                                                     boolean populateMetaFields) throws Exception {
    String instantTime = "00000000000010";
    HoodieTableMetaClient metaClient = createMetaClient();

    Properties properties = new Properties();
    if (!populateMetaFields) {
      properties = getPropertiesForKeyGen();
    }

    HoodieWriteConfig cfg = !enableOptimisticConsistencyGuard ? getConfigBuilder().withRollbackUsingMarkers(rollbackUsingMarkers).withAutoCommit(false)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true)
            .withMaxConsistencyCheckIntervalMs(1).withInitialConsistencyCheckIntervalMs(1).withEnableOptimisticConsistencyGuard(enableOptimisticConsistencyGuard).build()).build() :
        getConfigBuilder().withRollbackUsingMarkers(rollbackUsingMarkers).withAutoCommit(false)
            .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder()
                .withConsistencyCheckEnabled(true)
                .withEnableOptimisticConsistencyGuard(enableOptimisticConsistencyGuard)
                .withOptimisticConsistencyGuardSleepTimeMs(1).build())
            .withProperties(properties).build();
    HoodieJavaWriteClient client = getHoodieWriteClient(cfg);
    testConsistencyCheck(metaClient, instantTime, enableOptimisticConsistencyGuard);

    if (!enableOptimisticConsistencyGuard) {
      // Rollback of this commit should succeed with FailSafeCG
      client.rollback(instantTime);
      assertFalse(testTable.commitExists(instantTime),
          "After explicit rollback, commit file should not be present");
      // Marker directory must be removed after rollback
      assertFalse(metaClient.getStorage().exists(new StoragePath(metaClient.getMarkerFolderPath(instantTime))));
    } else {
      // if optimistic CG is enabled, commit should have succeeded.
      assertTrue(testTable.commitExists(instantTime),
          "With optimistic CG, first commit should succeed. commit file should be present");
      // Marker directory must be removed after rollback
      assertFalse(metaClient.getStorage().exists(new StoragePath(metaClient.getMarkerFolderPath(instantTime))));
      client.rollback(instantTime);
      assertFalse(testTable.commitExists(instantTime),
          "After explicit rollback, commit file should not be present");
    }
  }

  @ParameterizedTest
  @MethodSource("rollbackAfterConsistencyCheckFailureParams")
  public void testRollbackAfterConsistencyCheckFailureUsingFileList(boolean enableOptimisticConsistencyGuard) throws Exception {
    testRollbackAfterConsistencyCheckFailureUsingFileList(false, enableOptimisticConsistencyGuard, true);
  }

  @ParameterizedTest
  @MethodSource("rollbackAfterConsistencyCheckFailureParams")
  public void testRollbackAfterConsistencyCheckFailureUsingMarkers(boolean enableOptimisticConsistencyGuard) throws Exception {
    testRollbackAfterConsistencyCheckFailureUsingFileList(true, enableOptimisticConsistencyGuard, true);
  }

  @Test
  public void testRollbackFailedCommits() throws Exception {
    // HoodieFailedWritesCleaningPolicy cleaningPolicy, boolean populateMetaFields
    HoodieFailedWritesCleaningPolicy cleaningPolicy = HoodieFailedWritesCleaningPolicy.NEVER;
    boolean populateMetaFields = true;
    HoodieTestUtils.init(storageConf, basePath);
    HoodieJavaWriteClient client = new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));

    // perform 1 successful commit
    String commit1 = HoodieActiveTimeline.createNewInstantTime();
    writeBatch(client, commit1, commit1, Option.of(Arrays.asList(commit1)), commit1,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 300,
        0, true);

    // Perform 2 failed writes to table
    String commit2 = HoodieActiveTimeline.createNewInstantTime();
    writeBatch(client, commit2, commit1, Option.of(Arrays.asList(commit2)), commit1,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 300,
        0, false);
    client.close();
    String commit3 = HoodieActiveTimeline.createNewInstantTime();
    client = new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    writeBatch(client, commit3, commit2, Option.of(Arrays.asList(commit3)), commit3,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 300,
        0, false);
    client.close();
    // refresh data generator to delete records generated from failed commits
    dataGen = new HoodieTestDataGenerator();
    // Perform 1 successful write
    String commit4 = HoodieActiveTimeline.createNewInstantTime();
    client = new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    writeBatch(client, commit4, commit3, Option.of(Arrays.asList(commit4)), commit4,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 300,
        0, true);
    HoodieTableMetaClient metaClient = createMetaClient();

    assertTrue(metaClient.getActiveTimeline().getTimelineOfActions(
        CollectionUtils.createSet(ROLLBACK_ACTION)).countInstants() == 0);
    assertTrue(metaClient.getActiveTimeline().filterInflights().countInstants() == 2);
    assertTrue(metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants() == 2);
    // Await till enough time passes such that the first 2 failed commits heartbeats are expired
    boolean conditionMet = false;
    while (!conditionMet) {
      conditionMet = client.getHeartbeatClient().isHeartbeatExpired(commit3);
      Thread.sleep(2000);
    }
    client.close();
    client = new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, populateMetaFields));
    // Perform 1 successful write
    String commit5 = HoodieActiveTimeline.createNewInstantTime();
    writeBatch(client, commit5, commit4, Option.of(Arrays.asList(commit5)), commit5,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 300,
        0, true);
    client.clean();
    client.close();
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline().reload();
    if (cleaningPolicy.isLazy()) {
      assertTrue(
          timeline
              .getTimelineOfActions(CollectionUtils.createSet(ROLLBACK_ACTION))
              .countInstants()
              == 2);
      // Since we write rollbacks not clean, there should be no clean action on the timeline
      assertTrue(
          timeline
              .getTimelineOfActions(CollectionUtils.createSet(CLEAN_ACTION))
              .countInstants()
              == 0);
      assertTrue(timeline.getCommitsTimeline().filterCompletedInstants().countInstants() == 3);
    } else if (cleaningPolicy.isNever()) {
      // never will get translated to Lazy if OCC is enabled.
      assertTrue(
          timeline
              .getTimelineOfActions(CollectionUtils.createSet(ROLLBACK_ACTION))
              .countInstants()
              == 2);
      // There should be no clean or rollback action on the timeline
      assertTrue(
          timeline
              .getTimelineOfActions(CollectionUtils.createSet(CLEAN_ACTION))
              .countInstants()
              == 0);
      assertTrue(timeline.getCommitsTimeline().filterCompletedInstants().countInstants() == 3);
    }
  }

  @Test
  public void testRollbackFailedCommitsToggleCleaningPolicy() throws Exception {
    HoodieTestUtils.init(storageConf, basePath);
    HoodieFailedWritesCleaningPolicy cleaningPolicy = EAGER;
    HoodieJavaWriteClient client = new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, true));
    String commit1 = HoodieActiveTimeline.createNewInstantTime();
    // Perform 1 successful writes to table
    writeBatch(client, commit1, commit1, Option.of(Arrays.asList(commit1)), commit1,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 300,
        0, true);

    // Perform 1 failed writes to table
    String commit2 = HoodieActiveTimeline.createNewInstantTime();
    writeBatch(client, commit2, commit1, Option.of(Arrays.asList(commit2)), commit2,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 300,
        0, false);
    client.close();
    // Toggle cleaning policy to LAZY
    cleaningPolicy = HoodieFailedWritesCleaningPolicy.LAZY;
    String commit3 = HoodieActiveTimeline.createNewInstantTime();
    // Perform 2 failed writes to table
    client = new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, true));
    writeBatch(client, commit3, commit2, Option.of(Arrays.asList(commit3)), commit3,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 300,
        0, false);
    client.close();
    String commit4 = HoodieActiveTimeline.createNewInstantTime();
    client = new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, true));
    writeBatch(client, commit4, commit3, Option.of(Arrays.asList(commit4)), commit4,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 300,
        0, false);
    client.close();
    // Await till enough time passes such that the 2 failed commits heartbeats are expired
    boolean conditionMet = false;
    while (!conditionMet) {
      conditionMet = client.getHeartbeatClient().isHeartbeatExpired(commit4);
      Thread.sleep(2000);
    }
    client.clean();
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline().reload();
    assertTrue(timeline.getTimelineOfActions(
        CollectionUtils.createSet(ROLLBACK_ACTION)).countInstants() == 3);
    String commit5 = HoodieActiveTimeline.createNewInstantTime();
    // Perform 2 failed commits
    client = new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, true));
    writeBatch(client, commit5, commit4, Option.of(Arrays.asList(commit3)), commit3,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 300,
        0, false);
    client.close();
    String commit6 = HoodieActiveTimeline.createNewInstantTime();
    client = new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, true));
    writeBatch(client, commit6, commit5, Option.of(Arrays.asList(commit4)), commit4,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 300,
        0, false);
    client.close();
    // Toggle cleaning policy to EAGER
    cleaningPolicy = EAGER;
    client = new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, true));
    client.startCommit();
    timeline = metaClient.getActiveTimeline().reload();
    // since OCC is enabled, hudi auto flips the cleaningPolicy to Lazy.
    assertTrue(timeline.getTimelineOfActions(
        CollectionUtils.createSet(ROLLBACK_ACTION)).countInstants() == 3);
    assertTrue(timeline.getCommitsTimeline().filterCompletedInstants().countInstants() == 1);
    client.close();
  }

  @Test
  public void testParallelInsertAndCleanPreviousFailedCommits() throws Exception {
    HoodieFailedWritesCleaningPolicy cleaningPolicy = HoodieFailedWritesCleaningPolicy.LAZY;
    ExecutorService service = Executors.newFixedThreadPool(2);
    HoodieTestUtils.init(storageConf, basePath);
    HoodieJavaWriteClient client = new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, true));
    String commit1 = HoodieActiveTimeline.createNewInstantTime();
    // perform 1 successful write
    writeBatch(client, commit1, commit1, Option.of(Arrays.asList(commit1)), commit1,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 100,
        0, true);

    // Perform 2 failed writes to table
    String commit2 = HoodieActiveTimeline.createNewInstantTime();
    writeBatch(client, commit2, commit1, Option.of(Arrays.asList(commit2)), commit2,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 100,
        0, false);
    client.close();

    String commit3 = HoodieActiveTimeline.createNewInstantTime();
    client = new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, true));
    writeBatch(client, commit3, commit2, Option.of(Arrays.asList(commit3)), commit3,
        100, dataGen::generateInserts, HoodieJavaWriteClient::bulkInsert, false, 100, 100,
        0, false);
    client.close();
    // refresh data generator to delete records generated from failed commits
    dataGen = new HoodieTestDataGenerator();
    String commit4 = HoodieActiveTimeline.createNewInstantTime();
    // Create a successful commit
    Future<List<WriteStatus>> commit4Future = service.submit(() -> writeBatch(new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, true)),
        commit4, commit3, Option.of(Arrays.asList(commit4)), commit3, 100, dataGen::generateInserts,
        HoodieJavaWriteClient::bulkInsert, false, 100, 100, 0, true));
    commit4Future.get();
    HoodieTableMetaClient metaClient = createMetaClient();

    assertTrue(metaClient.getActiveTimeline().getTimelineOfActions(
        CollectionUtils.createSet(ROLLBACK_ACTION)).countInstants() == 0);
    assertTrue(metaClient.getActiveTimeline().filterInflights().countInstants() == 2);
    assertTrue(metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants() == 2);
    client = new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, true));
    // Await till enough time passes such that the first 2 failed commits heartbeats are expired
    boolean conditionMet = false;
    while (!conditionMet) {
      conditionMet = client.getHeartbeatClient().isHeartbeatExpired(commit3);
      Thread.sleep(2000);
    }
    String commit5 = HoodieActiveTimeline.createNewInstantTime();
    Future<List<WriteStatus>> commit5Future = service.submit(() -> writeBatch(new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, true)),
        commit5, commit4, Option.of(Arrays.asList(commit5)), commit5, 100, dataGen::generateInserts,
        HoodieJavaWriteClient::bulkInsert, false, 100, 100, 0, true));
    Future<HoodieCleanMetadata> clean1 = service.submit(() -> new HoodieJavaWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy, true)).clean());
    commit5Future.get();
    clean1.get();
    client.close();
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline().reload();
    assertTrue(timeline.getTimelineOfActions(
        CollectionUtils.createSet(ROLLBACK_ACTION)).countInstants() == 2);
    // Since we write rollbacks not clean, there should be no clean action on the timeline
    assertTrue(timeline.getTimelineOfActions(
        CollectionUtils.createSet(CLEAN_ACTION)).countInstants() == 0);
    assertTrue(timeline.getCommitsTimeline().filterCompletedInstants().countInstants() == 3);
    service.shutdown();
  }

  private Pair<StoragePath, List<WriteStatus>> testConsistencyCheck(HoodieTableMetaClient metaClient, String instantTime, boolean enableOptimisticConsistencyGuard)
      throws Exception {
    HoodieWriteConfig cfg = !enableOptimisticConsistencyGuard ? (getConfigBuilder().withAutoCommit(false)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true)
            .withMaxConsistencyCheckIntervalMs(1).withInitialConsistencyCheckIntervalMs(1).withEnableOptimisticConsistencyGuard(enableOptimisticConsistencyGuard).build())
        .build()) : (getConfigBuilder().withAutoCommit(false)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true)
            .withEnableOptimisticConsistencyGuard(enableOptimisticConsistencyGuard)
            .withOptimisticConsistencyGuardSleepTimeMs(1).build())
        .build());
    HoodieJavaWriteClient client = getHoodieWriteClient(cfg);

    client.startCommitWithTime(instantTime);
    List<HoodieRecord> writeRecords = dataGen.generateInserts(instantTime, 200);
    List<WriteStatus> result = client.bulkInsert(writeRecords, instantTime);

    // Create a dummy marker file to simulate the case that a marker file was created without data file.
    // This should fail the commit
    String partitionPath;
    String markerFolderPath = metaClient.getMarkerFolderPath(instantTime);
    FileSystem fs = (FileSystem) storage.getFileSystem();
    if (cfg.getMarkersType() == MarkerType.TIMELINE_SERVER_BASED) {
      String markerName = MarkerUtils.readTimelineServerBasedMarkersFromFileSystem(
              markerFolderPath, storage, context, 1).values().stream()
          .flatMap(Collection::stream).findFirst().get();
      partitionPath = new Path(markerFolderPath, markerName).getParent().toString();
    } else {
      partitionPath = Arrays
          .stream(fs.globStatus(new Path(String.format("%s/*/*/*/*", markerFolderPath)),
              path -> path.toString().contains(HoodieTableMetaClient.MARKER_EXTN)))
          .limit(1).map(status -> status.getPath().getParent().toString()).collect(Collectors.toList()).get(0);
    }

    Option<StoragePath> markerFilePath = WriteMarkersFactory.get(
            cfg.getMarkersType(), getHoodieTable(metaClient, cfg), instantTime)
        .create(partitionPath,
            FSUtils.makeBaseFileName(instantTime, "1-0-1", UUID.randomUUID().toString(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension()),
            IOType.MERGE);
    if (!enableOptimisticConsistencyGuard) {
      Exception e = assertThrows(HoodieCommitException.class, () -> {
        client.commit(instantTime, result);
      }, "Commit should fail due to consistency check");
      assertTrue(e.getCause() instanceof HoodieIOException);
    } else {
      // with optimistic CG, commit should succeed
      client.commit(instantTime, result);
    }
    return Pair.of(markerFilePath.get(), result);
  }

  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, String schemaStr, long smallFileSize, boolean populateMetaFields, Properties props) {
    return getSmallInsertWriteConfig(insertSplitSize, schemaStr, smallFileSize, false, populateMetaFields, props);
  }

  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, String schemaStr, long smallFileSize, boolean mergeAllowDuplicateInserts,
                                                      boolean populateMetaFields, Properties props) {
    HoodieWriteConfig.Builder builder = getConfigBuilder(schemaStr);
    if (!populateMetaFields) {
      builder.withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(IndexType.SIMPLE).build());
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
        .withProps(props)
        .build();
  }

  private HoodieWriteConfig getParallelWritingWriteConfig(HoodieFailedWritesCleaningPolicy cleaningPolicy, boolean populateMetaFields) {
    Properties properties = new Properties();
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "3000");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY, "20");
    if (!populateMetaFields) {
      getPropertiesForKeyGen(populateMetaFields).entrySet().forEach(kv ->
          properties.put(kv.getKey(), kv.getValue()));
    }
    return getConfigBuilder()
        .withEmbeddedTimelineServerEnabled(false)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(cleaningPolicy)
            .withAutoClean(false).build())
        .withTimelineLayoutVersion(1)
        .withHeartbeatIntervalInMs(3 * 1000)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withAutoCommit(false)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withProperties(properties).build();
  }

  public static class WriteClientBrokenClustering<T extends HoodieRecordPayload> extends org.apache.hudi.client.HoodieJavaWriteClient<T> {

    public WriteClientBrokenClustering(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
      super(context, clientConfig);
    }

    @Override
    protected void runTableServicesInline(HoodieTable table, HoodieCommitMetadata metadata, Option<Map<String, String>> extraMetadata) {
      if (config.inlineClusteringEnabled()) {
        throw new HoodieException(CLUSTERING_FAILURE);
      }
    }

  }
}
