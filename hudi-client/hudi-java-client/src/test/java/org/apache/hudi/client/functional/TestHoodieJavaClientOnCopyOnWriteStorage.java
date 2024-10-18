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
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy;
import org.apache.hudi.client.common.JavaTaskContextSupplier;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.InstantFactory;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCorruptedDataException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.upgrade.JavaUpgradeDowngradeHelper;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FACTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings("unchecked")
public class TestHoodieJavaClientOnCopyOnWriteStorage extends HoodieJavaClientTestHarness {

  private static Stream<Arguments> rollbackAfterConsistencyCheckFailureParams() {
    return Stream.of(
        Arguments.of(true),
        Arguments.of(false)
    );
  }

  @BeforeEach
  public void setUpTestTable() {
    testTable = HoodieMetadataTestTable.of(metaClient);
  }

  private static final Function<Object, Object> IDENTITY = Function.identity();

  private final Function<HoodieWriteConfig, BaseHoodieWriteClient> createBrokenClusteringClient =
      config -> new WriteClientBrokenClustering<>(context, config);

  private final Function2<HoodieTable, HoodieTableMetaClient, HoodieWriteConfig> getHoodieTable =
      (metaClient, config) -> getHoodieTable(metaClient, config);

  @Override
  protected Object castInsertFirstBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime,
                                        String initCommitTime, int numRecordsInThisCommit,
                                        Function3<Object, BaseHoodieWriteClient, Object, String> writeFn, boolean isPreppedAPI,
                                        boolean assertForCommit, int expRecordsInThisCommit,
                                        boolean filterForCommitTimeWithAssert, InstantFactory instantFactory) throws Exception {
    return insertFirstBatch(writeConfig, (HoodieJavaWriteClient) client, newCommitTime, initCommitTime, numRecordsInThisCommit,
        (writeClient, records, commitTime) -> (List<WriteStatus>) writeFn.apply(writeClient, records, commitTime),
        isPreppedAPI, assertForCommit, expRecordsInThisCommit, filterForCommitTimeWithAssert, instantFactory);
  }

  @Override
  protected Object castWriteBatch(BaseHoodieWriteClient client, String newCommitTime, String prevCommitTime,
                                  Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                  Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                  Function3<Object, BaseHoodieWriteClient, Object, String> writeFn,
                                  boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits, boolean doCommit,
                                  boolean filterForCommitTimeWithAssert, InstantFactory instantFactory) throws Exception {
    return writeBatch((HoodieJavaWriteClient) client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        (writeClient, records, commitTime) -> (List<WriteStatus>) writeFn.apply(writeClient, records, commitTime),
        assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, doCommit, filterForCommitTimeWithAssert, instantFactory);
  }

  @Override
  protected Object castUpdateBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime, String prevCommitTime,
                                   Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                   Function3<Object, BaseHoodieWriteClient, Object, String> writeFn, boolean isPreppedAPI,
                                   boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                   boolean filterForCommitTimeWithAssert, InstantFactory instantFactory) throws Exception {
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateUniqueUpdates);

    return writeBatch((HoodieJavaWriteClient) client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        (writeClient, records, commitTime) -> (List<WriteStatus>) writeFn.apply(writeClient, records, commitTime), assertForCommit, expRecordsInThisCommit, expTotalRecords,
        expTotalCommits, false, filterForCommitTimeWithAssert, instantFactory);
  }

  @Override
  protected Object castDeleteBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime,
                                   String prevCommitTime, String initCommitTime, int numRecordsInThisCommit, boolean isPreppedAPI,
                                   boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords,
                                   boolean filterForCommitTimeWithAssert, TimelineFactory timelineFactory, InstantFactory instantFactory) throws Exception {
    return deleteBatch(writeConfig, (HoodieJavaWriteClient) client, newCommitTime, prevCommitTime, initCommitTime, numRecordsInThisCommit,
        isPreppedAPI, assertForCommit, expRecordsInThisCommit, expTotalRecords, filterForCommitTimeWithAssert, timelineFactory, instantFactory);
  }

  @Override
  protected String[] assertTheEntireDatasetHasAllRecordsStill(int expectedRecords) {
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(200, countRowsInPaths(basePath, storage, fullPartitionPaths), "Must contain " + 200 + " records");
    return fullPartitionPaths;
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient insert API.
   */
  @Test
  public void testAutoCommitOnInsert() throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.insert(recordRDD, instantTime), false, true, INSTANT_FACTORY);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient insertPrepped API.
   */
  @Test
  public void testAutoCommitOnInsertPrepped() throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.insertPreppedRecords(recordRDD, instantTime), true, true, INSTANT_FACTORY);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient upsert API.
   */
  @Test
  public void testAutoCommitOnUpsert() throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.upsert(recordRDD, instantTime), false, true, INSTANT_FACTORY);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient upsert Prepped API.
   */
  @Test
  public void testAutoCommitOnUpsertPrepped() throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.upsertPreppedRecords(recordRDD, instantTime), true, true, INSTANT_FACTORY);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient bulk-insert API.
   */
  @Test
  public void testAutoCommitOnBulkInsert() throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.bulkInsert(recordRDD, instantTime), false, true, INSTANT_FACTORY);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient bulk-insert prepped API.
   */
  @Test
  public void testAutoCommitOnBulkInsertPrepped() throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.bulkInsertPreppedRecords(recordRDD, instantTime,
        Option.empty()), true, true, INSTANT_FACTORY);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient insert API.
   */
  @Test
  public void testDeduplicationOnInsert() throws Exception {
    testDeduplication((client, records, commitTime) -> (List<WriteStatus>) client.insert(records, commitTime), true, false);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient insert API.
   */
  @Test
  public void testDeduplicationKeepOperationFieldOnInsert() throws Exception {
    testDeduplication((client, records, commitTime) -> (List<WriteStatus>) client.insert(records, commitTime), true, true);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient bulk-insert API.
   */
  @Test
  public void testDeduplicationOnBulkInsert() throws Exception {
    testDeduplication((client, records, commitTime) -> (List<WriteStatus>) client.bulkInsert(records, commitTime), true, false);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient upsert API.
   */
  @Test
  public void testDeduplicationOnUpsert() throws Exception {
    testDeduplication((client, records, commitTime) -> (List<WriteStatus>) client.upsert(records, commitTime), true, false);
  }

  /**
   * Test Upsert API.
   */
  @Test
  public void testUpserts() throws Exception {
    testUpsertsInternal((writeClient, recordRDD, instantTime) -> writeClient.upsert(recordRDD, instantTime), true, false, JavaUpgradeDowngradeHelper.getInstance());
  }

  /**
   * Test UpsertPrepped API.
   */
  @Test
  public void testUpsertsPrepped() throws Exception {
    testUpsertsInternal((writeClient, recordRDD, instantTime) -> writeClient.upsertPreppedRecords(recordRDD, instantTime), true, true, JavaUpgradeDowngradeHelper.getInstance());
  }

  @Override
  protected void testMergeHandle(HoodieWriteConfig config) throws IOException {
    final String instantTime = "007";
    HoodieTableMetaClient metaClient = createMetaClient();
    HoodieTable table = getHoodieTable(metaClient, config);
    Pair<String, String> partitionAndBaseFilePaths = getPartitionAndBaseFilePathsFromLatestCommitMetadata(metaClient);
    HoodieBaseFile baseFile = new HoodieBaseFile(partitionAndBaseFilePaths.getRight());

    HoodieMergeHandle handle = null;
    try {
      handle = new HoodieMergeHandle(config, instantTime, table, new HashMap<>(),
          partitionAndBaseFilePaths.getLeft(), FSUtils.getFileId(baseFile.getFileName()), baseFile, new JavaTaskContextSupplier(),
          config.populateMetaFields() ? Option.empty() :
              Option.of((BaseKeyGenerator) HoodieAvroKeyGeneratorFactory.createKeyGenerator(config.getProps())));
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
      config.getProps().setProperty("hoodie.merge.data.validation.enabled", "true");
      HoodieWriteConfig cfg2 = HoodieWriteConfig.newBuilder().withProps(config.getProps()).build();
      // does the handle need to be closed to clean up the writer it contains?
      handle = new HoodieMergeHandle(cfg2, newInstantTime, table, new HashMap<>(),
          partitionAndBaseFilePaths.getLeft(), FSUtils.getFileId(baseFile.getFileName()), baseFile, new JavaTaskContextSupplier(),
          config.populateMetaFields() ? Option.empty() :
              Option.of((BaseKeyGenerator) HoodieAvroKeyGeneratorFactory.createKeyGenerator(config.getProps())));
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
    testHoodieConcatHandle(true, false, INSTANT_FACTORY);
  }

  /**
   * Test InsertPrepped API for HoodieConcatHandle.
   */
  @Test
  public void testInsertsPreppedWithHoodieConcatHandle() throws Exception {
    testHoodieConcatHandle(true, true, INSTANT_FACTORY);
  }

  /**
   * Test Insert API for HoodieConcatHandle when incoming entries contain duplicate keys.
   */
  @Test
  public void testInsertsWithHoodieConcatHandleOnDuplicateIncomingKeys() throws Exception {
    testHoodieConcatHandleOnDupInserts(false, INSTANT_FACTORY);
  }

  /**
   * Test InsertPrepped API for HoodieConcatHandle when incoming entries contain duplicate keys.
   */
  @Test
  public void testInsertsPreppedWithHoodieConcatHandleOnDuplicateIncomingKeys() throws Exception {
    testHoodieConcatHandleOnDupInserts(true, INSTANT_FACTORY);
  }

  /**
   * Tests deletion of records.
   */
  @Test
  public void testDeletes() throws Exception {
    Function3<Function2<List<HoodieRecord>, String, Integer>, String, Integer, List<HoodieRecord>> secondBatchGenFn =
        (String instantTime, Integer numRecordsInThisCommit, List<HoodieRecord> recordsInFirstBatch) -> {
          List<HoodieRecord> recordsInSecondBatch = new ArrayList<>();
          return (time, numRecords) -> {
            List<HoodieRecord> fewRecordsForDelete = recordsInFirstBatch.subList(0, 50);
            recordsInSecondBatch.addAll(dataGen.generateDeletesFromExistingRecords(fewRecordsForDelete));
            recordsInSecondBatch.addAll(dataGen.generateInserts(instantTime, 25));
            return recordsInSecondBatch;
          };
        };
    super.testDeletes(secondBatchGenFn, 75, 25, 175);
  }

  /**
   * When records getting inserted are deleted in the same write batch, hudi should have deleted those records and
   * not be available in read path.
   *
   * @throws Exception
   */
  @Test
  public void testDeletesForInsertsInSameBatch() throws Exception {
    super.testDeletesForInsertsInSameBatch(INSTANT_FACTORY);
  }

  @Test
  public void testAndValidateClusteringOutputFiles() throws IOException {
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder()
        .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
        .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName())
        .withInlineClustering(true).withInlineClusteringNumCommits(2).build();
    testAndValidateClusteringOutputFiles(createBrokenClusteringClient, clusteringConfig, IDENTITY, IDENTITY);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInlineScheduleClustering(boolean scheduleInlineClustering) throws IOException {
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringTargetPartitions(0).withAsyncClusteringMaxCommits(1).withInlineClustering(false).withScheduleInlineClustering(scheduleInlineClustering)
        .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName())
        .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName()).build();
    testInlineScheduleClustering(createBrokenClusteringClient, clusteringConfig, IDENTITY, IDENTITY);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testFailWritesOnInlineTableServiceExceptions(boolean shouldFail) throws IOException {
    testFailWritesOnInlineTableServiceExceptions(shouldFail, createBrokenClusteringClient);
  }

  /**
   * Test delete with delete api.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDeletesWithoutInserts(boolean populateMetaFields) {
    testDeletesWithoutInserts(populateMetaFields, IDENTITY, IDENTITY);
  }

  /**
   * Test to ensure commit metadata points to valid files.
   */
  @Test
  public void testCommitWritesRelativePaths() throws Exception {
    testCommitWritesRelativePaths(IDENTITY);
  }

  /**
   * Test to ensure commit metadata points to valid files.10.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMetadataStatsOnCommit(boolean populateMetaFields) throws Exception {
    testMetadataStatsOnCommit(populateMetaFields, IDENTITY);
  }

  /**
   * Tests behavior of committing only when consistency is verified.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testConsistencyCheckDuringFinalize(boolean enableOptimisticConsistencyGuard) throws Exception {
    testConsistencyCheckDuringFinalize(context, enableOptimisticConsistencyGuard, getHoodieTable, IDENTITY, IDENTITY);
  }

  private void testRollbackAfterConsistencyCheckFailureUsingFileList(boolean rollbackUsingMarkers, boolean enableOptimisticConsistencyGuard,
                                                                     boolean populateMetaFields) throws Exception {
    testRollbackAfterConsistencyCheckFailureUsingFileList(context, rollbackUsingMarkers, enableOptimisticConsistencyGuard,
        populateMetaFields, getHoodieTable, IDENTITY, IDENTITY);
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
    super.testRollbackFailedCommits(true);
  }

  @Test
  public void testRollbackFailedCommitsToggleCleaningPolicy() throws Exception {
    super.testRollbackFailedCommitsToggleCleaningPolicy(true);
  }

  @Test
  public void testParallelInsertAndCleanPreviousFailedCommits() throws Exception {
    super.testParallelInsertAndCleanPreviousFailedCommits(true);
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
