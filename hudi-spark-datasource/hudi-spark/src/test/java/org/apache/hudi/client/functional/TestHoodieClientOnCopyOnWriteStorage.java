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

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.SparkSingleFileSortPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkSingleFileSortExecutionStrategy;
import org.apache.hudi.client.clustering.update.strategy.SparkRejectUpdateStrategy;
import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass;
import org.apache.hudi.client.transaction.PreferWriterConflictResolutionStrategy;
import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.client.validator.SparkPreCommitValidator;
import org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator;
import org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieCorruptedDataException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.ClusteringTestUtils;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.INFLIGHT;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.apache.hudi.common.testutils.Transformations.randomSelectAsHoodieKeys;
import static org.apache.hudi.common.testutils.Transformations.recordsToRecordKeySet;
import static org.apache.hudi.config.HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE;
import static org.apache.hudi.config.HoodieClusteringConfig.EXECUTION_STRATEGY_CLASS_NAME;
import static org.apache.hudi.config.HoodieClusteringConfig.UPDATES_STRATEGY;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.testutils.Assertions.assertPartitionMetadata;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings("unchecked")
@Tag("functional")
public class TestHoodieClientOnCopyOnWriteStorage extends HoodieClientTestBase {

  private static final Map<String, String> STRATEGY_PARAMS = Collections.singletonMap("sortColumn", "record_key");

  private static Stream<Arguments> smallInsertHandlingParams() {
    return Arrays.stream(new Boolean[][] {{true}, {false}}).map(Arguments::of);
  }

  private static Stream<Arguments> populateMetaFieldsParams() {
    return Arrays.stream(new Boolean[][] {{true}, {false}}).map(Arguments::of);
  }

  private static Stream<Arguments> rollbackAfterConsistencyCheckFailureParams() {
    return Stream.of(
        Arguments.of(true, true),
        Arguments.of(true, false),
        Arguments.of(false, true),
        Arguments.of(false, false)
    );
  }

  private static final String COUNT_SQL_QUERY_FOR_VALIDATION = "select count(*) from <TABLE_NAME>";

  private static HoodiePreCommitValidatorConfig createPreCommitValidatorConfig(int expectedNumberOfRows) {
    return HoodiePreCommitValidatorConfig.newBuilder()
            .withPreCommitValidator(SqlQuerySingleResultPreCommitValidator.class.getName())
            .withPrecommitValidatorSingleResultSqlQueries(COUNT_SQL_QUERY_FOR_VALIDATION + "#" + expectedNumberOfRows)
            .build();
  }

  private static HoodieClusteringConfig.Builder createClusteringBuilder(boolean isInline, int inlineNumCommits) {
    return HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10).withClusteringTargetPartitions(0)
            .withInlineClustering(isInline).withInlineClusteringNumCommits(inlineNumCommits)
            .fromProperties(getDisabledRowWriterProperties());
  }

  private static HoodieLockConfig createLockConfig(ConflictResolutionStrategy conflictResolutionStrategy) {
    return HoodieLockConfig.newBuilder().withLockProvider(FileSystemBasedLockProviderTestClass.class)
            .withConflictResolutionStrategy(conflictResolutionStrategy).build();
  }

  private static HoodieCleanConfig createCleanConfig(HoodieFailedWritesCleaningPolicy policy, boolean autoClean) {
    return HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(policy).withAutoClean(autoClean).build();
  }

  @BeforeEach
  public void setUpTestTable() {
    testTable = HoodieSparkWriteableTestTable.of(metaClient);
  }

  private final Function<List, JavaRDD> list2Rdd = recordList -> jsc.parallelize(recordList, 1);

  private final Function<JavaRDD, List> rdd2List = AbstractJavaRDDLike::collect;

  private Function<HoodieWriteConfig, BaseHoodieWriteClient> createBrokenClusteringClient(Throwable throwable) {
    return config -> new WriteClientBrokenClustering<>(context, config, throwable);
  }

  private Function<HoodieWriteConfig, BaseHoodieWriteClient> createBrokenCleaningClient(Throwable throwable) {
    return config -> new WriteClientBrokenClean<>(context, config, throwable);
  }

  private final Function<HoodieWriteMetadata, HoodieWriteMetadata<List<WriteStatus>>> clusteringMetadataRdd2List =
      metadata -> metadata.clone(((JavaRDD)(metadata.getWriteStatuses())).collect());

  private final Function<HoodieWriteConfig, KeyGenerator> createKeyGenerator =
      config -> HoodieSparkKeyGeneratorFactory.createKeyGenerator(config.getProps());

  private final Function2<HoodieTable, HoodieTableMetaClient, HoodieWriteConfig> getHoodieTable =
      (metaClient, config) -> getHoodieTable(metaClient, config);

  @Override
  protected Object castInsertFirstBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime,
                                        String initCommitTime, int numRecordsInThisCommit,
                                        Function3<Object, BaseHoodieWriteClient, Object, String> writeFn, boolean isPreppedAPI,
                                        boolean assertForCommit, int expRecordsInThisCommit, boolean filterForCommitTimeWithAssert,
                                        InstantGenerator instantGenerator) throws Exception {
    return insertFirstBatch(writeConfig, (SparkRDDWriteClient) client, newCommitTime, initCommitTime, numRecordsInThisCommit,
        (writeClient, records, commitTime) -> (JavaRDD<WriteStatus>) writeFn.apply(writeClient, records, commitTime),
        isPreppedAPI, assertForCommit, expRecordsInThisCommit, filterForCommitTimeWithAssert, instantGenerator);
  }

  @Override
  protected Object castWriteBatch(BaseHoodieWriteClient client, String newCommitTime, String prevCommitTime,
                                  Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                  Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                  Function3<Object, BaseHoodieWriteClient, Object, String> writeFn,
                                  boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                  boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator, boolean skipCommit) throws Exception {
    return writeBatch((SparkRDDWriteClient) client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        (writeClient, records, commitTime) -> (JavaRDD<WriteStatus>) writeFn.apply(writeClient, records, commitTime),
        assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, filterForCommitTimeWithAssert, instantGenerator, skipCommit);
  }

  @Override
  protected Object castUpdateBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime, String prevCommitTime,
                                   Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                   Function3<Object, BaseHoodieWriteClient, Object, String> writeFn, boolean isPreppedAPI,
                                   boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                   boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator) throws Exception {
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateUniqueUpdates);

    return writeBatch((SparkRDDWriteClient) client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        (writeClient, records, commitTime) -> (JavaRDD<WriteStatus>) writeFn.apply(writeClient, records, commitTime), assertForCommit, expRecordsInThisCommit, expTotalRecords,
        expTotalCommits, filterForCommitTimeWithAssert, instantGenerator);
  }

  @Override
  protected Object castDeleteBatch(HoodieWriteConfig writeConfig, BaseHoodieWriteClient client, String newCommitTime,
                                   String prevCommitTime, String initCommitTime, int numRecordsInThisCommit, boolean isPreppedAPI,
                                   boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords,
                                   boolean filterForCommitTimeWithAssert, TimelineFactory timelineFactory, InstantGenerator instantGenerator) throws Exception {
    return deleteBatch(writeConfig, (SparkRDDWriteClient) client, newCommitTime, prevCommitTime, initCommitTime, numRecordsInThisCommit,
        isPreppedAPI, assertForCommit, expRecordsInThisCommit, expTotalRecords, filterForCommitTimeWithAssert, timelineFactory, instantGenerator);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient insert API.
   */
  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testAutoCommitOnInsert(boolean populateMetaFields) throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.insert(recordRDD, instantTime), false, populateMetaFields, INSTANT_GENERATOR);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient insertPrepped API.
   */
  @Test
  public void testAutoCommitOnInsertPrepped() throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.insertPreppedRecords(recordRDD, instantTime), true, true, INSTANT_GENERATOR);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient upsert API.
   */
  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testAutoCommitOnUpsert(boolean populateMetaFields) throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.upsert(recordRDD, instantTime), false, populateMetaFields, INSTANT_GENERATOR);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient upsert Prepped API.
   */
  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testAutoCommitOnUpsertPrepped(boolean populateMetaFields) throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.upsertPreppedRecords(recordRDD, instantTime), true, populateMetaFields, INSTANT_GENERATOR);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient bulk-insert API.
   */
  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testAutoCommitOnBulkInsert(boolean populateMetaFields) throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.bulkInsert(recordRDD, instantTime), false, populateMetaFields, INSTANT_GENERATOR);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient bulk-insert prepped API.
   */
  @Test
  public void testAutoCommitOnBulkInsertPrepped() throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.bulkInsertPreppedRecords(recordRDD, instantTime,
        Option.empty()), true, true, INSTANT_GENERATOR);
  }

  @Test
  public void testPreCommitValidatorsOnInsert() throws Exception {
    int numRecords = 200;
    HoodieWriteConfig config = getConfigBuilder()
        .withPreCommitValidatorConfig(createPreCommitValidatorConfig(200)).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn = (writeClient, recordRDD, instantTime) ->
          writeClient.bulkInsert(recordRDD, instantTime, Option.empty());
      String newCommitTime = client.createNewInstantTime();
      JavaRDD<WriteStatus> result = insertFirstBatch(config, client, newCommitTime,
          "000", numRecords, writeFn, false, false, numRecords, INSTANT_GENERATOR);
      assertTrue(testTable.commitExists(newCommitTime));
    }
  }

  @Test
  public void testPreCommitValidationFailureOnInsert() throws Exception {
    int numRecords = 200;
    //set wrong value for expected number of rows
    HoodieWriteConfig config = getConfigBuilder().withPreCommitValidatorConfig(createPreCommitValidatorConfig(500)).build();
    String newCommitTime = metaClient.createNewInstantTime();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn = (writeClient, recordRDD, instantTime) ->
          writeClient.bulkInsert(recordRDD, instantTime, Option.empty());
      JavaRDD<WriteStatus> result = insertFirstBatch(config, client, newCommitTime,
          "000", numRecords, writeFn, false, false, numRecords, INSTANT_GENERATOR);
      fail("Expected validation to fail because we only insert 200 rows. Validation is configured to expect 500 rows");
    } catch (HoodieInsertException e) {
      if (e.getCause() instanceof HoodieValidationException) {
        // expected because wrong value passed
      } else {
        throw e;
      }
    }

    assertFalse(testTable.commitExists(newCommitTime));
  }

  @Test
  public void testPreCommitValidationWithMultipleInflights() throws Exception {
    int numRecords = 200;
    HoodieWriteConfig config = getConfigBuilder()
        .withCleanConfig(createCleanConfig(HoodieFailedWritesCleaningPolicy.NEVER, true))
        //set wrong value for expected number of rows
        .withPreCommitValidatorConfig(createPreCommitValidatorConfig(500))
        .build();

    String instant1 = getHoodieWriteClient(config).createNewInstantTime();
    try {
      insertWithConfig(config, numRecords, instant1);
      fail("Expected validation to fail because we only insert 200 rows. Validation is configured to expect 500 rows");
    } catch (HoodieInsertException e) {
      if (e.getCause() instanceof HoodieValidationException) {
        // expected because wrong value passed
      } else {
        throw e;
      }
    }

    assertFalse(testTable.commitExists(instant1));
    assertTrue(testTable.inflightCommitExists(instant1));

    numRecords = 300;
    config = getConfigBuilder()
        .withCleanConfig(createCleanConfig(HoodieFailedWritesCleaningPolicy.NEVER, true))
        .withPreCommitValidatorConfig(createPreCommitValidatorConfig(numRecords))
        .build();
    String instant2 = getHoodieWriteClient(config).createNewInstantTime();
    // expect pre-commit validators to succeed. Note that validator is expected to exclude inflight instant1
    insertWithConfig(config, numRecords, instant2);
    assertTrue(testTable.inflightCommitExists(instant1));
    assertTrue(testTable.commitExists(instant2));
  }

  private void insertWithConfig(HoodieWriteConfig config, int numRecords, String instant) throws Exception {
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn = (writeClient, recordRDD, instantTime) ->
          writeClient.bulkInsert(recordRDD, instantTime, Option.empty());
      JavaRDD<WriteStatus> result = insertFirstBatch(config, client, instant,
          "000", numRecords, writeFn, false, false, numRecords, INSTANT_GENERATOR);
    }
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient insert API.
   */
  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testDeduplicationOnInsert(boolean populateMetaFields) throws Exception {
    testDeduplication((client, records, commitTime) -> (List<WriteStatus>) rdd2List.apply((JavaRDD<WriteStatus>)client.insert(list2Rdd.apply(records), commitTime)), populateMetaFields, false);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient insert API.
   */
  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testDeduplicationKeepOperationFieldOnInsert(boolean populateMetaFields) throws Exception {
    testDeduplication((client, records, commitTime) -> (List<WriteStatus>) rdd2List.apply((JavaRDD<WriteStatus>)client.insert(list2Rdd.apply(records), commitTime)), populateMetaFields, true);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient bulk-insert API.
   */
  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testDeduplicationOnBulkInsert(boolean populateMetaFields) throws Exception {
    testDeduplication((client, records, commitTime) -> (List<WriteStatus>) rdd2List.apply((JavaRDD<WriteStatus>)client.bulkInsert(list2Rdd.apply(records), commitTime)), populateMetaFields, false);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient upsert API.
   */
  @Test
  public void testDeduplicationOnUpsert() throws Exception {
    testDeduplication((client, records, commitTime) -> (List<WriteStatus>) rdd2List.apply((JavaRDD<WriteStatus>)client.upsert(list2Rdd.apply(records), commitTime)), true, false);
  }

  /**
   * Test Upsert API.
   */
  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testUpserts(boolean populateMetaFields) throws Exception {
    testUpsertsInternal((writeClient, recordRDD, instantTime) -> writeClient.upsert(recordRDD, instantTime), populateMetaFields, false, SparkUpgradeDowngradeHelper.getInstance());
  }

  /**
   * Test UpsertPrepped API.
   */
  @Test
  public void testUpsertsPrepped() throws Exception {
    testUpsertsInternal((writeClient, recordRDD, instantTime) -> writeClient.upsertPreppedRecords(recordRDD, instantTime), true, true, SparkUpgradeDowngradeHelper.getInstance());
  }

  @Override
  protected void testMergeHandle(HoodieWriteConfig config) throws IOException {
    final String instantTime = "007";
    HoodieTableMetaClient metaClient = HoodieClientTestUtils.createMetaClient(jsc, basePath);
    HoodieTable table = getHoodieTable(metaClient, config);
    Pair<String, String> partitionAndBaseFilePaths = getPartitionAndBaseFilePathsFromLatestCommitMetadata(metaClient);
    String partitionPath = partitionAndBaseFilePaths.getLeft();
    String baseFilePath = partitionAndBaseFilePaths.getRight();
    jsc.parallelize(Arrays.asList(1)).map(e -> {

      HoodieBaseFile baseFile = new HoodieBaseFile(baseFilePath);

      HoodieMergeHandle handle = null;
      try {
        handle = new HoodieMergeHandle(config, instantTime, table, new HashMap<>(),
            partitionPath, FSUtils.getFileId(baseFile.getFileName()), baseFile, new SparkTaskContextSupplier(),
            config.populateMetaFields() ? Option.empty() :
                Option.of((BaseKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(config.getProps())));
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
        config.getProps().setProperty("hoodie.merge.data.validation.enabled", "true");
        HoodieWriteConfig cfg2 = HoodieWriteConfig.newBuilder().withProps(config.getProps()).build();
        handle = new HoodieMergeHandle(cfg2, newInstantTime, table, new HashMap<>(),
            partitionPath, FSUtils.getFileId(baseFile.getFileName()), baseFile, new SparkTaskContextSupplier(),
            config.populateMetaFields() ? Option.empty() :
                Option.of((BaseKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(config.getProps())));
        WriteStatus writeStatus = new WriteStatus(false, 0.0);
        writeStatus.setStat(new HoodieWriteStat());
        writeStatus.getStat().setNumWrites(0);
        handle.performMergeDataValidationCheck(writeStatus);
        fail("The above line should have thrown an exception");
      } catch (HoodieCorruptedDataException e2) {
        // expected
      } finally {
        if (handle != null) {
          try {
            handle.close();
          } catch (Exception ex) {
            // ignore exception from validation check
          }
        }
      }
      return true;
    }).collect();
  }

  @Test
  public void testRestoreWithSavepointBeyondArchival() throws Exception {
    HoodieWriteConfig config = getConfigBuilder().withRollbackUsingMarkers(true).build();
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(EAGER)
        .withRollbackUsingMarkers(true)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().withArchiveBeyondSavepoint(true).build())
        .withProps(config.getProps()).build();

    HoodieTableMetaClient.newTableBuilder()
        .fromMetaClient(metaClient)
        .setPopulateMetaFields(config.populateMetaFields())
        .initTable(metaClient.getStorageConf().newInstance(), metaClient.getBasePath());

    SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig);

    // Write 1 (only inserts)
    String newCommitTime = "001";
    String initCommitTime = "000";
    int numRecords = 200;
    insertFirstBatch(hoodieWriteConfig, client, newCommitTime, initCommitTime, numRecords, SparkRDDWriteClient::insert,
        false, true, numRecords, config.populateMetaFields(), INSTANT_GENERATOR);

    // Write 2 (updates)
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    numRecords = 100;
    String commitTimeBetweenPrevAndNew = "002";
    updateBatch(hoodieWriteConfig, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), initCommitTime, numRecords, SparkRDDWriteClient::upsert, false, true,
        numRecords, 200, 2, config.populateMetaFields(), INSTANT_GENERATOR);

    // Delete 1
    prevCommitTime = newCommitTime;
    newCommitTime = "005";
    numRecords = 50;

    deleteBatch(hoodieWriteConfig, client, newCommitTime, prevCommitTime,
        initCommitTime, numRecords, false, true,
        0, 150, config.populateMetaFields(), TIMELINE_FACTORY, INSTANT_GENERATOR);

    HoodieWriteConfig newConfig = getConfigBuilder().withProps(config.getProps()).withTimelineLayoutVersion(
            TimelineLayoutVersion.CURR_VERSION)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().withArchiveBeyondSavepoint(true).build()).build();
    client = getHoodieWriteClient(newConfig);

    client.savepoint("004", "user1", "comment1");

    // verify that restore fails when "hoodie.archive.beyond.savepoint" is enabled.
    SparkRDDWriteClient finalClient = client;
    assertThrows(IllegalArgumentException.class, () -> finalClient.restoreToSavepoint("004"),
            "Restore should not be supported when " + HoodieArchivalConfig.ARCHIVE_BEYOND_SAVEPOINT.key() + " is enabled");
  }

  /**
   * Test Insert API for HoodieConcatHandle.
   */
  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testInsertsWithHoodieConcatHandle(boolean populateMetaFields) throws Exception {
    testHoodieConcatHandle(populateMetaFields, false, INSTANT_GENERATOR);
  }

  /**
   * Test InsertPrepped API for HoodieConcatHandle.
   */
  @Test
  public void testInsertsPreppedWithHoodieConcatHandle() throws Exception {
    testHoodieConcatHandle(true, true, INSTANT_GENERATOR);
  }

  /**
   * Test Insert API for HoodieConcatHandle when incoming entries contain duplicate keys.
   */
  @Test
  public void testInsertsWithHoodieConcatHandleOnDuplicateIncomingKeys() throws Exception {
    testHoodieConcatHandleOnDupInserts(false, INSTANT_GENERATOR);
  }

  /**
   * Test InsertPrepped API for HoodieConcatHandle when incoming entries contain duplicate keys.
   */
  @Test
  public void testInsertsPreppedWithHoodieConcatHandleOnDuplicateIncomingKeys() throws Exception {
    testHoodieConcatHandleOnDupInserts(true, INSTANT_GENERATOR);
  }

  @Test
  public void testBulkInsertWithCustomPartitioner() {
    HoodieWriteConfig config = getConfigBuilder().withRollbackUsingMarkers(true).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      final String commitTime1 = "001";
      WriteClientTestUtils.startCommitWithTime(client, commitTime1);
      List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, 100);
      JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 10);
      BulkInsertPartitioner<JavaRDD<HoodieRecord>> partitioner = new RDDCustomColumnsSortPartitioner(new String[] {"rider"}, HoodieTestDataGenerator.AVRO_SCHEMA, config);
      List<WriteStatus> statuses = client.bulkInsert(insertRecordsRDD1, commitTime1, Option.of(partitioner)).collect();
      assertNoWriteErrors(statuses);
    }
  }

  @Test
  public void testPendingRestore() throws IOException {
    HoodieWriteConfig config = getConfigBuilder().withMetadataConfig(
        HoodieMetadataConfig.newBuilder().enable(false).build()).build();
    StoragePath completeRestoreFile = null;
    StoragePath backupCompletedRestoreFile = null;
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      insertBatchRecords(client, "001", 100, 3, 2, SparkRDDWriteClient::insert);

      // inject a pending restore
      client.savepoint("001", "user1", "comment1");

      client.restoreToInstant("001", false);
      // remove completed restore instant from timeline to mimic pending restore.
      HoodieInstant restoreCompleted =
          metaClient.reloadActiveTimeline().getRestoreTimeline().filterCompletedInstants()
              .getInstants().get(0);
      completeRestoreFile = new StoragePath(
          config.getBasePath() + StoragePath.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME
              + StoragePath.SEPARATOR + HoodieTableMetaClient.TIMELINEFOLDER_NAME + StoragePath.SEPARATOR + INSTANT_FILE_NAME_GENERATOR.getFileName(restoreCompleted));
      backupCompletedRestoreFile = new StoragePath(
          config.getBasePath() + StoragePath.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME
              + StoragePath.SEPARATOR + HoodieTableMetaClient.TIMELINEFOLDER_NAME + StoragePath.SEPARATOR + INSTANT_FILE_NAME_GENERATOR.getFileName(restoreCompleted) + ".backup");
      metaClient.getStorage().rename(completeRestoreFile, backupCompletedRestoreFile);
    }

    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      // since restore is pending, should fail the commit
      assertThrows(IllegalArgumentException.class, client::startCommit);
    }
    // add back the restore file.
    metaClient.getStorage().rename(backupCompletedRestoreFile, completeRestoreFile);

    // retrigger a new commit, should succeed.
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      insertBatchRecords(client, "003", 100, 3, 2, SparkRDDWriteClient::insert);
    }
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
            List<HoodieRecord> fewRecordsForUpdate = recordsInFirstBatch.subList(50, 100);
            recordsInSecondBatch.addAll(dataGen.generateDeletesFromExistingRecords(fewRecordsForDelete));
            recordsInSecondBatch.addAll(fewRecordsForUpdate);
            return recordsInSecondBatch;
          };
        };
    super.testDeletes(secondBatchGenFn, 100, 50, 150);
  }

  /**
   * When records getting inserted are deleted in the same write batch, hudi should have deleted those records and
   * not be available in read path.
   *
   * @throws Exception
   */
  @Test
  public void testDeletesForInsertsInSameBatch() throws Exception {
    super.testDeletesForInsertsInSameBatch(INSTANT_GENERATOR);
  }

  private Pair<JavaRDD<WriteStatus>, List<HoodieRecord>> insertBatchRecords(SparkRDDWriteClient client, String commitTime,
                                                                            Integer recordNum, int expectStatusSize, int numSlices,
                                                                            Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn) throws IOException {
    return insertBatchRecords(client, commitTime, recordNum, expectStatusSize, numSlices, writeFn, false);
  }

  private Pair<JavaRDD<WriteStatus>, List<HoodieRecord>> insertBatchRecords(SparkRDDWriteClient client, String commitTime,
                                                                            Integer recordNum, int expectStatusSize, int numSlices,
                                                                            Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                                                            boolean skipCommit) throws IOException {
    WriteClientTestUtils.startCommitWithTime(client, commitTime);
    List<HoodieRecord> inserts = dataGen.generateInserts(commitTime, recordNum);
    JavaRDD<HoodieRecord> insertRecordsRDD = jsc.parallelize(inserts, numSlices);
    List<WriteStatus> statusList = writeFn.apply(client, insertRecordsRDD, commitTime).collect();
    assertNoWriteErrors(statusList);
    JavaRDD<WriteStatus> recreatedStatuses = jsc.parallelize(statusList, numSlices);
    if (!skipCommit) {
      client.commit(commitTime, recreatedStatuses, Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    }
    assertEquals(expectStatusSize, recreatedStatuses.count(), "check expect status size.");
    return Pair.of(recreatedStatuses, inserts);
  }

  @Test
  public void testUpdateRejectForClustering() throws IOException {
    final String testPartitionPath = "2016/09/26";
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    Properties props = new Properties();
    props.setProperty(ASYNC_CLUSTERING_ENABLE.key(), "true");
    props.setProperty(UPDATES_STRATEGY.key(), SparkRejectUpdateStrategy.class.getName());
    HoodieWriteConfig config = getSmallInsertWriteConfig(100,
        TRIP_EXAMPLE_SCHEMA, dataGen.getEstimatedFileSizeInBytes(150), true, props);
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);

    //1. insert to generate 2 file group
    Pair<JavaRDD<WriteStatus>, List<HoodieRecord>> upsertResult = insertBatchRecords(client, "001", 600, 2, 1, SparkRDDWriteClient::upsert);
    List<HoodieRecord> inserts1 = upsertResult.getValue();
    List<String> fileGroupIds1 = table.getFileSystemView().getAllFileGroups(testPartitionPath)
        .map(fileGroup -> fileGroup.getFileGroupId().getFileId()).collect(Collectors.toList());
    assertEquals(2, fileGroupIds1.size());

    // 2. generate clustering plan for fileGroupIds1 file groups
    List<List<FileSlice>> firstInsertFileSlicesList = table.getFileSystemView().getAllFileGroups(testPartitionPath)
        .map(fileGroup -> fileGroup.getAllFileSlices().collect(Collectors.toList())).collect(Collectors.toList());
    List<FileSlice>[] fileSlices = (List<FileSlice>[]) firstInsertFileSlicesList.toArray(new List[0]);
    createRequestedClusterInstant(this.metaClient, "002", fileSlices);

    // 3. insert one record with no updating reject exception, and not merge the small file, just generate a new file group
    insertBatchRecords(client, "003", 1, 1, 1, SparkRDDWriteClient::upsert);
    List<String> fileGroupIds2 = table.getFileSystemView().getAllFileGroups(testPartitionPath)
        .map(fileGroup -> fileGroup.getFileGroupId().getFileId()).collect(Collectors.toList());
    assertEquals(3, fileGroupIds2.size());

    // 4. update one record for the clustering two file groups, throw reject update exception
    String commitTime4 = "004";
    WriteClientTestUtils.startCommitWithTime(client, commitTime4);
    List<HoodieRecord> insertsAndUpdates3 = new ArrayList<>(dataGen.generateUpdates(commitTime4, inserts1));
    String assertMsg = String.format("Not allowed to update the clustering files in partition: %s "
        + "For pending clustering operations, we are not going to support update for now.", testPartitionPath);
    assertThrows(HoodieUpsertException.class, () -> {
      client.upsert(jsc.parallelize(insertsAndUpdates3, 1), commitTime4).collect();
    }, assertMsg);

    // 5. insert one record with no updating reject exception, will merge the small file
    JavaRDD<WriteStatus> statuses = (JavaRDD<WriteStatus>)
        insertBatchRecords(client, "005", 1, 1, 1, SparkRDDWriteClient::upsert).getKey();
    fileGroupIds2.removeAll(fileGroupIds1);
    assertEquals(fileGroupIds2.get(0), statuses.collect().get(0).getFileId());
    List<String> firstInsertFileGroupIds4 = table.getFileSystemView().getAllFileGroups(testPartitionPath)
        .map(fileGroup -> fileGroup.getFileGroupId().getFileId()).collect(Collectors.toList());
    assertEquals(3, firstInsertFileGroupIds4.size());
  }

  /**
   * Test scenario of new file-group getting added during upsert().
   */
  @Test
  public void testSmallInsertHandlingForUpserts() throws Exception {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    // hold upto 200 records max
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit,
        TRIP_EXAMPLE_SCHEMA, dataGen.getEstimatedFileSizeInBytes(150));

    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    FileFormatUtils fileUtils = getFileUtilsInstance(metaClient);

    // Inserts => will write file1
    String commitTime1 = "001";
    WriteClientTestUtils.startCommitWithTime(client, commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, insertSplitLimit); // this writes ~500kb
    Set<String> keys1 = recordsToRecordKeySet(inserts1);

    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
    List<WriteStatus> statusList = client.upsert(insertRecordsRDD1, commitTime1).collect();
    writeClient.commit(commitTime1, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    assertNoWriteErrors(statusList);

    assertEquals(1, statusList.size(), "Just 1 file needs to be added.");
    String file1 = statusList.get(0).getFileId();
    assertEquals(100,
        fileUtils.readRowKeys(storage, new StoragePath(basePath, statusList.get(0).getStat().getPath()))
            .size(), "file should contain 100 records");

    // Update + Inserts such that they just expand file1
    String commitTime2 = "002";
    WriteClientTestUtils.startCommitWithTime(client, commitTime2);
    List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime2, 40);
    Set<String> keys2 = recordsToRecordKeySet(inserts2);
    List<HoodieRecord> insertsAndUpdates2 = new ArrayList<>();
    insertsAndUpdates2.addAll(inserts2);
    insertsAndUpdates2.addAll(dataGen.generateUpdates(commitTime2, inserts1));

    JavaRDD<HoodieRecord> insertAndUpdatesRDD2 = jsc.parallelize(insertsAndUpdates2, 1);
    statusList = client.upsert(insertAndUpdatesRDD2, commitTime2).collect();
    client.commit(commitTime2, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    assertNoWriteErrors(statusList);

    assertEquals(1, statusList.size(), "Just 1 file needs to be updated.");
    assertEquals(file1, statusList.get(0).getFileId(), "Existing file should be expanded");
    assertEquals(commitTime1, statusList.get(0).getStat().getPrevCommit(), "Existing file should be expanded");
    StoragePath newFile = new StoragePath(basePath, statusList.get(0).getStat().getPath());
    assertEquals(140, fileUtils.readRowKeys(storage, newFile).size(),
        "file should contain 140 records");

    List<GenericRecord> records = fileUtils.readAvroRecords(storage, newFile);
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      assertEquals(commitTime2, record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString(), "only expect commit2");
      assertTrue(keys2.contains(recordKey) || keys1.contains(recordKey), "key expected to be part of commit2");
    }

    // update + inserts such that file1 is updated and expanded, a new file2 is created.
    String commitTime3 = "003";
    WriteClientTestUtils.startCommitWithTime(client, commitTime3);
    List<HoodieRecord> insertsAndUpdates3 = dataGen.generateInserts(commitTime3, 200);
    Set<String> keys3 = recordsToRecordKeySet(insertsAndUpdates3);
    List<HoodieRecord> updates3 = dataGen.generateUpdates(commitTime3, inserts2);
    insertsAndUpdates3.addAll(updates3);

    JavaRDD<HoodieRecord> insertAndUpdatesRDD3 = jsc.parallelize(insertsAndUpdates3, 1);
    statusList = client.upsert(insertAndUpdatesRDD3, commitTime3).collect();
    client.commit(commitTime3, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    assertNoWriteErrors(statusList);
    assertEquals(2, statusList.size(), "2 files needs to be committed.");
    HoodieTableMetaClient metadata = createMetaClient();

    HoodieTable table = getHoodieTable(metadata, config);
    BaseFileOnlyView fileSystemView = table.getBaseFileOnlyView();
    List<HoodieBaseFile> files =
        fileSystemView.getLatestBaseFilesBeforeOrOn(testPartitionPath, commitTime3).collect(Collectors.toList());
    int numTotalInsertsInCommit3 = 0;
    int numTotalUpdatesInCommit3 = 0;
    for (HoodieBaseFile file : files) {
      if (file.getFileName().contains(file1)) {
        assertEquals(commitTime3, file.getCommitTime(), "Existing file should be expanded");
        records = fileUtils.readAvroRecords(storage, new StoragePath(file.getPath()));
        for (GenericRecord record : records) {
          String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
          String recordCommitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
          if (recordCommitTime.equals(commitTime3)) {
            if (keys2.contains(recordKey)) {
              keys2.remove(recordKey);
              numTotalUpdatesInCommit3++;
            } else {
              numTotalInsertsInCommit3++;
            }
          }
        }
        assertEquals(0, keys2.size(), "All keys added in commit 2 must be updated in commit3 correctly");
      } else {
        assertEquals(commitTime3, file.getCommitTime(), "New file must be written for commit 3");
        records = fileUtils.readAvroRecords(storage, new StoragePath(file.getPath()));
        for (GenericRecord record : records) {
          String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
          assertEquals(commitTime3, record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString(),
              "only expect commit3");
          assertTrue(keys3.contains(recordKey), "key expected to be part of commit3");
        }
        numTotalInsertsInCommit3 += records.size();
      }
    }
    assertEquals(numTotalUpdatesInCommit3, inserts2.size(), "Total updates in commit3 must add up");
    assertEquals(numTotalInsertsInCommit3, keys3.size(), "Total inserts in commit3 must add up");
  }

  /**
   * Test scenario of new file-group getting added during insert().
   */
  @ParameterizedTest
  @MethodSource("smallInsertHandlingParams")
  public void testSmallInsertHandlingForInserts(boolean mergeAllowDuplicateInserts) throws Exception {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit, false, mergeAllowDuplicateInserts); // hold upto 200 records max
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    FileFormatUtils fileUtils = getFileUtilsInstance(metaClient);

    // Inserts => will write file1
    String commitTime1 = "001";
    Pair<JavaRDD<WriteStatus>, List<HoodieRecord>> insertResult = insertBatchRecords(client, commitTime1,
            insertSplitLimit, 1, 1, SparkRDDWriteClient::insert); // this writes ~500kb
    List<WriteStatus> statuses = insertResult.getLeft().collect();
    Set<String> keys1 = recordsToRecordKeySet(insertResult.getRight());
    assertPartitionMetadata(basePath, new String[] {testPartitionPath}, storage);
    String file1 = statuses.get(0).getFileId();
    assertEquals(100,
        fileUtils.readRowKeys(storage, new StoragePath(basePath, statuses.get(0).getStat().getPath()))
            .size(), "file should contain 100 records");

    // Second, set of Inserts should just expand file1
    String commitTime2 = "002";
    insertResult = insertBatchRecords(client, commitTime2, 40, 1, 1, SparkRDDWriteClient::insert);
    Set<String> keys2 = recordsToRecordKeySet(insertResult.getRight());
    statuses = insertResult.getLeft().collect();
    assertEquals(file1, statuses.get(0).getFileId(), "Existing file should be expanded");
    assertEquals(commitTime1, statuses.get(0).getStat().getPrevCommit(), "Existing file should be expanded");

    StoragePath newFile = new StoragePath(basePath, statuses.get(0).getStat().getPath());
    assertEquals(140, fileUtils.readRowKeys(storage, newFile).size(),
        "file should contain 140 records");
    List<GenericRecord> records = fileUtils.readAvroRecords(storage, newFile);
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      String recCommitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
      assertTrue(commitTime1.equals(recCommitTime) || commitTime2.equals(recCommitTime),
          "Record expected to be part of commit 1 or commit2");
      assertTrue(keys2.contains(recordKey) || keys1.contains(recordKey),
          "key expected to be part of commit 1 or commit2");
    }

    // Lots of inserts such that file1 is updated and expanded, a new file2 is created.
    String commitTime3 = "003";
    insertResult = insertBatchRecords(client, commitTime3, 200, 2, 1, SparkRDDWriteClient::insert);
    statuses = insertResult.getLeft().collect();
    assertEquals(340,
        fileUtils.readRowKeys(storage, new StoragePath(basePath, statuses.get(0).getStat().getPath())).size()
            + fileUtils.readRowKeys(storage, new StoragePath(basePath, statuses.get(1).getStat().getPath())).size(),
        "file should contain 340 records");

    HoodieTableMetaClient metaClient = createMetaClient();
    HoodieTable table = getHoodieTable(metaClient, config);
    List<HoodieBaseFile> files = table.getBaseFileOnlyView()
        .getLatestBaseFilesBeforeOrOn(testPartitionPath, commitTime3).collect(Collectors.toList());
    assertEquals(2, files.size(), "Total of 2 valid data files");

    int totalInserts = 0;
    for (HoodieBaseFile file : files) {
      assertEquals(commitTime3, file.getCommitTime(), "All files must be at commit 3");
      totalInserts += fileUtils.readAvroRecords(storage, new StoragePath(file.getPath())).size();
    }
    assertEquals(340, totalInserts, "Total number of records must add up");
  }

  /**
   * Test delete with delete api.
   */
  @Test
  public void testDeletesWithDeleteApi() throws Exception {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    // hold upto 200 records max
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit,
        TRIP_EXAMPLE_SCHEMA, dataGen.getEstimatedFileSizeInBytes(150));
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});

    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Inserts => will write file1
    String commitTime1 = "001";
    WriteClientTestUtils.startCommitWithTime(client, commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, insertSplitLimit); // this writes ~500kb
    Set<String> keys1 = recordsToRecordKeySet(inserts1);
    List<String> keysSoFar = new ArrayList<>(keys1);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
    List<WriteStatus> statusList = client.upsert(insertRecordsRDD1, commitTime1).collect();
    client.commit(commitTime1, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    assertNoWriteErrors(statusList);

    assertEquals(1, statusList.size(), "Just 1 file needs to be added.");
    String file1 = statusList.get(0).getFileId();
    assertEquals(100, getFileUtilsInstance(metaClient).readRowKeys(
        storage, new StoragePath(basePath, statusList.get(0).getStat().getPath())).size(), "file should contain 100 records");

    // Delete 20 among 100 inserted
    testDeletes(client, inserts1, 20, file1, "002", 80, keysSoFar);

    // Insert and update 40 records
    Pair<Set<String>, List<HoodieRecord>> updateBatch2 = testUpdates("003", client, 40, 120);
    keysSoFar.addAll(updateBatch2.getLeft());

    // Delete 10 records among 40 updated
    testDeletes(client, updateBatch2.getRight(), 10, file1, "004", 110, keysSoFar);

    // do another batch of updates
    Pair<Set<String>, List<HoodieRecord>> updateBatch3 = testUpdates("005", client, 40, 150);
    keysSoFar.addAll(updateBatch3.getLeft());

    // delete non existent keys
    String commitTime6 = "006";
    WriteClientTestUtils.startCommitWithTime(client, commitTime6);

    List<HoodieRecord> dummyInserts3 = dataGen.generateInserts(commitTime6, 20);
    List<HoodieKey> hoodieKeysToDelete3 = randomSelectAsHoodieKeys(dummyInserts3, 20);
    JavaRDD<HoodieKey> deleteKeys3 = jsc.parallelize(hoodieKeysToDelete3, 1);
    statusList = client.delete(deleteKeys3, commitTime6).collect();
    client.commit(commitTime6, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    assertNoWriteErrors(statusList);
    assertEquals(0, statusList.size(), "Just 0 write status for delete.");

    assertTheEntireDatasetHasAllRecordsStill(150);

    // delete another batch. previous delete commit should have persisted the schema. If not,
    // this will throw exception
    testDeletes(client, updateBatch3.getRight(), 10, file1, "007", 140, keysSoFar);
  }

  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testSimpleClustering(boolean populateMetaFields) throws Exception {
    initMetaClient(getPropertiesForKeyGen(populateMetaFields));
    testInsertAndClustering(createClusteringBuilder(true, 1).build(), populateMetaFields, true,
            false, SqlQueryEqualityPreCommitValidator.class.getName(), COUNT_SQL_QUERY_FOR_VALIDATION, "");
  }

  @Test
  public void testAndValidateClusteringOutputFiles() throws IOException {
    testAndValidateClusteringOutputFiles(createBrokenClusteringClient(new HoodieException(CLUSTERING_FAILURE)), createClusteringBuilder(true, 2).build(), list2Rdd, rdd2List);
  }

  @Test
  public void testRollbackOfRegularCommitWithPendingReplaceCommitInTimeline() throws Exception {
    // trigger clustering, but do not complete
    testInsertAndClustering(createClusteringBuilder(true, 1).build(), true, false,
            false, SqlQueryEqualityPreCommitValidator.class.getName(), COUNT_SQL_QUERY_FOR_VALIDATION, "");

    // trigger another partial commit, followed by valid commit. rollback of partial commit should succeed.
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder();
    SparkRDDWriteClient client = getHoodieWriteClient(cfgBuilder.build());
    String commitTime1 = client.createNewInstantTime();
    insertBatchRecords(client, commitTime1, 200, 1, 2, SparkRDDWriteClient::upsert, true).getLeft();

    HoodieTableMetaClient metaClient = createMetaClient();
    assertEquals(2, metaClient.getActiveTimeline().getCommitsTimeline().filterInflightsAndRequested().countInstants());

    // trigger another commit. this should rollback latest partial commit.
    JavaRDD<WriteStatus> statuses = (JavaRDD<WriteStatus>)
        insertBatchRecords(client, commitTime1, 200, 1, 2, SparkRDDWriteClient::upsert).getLeft();
    metaClient.reloadActiveTimeline();
    // rollback should have succeeded. Essentially, the pending clustering should not hinder the rollback of regular commits.
    assertEquals(1, metaClient.getActiveTimeline().getCommitsTimeline().filterInflightsAndRequested().countInstants());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInlineScheduleClustering(boolean scheduleInlineClustering) throws IOException {
    HoodieClusteringConfig clusteringConfig = createClusteringBuilder(false, 1)
            .withAsyncClusteringMaxCommits(1).withScheduleInlineClustering(scheduleInlineClustering).build();
    testInlineScheduleClustering(createBrokenClusteringClient(new HoodieException(CLUSTERING_FAILURE)), clusteringConfig, list2Rdd, rdd2List);
  }

  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testClusteringWithSortColumns(boolean populateMetaFields) throws Exception {
    initMetaClient(getPropertiesForKeyGen(populateMetaFields));
    // setup clustering config.
    HoodieClusteringConfig clusteringConfig = createClusteringBuilder(true, 1)
        .withClusteringSortColumns(populateMetaFields ? "_hoodie_record_key" : "_row_key").build();
    testInsertAndClustering(clusteringConfig, populateMetaFields, true, false, SqlQueryEqualityPreCommitValidator.class.getName(), COUNT_SQL_QUERY_FOR_VALIDATION, "");
  }

  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testClusteringWithSortOneFilePerGroup(boolean populateMetaFields) throws Exception {
    initMetaClient(getPropertiesForKeyGen(populateMetaFields));
    // setup clustering config.
    HoodieClusteringConfig clusteringConfig = createClusteringBuilder(true, 1)
        .withClusteringSortColumns("begin_lat,begin_lon")
        .withClusteringPlanStrategyClass(SparkSingleFileSortPlanStrategy.class.getName())
        .withClusteringExecutionStrategyClass(SparkSingleFileSortExecutionStrategy.class.getName())
        .build();
    // note that assertSameFileIds is true for this test because of the plan and execution strategy
    testInsertAndClustering(clusteringConfig, populateMetaFields, true, true, SqlQueryEqualityPreCommitValidator.class.getName(), COUNT_SQL_QUERY_FOR_VALIDATION, "");
  }

  @Test
  public void testPendingClusteringRollback() throws Exception {
    boolean populateMetaFields = true;
    // start clustering, but don't commit
    List<HoodieRecord> allRecords = testInsertAndClustering(createClusteringBuilder(true, 1).build(), populateMetaFields, false);
    HoodieTableMetaClient metaClient = createMetaClient();
    List<Pair<HoodieInstant, HoodieClusteringPlan>> pendingClusteringPlans = getAndAssertPendingClusteringPlans(true, metaClient);
    HoodieInstant pendingClusteringInstant = pendingClusteringPlans.get(0).getLeft();

    // complete another commit after pending clustering
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(EAGER);
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig config = cfgBuilder.build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    dataGen = new HoodieTestDataGenerator();
    String commitTime = client.createNewInstantTime();
    allRecords.addAll(dataGen.generateInserts(commitTime, 200));
    assertThrows(HoodieUpsertException.class, () -> writeAndVerifyBatch(client, allRecords, commitTime, populateMetaFields));
    // verify pending clustering can be rolled back (even though there is a completed commit greater than pending clustering)
    client.rollback(pendingClusteringInstant.requestedTime());
    metaClient.reloadActiveTimeline();
    // verify there are no pending clustering instants
    assertEquals(0, ClusteringUtils.getAllPendingClusteringPlans(metaClient).count());

    // delete rollback.completed instant to mimic failed rollback of clustering. and then trigger rollback of clustering again. same rollback instant should be used.
    HoodieInstant rollbackInstant = metaClient.getActiveTimeline().getRollbackTimeline().lastInstant().get();
    FileCreateUtilsLegacy.deleteRollbackCommit(metaClient.getBasePath().toString(), rollbackInstant.requestedTime());
    metaClient.reloadActiveTimeline();

    // create replace commit requested meta file so that rollback will not throw FileNotFoundException
    // create file slice with instantTime 001 and build clustering plan including this created 001 file slice.
    HoodieClusteringPlan clusteringPlan = ClusteringTestUtils.createClusteringPlan(metaClient, pendingClusteringInstant.requestedTime(), "1");
    // create requested replace commit
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
        .setClusteringPlan(clusteringPlan).setOperationType(WriteOperationType.CLUSTER.name()).build();

    FileCreateUtilsLegacy.createRequestedClusterCommit(metaClient.getBasePath().toString(), pendingClusteringInstant.requestedTime(), requestedReplaceMetadata);

    // trigger clustering again. no new rollback instants should be generated.
    try {
      client.cluster(pendingClusteringInstant.requestedTime(), false);
      // new replace commit metadata generated is fake one. so, clustering will fail. but the intention of test is ot check for duplicate rollback instants.
    } catch (Exception e) {
      //ignore.
    }

    metaClient.reloadActiveTimeline();
    // verify that there is no new rollback instant generated
    HoodieInstant newRollbackInstant = metaClient.getActiveTimeline().getRollbackTimeline().lastInstant().get();
    assertEquals(rollbackInstant.requestedTime(), newRollbackInstant.requestedTime());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInflightClusteringRollbackWhenUpdatesAllowed(boolean rollbackPendingClustering) throws Exception {
    // setup clustering config with update strategy to allow updates during ingestion
    HoodieClusteringConfig clusteringConfig = createClusteringBuilder(true, 1)
        .withClusteringUpdatesStrategy("org.apache.hudi.client.clustering.update.strategy.SparkAllowUpdateStrategy")
        .withRollbackPendingClustering(rollbackPendingClustering).build();

    // start clustering, but don't commit keep it inflight
    List<HoodieRecord> allRecords = testInsertAndClustering(clusteringConfig, true, false);
    HoodieTableMetaClient metaClient = createMetaClient();
    List<Pair<HoodieInstant, HoodieClusteringPlan>> pendingClusteringPlans = getAndAssertPendingClusteringPlans(true, metaClient);
    HoodieInstant pendingClusteringInstant = pendingClusteringPlans.get(0).getLeft();
    assertEquals(pendingClusteringInstant.getState(), INFLIGHT);

    // make an update to a filegroup within the partition that is pending clustering
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder(EAGER);
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    cfgBuilder.withClusteringConfig(clusteringConfig);
    cfgBuilder.withProperties(getPropertiesForKeyGen(true));
    HoodieWriteConfig config = cfgBuilder.build();
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    String commitTime = client.createNewInstantTime();
    allRecords.addAll(dataGen.generateUpdates(commitTime, 200));
    writeAndVerifyBatch(client, allRecords, commitTime, true);

    // verify inflight clustering was rolled back
    metaClient.reloadActiveTimeline();
    if (rollbackPendingClustering) {
      // if rollbackPendingClustering is true, first one will be rolled back and 2nd one will succeed
      assertEquals(1, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().filter(instant -> instant.getAction().equals(REPLACE_COMMIT_ACTION)).countInstants());
    } else {
      // if rollbackPendingClustering is false, two completed RC should be found
      assertEquals(2, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().filter(instant -> instant.getAction().equals(REPLACE_COMMIT_ACTION)).countInstants());
    }
  }

  @Test
  public void testClusteringWithFailingValidator() throws Exception {
    HoodieClusteringConfig clusteringConfig = createClusteringBuilder(true, 1)
        .withClusteringSortColumns("_hoodie_record_key").withInlineClustering(true).build();
    try {
      testInsertAndClustering(clusteringConfig, true, true, false, FailingPreCommitValidator.class.getName(), COUNT_SQL_QUERY_FOR_VALIDATION, "");
      fail("expected pre-commit clustering validation to fail");
    } catch (HoodieValidationException e) {
      // expected
    }
  }

  @Test
  public void testClusteringInvalidConfigForSqlQueryValidator() throws Exception {
    try {
      boolean populateMetaFields = false;
      initMetaClient(getPropertiesForKeyGen(populateMetaFields));
      testInsertAndClustering(createClusteringBuilder(true, 1).build(), populateMetaFields, true,
              false, SqlQueryEqualityPreCommitValidator.class.getName(), "", "");
      fail("expected pre-commit clustering validation to fail because sql query is not configured");
    } catch (HoodieValidationException e) {
      // expected
    }
  }

  @Test
  public void testClusteringInvalidConfigForSqlQuerySingleResultValidator() throws Exception {
    boolean populateMetaFields = false;
    initMetaClient(getPropertiesForKeyGen(populateMetaFields));
    testInsertAndClustering(createClusteringBuilder(true, 1).build(), populateMetaFields, true,
            false, SqlQuerySingleResultPreCommitValidator.class.getName(),
        "", COUNT_SQL_QUERY_FOR_VALIDATION + "#400");
  }

  @Test
  public void testClusteringInvalidConfigForSqlQuerySingleResultValidatorFailure() throws Exception {
    try {
      boolean populateMetaFields = false;
      initMetaClient(getPropertiesForKeyGen(populateMetaFields));
      testInsertAndClustering(createClusteringBuilder(true, 1).build(), populateMetaFields,
              true, false, SqlQuerySingleResultPreCommitValidator.class.getName(),
          "", COUNT_SQL_QUERY_FOR_VALIDATION + "#802");
      fail("expected pre-commit clustering validation to fail because of count mismatch. expect 400 rows, not 802");
    } catch (HoodieValidationException e) {
      // expected
    }
  }

  private List<HoodieRecord> testInsertAndClustering(HoodieClusteringConfig clusteringConfig, boolean populateMetaFields, boolean completeClustering) throws Exception {
    return testInsertAndClustering(clusteringConfig, populateMetaFields, completeClustering, false, "", "", "");
  }

  private List<HoodieRecord> testInsertAndClustering(HoodieClusteringConfig clusteringConfig, boolean populateMetaFields,
                                                     boolean completeClustering, boolean assertSameFileIds, String validatorClasses,
                                                     String sqlQueryForEqualityValidation, String sqlQueryForSingleResultValidation) throws Exception {
    Pair<Pair<List<HoodieRecord>, List<String>>, Set<HoodieFileGroupId>> allRecords = testInsertTwoBatches(
        populateMetaFields, createBrokenClusteringClient(new HoodieException(CLUSTERING_FAILURE)));
    testClustering(clusteringConfig, populateMetaFields, completeClustering, assertSameFileIds, validatorClasses, sqlQueryForEqualityValidation,
            sqlQueryForSingleResultValidation, allRecords, clusteringMetadataRdd2List, createKeyGenerator);
    return allRecords.getLeft().getLeft();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testFailWritesOnInlineTableServiceExceptions(boolean shouldFail) throws IOException {
    testFailWritesOnInlineTableServiceThrowable(shouldFail, shouldFail, createBrokenClusteringClient(new HoodieException(CLUSTERING_FAILURE)), CLUSTERING_FAILURE);
  }

  @Test
  public void testFailWritesOnInlineTableServiceErrors() throws IOException {
    testFailWritesOnInlineTableServiceThrowable(false, true, createBrokenClusteringClient(new OutOfMemoryError(CLUSTERING_FAILURE)), CLUSTERING_FAILURE);
  }

  @Test
  public void testFailWritesOnInlineCleanExceptions() throws IOException {
    testFailWritesOnInlineTableServiceThrowable(true, true, createBrokenCleaningClient(new HoodieException(CLEANING_FAILURE)), CLEANING_FAILURE);
    testFailWritesOnInlineTableServiceThrowable(false, true, createBrokenCleaningClient(new HoodieException(CLEANING_FAILURE)), CLEANING_FAILURE);
    testFailWritesOnInlineTableServiceThrowable(true, true, createBrokenCleaningClient(new OutOfMemoryError(CLEANING_FAILURE)), CLEANING_FAILURE);
  }

  /**
   * Test scenario of writing more file groups than existing number of file groups in partition.
   */
  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testInsertOverwritePartitionHandlingWithMoreRecords(boolean populateMetaFields) throws Exception {
    initMetaClient(getPropertiesForKeyGen(populateMetaFields));
    verifyInsertOverwritePartitionHandling(1000, 3000, populateMetaFields);
  }

  /**
   * Test scenario of writing fewer file groups than existing number of file groups in partition.
   */
  @Test
  public void testInsertOverwritePartitionHandlingWithFewerRecords() throws Exception {
    verifyInsertOverwritePartitionHandling(3000, 1000, true);
  }

  /**
   * Test scenario of writing similar number file groups in partition.
   */
  @Test
  public void testInsertOverwritePartitionHandlingWithSimilarNumberOfRecords() throws Exception {
    verifyInsertOverwritePartitionHandling(3000, 3000, true);
  }

  /**
   * 1) Do write1 (upsert) with 'batch1RecordsCount' number of records.
   * 2) Do write2 (insert overwrite) with 'batch2RecordsCount' number of records.
   * <p>
   * Verify that all records in step1 are overwritten
   */
  private void verifyInsertOverwritePartitionHandling(int batch1RecordsCount, int batch2RecordsCount, boolean populateMetaFields) throws Exception {
    final String testPartitionPath = "americas";
    HoodieWriteConfig config = getSmallInsertWriteConfig(2000,
        TRIP_EXAMPLE_SCHEMA, dataGen.getEstimatedFileSizeInBytes(150), populateMetaFields, getPropertiesForKeyGen(populateMetaFields));
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});

    // Do Inserts
    String commit1 = "001";
    List<WriteStatus> statuses = writeAndVerifyBatch(client, dataGen.generateInserts(commit1, batch1RecordsCount), commit1, populateMetaFields);
    Set<String> batch1Buckets = getFileIdsFromWriteStatus(statuses);

    // Do Insert Overwrite
    String commitTime2 = "002";
    WriteClientTestUtils.startCommitWithTime(writeClient, commitTime2, REPLACE_COMMIT_ACTION);
    List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime2, batch2RecordsCount);
    List<HoodieRecord> insertsAndUpdates2 = new ArrayList<>(inserts2);
    JavaRDD<HoodieRecord> insertAndUpdatesRDD2 = jsc.parallelize(insertsAndUpdates2, 2);
    HoodieWriteResult writeResult = client.insertOverwrite(insertAndUpdatesRDD2, commitTime2);
    List<WriteStatus> statusList = jsc.parallelize(writeResult.getWriteStatuses().collect(), 2).collect();
    client.commit(commitTime2, jsc.parallelize(statusList), Option.empty(), REPLACE_COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    assertNoWriteErrors(statusList);

    assertEquals(batch1Buckets, new HashSet<>(writeResult.getPartitionToReplaceFileIds().get(testPartitionPath)));
    verifyRecordsWritten(commitTime2, populateMetaFields, inserts2, statusList, config,
        HoodieSparkKeyGeneratorFactory.createKeyGenerator(config.getProps()));
  }

  private Set<String> getFileIdsFromWriteStatus(List<WriteStatus> statuses) {
    return statuses.stream().map(WriteStatus::getFileId).collect(Collectors.toSet());
  }

  /**
   * Test scenario of writing fewer file groups for first partition than second and third partition.
   */
  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void verifyDeletePartitionsHandlingWithFewerRecordsFirstPartition(boolean populateMetaFields) throws Exception {
    initMetaClient(getPropertiesForKeyGen(populateMetaFields));
    verifyDeletePartitionsHandling(1000, 3000, 3000, populateMetaFields);
  }

  /**
   * Test scenario of writing similar number file groups in partition.
   */
  @Test
  public void verifyDeletePartitionsHandlingWithSimilarNumberOfRecords() throws Exception {
    verifyDeletePartitionsHandling(3000, 3000, 3000, true);
  }

  /**
   * Test scenario of writing more file groups for first partition than second and third partition.
   */
  @Test
  public void verifyDeletePartitionsHandlingHandlingWithFewerRecordsSecondThirdPartition() throws Exception {
    verifyDeletePartitionsHandling(3000, 1000, 1000, true);
  }

  private Set<String> insertPartitionRecordsWithCommit(SparkRDDWriteClient client, int recordsCount, String commitTime1, String partitionPath) throws IOException {
    WriteClientTestUtils.startCommitWithTime(client, commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInsertsForPartition(commitTime1, recordsCount, partitionPath);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 2);
    List<WriteStatus> statusList = client.upsert(insertRecordsRDD1, commitTime1).collect();
    client.commit(commitTime1, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    assertNoWriteErrors(statusList);
    Set<String> batchBuckets = statusList.stream().map(WriteStatus::getFileId).collect(Collectors.toSet());
    verifyRecordsWritten(commitTime1, true, inserts1, statusList, client.getConfig(),
        HoodieSparkKeyGeneratorFactory.createKeyGenerator(client.getConfig().getProps()));
    return batchBuckets;
  }

  private Set<String> deletePartitionWithCommit(SparkRDDWriteClient client, String commitTime, List<String> deletePartitionPath) {
    WriteClientTestUtils.startCommitWithTime(client, commitTime, REPLACE_COMMIT_ACTION);
    HoodieWriteResult writeResult = client.deletePartitions(deletePartitionPath, commitTime);
    JavaRDD<WriteStatus> writeStatusJavaRDD = jsc.parallelize(writeResult.getWriteStatuses().collect(), 1);
    client.commit(commitTime, writeStatusJavaRDD, Option.empty(), REPLACE_COMMIT_ACTION, writeResult.getPartitionToReplaceFileIds(), Option.empty());
    Set<String> deletePartitionReplaceFileIds =
        writeResult.getPartitionToReplaceFileIds().entrySet()
            .stream().flatMap(entry -> entry.getValue().stream()).collect(Collectors.toSet());
    return deletePartitionReplaceFileIds;
  }

  /**
   * 1) Do write1 (upsert) with 'batch1RecordsCount' number of records for first partition.
   * 2) Do write2 (upsert) with 'batch2RecordsCount' number of records for second partition.
   * 3) Do write3 (upsert) with 'batch3RecordsCount' number of records for third partition.
   * 4) delete first partition and check result.
   * 5) delete second and third partition and check result.
   */
  private void verifyDeletePartitionsHandling(int batch1RecordsCount, int batch2RecordsCount, int batch3RecordsCount,
                                              boolean populateMetaFields) throws Exception {
    HoodieWriteConfig config = getSmallInsertWriteConfig(2000,
        TRIP_EXAMPLE_SCHEMA, dataGen.getEstimatedFileSizeInBytes(150), populateMetaFields, getPropertiesForKeyGen(populateMetaFields));
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    dataGen = new HoodieTestDataGenerator();

    // Do Inserts for DEFAULT_FIRST_PARTITION_PATH
    String commitTime1 = "001";
    Set<String> batch1Buckets =
        this.insertPartitionRecordsWithCommit(client, batch1RecordsCount, commitTime1, DEFAULT_FIRST_PARTITION_PATH);

    // Do Inserts for DEFAULT_SECOND_PARTITION_PATH
    String commitTime2 = "002";
    Set<String> batch2Buckets =
        this.insertPartitionRecordsWithCommit(client, batch2RecordsCount, commitTime2, DEFAULT_SECOND_PARTITION_PATH);

    // Do Inserts for DEFAULT_THIRD_PARTITION_PATH
    String commitTime3 = "003";
    Set<String> batch3Buckets =
        this.insertPartitionRecordsWithCommit(client, batch3RecordsCount, commitTime3, DEFAULT_THIRD_PARTITION_PATH);

    // delete DEFAULT_FIRST_PARTITION_PATH
    String commitTime4 = "004";
    Set<String> deletePartitionReplaceFileIds1 =
        deletePartitionWithCommit(client, commitTime4, Arrays.asList(DEFAULT_FIRST_PARTITION_PATH));
    assertEquals(batch1Buckets, deletePartitionReplaceFileIds1);
    List<HoodieBaseFile> baseFiles = HoodieClientTestUtils.getLatestBaseFiles(basePath, storage,
        String.format("%s/%s/*", basePath, DEFAULT_FIRST_PARTITION_PATH));
    assertEquals(0, baseFiles.size());
    baseFiles = HoodieClientTestUtils.getLatestBaseFiles(basePath, storage,
        String.format("%s/%s/*", basePath, DEFAULT_SECOND_PARTITION_PATH));
    assertTrue(baseFiles.size() > 0);
    baseFiles = HoodieClientTestUtils.getLatestBaseFiles(basePath, storage,
        String.format("%s/%s/*", basePath, DEFAULT_THIRD_PARTITION_PATH));
    assertTrue(baseFiles.size() > 0);

    // delete DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH
    String commitTime5 = "005";
    Set<String> deletePartitionReplaceFileIds2 =
        deletePartitionWithCommit(client, commitTime5, Arrays.asList(DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH));
    Set<String> expectedFileId = new HashSet<>();
    expectedFileId.addAll(batch2Buckets);
    expectedFileId.addAll(batch3Buckets);
    assertEquals(expectedFileId, deletePartitionReplaceFileIds2);

    baseFiles = HoodieClientTestUtils.getLatestBaseFiles(basePath, storage,
        String.format("%s/%s/*", basePath, DEFAULT_FIRST_PARTITION_PATH),
        String.format("%s/%s/*", basePath, DEFAULT_SECOND_PARTITION_PATH),
        String.format("%s/%s/*", basePath, DEFAULT_THIRD_PARTITION_PATH));
    assertEquals(0, baseFiles.size());
  }

  private Pair<Set<String>, List<HoodieRecord>> testUpdates(String instantTime, SparkRDDWriteClient client,
                                                            int sizeToInsertAndUpdate, int expectedRecords)
      throws IOException {
    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    List<HoodieRecord> inserts = dataGen.generateInserts(instantTime, sizeToInsertAndUpdate);
    Set<String> keys = recordsToRecordKeySet(inserts);
    List<HoodieRecord> insertsAndUpdates = new ArrayList<>();
    insertsAndUpdates.addAll(inserts);
    insertsAndUpdates.addAll(dataGen.generateUpdates(instantTime, inserts));

    JavaRDD<HoodieRecord> insertAndUpdatesRDD = jsc.parallelize(insertsAndUpdates, 1);
    List<WriteStatus> statusList = client.upsert(insertAndUpdatesRDD, instantTime).collect();
    client.commit(instantTime, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap());
    assertNoWriteErrors(statusList);

    assertTheEntireDatasetHasAllRecordsStill(expectedRecords);
    return Pair.of(keys, inserts);
  }

  private void testDeletes(SparkRDDWriteClient client, List<HoodieRecord> previousRecords, int sizeToDelete,
                           String existingFile, String instantTime, int expectedRecords, List<String> keys) {
    WriteClientTestUtils.startCommitWithTime(client, instantTime);

    List<HoodieKey> hoodieKeysToDelete = randomSelectAsHoodieKeys(previousRecords, sizeToDelete);
    JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(hoodieKeysToDelete, 1);
    List<WriteStatus> statusList = client.delete(deleteKeys, instantTime).collect();
    client.commit(instantTime, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    assertNoWriteErrors(statusList);

    assertEquals(1, statusList.size(), "Just 1 file needs to be added.");
    assertEquals(existingFile, statusList.get(0).getFileId(), "Existing file should be expanded");

    assertTheEntireDatasetHasAllRecordsStill(expectedRecords);

    StoragePath newFile = new StoragePath(basePath, statusList.get(0).getStat().getPath());
    assertEquals(expectedRecords,
        getFileUtilsInstance(metaClient).readRowKeys(storage, newFile).size(),
        "file should contain 110 records");

    List<GenericRecord> records = getFileUtilsInstance(metaClient).readAvroRecords(storage, newFile);
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      assertTrue(keys.contains(recordKey), "key expected to be part of " + instantTime);
      assertFalse(hoodieKeysToDelete.contains(recordKey), "Key deleted");
    }
  }

  @Override
  protected String[] assertTheEntireDatasetHasAllRecordsStill(int expectedRecords) {
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(expectedRecords,
            HoodieClientTestUtils.read(jsc, basePath, sqlContext, storage, fullPartitionPaths).count(), "Must contain " + expectedRecords + " records");
    return fullPartitionPaths;
  }

  /**
   * Test delete with delete api.
   */
  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testDeletesWithoutInserts(boolean populateMetaFields) throws Exception {
    initMetaClient(getPropertiesForKeyGen(populateMetaFields));
    testDeletesWithoutInserts(populateMetaFields, list2Rdd, rdd2List);
  }

  /**
   * Test to ensure commit metadata points to valid files.
   */
  @Test
  public void testCommitWritesRelativePaths() throws Exception {
    testCommitWritesRelativePaths(list2Rdd);
  }

  /**
   * Test to ensure commit metadata points to valid files.10.
   */
  @ParameterizedTest
  @MethodSource("populateMetaFieldsParams")
  public void testMetadataStatsOnCommit(boolean populateMetaFields) throws Exception {
    initMetaClient(getPropertiesForKeyGen(populateMetaFields));
    testMetadataStatsOnCommit(populateMetaFields, list2Rdd);
  }

  /**
   * Tests behavior of committing only when consistency is verified.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testConsistencyCheckDuringFinalize(boolean enableOptimisticConsistencyGuard) throws Exception {
    testConsistencyCheckDuringFinalize(context, enableOptimisticConsistencyGuard, getHoodieTable, list2Rdd, rdd2List);
  }

  private void testRollbackAfterConsistencyCheckFailureUsingFileList(boolean rollbackUsingMarkers, boolean enableOptimisticConsistencyGuard,
                                                                     boolean populateMetaFields) throws Exception {
    testRollbackAfterConsistencyCheckFailureUsingFileList(context, rollbackUsingMarkers, enableOptimisticConsistencyGuard,
            populateMetaFields, getHoodieTable, list2Rdd, rdd2List);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRollbackAfterConsistencyCheckFailureUsingFileList(boolean enableOptimisticConsistencyGuard) throws Exception {
    testRollbackAfterConsistencyCheckFailureUsingFileList(false, enableOptimisticConsistencyGuard, true);
  }

  @ParameterizedTest
  @MethodSource("rollbackAfterConsistencyCheckFailureParams")
  public void testRollbackAfterConsistencyCheckFailureUsingMarkers(boolean enableOptimisticConsistencyGuard, boolean populateMetCols) throws Exception {
    initMetaClient(getPropertiesForKeyGen(populateMetCols));
    testRollbackAfterConsistencyCheckFailureUsingFileList(true, enableOptimisticConsistencyGuard, populateMetCols);
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

  @Test
  public void testMultiOperationsPerCommit() throws IOException {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder()
        .withAllowMultiWriteOnSameInstant(true);
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    HoodieWriteConfig cfg = cfgBuilder.build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    String firstInstantTime = "0000";
    WriteClientTestUtils.startCommitWithTime(client, firstInstantTime);
    int numRecords = 200;
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(dataGen.generateInserts(firstInstantTime, numRecords), 1);
    JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, firstInstantTime);
    assertTrue(client.commit(firstInstantTime, result), "Commit should succeed");
    assertTrue(testTable.commitExists(firstInstantTime), "After explicit commit, commit file should be created");

    String[] fullPartitionPaths = assertTheEntireDatasetHasAllRecordsStill(numRecords);

    String nextInstantTime = "0001";
    WriteClientTestUtils.startCommitWithTime(client, nextInstantTime);
    JavaRDD<HoodieRecord> updateRecords = jsc.parallelize(dataGen.generateUpdates(nextInstantTime, numRecords), 1);
    JavaRDD<HoodieRecord> insertRecords = jsc.parallelize(dataGen.generateInserts(nextInstantTime, numRecords), 1);
    JavaRDD<WriteStatus> inserts = client.bulkInsert(insertRecords, nextInstantTime);
    JavaRDD<WriteStatus> upserts = client.upsert(updateRecords, nextInstantTime);
    assertTrue(client.commit(nextInstantTime, inserts.union(upserts)), "Commit should succeed");
    assertTrue(testTable.commitExists(firstInstantTime), "After explicit commit, commit file should be created");
    int totalRecords = 2 * numRecords;
    assertEquals(totalRecords,
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, storage, fullPartitionPaths).count(), "Must contain " + totalRecords + " records");
  }

  @Test
  public void testClusteringCommitInPresenceOfInflightCommit() throws Exception {
    Properties properties = getDisabledRowWriterProperties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    HoodieLockConfig lockConfig = createLockConfig(new PreferWriterConflictResolutionStrategy());
    HoodieCleanConfig cleanConfig = createCleanConfig(HoodieFailedWritesCleaningPolicy.LAZY, false);
    HoodieWriteConfig insertWriteConfig = getConfigBuilder()
        .withCleanConfig(cleanConfig)
        .withLockConfig(lockConfig)
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withProperties(properties)
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(insertWriteConfig);

    // Create a base commit on a file.
    int numRecords = 200;
    String firstCommit = client.createNewInstantTime();
    String partitionStr = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionStr});
    writeBatch(client, firstCommit, "000", Option.of(Arrays.asList("000")), "000",
        numRecords, dataGenerator::generateInserts, SparkRDDWriteClient::insert, true, numRecords, numRecords,
        1, INSTANT_GENERATOR);

    // Do an upsert operation without autocommit.
    String inflightCommit = client.createNewInstantTime();
    writeBatch(client, inflightCommit, firstCommit, Option.of(Arrays.asList("000")), "000",
        100, dataGenerator::generateUniqueUpdates, SparkRDDWriteClient::upsert, false, 0, 200,
        2, true, INSTANT_GENERATOR, true);

    // Schedule and execute a clustering plan on the same partition. During conflict resolution the commit should fail.
    HoodieWriteConfig clusteringWriteConfig = getConfigBuilder()
        .withCleanConfig(cleanConfig)
        .withClusteringConfig(createClusteringBuilder(true, 1).build())
        .withPreCommitValidatorConfig(createPreCommitValidatorConfig(200))
        .withLockConfig(lockConfig)
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withProperties(properties)
        .build();

    // create client with new config.
    SparkRDDWriteClient<?> clusteringWriteClient = getHoodieWriteClient(clusteringWriteConfig);

    // Schedule and execute clustering, this should fail since there is a conflict between ingestion inflight commit.
    String clusteringCommitTime = clusteringWriteClient.scheduleClustering(Option.empty()).get();
    assertThrows(HoodieClusteringException.class, () -> clusteringWriteClient.cluster(clusteringCommitTime, true));

    // Do a rollback on the replacecommit that is failed
    clusteringWriteClient.rollback(clusteringCommitTime);

    // Verify the timeline
    List<HoodieInstant> instants = metaClient.reloadActiveTimeline().getInstants();
    assertEquals(3, instants.size());
    assertEquals(HoodieActiveTimeline.ROLLBACK_ACTION, instants.get(2).getAction());
    assertEquals(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, COMMIT_ACTION, inflightCommit), instants.get(1));
  }

  @Test
  public void testIngestionCommitInPresenceOfCompletedClusteringCommit() throws Exception {
    Properties properties = getDisabledRowWriterProperties();
    properties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY, basePath + "/.hoodie/.locks");
    HoodieCleanConfig cleanConfig = createCleanConfig(HoodieFailedWritesCleaningPolicy.LAZY, false);
    HoodieWriteConfig insertWriteConfig = getConfigBuilder()
        .withCleanConfig(cleanConfig)
        .withLockConfig(createLockConfig(new PreferWriterConflictResolutionStrategy()))
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withProperties(properties)
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(insertWriteConfig);

    // Create a base commit on a file.
    int numRecords = 200;
    String firstCommit = client.createNewInstantTime();
    String partitionStr = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionStr});
    writeBatch(client, firstCommit, "000", Option.of(Arrays.asList("000")), "000",
        numRecords, dataGenerator::generateInserts, SparkRDDWriteClient::insert, true, numRecords, numRecords,
        1, INSTANT_GENERATOR);

    // Create and temporarily block a lower timestamp for ingestion.
    String inflightCommit = client.createNewInstantTime();
    JavaRDD<WriteStatus> ingestionResult = writeBatch(client, inflightCommit, firstCommit, Option.of(Arrays.asList("000")), "000",
        100, dataGenerator::generateUniqueUpdates, SparkRDDWriteClient::upsert, false, 0, 200,
        2, true, INSTANT_GENERATOR, true);

    // Schedule and execute a clustering plan on the same partition. During conflict resolution the commit should fail.
    // Since it is harder to test corner cases where the ingestion writer is at dedupe step right before the inflight file creation
    // and clustering commit is just about to complete, using the default conflict resolutions strategy only for clustering job
    // To create a successful commit.
    HoodieWriteConfig clusteringWriteConfig = getConfigBuilder()
        .withCleanConfig(cleanConfig)
        .withClusteringConfig(createClusteringBuilder(true, 1).build())
        .withPreCommitValidatorConfig(createPreCommitValidatorConfig(200))
        .withLockConfig(createLockConfig(new SimpleConcurrentFileWritesConflictResolutionStrategy()))
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withProperties(properties)
        .build();

    // create client with new config.
    SparkRDDWriteClient<?> clusteringWriteClient = getHoodieWriteClient(clusteringWriteConfig);

    // Schedule and execute clustering, this complete successfully.
    String clusteringCommitTime = clusteringWriteClient.scheduleClustering(Option.empty()).get();
    clusteringWriteClient.cluster(clusteringCommitTime, true);

    // When ingestion writer is committing it should throw an exception.
    assertThrows(HoodieWriteConflictException.class, () -> client.commit(inflightCommit, ingestionResult));
  }

  protected HoodieInstant createRequestedClusterInstant(HoodieTableMetaClient metaClient, String clusterTime, List<FileSlice>[] fileSlices) throws IOException {
    HoodieClusteringPlan clusteringPlan =
        ClusteringUtils.createClusteringPlan(EXECUTION_STRATEGY_CLASS_NAME.defaultValue(), STRATEGY_PARAMS, fileSlices, Collections.emptyMap());

    HoodieInstant clusteringInstant = INSTANT_GENERATOR.createNewInstant(REQUESTED, CLUSTERING_ACTION, clusterTime);
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
        .setClusteringPlan(clusteringPlan).setOperationType(WriteOperationType.CLUSTER.name()).build();
    metaClient.getActiveTimeline().saveToPendingClusterCommit(clusteringInstant, requestedReplaceMetadata);
    return clusteringInstant;
  }

  public static class FailingPreCommitValidator<T extends HoodieRecordPayload, I, K, O extends HoodieData<WriteStatus>> extends SparkPreCommitValidator<T, I, K, O> {

    public FailingPreCommitValidator(HoodieSparkTable table, HoodieEngineContext context, HoodieWriteConfig config) {
      super(table, context, config);
    }

    @Override
    protected void validateRecordsBeforeAndAfter(final Dataset<Row> before, final Dataset<Row> after, final Set<String> partitionsAffected) {
      throw new HoodieValidationException("simulate failure");
    }
  }

  public static class WriteClientBrokenBase<T extends HoodieRecordPayload> extends org.apache.hudi.client.SparkRDDWriteClient<T> {
    protected final Throwable throwable;

    public WriteClientBrokenBase(HoodieEngineContext context, HoodieWriteConfig clientConfig, Throwable throwable) {
      super(context, clientConfig);
      this.throwable = throwable;
    }
  }

  public static class WriteClientBrokenClustering<T extends HoodieRecordPayload> extends WriteClientBrokenBase<T> {
    public WriteClientBrokenClustering(HoodieEngineContext context, HoodieWriteConfig clientConfig, Throwable throwable) {
      super(context, clientConfig, throwable);
    }

    @Override
    protected void runTableServicesInlineInternal(HoodieTable table, HoodieCommitMetadata metadata, Option<Map<String, String>> extraMetadata) {
      if (config.inlineClusteringEnabled()) {
        if (throwable instanceof Error) {
          throw (Error) throwable;
        }
        throw (HoodieException) throwable;
      }
    }
  }

  public static class WriteClientBrokenClean<T extends HoodieRecordPayload> extends WriteClientBrokenBase<T> {
    public WriteClientBrokenClean(HoodieEngineContext context, HoodieWriteConfig clientConfig, Throwable throwable) {
      super(context, clientConfig, throwable);
    }

    @Override
    protected void autoCleanOnCommit() {
      if (throwable instanceof Error) {
        throw (Error) throwable;
      }
      throw (HoodieException) throwable;
    }
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
