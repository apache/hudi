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

package org.apache.hudi.testutils;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.SparkHoodieIndexFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieSparkTable;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.testutils.Assertions.assertPartitionMetadataForKeys;
import static org.apache.hudi.testutils.Assertions.assertPartitionMetadataForRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base Class providing setup/cleanup and utility methods for testing Hoodie Client facing tests.
 */
public class HoodieClientTestBase extends HoodieSparkClientTestHarness {

  protected static final Logger LOG = LoggerFactory.getLogger(HoodieClientTestBase.class);

  @BeforeEach
  public void setUp() throws Exception {
    initResources();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  public HoodieSparkTable getHoodieTable(HoodieTableMetaClient metaClient, HoodieWriteConfig config) {
    HoodieSparkTable table = HoodieSparkTable.create(config, context, metaClient);
    ((SyncableFileSystemView) (table.getSliceView())).reset();
    return table;
  }

  /**
   * Ensure records have location field set.
   *
   * @param taggedRecords Tagged Records
   * @param instantTime Commit Timestamp
   */
  public static void checkTaggedRecords(List<HoodieRecord> taggedRecords, String instantTime) {
    for (HoodieRecord rec : taggedRecords) {
      assertTrue(rec.isCurrentLocationKnown(), "Record " + rec + " found with no location.");
      assertEquals(rec.getCurrentLocation().getInstantTime(), instantTime,
          "All records should have commit time " + instantTime + ", since updates were made");
    }
  }

  /**
   * Helper to generate records generation function for testing Prepped version of API. Prepped APIs expect the records
   * to be already de-duped and have location set. This wrapper takes care of record-location setting. Uniqueness is
   * guaranteed by record-generation function itself.
   *
   * @param writeConfig       Hoodie Write Config
   * @param recordsGenFunction Records Generation function
   * @return Wrapped function
   */
  public static Function2<List<HoodieRecord>, String, Integer> wrapRecordsGenFunctionForPreppedCalls(
      final String basePath,
      final StorageConfiguration<?> storageConf,
      final HoodieSparkEngineContext context,
      final HoodieWriteConfig writeConfig,
      final Function2<List<HoodieRecord>, String, Integer> recordsGenFunction) {
    return (commit, numRecords) -> {
      final HoodieIndex index = SparkHoodieIndexFactory.createIndex(writeConfig);
      List<HoodieRecord> records = recordsGenFunction.apply(commit, numRecords);
      final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, basePath);
      HoodieSparkTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
      JavaRDD<HoodieRecord> taggedRecords = tagLocation(index, context, context.getJavaSparkContext().parallelize(records, 1), table);
      return taggedRecords.collect();
    };
  }

  /**
   * Helper to generate records generation function for testing Prepped version of API. Prepped APIs expect the records
   * to be already de-duped and have location set. This wrapper takes care of record-location setting. Uniqueness is
   * guaranteed by record-generation function itself.
   *
   * @param writeConfig       Hoodie Write Config
   * @param recordsGenFunction Records Generation function (for partition)
   * @return Wrapped function
   */
  public static Function3<List<HoodieRecord>, String, Integer, String> wrapPartitionRecordsGenFunctionForPreppedCalls(
      final String basePath,
      final StorageConfiguration<?> storageConf,
      final HoodieSparkEngineContext context,
      final HoodieWriteConfig writeConfig,
      final Function3<List<HoodieRecord>, String, Integer, String> recordsGenFunction) {
    return (commit, numRecords, partition) -> {
      final HoodieIndex index = SparkHoodieIndexFactory.createIndex(writeConfig);
      List<HoodieRecord> records = recordsGenFunction.apply(commit, numRecords, partition);
      final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, basePath);
      HoodieSparkTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
      JavaRDD<HoodieRecord> taggedRecords = tagLocation(index, context, context.getJavaSparkContext().parallelize(records, 1), table);
      return taggedRecords.collect();
    };
  }

  /**
   * Helper to generate delete keys generation function for testing Prepped version of API. Prepped APIs expect the keys
   * to be already de-duped and have location set. This wrapper takes care of record-location setting. Uniqueness is
   * guaranteed by key-generation function itself.
   *
   * @param writeConfig    Hoodie Write Config
   * @param keyGenFunction Keys Generation function
   * @return Wrapped function
   */
  public static Function<Integer, List<HoodieKey>> wrapDeleteKeysGenFunctionForPreppedCalls(
      final String basePath,
      final StorageConfiguration<?> storageConf,
      final HoodieSparkEngineContext context,
      final HoodieWriteConfig writeConfig,
      final Function<Integer, List<HoodieKey>> keyGenFunction) {
    return (numRecords) -> {
      final HoodieIndex index = SparkHoodieIndexFactory.createIndex(writeConfig);
      List<HoodieKey> records = keyGenFunction.apply(numRecords);
      final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, basePath);
      HoodieSparkTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
      JavaRDD<HoodieRecord> recordsToDelete = context.getJavaSparkContext().parallelize(records, 1)
          .map(key -> new HoodieAvroRecord(key, new EmptyHoodieRecordPayload()));
      JavaRDD<HoodieRecord> taggedRecords = tagLocation(index, context, recordsToDelete, table);
      return taggedRecords.map(record -> record.getKey()).collect();
    };
  }

  /**
   * Generate wrapper for record generation function for testing Prepped APIs.
   *
   * @param isPreppedAPI Flag to indicate if this is for testing prepped-version of APIs
   * @param writeConfig Hoodie Write Config
   * @param wrapped Actual Records Generation function
   * @return Wrapped Function
   */
  @Override
  public Function2<List<HoodieRecord>, String, Integer> generateWrapRecordsFn(boolean isPreppedAPI,
      HoodieWriteConfig writeConfig,
      Function2<List<HoodieRecord>, String, Integer> wrapped) {
    if (isPreppedAPI) {
      return wrapRecordsGenFunctionForPreppedCalls(basePath, storageConf, context, writeConfig, wrapped);
    } else {
      return wrapped;
    }
  }

  /**
   * Generate wrapper for record generation function for testing Prepped APIs.
   *
   * @param isPreppedAPI Flag to indicate if this is for testing prepped-version of APIs
   * @param writeConfig Hoodie Write Config
   * @param wrapped Actual Records Generation function (for partition)
   * @return Wrapped Function
   */
  public Function3<List<HoodieRecord>, String, Integer, String> generateWrapRecordsForPartitionFn(boolean isPreppedAPI,
      HoodieWriteConfig writeConfig, Function3<List<HoodieRecord>, String, Integer, String> wrapped) {
    if (isPreppedAPI) {
      return wrapPartitionRecordsGenFunctionForPreppedCalls(basePath, storageConf, context, writeConfig, wrapped);
    } else {
      return wrapped;
    }
  }

  /**
   * Generate wrapper for delete key generation function for testing Prepped APIs.
   *
   * @param isPreppedAPI Flag to indicate if this is for testing prepped-version of APIs
   * @param writeConfig Hoodie Write Config
   * @param wrapped Actual Records Generation function
   * @return Wrapped Function
   */
  public Function<Integer, List<HoodieKey>> generateWrapDeleteKeysFn(boolean isPreppedAPI,
      HoodieWriteConfig writeConfig, Function<Integer, List<HoodieKey>> wrapped) {
    if (isPreppedAPI) {
      return wrapDeleteKeysGenFunctionForPreppedCalls(basePath, storageConf, context, writeConfig, wrapped);
    } else {
      return wrapped;
    }
  }

  public JavaRDD<WriteStatus> insertFirstBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
                                               String initCommitTime, int numRecordsInThisCommit,
                                               Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
                                               boolean assertForCommit, int expRecordsInThisCommit, InstantGenerator instantGenerator) throws Exception {
    return insertFirstBatch(writeConfig, client, newCommitTime, initCommitTime, numRecordsInThisCommit, writeFn, isPreppedAPI,
        assertForCommit, expRecordsInThisCommit, true, instantGenerator);
  }

  public JavaRDD<WriteStatus> insertFirstBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
                                               String initCommitTime, int numRecordsInThisCommit,
                                               Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
                                               boolean assertForCommit, int expRecordsInThisCommit, boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator) throws Exception {
    return insertFirstBatch(writeConfig, client, newCommitTime, initCommitTime, numRecordsInThisCommit, writeFn, isPreppedAPI,
        assertForCommit, expRecordsInThisCommit, filterForCommitTimeWithAssert, instantGenerator, false);
  }

  /**
   * Helper to insert first batch of records and do regular assertions on the state after successful completion.
   *
   * @param writeConfig            Hoodie Write Config
   * @param client                 Hoodie Write Client
   * @param newCommitTime          New Commit Timestamp to be used
   * @param initCommitTime         Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param writeFn                Write Function to be used for insertion
   * @param isPreppedAPI           Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit        Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public JavaRDD<WriteStatus> insertFirstBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
      String initCommitTime, int numRecordsInThisCommit,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
      boolean assertForCommit, int expRecordsInThisCommit, boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator, boolean skipCommit) throws Exception {
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateInserts);

    return writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime, numRecordsInThisCommit,
        recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expRecordsInThisCommit, 1,
        filterForCommitTimeWithAssert, instantGenerator, skipCommit);
  }

  /**
   * Helper to insert another batch of records and do regular assertions on the state after successful completion.
   *
   * @param writeConfig            Hoodie Write Config
   * @param client                 Hoodie Write Client
   * @param newCommitTime          New Commit Timestamp to be used
   * @param initCommitTime         Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param writeFn                Write Function to be used for insertion
   * @param isPreppedAPI           Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit        Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @param expTotalRecords        Expected number of records when scanned
   * @param expTotalCommits        Expected number of commits (including this commit)
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public JavaRDD<WriteStatus> insertBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
      String initCommitTime, int numRecordsInThisCommit,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
      Option<String> partition, InstantGenerator instantGenerator) throws Exception {

    if (partition.isPresent()) {
      final Function3<List<HoodieRecord>, String, Integer, String> recordGenFunction =
          generateWrapRecordsForPartitionFn(isPreppedAPI, writeConfig, dataGen::generateInsertsForPartition);

      return writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime, numRecordsInThisCommit,
          recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits,
          partition.get(), instantGenerator);
    } else {
      final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
          generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateInserts);

      return writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime, numRecordsInThisCommit,
          recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, instantGenerator);
    }
  }

  public JavaRDD<WriteStatus> updateBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
                                          String prevCommitTime, Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime,
                                          int numRecordsInThisCommit,
                                          Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
                                          boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                          InstantGenerator instantGenerator) throws Exception {
    return updateBatch(writeConfig, client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, writeFn,
        isPreppedAPI, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, true, instantGenerator);
  }

  public JavaRDD<WriteStatus> updateBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
                                          String prevCommitTime, Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime,
                                          int numRecordsInThisCommit,
                                          Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
                                          boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                          boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator) throws Exception {
    return updateBatch(writeConfig, client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit,
        writeFn, isPreppedAPI, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, filterForCommitTimeWithAssert, instantGenerator,
        false);
  }

  /**
   * Helper to upsert batch of records and do regular assertions on the state after successful completion.
   *
   * @param writeConfig                  Hoodie Write Config
   * @param client                       Hoodie Write Client
   * @param newCommitTime                New Commit Timestamp to be used
   * @param prevCommitTime               Commit Timestamp used in previous commit
   * @param commitTimesBetweenPrevAndNew Sample of Timestamps between prevCommitTime and newCommitTime
   * @param initCommitTime               Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit       Number of records to be added in the new commit
   * @param writeFn                      Write Function to be used for upsert
   * @param isPreppedAPI                 Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit              Enable Assertion of Writes
   * @param expRecordsInThisCommit       Expected number of records in this commit
   * @param expTotalRecords              Expected number of records when scanned
   * @param expTotalCommits              Expected number of commits (including this commit)
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public JavaRDD<WriteStatus> updateBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
                                          String prevCommitTime, Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime,
                                          int numRecordsInThisCommit,
                                          Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
                                          boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                          boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator,
                                          boolean skipCommit) throws Exception {
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateUniqueUpdates);

    return writeBatch(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime,
        numRecordsInThisCommit, recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords,
        expTotalCommits, filterForCommitTimeWithAssert, instantGenerator, skipCommit);
  }

  public JavaRDD<WriteStatus> deleteBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
                                          String initCommitTime, int numRecordsInThisCommit, boolean isPreppedAPI, boolean assertForCommit,
                                          int expRecordsInThisCommit, int expTotalRecords, TimelineFactory timelineFactory, InstantGenerator instantGenerator) throws Exception {
    return deleteBatch(writeConfig, client, newCommitTime, prevCommitTime, initCommitTime, numRecordsInThisCommit, isPreppedAPI,
        assertForCommit, expRecordsInThisCommit, expTotalRecords, true, timelineFactory, instantGenerator);
  }

  /**
   * Helper to delete batch of keys and do regular assertions on the state after successful completion.
   *
   * @param writeConfig            Hoodie Write Config
   * @param client                 Hoodie Write Client
   * @param newCommitTime          New Commit Timestamp to be used
   * @param prevCommitTime         Commit Timestamp used in previous commit
   * @param initCommitTime         Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param isPreppedAPI           Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit        Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @param expTotalRecords        Expected number of records when scanned
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public JavaRDD<WriteStatus> deleteBatch(HoodieWriteConfig writeConfig, SparkRDDWriteClient client, String newCommitTime,
      String prevCommitTime, String initCommitTime, int numRecordsInThisCommit, boolean isPreppedAPI,
      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords,
      boolean filterForCommitTimeWithAssert, TimelineFactory timelineFactory, InstantGenerator instantGenerator) throws Exception {

    if (isPreppedAPI) {
      final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
          generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateUniqueDeleteRecords);

      // Delete 1 (only deletes)
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, numRecordsInThisCommit);
      JavaRDD<HoodieRecord> deleteRecords = jsc.parallelize(records, 1);

      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> deleteFn = SparkRDDWriteClient::deletePrepped;
      JavaRDD<WriteStatus> result = deleteFn.apply(client, deleteRecords, newCommitTime);
      return getWriteStatusAndVerifyDeleteOperation(newCommitTime, prevCommitTime, initCommitTime, assertForCommit, expRecordsInThisCommit, expTotalRecords,
          filterForCommitTimeWithAssert, result, timelineFactory, instantGenerator, client);
    } else {
      final Function<Integer, List<HoodieKey>> keyGenFunction =
          generateWrapDeleteKeysFn(isPreppedAPI, writeConfig, dataGen::generateUniqueDeletes);

      // Delete 1 (only deletes)
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieKey> keysToDelete = keyGenFunction.apply(numRecordsInThisCommit);
      JavaRDD<HoodieKey> deleteRecords = jsc.parallelize(keysToDelete, 1);

      // check the partition metadata is written out
      assertPartitionMetadataForKeys(basePath, keysToDelete, storage);

      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieKey>, String> deleteFn = SparkRDDWriteClient::delete;
      JavaRDD<WriteStatus> result = deleteFn.apply(client, deleteRecords, newCommitTime);
      List<WriteStatus> resultList = result.collect();
      return getWriteStatusAndVerifyDeleteOperation(newCommitTime, prevCommitTime, initCommitTime, assertForCommit, expRecordsInThisCommit, expTotalRecords,
          filterForCommitTimeWithAssert, jsc.parallelize(resultList), timelineFactory, instantGenerator, client);
    }
  }

  public JavaRDD<WriteStatus> writeBatch(SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
                                         Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                         Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                         Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                         boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords,
                                         int expTotalCommits, InstantGenerator instantGenerator) throws Exception {
    return writeBatch(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, true, instantGenerator);
  }

  public JavaRDD<WriteStatus> writeBatch(SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
                                         Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                         Function3<List<HoodieRecord>, String, Integer, String> recordGenFunction,
                                         Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                         boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                         String partition, InstantGenerator instantGenerator) throws Exception {
    return writeBatch(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, true, partition, instantGenerator);
  }

  public JavaRDD<WriteStatus> writeBatch(SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
                                         Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                         Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                         Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                         boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                         boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator) throws Exception {
    return writeBatch(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, filterForCommitTimeWithAssert, instantGenerator,
        false);
  }

  /**
   * Helper to insert/upsert batch of records and do regular assertions on the state after successful completion.
   *
   * @param client                       Hoodie Write Client
   * @param newCommitTime                New Commit Timestamp to be used
   * @param prevCommitTime               Commit Timestamp used in previous commit
   * @param commitTimesBetweenPrevAndNew Sample of Timestamps between prevCommitTime and newCommitTime
   * @param initCommitTime               Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit       Number of records to be added in the new commit
   * @param recordGenFunction            Records Generation Function
   * @param writeFn                      Write Function to be used for upsert
   * @param assertForCommit              Enable Assertion of Writes
   * @param expRecordsInThisCommit       Expected number of records in this commit
   * @param expTotalRecords              Expected number of records when scanned
   * @param expTotalCommits              Expected number of commits (including this commit)
   * @throws Exception in case of error
   */
  public JavaRDD<WriteStatus> writeBatch(SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
                                         Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                         Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                         Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                         boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                         boolean filterForCommitTimeWithAssert, InstantGenerator instantGenerator,
                                         boolean skipCommit) throws Exception {

    List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, numRecordsInThisCommit);
    return writeBatchHelper(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime,
        numRecordsInThisCommit, records, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords,
        expTotalCommits, filterForCommitTimeWithAssert, instantGenerator, skipCommit);
  }

  public JavaRDD<WriteStatus> writeBatch(SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
                                         Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                         Function3<List<HoodieRecord>, String, Integer, String> recordGenFunction,
                                         Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                         boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                         boolean filterForCommitTimeWithAssert,
                                         String partition,
                                         InstantGenerator instantGenerator) throws Exception {

    List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, numRecordsInThisCommit, partition);
    return writeBatchHelper(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime,
        numRecordsInThisCommit, records, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords,
        expTotalCommits, filterForCommitTimeWithAssert, instantGenerator);
  }

  private JavaRDD<WriteStatus> writeBatchHelper(SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
                                                Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime,
                                                int numRecordsInThisCommit, List<HoodieRecord> records,
                                                Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                                boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords,
                                                int expTotalCommits, boolean filterForCommitTimeWithAssert,
                                                InstantGenerator instantGenerator) throws IOException {
    return writeBatchHelper(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, records, writeFn,
        assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, filterForCommitTimeWithAssert,
        instantGenerator, false);
  }

  private JavaRDD<WriteStatus> writeBatchHelper(SparkRDDWriteClient client, String newCommitTime, String prevCommitTime,
                                                Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime,
                                                int numRecordsInThisCommit, List<HoodieRecord> records,
                                                Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
                                                boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords,
                                                int expTotalCommits, boolean filterForCommitTimeWithAssert,
                                                InstantGenerator instantGenerator, boolean skipCommit) throws IOException {
    // Write 1 (only inserts)
    WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    List<WriteStatus> statusList = writeFn.apply(client, writeRecords, newCommitTime).collect();
    JavaRDD<WriteStatus> result = jsc.parallelize(statusList, 1);
    assertNoWriteErrors(statusList);
    // validate isMetadataTable() in write status
    statusList.forEach(writeStatus -> assertFalse(writeStatus.isMetadataTable()));

    if (!skipCommit) {
      client.commit(newCommitTime, result);
    }
    // check the partition metadata is written out
    assertPartitionMetadataForRecords(basePath, records, storage);

    // verify that there is a commit
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, basePath);
    HoodieTimeline timeline = metaClient.getCommitsTimeline();

    if (assertForCommit) {
      assertEquals(expTotalCommits, timeline.findInstantsAfter(initCommitTime, Integer.MAX_VALUE).countInstants(),
          "Expecting " + expTotalCommits + " commits.");
      assertEquals(newCommitTime, timeline.lastInstant().get().requestedTime(),
          "Latest commit should be " + newCommitTime);
      if (filterForCommitTimeWithAssert) { // when meta cols are disabled, we can't really do per commit assertion.
        assertEquals(expRecordsInThisCommit,
            HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime, true, instantGenerator).count(),
            "Must contain " + expRecordsInThisCommit + " records");
      }

      // Check the entire dataset has all records still
      String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
      for (int i = 0; i < fullPartitionPaths.length; i++) {
        fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
      }
      assertEquals(expTotalRecords,
          HoodieClientTestUtils.read(jsc, basePath, sqlContext, storage, fullPartitionPaths)
              .count(),
          "Must contain " + expTotalRecords + " records");

      if (filterForCommitTimeWithAssert) {
        // Check that the incremental consumption from prevCommitTime
        assertEquals(
            HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime, true, instantGenerator).count(),
            HoodieClientTestUtils.countRecordsOptionallySince(jsc, basePath, sqlContext, timeline,
                Option.of(prevCommitTime)),
            "Incremental consumption from " + prevCommitTime
                + " should give all records in latest commit");
        if (commitTimesBetweenPrevAndNew.isPresent()) {
          commitTimesBetweenPrevAndNew.get().forEach(ct -> {
            assertEquals(
                HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime, true, instantGenerator)
                    .count(),
                HoodieClientTestUtils.countRecordsOptionallySince(jsc, basePath, sqlContext, timeline, Option.of(ct)),
                "Incremental consumption from " + ct + " should give all records in latest commit");
          });
        }
      }
    }
    return result;
  }

  private JavaRDD<WriteStatus> getWriteStatusAndVerifyDeleteOperation(String newCommitTime, String prevCommitTime, String initCommitTime, boolean assertForCommit, int expRecordsInThisCommit,
                                                                      int expTotalRecords, boolean filerForCommitTimeWithAssert, JavaRDD<WriteStatus> result,
                                                                      TimelineFactory timelineFactory, InstantGenerator instantGenerator,
                                                                      SparkRDDWriteClient client) {

    client.commit(newCommitTime, result);

    // verify that there is a commit
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, basePath);
    HoodieTimeline timeline = timelineFactory.createActiveTimeline(metaClient).getCommitAndReplaceTimeline();

    if (assertForCommit) {
      assertEquals(3, timeline.findInstantsAfter(initCommitTime, Integer.MAX_VALUE).countInstants(),
          "Expecting 3 commits.");
      assertEquals(newCommitTime, timeline.lastInstant().get().requestedTime(),
          "Latest commit should be " + newCommitTime);
      if (filerForCommitTimeWithAssert) { // if meta cols are disabled, we can't do assertion based on assertion time
        assertEquals(expRecordsInThisCommit,
            HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime, true, instantGenerator).count(),
            "Must contain " + expRecordsInThisCommit + " records");
      }

      // Check the entire dataset has all records still
      String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
      for (int i = 0; i < fullPartitionPaths.length; i++) {
        fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
      }
      assertEquals(expTotalRecords,
          HoodieClientTestUtils.read(jsc, basePath, sqlContext, storage, fullPartitionPaths)
              .count(),
          "Must contain " + expTotalRecords + " records");

      if (filerForCommitTimeWithAssert) {
        // Check that the incremental consumption from prevCommitTime
        assertEquals(
            HoodieClientTestUtils.readCommit(basePath, sqlContext, timeline, newCommitTime, true, instantGenerator).count(),
            HoodieClientTestUtils.countRecordsOptionallySince(jsc, basePath, sqlContext, timeline,
                Option.of(prevCommitTime)),
            "Incremental consumption from " + prevCommitTime
                + " should give no records in latest commit, since it is a delete operation");
      }
    }
    return result;
  }

  /**
   * Insert a batch of records without commit(so that the instant is in-flight).
   *
   * @param newCommitTime The commit time
   * @param numRecords    The number of records to insert
   */
  @SuppressWarnings("rawtypes, unchecked")
  protected void insertBatchWithoutCommit(String newCommitTime, int numRecords) {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .build();

    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      List<WriteStatus> statuses = client.insert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);
    }
  }

  /**
   * Update a batch of records without commit(so that the instant is in-flight).
   *
   * @param newCommitTime The commit time
   * @param baseRecordsToUpdate The base records to update
   */
  @SuppressWarnings("rawtypes, unchecked")
  protected void updateBatchWithoutCommit(String newCommitTime, List<HoodieRecord> baseRecordsToUpdate) throws IOException {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withHeartbeatTolerableMisses(0)
        .build();

    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, baseRecordsToUpdate);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
      client.upsert(writeRecords, newCommitTime).collect();
    }
  }

  /**
   * Asserts the row number in data generator equals to {@code numRows}.
   *
   * @param numRows The expected row number
   */
  protected void assertRowNumberEqualsTo(int numRows) {
    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(numRows,
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, storage, fullPartitionPaths).count(),
        "Must contain " + numRows + " records");
  }

  /**
   * Get Cleaner state corresponding to a partition path.
   *
   * @param hoodieCleanStatsTwo List of Clean Stats
   * @param partitionPath Partition path for filtering
   * @return Cleaner state corresponding to partition path
   */
  public HoodieCleanStat getCleanStat(List<HoodieCleanStat> hoodieCleanStatsTwo, String partitionPath) {
    return hoodieCleanStatsTwo.stream().filter(e -> e.getPartitionPath().equals(partitionPath)).findFirst().orElse(null);
  }

  public static FileFormatUtils getFileUtilsInstance(HoodieTableMetaClient metaClient) {
    return HoodieIOFactory.getIOFactory(metaClient.getStorage())
        .getFileFormatUtils(metaClient.getTableConfig().getBaseFileFormat());
  }
}
