/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client.functional;

import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.execution.bulkinsert.NonSortPartitioner;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getCommitTimeAtUTC;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.testutils.HoodieClientTestBase.wrapRecordsGenFunctionForPreppedCalls;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHoodieClientOnLsmStorage extends HoodieClientTestBase {

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testInsert(HoodieTableType tableType) throws IOException {
    LsmTableTestContext testContext = createTestContext(tableType);
    try (SparkRDDWriteClient client = getHoodieWriteClient(testContext.writeConfig)) {
      String instantTime = getCommitTimeAtUTC(1);
      List<HoodieRecord> records = generateInserts(testContext.dataGenerator, instantTime);
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      commitWrite(client, instantTime, client.insert(jsc.parallelize(records, 2), instantTime));
      assertCompletedOperation(testContext.metaClient, instantTime, WriteOperationType.INSERT);
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testInsertPrepped(HoodieTableType tableType) throws IOException {
    LsmTableTestContext testContext = createTestContext(tableType);
    try (SparkRDDWriteClient client = getHoodieWriteClient(testContext.writeConfig)) {
      String instantTime = getCommitTimeAtUTC(1);
      List<HoodieRecord> records = wrapRecordsGenFunctionForPreppedCalls(
          testContext.tablePath, storageConf, context, testContext.writeConfig,
          testContext.dataGenerator::generateInserts).apply(instantTime, 4);
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      commitWrite(client, instantTime, client.insertPreppedRecords(jsc.parallelize(records, 2), instantTime));
      assertCompletedOperation(testContext.metaClient, instantTime, WriteOperationType.INSERT_PREPPED);
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testBulkInsert(HoodieTableType tableType) throws IOException {
    LsmTableTestContext testContext = createTestContext(tableType);
    try (SparkRDDWriteClient client = getHoodieWriteClient(testContext.writeConfig)) {
      String instantTime = getCommitTimeAtUTC(1);
      List<HoodieRecord> records = generateInserts(testContext.dataGenerator, instantTime);
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      commitWrite(client, instantTime, client.bulkInsert(jsc.parallelize(records, 2), instantTime));
      assertCompletedOperation(testContext.metaClient, instantTime, WriteOperationType.BULK_INSERT);
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testBulkInsertPrepped(HoodieTableType tableType) throws IOException {
    LsmTableTestContext testContext = createTestContext(tableType);
    try (SparkRDDWriteClient client = getHoodieWriteClient(testContext.writeConfig)) {
      String instantTime = getCommitTimeAtUTC(1);
      List<HoodieRecord> records = wrapRecordsGenFunctionForPreppedCalls(
          testContext.tablePath, storageConf, context, testContext.writeConfig,
          testContext.dataGenerator::generateInserts).apply(instantTime, 4);
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      commitWrite(client, instantTime, client.bulkInsertPreppedRecords(
          jsc.parallelize(records, 2), instantTime, Option.empty()));
      assertCompletedOperation(
          testContext.metaClient, instantTime, WriteOperationType.BULK_INSERT_PREPPED);
    }
  }

  @Test
  void testRejectsCustomBulkInsertPartitionerBeforeInflight() throws IOException {
    LsmTableTestContext testContext = createTestContext(HoodieTableType.COPY_ON_WRITE);
    try (SparkRDDWriteClient client = getHoodieWriteClient(testContext.writeConfig)) {
      String instantTime = getCommitTimeAtUTC(1);
      List<HoodieRecord> records = generateInserts(testContext.dataGenerator, instantTime);
      WriteClientTestUtils.startCommitWithTime(client, instantTime);

      HoodieInsertException exception = assertThrows(HoodieInsertException.class, () -> client.bulkInsert(
          jsc.parallelize(records, 2), instantTime, Option.of(new NonSortPartitioner<>())));
      assertTrue(exception.getCause() instanceof IllegalArgumentException);
      assertEquals(
          "User-defined bulk insert partitioners are not supported for LSM tables because their record-key ordering cannot be verified",
          exception.getCause().getMessage());

      HoodieInstant instant = testContext.metaClient.reloadActiveTimeline().getInstants().stream()
          .filter(candidate -> candidate.requestedTime().equals(instantTime))
          .findFirst()
          .orElseThrow(() -> new AssertionError("No instant " + instantTime));
      assertEquals(HoodieInstant.State.REQUESTED, instant.getState());
    }
  }

  @Test
  void testRejectsCustomClusteringSortBeforePlanGeneration() throws IOException {
    LsmTableTestContext testContext = createTestContext(HoodieTableType.COPY_ON_WRITE);
    testContext.writeConfig.setValue(
        HoodieClusteringConfig.PLAN_STRATEGY_SINGLE_GROUP_CLUSTERING_ENABLED, "true");
    testContext.writeConfig.setValue(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS, "begin_lat");

    try (SparkRDDWriteClient client = getHoodieWriteClient(testContext.writeConfig)) {
      bootstrapTable(testContext, client);
      assertThrows(HoodieClusteringException.class, () -> client.scheduleClustering(Option.empty()));
      assertTrue(testContext.metaClient.reloadActiveTimeline().filterPendingClusteringTimeline().empty());
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testUpsert(HoodieTableType tableType) throws IOException {
    LsmTableTestContext testContext = createTestContext(tableType);
    try (SparkRDDWriteClient client = getHoodieWriteClient(testContext.writeConfig)) {
      bootstrapTable(testContext, client);
      String instantTime = getCommitTimeAtUTC(2);
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      commitWrite(client, instantTime, client.upsert(jsc.parallelize(
          testContext.dataGenerator.generateUniqueUpdates(instantTime, 4), 2), instantTime));
      assertCompletedOperation(testContext.metaClient, instantTime, WriteOperationType.UPSERT);
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testUpsertPrepped(HoodieTableType tableType) throws IOException {
    LsmTableTestContext testContext = createTestContext(tableType);
    try (SparkRDDWriteClient client = getHoodieWriteClient(testContext.writeConfig)) {
      bootstrapTable(testContext, client);
      String instantTime = getCommitTimeAtUTC(2);
      List<HoodieRecord> records = wrapRecordsGenFunctionForPreppedCalls(
          testContext.tablePath, storageConf, context, testContext.writeConfig,
          testContext.dataGenerator::generateUniqueUpdates).apply(instantTime, 4);
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      commitWrite(client, instantTime, client.upsertPreppedRecords(jsc.parallelize(records, 2), instantTime));
      assertCompletedOperation(testContext.metaClient, instantTime, WriteOperationType.UPSERT_PREPPED);
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testDelete(HoodieTableType tableType) throws IOException {
    LsmTableTestContext testContext = createTestContext(tableType);
    try (SparkRDDWriteClient client = getHoodieWriteClient(testContext.writeConfig)) {
      bootstrapTable(testContext, client);
      String instantTime = getCommitTimeAtUTC(2);
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      commitWrite(client, instantTime, client.delete(
          jsc.parallelize(testContext.dataGenerator.generateUniqueDeletes(2), 2), instantTime));
      assertCompletedOperation(testContext.metaClient, instantTime, WriteOperationType.DELETE);
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testDeletePrepped(HoodieTableType tableType) throws IOException {
    LsmTableTestContext testContext = createTestContext(tableType);
    try (SparkRDDWriteClient client = getHoodieWriteClient(testContext.writeConfig)) {
      bootstrapTable(testContext, client);
      String instantTime = getCommitTimeAtUTC(2);
      List<HoodieRecord> records = wrapRecordsGenFunctionForPreppedCalls(
          testContext.tablePath, storageConf, context, testContext.writeConfig,
          testContext.dataGenerator::generateUniqueDeleteRecords).apply(instantTime, 2);
      WriteClientTestUtils.startCommitWithTime(client, instantTime);
      commitWrite(client, instantTime, client.deletePrepped(jsc.parallelize(records, 2), instantTime));
      assertCompletedOperation(testContext.metaClient, instantTime, WriteOperationType.DELETE_PREPPED);
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testInsertOverwrite(HoodieTableType tableType) throws IOException {
    LsmTableTestContext testContext = createTestContext(tableType);
    try (SparkRDDWriteClient client = getHoodieWriteClient(testContext.writeConfig)) {
      bootstrapTable(testContext, client);
      String instantTime = getCommitTimeAtUTC(2);
      List<HoodieRecord> records = testContext.dataGenerator.generateInsertsForPartition(
          instantTime, 3, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
      WriteClientTestUtils.startCommitWithTime(client, instantTime, HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieWriteResult result = client.insertOverwrite(jsc.parallelize(records, 1), instantTime);
      commitReplace(client, instantTime, result);
      assertReplaceCommit(
          testContext.metaClient, instantTime, WriteOperationType.INSERT_OVERWRITE,
          result.getPartitionToReplaceFileIds(), HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testInsertOverwriteTable(HoodieTableType tableType) throws IOException {
    LsmTableTestContext testContext = createTestContext(tableType);
    try (SparkRDDWriteClient client = getHoodieWriteClient(testContext.writeConfig)) {
      bootstrapTable(testContext, client);
      String instantTime = getCommitTimeAtUTC(2);
      List<HoodieRecord> records = testContext.dataGenerator.generateInsertsForPartition(
          instantTime, 3, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH);
      WriteClientTestUtils.startCommitWithTime(client, instantTime, HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieWriteResult result = client.insertOverwriteTable(jsc.parallelize(records, 1), instantTime);
      commitReplace(client, instantTime, result);
      assertReplaceCommit(
          testContext.metaClient, instantTime, WriteOperationType.INSERT_OVERWRITE_TABLE,
          result.getPartitionToReplaceFileIds(), HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH);
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testDeletePartition(HoodieTableType tableType) throws IOException {
    LsmTableTestContext testContext = createTestContext(tableType);
    try (SparkRDDWriteClient client = getHoodieWriteClient(testContext.writeConfig)) {
      bootstrapTable(testContext, client);
      String instantTime = getCommitTimeAtUTC(2);
      WriteClientTestUtils.startCommitWithTime(client, instantTime, HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieWriteResult result = client.deletePartitions(
          Collections.singletonList(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH), instantTime);
      commitReplace(client, instantTime, result);
      assertReplaceCommit(
          testContext.metaClient, instantTime, WriteOperationType.DELETE_PARTITION,
          result.getPartitionToReplaceFileIds(), HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH);
    }
  }

  private LsmTableTestContext createTestContext(HoodieTableType tableType) throws IOException {
    String tablePath = basePath + "_" + tableType.name().toLowerCase() + "_lsm";
    Properties tableProperties = getPropertiesForKeyGen(true);
    tableProperties.setProperty(
        HoodieTableConfig.TABLE_STORAGE_LAYOUT.key(),
        HoodieTableConfig.TableStorageLayout.LSM_TREE.configValue());
    HoodieTableMetaClient lsmMetaClient = HoodieTestUtils.init(storageConf, tablePath, tableType, tableProperties);
    assertEquals(
        HoodieTableConfig.TableStorageLayout.LSM_TREE,
        lsmMetaClient.getTableConfig().getTableStorageLayout());

    Properties writeProperties = new Properties();
    writeProperties.setProperty(
        HoodieTableConfig.TABLE_STORAGE_LAYOUT.key(),
        HoodieTableConfig.TableStorageLayout.LSM_TREE.configValue());
    writeProperties.setProperty(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet");
    HoodieWriteConfig writeConfig = getConfigBuilder()
        .withPath(tablePath)
        .withEmbeddedTimelineServerEnabled(false)
        .withProperties(writeProperties)
        .build();
    return new LsmTableTestContext(
        tablePath, lsmMetaClient, writeConfig, new HoodieTestDataGenerator(0x19437));
  }

  private void bootstrapTable(LsmTableTestContext testContext, SparkRDDWriteClient client) {
    String instantTime = getCommitTimeAtUTC(1);
    List<HoodieRecord> records = generateInserts(testContext.dataGenerator, instantTime);
    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    commitWrite(client, instantTime, client.insert(jsc.parallelize(records, 2), instantTime));
  }

  private List<HoodieRecord> generateInserts(HoodieTestDataGenerator dataGenerator, String instantTime) {
    List<HoodieRecord> records = new ArrayList<>();
    records.addAll(dataGenerator.generateInsertsForPartition(
        instantTime, 6, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH));
    records.addAll(dataGenerator.generateInsertsForPartition(
        instantTime, 6, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH));
    return records;
  }

  private void commitWrite(SparkRDDWriteClient client, String instantTime, JavaRDD<WriteStatus> writeStatuses) {
    assertNoWriteErrors(writeStatuses.collect());
    assertTrue(client.commit(instantTime, writeStatuses));
  }

  private void commitReplace(SparkRDDWriteClient client, String instantTime, HoodieWriteResult writeResult) {
    assertNoWriteErrors(writeResult.getWriteStatuses().collect());
    assertTrue(client.commit(
        instantTime,
        writeResult.getWriteStatuses(),
        Option.empty(),
        HoodieTimeline.REPLACE_COMMIT_ACTION,
        writeResult.getPartitionToReplaceFileIds()));
  }

  private void assertCompletedOperation(
      HoodieTableMetaClient metaClient, String instantTime, WriteOperationType operationType) throws IOException {
    HoodieInstant instant = findCompletedInstant(metaClient, instantTime);
    HoodieCommitMetadata commitMetadata = metaClient.getActiveTimeline().readCommitMetadata(instant);
    assertEquals(operationType, commitMetadata.getOperationType());
  }

  private void assertReplaceCommit(
      HoodieTableMetaClient metaClient,
      String instantTime,
      WriteOperationType operationType,
      Map<String, List<String>> expectedReplacedFileIds,
      String expectedPartition) throws IOException {
    HoodieInstant instant = findCompletedInstant(metaClient, instantTime);
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, instant.getAction());
    HoodieReplaceCommitMetadata commitMetadata = metaClient.getActiveTimeline().readReplaceCommitMetadata(instant);
    assertEquals(operationType, commitMetadata.getOperationType());
    assertEquals(expectedReplacedFileIds, commitMetadata.getPartitionToReplaceFileIds());
    assertTrue(commitMetadata.getPartitionToReplaceFileIds().containsKey(expectedPartition));
    assertFalse(commitMetadata.getPartitionToReplaceFileIds().get(expectedPartition).isEmpty());
  }

  private HoodieInstant findCompletedInstant(HoodieTableMetaClient metaClient, String instantTime) {
    return metaClient.reloadActiveTimeline().filterCompletedInstants().getInstants().stream()
        .filter(instant -> instant.requestedTime().equals(instantTime))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No completed instant " + instantTime));
  }

  private static class LsmTableTestContext {
    private final String tablePath;
    private final HoodieTableMetaClient metaClient;
    private final HoodieWriteConfig writeConfig;
    private final HoodieTestDataGenerator dataGenerator;

    private LsmTableTestContext(
        String tablePath,
        HoodieTableMetaClient metaClient,
        HoodieWriteConfig writeConfig,
        HoodieTestDataGenerator dataGenerator) {
      this.tablePath = tablePath;
      this.metaClient = metaClient;
      this.writeConfig = writeConfig;
      this.dataGenerator = dataGenerator;
    }
  }
}
