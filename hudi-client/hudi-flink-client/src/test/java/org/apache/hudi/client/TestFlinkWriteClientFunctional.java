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

package org.apache.hudi.client;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.clustering.plan.strategy.FlinkSizeBasedClusteringPlanStrategyRecently;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.FlinkCreateHandle;
import org.apache.hudi.io.FlinkMergeHandle;
import org.apache.hudi.io.FlinkWriteHandleFactory;
import org.apache.hudi.io.HoodieWriteMergeHandle;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.testutils.HoodieFlinkClientTestHarness;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Functional coverage for the Flink client write boundary.
 *
 * <p>The datasource bucket assigner hands this client records that already carry a target file group.
 * These tests construct that same input directly so client and handle behavior can be exercised without
 * depending on the datasource module.
 */
class TestFlinkWriteClientFunctional extends HoodieFlinkClientTestHarness {

  private static final String PARTITION_PATH = "2026/07/23";
  private static final String FILE_ID = "f0";
  private static final String SCHEMA = "{"
      + "\"type\":\"record\","
      + "\"name\":\"flink_write_test\","
      + "\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\"},"
      + "{\"name\":\"name\",\"type\":\"string\"},"
      + "{\"name\":\"ts\",\"type\":\"long\"}"
      + "]}";

  private HoodieWriteConfig writeConfig;

  @BeforeEach
  void setUp() {
    initPath();
    initFileSystem();
  }

  @AfterEach
  void tearDown() throws IOException {
    cleanupResources();
  }

  static Stream<Arguments> tableTypesAndCdc() {
    return Stream.of(
        Arguments.of(HoodieTableType.COPY_ON_WRITE, false),
        Arguments.of(HoodieTableType.COPY_ON_WRITE, true),
        Arguments.of(HoodieTableType.MERGE_ON_READ, false),
        Arguments.of(HoodieTableType.MERGE_ON_READ, true));
  }

  @ParameterizedTest
  @MethodSource("tableTypesAndCdc")
  void testInsertAndUpsertWriteFilesAndCommitMetadata(HoodieTableType tableType, boolean cdcEnabled)
      throws IOException {
    initWriteClient(tableType, cdcEnabled, false);

    String insertInstant = writeClient.startCommit();
    transitionToInflight(insertInstant);
    List<HoodieRecord> firstBatch = new ArrayList<>(Arrays.asList(
        insertRecord("id1", "one", 1L),
        insertRecord("id2", "two", 2L)));
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      firstBatch.add(insertRecord("id3", "three", 3L));
    }
    List<WriteStatus> firstInsertStatuses = writeClient.insert(firstBatch, insertInstant);
    assertWriteStatuses(firstInsertStatuses, firstBatch.size());

    // A second COW mini-batch for the same bucket exercises the incremental/replace handle.
    List<WriteStatus> insertStatuses = tableType == HoodieTableType.COPY_ON_WRITE
        ? writeClient.insert(Arrays.asList(insertRecord("id3", "three", 3L)), insertInstant)
        : firstInsertStatuses;
    if (tableType == HoodieTableType.COPY_ON_WRITE && cdcEnabled) {
      insertStatuses = writeClient.upsert(
          Arrays.asList(updateRecord("id1", "one-mini-batch-update", 4L, insertInstant)),
          insertInstant);
    }
    assertWriteStatuses(insertStatuses, 3);
    assertTrue(writeClient.commit(insertInstant, insertStatuses));
    assertCommitMetadata(insertInstant, tableType, 3);

    writeClient.cleanHandles();
    String updateInstant = writeClient.startCommit();
    transitionToInflight(updateInstant);
    List<HoodieRecord> updates = Arrays.asList(
        updateRecord("id1", "one-updated", 11L, insertInstant),
        deleteRecord("id2", 12L, insertInstant));
    List<WriteStatus> updateStatuses = writeClient.upsert(updates, updateInstant);
    long expectedWrites = tableType == HoodieTableType.COPY_ON_WRITE ? 2 : 1;
    assertWriteStatuses(updateStatuses, expectedWrites);
    assertEquals(1,
        updateStatuses.stream().map(WriteStatus::getStat).mapToLong(stat -> stat.getNumDeletes()).sum());
    assertTrue(writeClient.commit(updateInstant, updateStatuses));
    assertCommitMetadata(updateInstant, tableType, expectedWrites);

    if (tableType == HoodieTableType.COPY_ON_WRITE && cdcEnabled) {
      assertTrue(updateStatuses.stream()
          .map(WriteStatus::getStat)
          .anyMatch(stat -> stat.getCdcStats() != null && !stat.getCdcStats().isEmpty()));
    }
  }

  @Test
  void testCopyOnWriteCleansRetryFiles() throws IOException {
    context = new HoodieFlinkEngineContext(
        new HoodieFlinkEngineContext.DefaultTaskContextSupplier() {
          @Override
          public Supplier<Long> getAttemptIdSupplier() {
            return () -> 1L;
          }
        });
    initWriteClient(HoodieTableType.COPY_ON_WRITE, false, false);

    String insertInstant = writeClient.startCommit();
    transitionToInflight(insertInstant);
    StoragePath staleInsertPath = createInvalidRetryFile(insertInstant);
    List<WriteStatus> insertStatuses = writeClient.insert(
        Collections.singletonList(insertRecord("id1", "one", 1L)), insertInstant);
    assertWriteStatuses(insertStatuses, 1);
    assertFalse(metaClient.getStorage().exists(staleInsertPath));
    assertTrue(writeClient.commit(insertInstant, insertStatuses));

    writeClient.cleanHandles();
    String updateInstant = writeClient.startCommit();
    transitionToInflight(updateInstant);
    StoragePath staleUpdatePath = createInvalidRetryFile(updateInstant);
    List<WriteStatus> updateStatuses = writeClient.upsert(
        Collections.singletonList(updateRecord("id1", "one-updated", 2L, insertInstant)),
        updateInstant);
    assertWriteStatuses(updateStatuses, 1);
    assertFalse(metaClient.getStorage().exists(staleUpdatePath));
    assertTrue(writeClient.commit(updateInstant, updateStatuses));
  }

  @Test
  void testCopyOnWriteHandleRolloverAndGracefulCloseCleanup() throws IOException {
    initWriteClient(HoodieTableType.COPY_ON_WRITE, false, false);

    String insertInstant = writeClient.startCommit();
    transitionToInflight(insertInstant);
    writeClient.insert(Arrays.asList(
        insertRecord("id1", "one", 1L),
        insertRecord("id2", "two", 2L)), insertInstant);
    List<WriteStatus> insertStatuses = writeClient.insert(
        Collections.singletonList(insertRecord("id3", "three", 3L)), insertInstant);
    assertWriteStatuses(insertStatuses, 3);

    HoodieFlinkTable table = writeClient.getHoodieTable();
    FlinkCreateHandle rolloverHandle = new FlinkCreateHandle(
        writeConfig, insertInstant, table, PARTITION_PATH, FILE_ID, table.getTaskContextSupplier());
    assertTrue(rolloverHandle.canWrite(insertRecord("id4", "four", 4L)));
    assertNotEquals(insertStatuses.get(0).getStat().getPath(), rolloverHandle.getWritePath().toString());
    rolloverHandle.closeGracefully();
    // Closing gracefully is intentionally idempotent.
    rolloverHandle.closeGracefully();

    FlinkCreateHandle failingHandle = new FlinkCreateHandle(
        writeConfig, insertInstant, table, PARTITION_PATH, FILE_ID, table.getTaskContextSupplier()) {
      @Override
      public List<WriteStatus> close() {
        super.close();
        throw new IllegalStateException("expected close failure");
      }
    };
    StoragePath failedCreatePath = failingHandle.getWritePath();
    failingHandle.closeGracefully();
    assertFalse(metaClient.getStorage().exists(failedCreatePath));
    assertTrue(writeClient.commit(insertInstant, insertStatuses));

    String mergeInstant = writeClient.startCommit();
    transitionToInflight(mergeInstant);
    table = writeClient.getHoodieTable();
    FlinkMergeHandle failingMergeHandle = new FlinkMergeHandle(
        writeConfig,
        mergeInstant,
        table,
        Collections.<HoodieRecord>emptyList().iterator(),
        PARTITION_PATH,
        FILE_ID,
        table.getTaskContextSupplier()) {
      @Override
      public List<WriteStatus> close() {
        super.close();
        throw new IllegalStateException("expected close failure");
      }
    };
    StoragePath failedMergePath = failingMergeHandle.getWritePath();
    failingMergeHandle.closeGracefully();
    assertFalse(metaClient.getStorage().exists(failedMergePath));
  }

  @Test
  void testScheduleClusteringFromRecentlyWrittenPartition() throws IOException {
    initWriteClient(HoodieTableType.COPY_ON_WRITE, false, true);

    String insertInstant = writeClient.startCommit();
    transitionToInflight(insertInstant);
    List<WriteStatus> statuses = writeClient.insert(Arrays.asList(
        insertRecord("id1", "one", 1L),
        insertRecord("id2", "two", 2L)), insertInstant);
    assertTrue(writeClient.commit(insertInstant, statuses));

    Option<String> clusteringInstant = writeClient.scheduleClustering(Option.empty());
    assertTrue(clusteringInstant.isPresent());
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant requestedInstant = metaClient.getInstantGenerator()
        .getClusteringCommitRequestedInstant(clusteringInstant.get());
    HoodieClusteringPlan clusteringPlan = ClusteringUtils
        .getClusteringPlan(metaClient, requestedInstant).get().getRight();
    assertEquals(1, clusteringPlan.getInputGroups().size());
    assertEquals(PARTITION_PATH,
        clusteringPlan.getInputGroups().get(0).getSlices().get(0).getPartitionPath());
  }

  @Test
  void testPreppedWriteEntryPointsCommitMetadata() throws IOException {
    initWriteClient(HoodieTableType.COPY_ON_WRITE, false, false);

    String insertInstant = writeClient.startCommit();
    transitionToInflight(insertInstant);
    List<WriteStatus> insertStatuses = writeClient.insert(Arrays.asList(
        insertRecord("id1", "one", 1L),
        insertRecord("id2", "two", 2L)), insertInstant);
    assertTrue(writeClient.commit(insertInstant, insertStatuses));

    writeClient.cleanHandles();
    String upsertInstant = writeClient.startCommit();
    transitionToInflight(upsertInstant);
    List<WriteStatus> upsertStatuses = writeClient.upsertPreppedRecords(
        Collections.singletonList(updateRecord("id1", "one-prepped", 3L, insertInstant)),
        upsertInstant);
    assertWriteStatuses(upsertStatuses, 2);
    assertTrue(writeClient.commit(upsertInstant, upsertStatuses));
    assertCommitMetadata(upsertInstant, HoodieTableType.COPY_ON_WRITE, 2);

    writeClient.cleanHandles();
    String bulkInsertInstant = writeClient.startCommit();
    transitionToInflight(bulkInsertInstant);
    List<WriteStatus> bulkInsertStatuses = writeClient.bulkInsertPreppedRecords(
        Collections.singletonList(insertRecord("id3", "three", 4L)),
        bulkInsertInstant,
        Option.empty());
    assertWriteStatuses(bulkInsertStatuses, 1);
    assertTrue(writeClient.commit(bulkInsertInstant, bulkInsertStatuses));
    assertCommitMetadata(bulkInsertInstant, HoodieTableType.COPY_ON_WRITE, 1);
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void testClientRoutesOverwriteAndDeleteActionsAfterPriorCommit() throws IOException {
    initWriteClient(HoodieTableType.COPY_ON_WRITE, false, false);
    String insertInstant = writeClient.startCommit();
    transitionToInflight(insertInstant);
    List<WriteStatus> insertStatuses = writeClient.insert(
        Collections.singletonList(insertRecord("id1", "one", 1L)), insertInstant);
    assertTrue(writeClient.commit(insertInstant, insertStatuses));
    Map<String, List<String>> replacedFileIds =
        writeClient.getPartitionToReplacedFileIds(WriteOperationType.INSERT_OVERWRITE, insertStatuses);
    assertEquals(Collections.singleton(FILE_ID),
        new HashSet<>(replacedFileIds.get(PARTITION_PATH)));

    HoodieFlinkTable table = mock(HoodieFlinkTable.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    HoodieWriteMetadata<List<WriteStatus>> metadata = new HoodieWriteMetadata<>();
    metadata.setWriteStatuses(Collections.emptyList());
    when(table.insertOverwrite(any(), any(), any(), anyString(), any())).thenReturn(metadata);
    when(table.insertOverwriteTable(any(), any(), any(), anyString(), any())).thenReturn(metadata);
    when(table.delete(any(), anyString(), any())).thenReturn(metadata);
    when(table.deletePrepped(any(), anyString(), any())).thenReturn(metadata);
    when(table.deletePartitions(any(), anyString(), any())).thenReturn(metadata);

    HoodieFlinkWriteClient routingClient = new HoodieFlinkWriteClient(context, writeConfig) {
      @Override
      protected org.apache.hudi.table.HoodieTable createTable(
          HoodieWriteConfig config, HoodieTableMetaClient ignoredMetaClient) {
        return table;
      }
    };
    FlinkWriteHandleFactory.Factory handleFactory = mock(FlinkWriteHandleFactory.Factory.class);
    FlinkCreateHandle writeHandle = mock(FlinkCreateHandle.class);
    when(handleFactory.create(any(), any(), any(), anyString(), any(), any())).thenReturn(writeHandle);
    BucketInfo bucketInfo = new BucketInfo(BucketType.INSERT, FILE_ID, PARTITION_PATH);
    List<HoodieKey> deleteKeys = Collections.singletonList(new HoodieKey("id1", PARTITION_PATH));
    List<HoodieRecord> preppedDeletes = Collections.emptyList();
    List<String> partitions = Collections.singletonList(PARTITION_PATH);

    try (MockedStatic<FlinkWriteHandleFactory> factory = Mockito.mockStatic(FlinkWriteHandleFactory.class)) {
      factory.when(() -> FlinkWriteHandleFactory.getFactory(any(), any(), anyBoolean()))
          .thenReturn(handleFactory);
      assertTrue(routingClient.insertOverwrite(
          Collections.<HoodieRecord>emptyList().iterator(), bucketInfo, "001").isEmpty());
      assertTrue(routingClient.insertOverwriteTable(
          Collections.<HoodieRecord>emptyList().iterator(), bucketInfo, "002").isEmpty());
      assertTrue(routingClient.delete(deleteKeys, "003").isEmpty());
      assertTrue(routingClient.deletePrepped(preppedDeletes, "004").isEmpty());
      assertTrue(routingClient.deletePartitions(partitions, "005").isEmpty());

      verify(table).insertOverwrite(eq(context), eq(writeHandle), eq(bucketInfo), eq("001"), any());
      verify(table).insertOverwriteTable(eq(context), eq(writeHandle), eq(bucketInfo), eq("002"), any());
      verify(table).delete(eq(context), eq("003"), eq(deleteKeys));
      verify(table).deletePrepped(eq(context), eq("004"), eq(preppedDeletes));
      verify(table).deletePartitions(eq(context), eq("005"), eq(partitions));
    } finally {
      routingClient.close();
    }
  }

  private HoodieRecord insertRecord(String key, String name, long ts) {
    return record(key, name, ts, HoodieOperation.INSERT, "I");
  }

  private HoodieRecord updateRecord(String key, String name, long ts, String instantTime) {
    return record(key, name, ts, HoodieOperation.UPDATE_AFTER, instantTime);
  }

  private HoodieRecord deleteRecord(String key, long ts, String instantTime) {
    return record(key, "deleted", ts, HoodieOperation.DELETE, instantTime);
  }

  private HoodieRecord record(
      String key, String name, long ts, HoodieOperation operation, String locationInstant) {
    GenericRowData row = GenericRowData.of(
        StringData.fromString(key), StringData.fromString(name), ts);
    HoodieFlinkRecord record = new HoodieFlinkRecord(
        new HoodieKey(key, PARTITION_PATH), operation, ts, row);
    record.setCurrentLocation(new HoodieRecordLocation(locationInstant, FILE_ID));
    return record;
  }

  private void initWriteClient(
      HoodieTableType tableType, boolean cdcEnabled, boolean useRecentClusteringStrategy)
      throws IOException {
    Properties tableProperties = new Properties();
    tableProperties.setProperty(HoodieTableConfig.CDC_ENABLED.key(), Boolean.toString(cdcEnabled));
    tableProperties.setProperty(
        HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key(),
        HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER.name());
    tableProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "id");
    tableProperties.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    tableProperties.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), "ts");
    tableProperties.setProperty(
        HoodieWriteConfig.MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE.key(), "false");
    metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableName("flink_write_client_test")
        .setTableType(tableType)
        .fromProperties(tableProperties)
        .initTable(storageConf, basePath);

    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withEngineType(EngineType.FLINK)
        .withSchema(SCHEMA)
        .withProperties(tableProperties)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withMergeHandleClassName(HoodieWriteMergeHandle.class.getName());
    if (useRecentClusteringStrategy) {
      builder.withClusteringConfig(HoodieClusteringConfig.newBuilder()
          .withEngineType(EngineType.FLINK)
          .withClusteringPlanStrategyClass(FlinkSizeBasedClusteringPlanStrategyRecently.class.getName())
          .withClusteringPlanSmallFileLimit(Long.MAX_VALUE)
          .withClusteringMaxNumGroups(10)
          .withClusteringSortColumns("id")
          .build());
    }
    writeConfig = builder.build();
    writeClient = new HoodieFlinkWriteClient<>(context, writeConfig);
  }

  private void assertWriteStatuses(List<WriteStatus> statuses, long expectedRecords) {
    assertFalse(statuses.isEmpty());
    assertTrue(statuses.stream().noneMatch(WriteStatus::hasErrors));
    assertEquals(expectedRecords,
        statuses.stream().map(WriteStatus::getStat).mapToLong(stat -> stat.getNumWrites()).sum());
    statuses.forEach(status -> {
      assertEquals(PARTITION_PATH, status.getStat().getPartitionPath());
      assertNotNull(status.getStat().getPath());
    });
  }

  private void transitionToInflight(String instantTime) {
    metaClient.reloadActiveTimeline();
    metaClient.getActiveTimeline().transitionRequestedToInflight(
        metaClient.getCommitActionType(), instantTime);
  }

  private StoragePath createInvalidRetryFile(String instantTime) throws IOException {
    StoragePath partitionPath = new StoragePath(metaClient.getBasePath(), PARTITION_PATH);
    metaClient.getStorage().createDirectory(partitionPath);
    String fileName = FSUtils.makeBaseFileName(
        instantTime,
        FSUtils.makeWriteToken(0, 1, 0),
        FILE_ID,
        metaClient.getTableConfig().getBaseFileFormat().getFileExtension());
    StoragePath retryPath = new StoragePath(partitionPath, fileName);
    metaClient.getStorage().create(retryPath).close();
    return retryPath;
  }

  private void assertCommitMetadata(String instantTime, HoodieTableType tableType, long expectedRecords)
      throws IOException {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    String action = metaClient.getCommitActionType();
    HoodieInstant instant = metaClient.getActiveTimeline()
        .getTimelineOfActions(Collections.singleton(action))
        .filterCompletedInstants()
        .getInstantsAsStream()
        .filter(candidate -> candidate.requestedTime().equals(instantTime))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing completed instant " + instantTime));
    assertEquals(expectedRecords,
        metaClient.getActiveTimeline().readCommitMetadata(instant).fetchTotalRecordsWritten());
  }
}
