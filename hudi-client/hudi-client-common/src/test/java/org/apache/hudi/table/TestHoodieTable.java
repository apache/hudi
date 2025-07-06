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

package org.apache.hudi.table;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.HoodiePendingRollbackInfo;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;
import org.apache.hudi.table.storage.HoodieStorageLayout;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieTable extends HoodieCommonTestHarness {
  @ParameterizedTest
  @MethodSource("generateParametersForValidateTimestampInternal")
  void testValidateForLatestTimestampInterval(
      boolean shouldEnableTimestampOrderingValidation,
      boolean supportsOcc,
      boolean shouldValidateForLatestTimestamp
  ) throws IOException {
    initMetaClient();
    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.MEMORY).build())
        .withEnableTimestampOrderingValidation(shouldEnableTimestampOrderingValidation);
    if (supportsOcc) {
      writeConfigBuilder.withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL);
    }

    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTable hoodieTable = new TestBaseHoodieTable(writeConfigBuilder.build(), context, metaClient);

    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient);
    HoodieInstant instant1 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "002");
    timeline.createNewInstant(instant1);
    if (shouldValidateForLatestTimestamp) {
      HoodieInstant lastEntry = metaClient.getActiveTimeline().getWriteTimeline().lastInstant().get();
      IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> hoodieTable.validateForLatestTimestampInternal("001"));
      assertEquals(String.format("Found later commit time %s, compared to the current instant 001, hence failing to create requested commit meta file", lastEntry), exception.getMessage());
    } else {
      assertDoesNotThrow(() -> hoodieTable.validateForLatestTimestampInternal("001"));
    }
  }

  private static Stream<Arguments> generateParametersForValidateTimestampInternal() {
    return Stream.of(
      Arguments.of(true, true, true),
      Arguments.of(true, false, false),
      Arguments.of(false, false, false)
    );
  }

  @Test
  void getIndexReturnsCachedInstance() throws IOException {
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .build();
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTable hoodieTable = new TestBaseHoodieTable(writeConfig, context, metaClient);

    HoodieIndex<?, ?> index = hoodieTable.getIndex();
    assertSame(index, hoodieTable.getIndex());
  }

  @Test
  void getStorageLayoutReturnsCachedInstance() throws IOException {
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .build();
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTable hoodieTable = new TestBaseHoodieTable(writeConfig, context, metaClient);

    HoodieStorageLayout storageLayout = hoodieTable.getStorageLayout();
    assertSame(storageLayout, hoodieTable.getStorageLayout());
  }

  @Test
  void testGetEngineContext() throws IOException {
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .build();
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTable hoodieTable = new TestBaseHoodieTable(writeConfig, context, metaClient);

    // before serialization, context is the same one that is passed in
    assertSame(context, hoodieTable.getContext());
    // after serialization, we expect a local context to be made
    HoodieTable deserializedTable = SerializationUtils.deserialize(SerializationUtils.serialize(hoodieTable));
    assertTrue(deserializedTable.getContext() instanceof HoodieLocalEngineContext);
  }

  @Test
  void testRollbackInflightInstant() throws IOException {
    // Setup.
    initMetaClient();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .build();
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTable hoodieTable =
        new TestBaseHoodieTable(writeConfig, context, metaClient);
    // Prepare test inputs.
    HoodieInstant inflightInstant = new HoodieInstant(
        HoodieInstant.State.INFLIGHT, COMPACTION_ACTION, "123");
    // Mock getPendingRollbackInstantFunc behavior.
    Function<String, Option<HoodiePendingRollbackInfo>>
        getPendingRollbackInstantFunc = mock(Function.class);
    HoodiePendingRollbackInfo pendingRollbackInfo = new HoodiePendingRollbackInfo(
        inflightInstant, new HoodieRollbackPlan());
    when(getPendingRollbackInstantFunc.apply("123"))
        .thenReturn(Option.of(pendingRollbackInfo));

    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    timeline.createNewInstant(inflightInstant);
    // Case 1: Execute the method with pending rollback instant.
    hoodieTable.rollbackInflightInstant(
        inflightInstant, getPendingRollbackInstantFunc);
    // Validate that function scheduleRollback is not called.
    assertEquals(0, ((TestBaseHoodieTable)hoodieTable).getCountOfScheduleRollbackFunctionCalls());

    // Reset the parameters.
    when(getPendingRollbackInstantFunc.apply("123"))
        .thenReturn(Option.empty());
    timeline.createNewInstant(inflightInstant);
    // Case 2: Execute the method without pending rollback instant.
    hoodieTable.rollbackInflightInstant(
        inflightInstant, getPendingRollbackInstantFunc);
    // Validate that function scheduleRollback is called.
    assertEquals(1, ((TestBaseHoodieTable)hoodieTable).getCountOfScheduleRollbackFunctionCalls());
  }

  @Test
  void testFailedDeletions() throws Exception {
    initMetaClient();
    boolean enablePostCommitMetadataValidation = true;
    boolean testPartitionedTable = true;
    // write some base files and corresponding markers
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    HoodieEngineContext context = new HoodieLocalEngineContext(HoodieTestUtils.getDefaultHadoopConf());
    String partitionA = testPartitionedTable ? "partA" : StringUtils.EMPTY_STRING;
    String partitionB = testPartitionedTable ? "partB" : StringUtils.EMPTY_STRING;
    String f0 = testTable.addRequestedCommit("001")
        .getFileIdsWithBaseFilesInPartitions(partitionA).get(partitionA);
    String f1 = testTable.addCommit("001")
        .getFileIdsWithBaseFilesInPartitions(partitionB).get(partitionB);

    testTable.forCommit("001")
        .withMarkerFile(partitionA, f0, IOType.CREATE)
        .withMarkerFile(partitionB, f1, IOType.CREATE);

    HoodieWriteConfig writerConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(false).build())
        .withMarkersType(MarkerType.DIRECT.name())
        .doValidateCommitMetadataConsistency(enablePostCommitMetadataValidation)
        .withEmbeddedTimelineServerEnabled(false)
        .withMetricsConfig(HoodieMetricsConfig.newBuilder().on(true).build()).build();

    HoodieTable hoodieTable = new TestBaseHoodieTable(writerConfig, context, metaClient);

    FileStatus[] baseFiles = testTable.listAllBaseFiles();

    List<HoodieWriteStat> stats = new ArrayList<>();
    List<HoodieWriteStat> finalStats = stats;
    Arrays.stream(baseFiles).forEach(fileStatus -> {
      if (!fileStatus.getPath().getName().contains("1-2-3")) {
        HoodieWriteStat stat = new HoodieWriteStat();
        String partitionPath = fileStatus.getPath().getParent().getName();
        if (!testPartitionedTable) {
          stat.setPath(fileStatus.getPath().getName());
        } else {
          stat.setPath(partitionPath + "/" + fileStatus.getPath().getName());
        }
        finalStats.add(stat);
      }
    });

    // there should not be any additional data files found.
    assertTrue(hoodieTable.getInvalidDataFilePaths("001", finalStats, true, "").isEmpty());

    HoodieTableMetaClient mockedMetaClient = mock(HoodieTableMetaClient.class);
    when(mockedMetaClient.getBasePath()).thenReturn(basePath);
    when(mockedMetaClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
    HoodieWrapperFileSystem wrapperFileSystem = mock(HoodieWrapperFileSystem.class);
    when(mockedMetaClient.getFs()).thenReturn(wrapperFileSystem);

    // lets do reconcile and again check for invalid files.
    hoodieTable.reconcileAgainstMarkers(context, mockedMetaClient, "001", stats, false, false);

    // lets test failure scenarios
    f0 = testTable.addRequestedCommit("002")
        .getFileIdsWithBaseFilesInPartitions(partitionA).get(partitionB);
    f1 = testTable.addCommit("002")
        .getFileIdsWithBaseFilesInPartitions(partitionB).get(partitionB);

    testTable.forCommit("002")
        .withMarkerFile(partitionA, f0, IOType.CREATE)
        .withMarkerFile(partitionB, f1, IOType.CREATE);

    // create another data file to simulate spark task retries
    String duplicateDataFile = createDuplicateDataFile(partitionA, f0, "1-2-3");
    hoodieTable = new TestBaseHoodieTable(writerConfig, context, metaClient);

    baseFiles = testTable.listAllBaseFiles();

    stats = new ArrayList<>();
    List<HoodieWriteStat> finalStats1 = stats;
    Arrays.stream(baseFiles).forEach(fileStatus -> {
      if (!fileStatus.getPath().getName().contains("1-2-3")) {
        HoodieWriteStat stat = new HoodieWriteStat();
        String partitionPath = fileStatus.getPath().getParent().getName();
        if (!testPartitionedTable) {
          stat.setPath(fileStatus.getPath().getName());
        } else {
          stat.setPath(partitionPath + "/" + fileStatus.getPath().getName());
        }
        finalStats1.add(stat);
      }
    });

    Set<String> invalidDataFilePaths = hoodieTable.getInvalidDataFilePaths("001", finalStats1, true, "");
    assertEquals(1, invalidDataFilePaths.size());
    assertEquals(duplicateDataFile, invalidDataFilePaths.stream().findFirst().get());

    // lets do reconcile and test few scenarios
    // fs.delete returns true
    when(wrapperFileSystem.delete(new Path(basePath + "/" + duplicateDataFile), false)).thenReturn(true);
    // expected to not throw
    reconcileAndValidate(stats, hoodieTable, context, mockedMetaClient, false, basePath + "/" + duplicateDataFile);

    // fs.delete returns false and fs.exists return false.
    when(wrapperFileSystem.delete(new Path(basePath + "/" + duplicateDataFile), false)).thenReturn(false);
    when(wrapperFileSystem.exists(new Path(basePath + "/" + duplicateDataFile))).thenReturn(false);

    // expected to not throw
    reconcileAndValidate(stats, hoodieTable, context, mockedMetaClient, false, basePath + "/" + duplicateDataFile);

    // fs.delete returns false and fs.exists return true. expected to throw
    when(wrapperFileSystem.delete(new Path(basePath + "/" + duplicateDataFile), false)).thenReturn(false);
    when(wrapperFileSystem.exists(new Path(basePath + "/" + duplicateDataFile))).thenReturn(true);

    // expected to throw
    reconcileAndValidate(stats, hoodieTable, context, mockedMetaClient, true, basePath + "/" + duplicateDataFile);

    // fs.delete throws FileNotFound
    when(wrapperFileSystem.delete(new Path(basePath + "/" + duplicateDataFile), false)).thenThrow(new FileNotFoundException("failed to delete"));
    // expected to not throw
    reconcileAndValidate(stats, hoodieTable, context, mockedMetaClient, false, basePath + "/" + duplicateDataFile);
  }

  private void reconcileAndValidate(List<HoodieWriteStat> stats, HoodieTable table, HoodieEngineContext localContext, HoodieTableMetaClient mockedMetaClient, boolean expectedToThrow,
                                    String filePathForFailure) {
    List<HoodieWriteStat> finalStats2 = stats;
    HoodieTable finalHoodieTable = table;
    if (expectedToThrow) {
      HoodieException exception = assertThrows(
          HoodieException.class,
          () -> finalHoodieTable.reconcileAgainstMarkers(localContext, mockedMetaClient, "001", finalStats2, false, false)
      );
      assertEquals(
          "Failed to delete invalid paths during marker reconciliaton " + filePathForFailure,
          exception.getCause().getMessage());
    } else {
      finalHoodieTable.reconcileAgainstMarkers(localContext, mockedMetaClient, "001", finalStats2, false, false);
      // should not throw
    }
  }

  private String createDuplicateDataFile(String partition, String fileId, String writeToken) throws Exception {
    String dupDataFile = FSUtils.makeBaseFileName("001", writeToken, fileId, HoodieFileFormat.PARQUET.getFileExtension());
    createBaseFile(basePath, partition, dupDataFile, 1, Instant.now().toEpochMilli());
    // create marker for the dup data file
    FileCreateUtils.createMarkerFile(basePath, partition, "001", "001", fileId, IOType.CREATE, writeToken);
    return StringUtils.isNullOrEmpty(partition) ? dupDataFile : partition + "/" + dupDataFile;
  }

  public static String createBaseFile(String basePath, String partitionPath, String fileName, long length, long lastModificationTimeMilli)
      throws Exception {
    java.nio.file.Path parentPath = Paths.get(basePath, partitionPath);
    java.nio.file.Path baseFilePath = parentPath.resolve(fileName);
    Files.createFile(baseFilePath);
    try (RandomAccessFile raf = new RandomAccessFile(baseFilePath.toFile(), "rw")) {
      raf.setLength(length);
    }
    Files.setLastModifiedTime(baseFilePath, FileTime.fromMillis(lastModificationTimeMilli));
    return baseFilePath.toString();
  }

  private static class TestBaseHoodieTable extends HoodieTable {
    protected TestBaseHoodieTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
      super(config, context, metaClient);
    }

    private int countOfScheduleRollbackFunctionCalls = 0;

    public int getCountOfScheduleRollbackFunctionCalls() {
      return countOfScheduleRollbackFunctionCalls;
    }

    @Override
    protected HoodieIndex<?, ?> getIndex(HoodieWriteConfig config, HoodieEngineContext context) {
      return null;
    }

    @Override
    public HoodieWriteMetadata upsert(HoodieEngineContext context, String instantTime, Object records) {
      return null;
    }

    @Override
    public HoodieWriteMetadata insert(HoodieEngineContext context, String instantTime, Object records) {
      return null;
    }

    @Override
    public HoodieWriteMetadata delete(HoodieEngineContext context, String instantTime, Object keys) {
      return null;
    }

    @Override
    public HoodieWriteMetadata deletePrepped(HoodieEngineContext context, String instantTime, Object preppedRecords) {
      return null;
    }

    @Override
    public HoodieWriteMetadata upsertPrepped(HoodieEngineContext context, String instantTime, Object preppedRecords) {
      return null;
    }

    @Override
    public HoodieWriteMetadata insertPrepped(HoodieEngineContext context, String instantTime, Object preppedRecords) {
      return null;
    }

    @Override
    public HoodieWriteMetadata insertOverwrite(HoodieEngineContext context, String instantTime, Object records) {
      return null;
    }

    @Override
    public HoodieWriteMetadata insertOverwriteTable(HoodieEngineContext context, String instantTime, Object records) {
      return null;
    }

    @Override
    public HoodieWriteMetadata compact(HoodieEngineContext context, String compactionInstantTime) {
      return null;
    }

    @Override
    public HoodieWriteMetadata cluster(HoodieEngineContext context, String clusteringInstantTime) {
      return null;
    }

    @Override
    public void rollbackBootstrap(HoodieEngineContext context, String instantTime) {
    }

    @Override
    public HoodieCleanMetadata clean(HoodieEngineContext context, String cleanInstantTime) {
      return null;
    }

    @Override
    public Option<HoodieRollbackPlan> scheduleRollback(HoodieEngineContext context, String instantTime, HoodieInstant instantToRollback,
                                                       boolean skipTimelinePublish, boolean shouldRollbackUsingMarkers, boolean isRestore) {
      countOfScheduleRollbackFunctionCalls++;
      return null;
    }

    @Override
    public HoodieRollbackMetadata rollback(HoodieEngineContext context, String rollbackInstantTime, HoodieInstant commitInstant, boolean deleteInstants, boolean skipLocking) {
      return null;
    }

    @Override
    public Option<HoodieIndexCommitMetadata> index(HoodieEngineContext context, String indexInstantTime) {
      return null;
    }

    @Override
    public HoodieSavepointMetadata savepoint(HoodieEngineContext context, String instantToSavepoint, String user, String comment) {
      return null;
    }

    @Override
    public HoodieRestoreMetadata restore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
      return null;
    }

    @Override
    public Option<HoodieRestorePlan> scheduleRestore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
      return null;
    }

    @Override
    public void validateForLatestTimestamp(String instantTime) {
    }

    @Override
    public Option<HoodieIndexPlan> scheduleIndexing(HoodieEngineContext context, String indexInstantTime, List partitionsToIndex) {
      return null;
    }

    @Override
    public Option<HoodieCleanerPlan> scheduleCleaning(HoodieEngineContext context, String instantTime, Option extraMetadata) {
      return null;
    }

    @Override
    public HoodieBootstrapWriteMetadata bootstrap(HoodieEngineContext context, Option extraMetadata) {
      return null;
    }

    @Override
    public Option<HoodieClusteringPlan> scheduleClustering(HoodieEngineContext context, String instantTime, Option extraMetadata) {
      return null;
    }

    @Override
    public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context, String instantTime, Option extraMetadata) {
      return null;
    }

    @Override
    public HoodieWriteMetadata bulkInsertPrepped(HoodieEngineContext context, String instantTime, Object preppedRecords, Option bulkInsertPartitioner) {
      return null;
    }

    @Override
    public HoodieWriteMetadata deletePartitions(HoodieEngineContext context, String instantTime, List partitions) {
      return null;
    }

    @Override
    public HoodieWriteMetadata bulkInsert(HoodieEngineContext context, String instantTime, Object records, Option bulkInsertPartitioner) {
      return null;
    }
  }
}
