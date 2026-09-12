/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.hudi.metadata.index.record;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.BaseFileRecordParsingUtils;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.model.DataPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TestRecordIndexer {

  @Test
  void testGetDataCreatesDefinitionAndReturnsInitialization() throws IOException {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieData<HoodieRecord> records = mock(HoodieData.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getRecordIndexMaxParallelism()).thenReturn(4);

    DataPartitionAndRecords init = new DataPartitionAndRecords(2, Option.empty(), records);
    ExposedRecordIndexer indexer = new ExposedRecordIndexer(engineContext, writeConfig, metaClient, init);

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      List<IndexInitializationPlan> result = indexer.buildInitialization(IndexInitializationContext.of(
          "001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList), Lazy.lazily(Option::empty)));
      assertEquals(1, result.size());
      assertEquals(2, result.get(0).totalFileGroups());
      mockedUtil.verify(() -> HoodieTableMetadataUtil.createRecordIndexDefinition(any(), any()), times(1));
    }
  }

  @Test
  void testPostInitializationValidationAndUnpersist() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieData<HoodieRecord> records = mock(HoodieData.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);

    ExposedRecordIndexer indexer = new ExposedRecordIndexer(
        engineContext, writeConfig, metaClient, new DataPartitionAndRecords(1, Option.empty(), records));

    when(metadataConfig.isRecordIndexInitializationValidationEnabled()).thenReturn(false);
    indexer.callPost(metaClient, IndexInitializationPlan.of(1, "record_index", records), "record_index");
    assertFalse(indexer.validateCalled);
    verify(records, times(1)).unpersistWithDependencies();

    when(metadataConfig.isRecordIndexInitializationValidationEnabled()).thenReturn(true);
    indexer.callPost(metaClient, IndexInitializationPlan.of(1, "record_index", records), "record_index");
    assertTrue(indexer.validateCalled);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testGetDataWithRealEngineContextAndIndexDataContent() throws IOException {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getRecordIndexMaxParallelism()).thenReturn(4);

    HoodieData<HoodieRecord> records = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.parallelize(
        Collections.singletonList(HoodieMetadataPayload.createPartitionFilesRecord("p_record",
            Collections.singletonMap("f_record.parquet", 66L), Collections.emptyList())),
        1);

    ExposedRecordIndexer indexer = new ExposedRecordIndexer(
        engineContext, writeConfig, metaClient, new DataPartitionAndRecords(2, Option.empty(), records));

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      List<IndexInitializationPlan> result = indexer.buildInitialization(IndexInitializationContext.of(
          "001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList), Lazy.lazily(Option::empty)));
      assertEquals(1, result.size());
      assertEquals(1, result.get(0).dataPartitionAndRecords().get(0).indexRecords().collectAsList().size());
      assertEquals("p_record", result.get(0).dataPartitionAndRecords().get(0).indexRecords().collectAsList().get(0).getRecordKey());
      mockedUtil.verify(() -> HoodieTableMetadataUtil.createRecordIndexDefinition(any(), any()), times(1));
    }
  }

  @Test
  void testBuildUpdateWithEmptyCommitMetadataProducesEmptyRecords() {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getRecordIndexMaxParallelism()).thenReturn(4);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.hasRecordKey()).thenReturn(true);

    HoodieData<HoodieRecord> records = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.emptyHoodieData();
    ExposedRecordIndexer indexer = new ExposedRecordIndexer(
        engineContext, writeConfig, metaClient, new DataPartitionAndRecords(1, Option.empty(), records));

    List<IndexPartitionAndRecords> result = indexer.buildUpdate(IndexUpdateContext.of(
        "016",
        mock(HoodieBackedTableMetadata.class),
        Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
        new HoodieCommitMetadata()));

    assertEquals(1, result.size());
    assertEquals(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), result.get(0).indexPartitionName());
    assertEquals(0, result.get(0).indexRecords().collectAsList().size());
  }

  /**
   * Mocks a parquet table at {@code /tmp/hudi-record-index-test} whose record index is updated with a parallelism of 4.
   */
  private static HoodieTableMetaClient mockMetaClientForUpdate(HoodieWriteConfig writeConfig, HoodieTableConfig tableConfig) {
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getRecordIndexMaxParallelism()).thenReturn(4);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/hudi-record-index-test"));
    when(metaClient.getStorageConf()).thenReturn((StorageConfiguration) getDefaultStorageConf());
    return metaClient;
  }

  @Test
  @SuppressWarnings("unchecked")
  void testBuildUpdateWithNonEmptyCommitMetadataProducesPartitionEntry() {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieTableMetaClient metaClient = mockMetaClientForUpdate(writeConfig, tableConfig);
    when(tableConfig.hasRecordKey()).thenReturn(true);

    HoodieData<HoodieRecord> records = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.emptyHoodieData();
    ExposedRecordIndexer indexer = new ExposedRecordIndexer(
        engineContext, writeConfig, metaClient, new DataPartitionAndRecords(1, Option.empty(), records));

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("p1");
    final String fileID = UUID.randomUUID().toString();
    String baseFileName = FSUtils.makeBaseFileName("20240101010101", "1-0-1", fileID, HoodieFileFormat.PARQUET.getFileExtension());
    writeStat.setPath("p1/" + baseFileName);
    writeStat.setFileId(fileID);
    writeStat.setNumInserts(1);
    writeStat.setTotalWriteBytes(128L);
    commitMetadata.addWriteStat("p1", writeStat);

    List<HoodieRecord> indexRecords;
    HoodieMetadataPayload payload;
    try (MockedStatic<BaseFileRecordParsingUtils> mockedBaseFileParsingUtils =
             mockStatic(BaseFileRecordParsingUtils.class);
         MockedStatic<HoodieTableMetadataUtil> mockedMetadataUtil =
             mockStatic(HoodieTableMetadataUtil.class)) {
      mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(any()))
          .thenReturn(Option.empty());
      mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.reduceByKeys(any(), anyInt(), anyBoolean()))
          .thenAnswer(invocation -> invocation.getArgument(0));
      // the table carries record keys, so none are generated
      mockedBaseFileParsingUtils.when(() -> BaseFileRecordParsingUtils
              .generateRLIMetadataHoodieRecordsForBaseFile(any(), any(), any(), any(), any(), anyBoolean(), eq(false)))
          .thenReturn(Collections.singletonList(
              HoodieMetadataPayload.createRecordIndexUpdate(
                  "rk1", "p1", fileID, "20240101010101", 0)).iterator());

      List<IndexPartitionAndRecords> result = indexer.buildUpdate(IndexUpdateContext.of(
          "20240101010101",
          mock(HoodieBackedTableMetadata.class),
          Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
          commitMetadata));

      assertEquals(1, result.size());
      assertEquals(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), result.get(0).indexPartitionName());
      indexRecords = result.get(0).indexRecords().collectAsList();
      assertEquals(1, indexRecords.size());
      payload = (HoodieMetadataPayload) indexRecords.get(0).getData();
      assertEquals("p1", payload.getDataPartition());
    }

    HoodieRecordGlobalLocation location = payload.getRecordGlobalLocation();
    assertEquals("p1", location.getPartitionPath());
    assertEquals(fileID, location.getFileId());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testBuildUpdateDeletesRecordsOfFileGroupsReplacedByExternalWriter() {
    // files written outside Hudi are registered through replace commits without a known operation type. The records of the
    // replaced file groups leave the index unless the same key is written again in the commit.
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieTableMetaClient metaClient = mockMetaClientForUpdate(writeConfig, tableConfig);
    HoodieTableFileSystemView fsView = mock(HoodieTableFileSystemView.class);
    // such a table has no record key, so every row is keyed by file path and position, and the file ids, which are
    // file names rather than UUIDs, are stored as raw strings whatever the write config says
    when(tableConfig.hasRecordKey()).thenReturn(false);
    HoodieBaseFile replacedBaseFile = new HoodieBaseFile(new StoragePathInfo(
        new StoragePath("/tmp/hudi-record-index-test/p1/file_1.parquet_20240101010101_hudiext"), 100L, false, (short) 0, 0L, 0L));
    when(fsView.getLatestBaseFile("p1", "file_1.parquet")).thenReturn(Option.of(replacedBaseFile));

    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();
    // a fresh HoodieCommitMetadata carries UNKNOWN; a writer may also leave the operation type null
    commitMetadata.setOperationType(null);
    commitMetadata.addReplaceFileId("p1", "file_1.parquet");
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("p1");
    writeStat.setPath("p1/" + ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker("file_2.parquet", "20240101010102"));
    writeStat.setFileId("file_2.parquet");
    writeStat.setNumInserts(2);
    writeStat.setTotalWriteBytes(128L);
    commitMetadata.addWriteStat("p1", writeStat);

    HoodieData<HoodieRecord> records = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.emptyHoodieData();
    ExposedRecordIndexer indexer = new ExposedRecordIndexer(
        engineContext, writeConfig, metaClient, new DataPartitionAndRecords(1, Option.empty(), records));

    List<HoodieRecord> indexRecords;
    try (MockedStatic<BaseFileRecordParsingUtils> mockedBaseFileParsingUtils = mockStatic(BaseFileRecordParsingUtils.class);
         MockedStatic<HoodieTableMetadataUtil> mockedMetadataUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(any())).thenReturn(Option.empty());
      mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.reduceByKeys(any(), anyInt(), anyBoolean()))
          .thenAnswer(invocation -> invocation.getArgument(0));
      // the new file carries one new key and rewrites one key of the replaced file
      mockedBaseFileParsingUtils.when(() -> BaseFileRecordParsingUtils
              .generateRLIMetadataHoodieRecordsForBaseFile(any(), any(), any(), any(), any(), anyBoolean(), eq(true)))
          .thenReturn(Arrays.asList(
              HoodieMetadataPayload.createRecordIndexUpdate("p1/file_2.parquet_0", "p1", "file_2.parquet", "20240101010102", 1),
              HoodieMetadataPayload.createRecordIndexUpdate("p1/file_1.parquet_1", "p1", "file_2.parquet", "20240101010102", 1)).iterator());
      mockedBaseFileParsingUtils.when(() -> BaseFileRecordParsingUtils
              .generateRLIMetadataHoodieRecordsForReplacedBaseFile(any(), any(), any(), any(), anyBoolean(), eq(true)))
          .thenReturn(Arrays.asList(
              HoodieMetadataPayload.createRecordIndexDelete("p1/file_1.parquet_0", "p1", false),
              HoodieMetadataPayload.createRecordIndexDelete("p1/file_1.parquet_1", "p1", false)).iterator());

      List<IndexPartitionAndRecords> result = indexer.buildUpdate(IndexUpdateContext.of(
          "20240101010102", mock(HoodieBackedTableMetadata.class), Lazy.lazily(() -> fsView), commitMetadata));
      assertEquals(1, result.size());
      indexRecords = result.get(0).indexRecords().collectAsList();

      // the replaced file is read from its location on storage, without the external file marker, with generated keys
      mockedBaseFileParsingUtils.verify(() -> BaseFileRecordParsingUtils.generateRLIMetadataHoodieRecordsForReplacedBaseFile(
          eq("/tmp/hudi-record-index-test"), eq("p1"), eq(new StoragePath("/tmp/hudi-record-index-test/p1/file_1.parquet")), any(), eq(false), eq(true)));
    }

    Map<String, Boolean> recordKeyToIsDeleted = indexRecords.stream()
        .collect(Collectors.toMap(HoodieRecord::getRecordKey, record -> record.getData() instanceof EmptyHoodieRecordPayload));
    Map<String, Boolean> expected = new HashMap<>();
    expected.put("p1/file_2.parquet_0", false);
    expected.put("p1/file_1.parquet_1", false);
    expected.put("p1/file_1.parquet_0", true);
    assertEquals(expected, recordKeyToIsDeleted);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testBuildUpdateKeepsReplacedFileGroupsOfTableWithRecordKeys() {
    // a table that carries a record key keeps the behaviour it had before external files were supported: a replace
    // commit without a known operation type leaves the records of the replaced file groups in the index
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieTableMetaClient metaClient = mockMetaClientForUpdate(writeConfig, tableConfig);
    when(tableConfig.hasRecordKey()).thenReturn(true);

    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();
    commitMetadata.setOperationType(null);
    commitMetadata.addReplaceFileId("p1", "file_1.parquet");

    HoodieData<HoodieRecord> records = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.emptyHoodieData();
    ExposedRecordIndexer indexer = new ExposedRecordIndexer(
        engineContext, writeConfig, metaClient, new DataPartitionAndRecords(1, Option.empty(), records));

    HoodieTableFileSystemView fsView = mock(HoodieTableFileSystemView.class);
    List<IndexPartitionAndRecords> result = indexer.buildUpdate(IndexUpdateContext.of(
        "20240101010102", mock(HoodieBackedTableMetadata.class), Lazy.lazily(() -> fsView), commitMetadata));

    assertEquals(1, result.size());
    assertTrue(result.get(0).indexRecords().collectAsList().isEmpty());
    verifyNoInteractions(fsView);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testBuildUpdateRejectsWritingReplacedFileGroupsOfTableWithoutRecordKeys() {
    // the file id of such a table is the file path, so writing a replaced file group registers a file again under its
    // own name: the replaced content is gone and the file system view hides the file group
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieTableMetaClient metaClient = mockMetaClientForUpdate(writeConfig, tableConfig);
    when(tableConfig.hasRecordKey()).thenReturn(false);
    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.UNKNOWN);
    commitMetadata.addReplaceFileId("p1", "file_1.parquet");
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("p1");
    writeStat.setPath("p1/" + ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker("file_1.parquet", "20240101010102"));
    writeStat.setFileId("file_1.parquet");
    writeStat.setNumInserts(1);
    commitMetadata.addWriteStat("p1", writeStat);
    ExposedRecordIndexer indexer = new ExposedRecordIndexer(engineContext, writeConfig, metaClient,
        new DataPartitionAndRecords(1, Option.empty(), (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.emptyHoodieData()));

    try (MockedStatic<BaseFileRecordParsingUtils> mockedBaseFileParsingUtils = mockStatic(BaseFileRecordParsingUtils.class);
         MockedStatic<HoodieTableMetadataUtil> mockedMetadataUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(any())).thenReturn(Option.empty());
      mockedMetadataUtil.when(() -> HoodieTableMetadataUtil.reduceByKeys(any(), anyInt(), anyBoolean()))
          .thenAnswer(invocation -> invocation.getArgument(0));
      mockedBaseFileParsingUtils.when(() -> BaseFileRecordParsingUtils
              .generateRLIMetadataHoodieRecordsForBaseFile(any(), any(), any(), any(), any(), anyBoolean(), eq(true)))
          .thenReturn(Collections.emptyIterator());

      IllegalStateException rewritten = assertThrows(IllegalStateException.class, () -> indexer.buildUpdate(IndexUpdateContext.of(
          "20240101010102", mock(HoodieBackedTableMetadata.class), Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)), commitMetadata)));
      assertTrue(rewritten.getMessage().endsWith("[file_1.parquet]"), rewritten.getMessage());
    }
  }

  @Test
  void testSnapshotKeysOfTableWithoutRecordKeysNeedBaseFileWithoutLogFiles() {
    // the rows of such a table are keyed by their position in the base file, which the rows of a merged file slice
    // would not line up with
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieTableMetaClient metaClient = mockMetaClientForUpdate(writeConfig, tableConfig);
    when(tableConfig.hasRecordKey()).thenReturn(false);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.getCommitsTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filterCompletedInstants()).thenReturn(activeTimeline);
    when(activeTimeline.lastInstant()).thenReturn(Option.of(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, "20240101010101")));
    FileSlice fileSlice = new FileSlice("p1", "20240101010101", "file1");
    fileSlice.setBaseFile(new HoodieBaseFile(new StoragePathInfo(
        new StoragePath("/tmp/hudi-record-index-test/p1/file1_20240101010101_hudiext"), 100L, false, (short) 0, 0L, 0L)));
    fileSlice.addLogFile(new HoodieLogFile(new StoragePath("/tmp/hudi-record-index-test/p1/.file1_20240101010101.log.1_0-0-0")));

    // the local engine wraps the failure of the key reading task
    HoodieException logFiles = assertThrows(HoodieException.class, () -> BaseRecordIndexer.readRecordKeysFromFileSliceSnapshot(
        engineContext, Collections.singletonList(FileSliceAndPartition.of("p1", fileSlice)), 1, "test", metaClient, writeConfig).collectAsList());
    assertTrue(logFiles.getCause() instanceof IllegalStateException, String.valueOf(logFiles.getCause()));
    assertTrue(logFiles.getCause().getMessage().contains("needs a base file and no log files"), logFiles.getCause().getMessage());
  }

  @Test
  void testBuildCleanReturnsEmptyList() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieData<HoodieRecord> records = mock(HoodieData.class);
    ExposedRecordIndexer indexer = new ExposedRecordIndexer(
        engineContext, writeConfig, metaClient, new DataPartitionAndRecords(1, Option.empty(), records));

    assertTrue(indexer.buildClean(IndexCleanContext.of("017", mock(HoodieCleanMetadata.class))).isEmpty());
  }

  private static class ExposedRecordIndexer extends RecordIndexer {
    private final DataPartitionAndRecords predefined;
    private boolean validateCalled;

    ExposedRecordIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig,
                         HoodieTableMetaClient dataTableMetaClient, DataPartitionAndRecords predefined) {
      super(engineContext, dataTableWriteConfig, dataTableMetaClient);
      this.predefined = predefined;
    }

    @Override
    protected DataPartitionAndRecords initializeRecordIndexPartition(List<FileSliceAndPartition> latestMergedPartitionFileSliceList,
                                                                     int recordIndexMaxParallelism) {
      return predefined;
    }

    @Override
    protected void validateRecordIndex(HoodieData<HoodieRecord> recordIndexRecords, int fileGroupCount, HoodieTableMetaClient metadataMetaClient) {
      validateCalled = true;
    }

    void callPost(HoodieTableMetaClient metadataMetaClient, IndexInitializationPlan indexPartitionInitialization, String relativePartitionPath) {
      postInitialization(metadataMetaClient, indexPartitionInitialization.dataPartitionAndRecords().get(0).indexRecords(),
          indexPartitionInitialization.totalFileGroups(), relativePartitionPath);
    }
  }
}
