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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
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
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getRecordIndexMaxParallelism()).thenReturn(4);

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

  @Test
  @SuppressWarnings("unchecked")
  void testBuildUpdateWithNonEmptyCommitMetadataProducesPartitionEntry() {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getRecordIndexMaxParallelism()).thenReturn(4);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/hudi-record-index-test"));
    when(metaClient.getStorageConf()).thenReturn((org.apache.hudi.storage.StorageConfiguration) getDefaultStorageConf());

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
      mockedBaseFileParsingUtils.when(() -> BaseFileRecordParsingUtils
              .generateRLIMetadataHoodieRecordsForBaseFile(any(), any(), any(), any(), any(), anyBoolean()))
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
