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
import org.apache.hudi.common.model.FileSlice;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TestPartitionedRecordIndexer {

  @SuppressWarnings("unchecked")
  @Test
  void testInitializeWithRealEngineContextAndIndexDataContent() throws IOException {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient dataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getRecordIndexMaxParallelism()).thenReturn(8);
    when(dataMetaClient.getTableConfig()).thenReturn(tableConfig);

    HoodieData<HoodieRecord> p1Data = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.parallelize(
        Collections.singletonList(HoodieMetadataPayload.createPartitionFilesRecord("p1_data",
            Collections.singletonMap("f1.parquet", 1L), Collections.emptyList())),
        1);
    HoodieData<HoodieRecord> p2Data = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.parallelize(
        Collections.singletonList(HoodieMetadataPayload.createPartitionFilesRecord("p2_data",
            Collections.singletonMap("f2.parquet", 2L), Collections.emptyList())),
        1);

    DataPartitionAndRecords p1Init = new DataPartitionAndRecords(1, Option.of("p1"), p1Data);
    DataPartitionAndRecords p2Init = new DataPartitionAndRecords(2, Option.of("p2"), p2Data);

    FileSliceAndPartition fs1 = FileSliceAndPartition.of("p1", new FileSlice("p1", "001", "f1"));
    FileSliceAndPartition fs2 = FileSliceAndPartition.of("p2", new FileSlice("p2", "001", "f2"));
    List<FileSliceAndPartition> input = Arrays.asList(fs1, fs2);

    ExposedPartitionedRecordIndexer indexer = new ExposedPartitionedRecordIndexer(engineContext, writeConfig, dataMetaClient, p1Init, p2Init);

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      List<IndexInitializationPlan> initializationList = indexer.buildInitialization(IndexInitializationContext.of(
          "001", "002", Map.of(), Lazy.lazily(() -> input), Lazy.lazily(Option::empty)));

      mockedUtil.verify(() -> HoodieTableMetadataUtil.createRecordIndexDefinition(any(), any()));
      assertEquals(1, initializationList.size());
      assertEquals(2, initializationList.get(0).dataPartitionAndRecords().size());
      assertEquals(2, indexer.initializePartitionCalls);
    }
  }

  @Test
  void testInitializeWithEmptyFileSlicesReturnsEmptyInitialization() throws IOException {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient dataMetaClient = mock(HoodieTableMetaClient.class);

    ExposedPartitionedRecordIndexer indexer = new ExposedPartitionedRecordIndexer(engineContext, writeConfig, dataMetaClient, null, null);

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      List<IndexInitializationPlan> initializationList = indexer.buildInitialization(IndexInitializationContext.of(
          "001", "002", Map.of(), Lazy.lazily(Collections::emptyList), Lazy.lazily(Option::empty)));

      mockedUtil.verify(() -> HoodieTableMetadataUtil.createRecordIndexDefinition(any(), any()));
      assertEquals(1, initializationList.size());
      assertEquals(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), initializationList.get(0).indexPartitionName());
      assertEquals(0, initializationList.get(0).totalFileGroups());
      assertTrue(initializationList.get(0).dataPartitionAndRecords().isEmpty());
      assertEquals(0, indexer.initializePartitionCalls);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void testBuildUpdateWithEmptyCommitMetadataProducesEmptyRecords() {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient dataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getRecordIndexMaxParallelism()).thenReturn(8);
    when(dataMetaClient.getTableConfig()).thenReturn(tableConfig);

    HoodieData<HoodieRecord> empty = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.emptyHoodieData();
    ExposedPartitionedRecordIndexer indexer = new ExposedPartitionedRecordIndexer(
        engineContext, writeConfig, dataMetaClient,
        new DataPartitionAndRecords(1, Option.of("p1"), empty),
        new DataPartitionAndRecords(1, Option.of("p2"), empty));

    List<IndexPartitionAndRecords> result = indexer.buildUpdate(IndexUpdateContext.of(
        "018",
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
    HoodieTableMetaClient dataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getRecordIndexMaxParallelism()).thenReturn(8);
    when(metadataConfig.isRecordLevelIndexEnabled()).thenReturn(true);
    when(dataMetaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
    when(dataMetaClient.getBasePath()).thenReturn(new StoragePath("/tmp/hudi-partitioned-record-index-test"));
    doReturn(getDefaultStorageConf()).when(dataMetaClient).getStorageConf();

    HoodieData<HoodieRecord> empty = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.emptyHoodieData();
    ExposedPartitionedRecordIndexer indexer = new ExposedPartitionedRecordIndexer(
        engineContext, writeConfig, dataMetaClient,
        new DataPartitionAndRecords(1, Option.of("p1"), empty),
        new DataPartitionAndRecords(1, Option.of("p2"), empty));

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("p1");
    String fileId = "11111111-1111-1111-1111-111111111111";
    String baseFileName = FSUtils.makeBaseFileName("20240101010101", "1-0-1", fileId, HoodieFileFormat.PARQUET.getFileExtension());
    writeStat.setPath("p1/" + baseFileName);
    writeStat.setFileId(fileId);
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
                  "rk1", "p1", fileId, "20240101010101", 0)).iterator());

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

      mockedBaseFileParsingUtils.verify(() -> BaseFileRecordParsingUtils
          .generateRLIMetadataHoodieRecordsForBaseFile(any(), any(), anyInt(), any(), any(), eq(true)));
      mockedMetadataUtil.verify(() -> HoodieTableMetadataUtil
          .reduceByKeys(any(), anyInt(), eq(true)));
    }

    HoodieRecordGlobalLocation location = payload.getRecordGlobalLocation();
    assertEquals("p1", location.getPartitionPath());
    assertEquals(fileId, location.getFileId());
  }

  @SuppressWarnings("unchecked")
  @Test
  void testBuildCleanReturnsEmptyList() {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient dataMetaClient = mock(HoodieTableMetaClient.class);
    HoodieData<HoodieRecord> empty = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.emptyHoodieData();
    ExposedPartitionedRecordIndexer indexer = new ExposedPartitionedRecordIndexer(
        engineContext, writeConfig, dataMetaClient,
        new DataPartitionAndRecords(1, Option.of("p1"), empty),
        new DataPartitionAndRecords(1, Option.of("p2"), empty));

    assertTrue(indexer.buildClean(IndexCleanContext.of("019", mock(HoodieCleanMetadata.class))).isEmpty());
  }

  private static class ExposedPartitionedRecordIndexer extends PartitionedRecordIndexer {
    private final DataPartitionAndRecords partition1Data;
    private final DataPartitionAndRecords partition2Data;
    private int initializePartitionCalls;

    ExposedPartitionedRecordIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig,
                                    HoodieTableMetaClient dataTableMetaClient,
                                    DataPartitionAndRecords partition1Data,
                                    DataPartitionAndRecords partition2Data) {
      super(engineContext, dataTableWriteConfig, dataTableMetaClient);
      this.partition1Data = partition1Data;
      this.partition2Data = partition2Data;
    }

    @Override
    protected DataPartitionAndRecords initializeRecordIndexPartition(String dataPartition, List<FileSliceAndPartition> latestMergedPartitionFileSliceList,
                                                                     int recordIndexMaxParallelism) {
      initializePartitionCalls++;
      if ("p1".equals(dataPartition)) {
        return partition1Data;
      }
      return partition2Data;
    }
  }
}
