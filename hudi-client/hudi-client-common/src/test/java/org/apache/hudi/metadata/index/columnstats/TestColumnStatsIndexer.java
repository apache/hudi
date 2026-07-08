/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.hudi.metadata.index.columnstats;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexRestoreContext;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.model.FileInfo;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TestColumnStatsIndexer {
  private static final int PARALLELISM = 4;
  private static final int MAX_READER_BUFFER_SIZE = 1024;
  private static final int FILE_GROUP_COUNT = 5;

  private HoodieEngineContext engineContext;
  private HoodieWriteConfig writeConfig;
  private HoodieMetadataConfig metadataConfig;
  private HoodieTableMetaClient metaClient;
  private HoodieTableConfig tableConfig;
  private HoodieRecordMerger recordMerger;

  @BeforeEach
  void setUp() {
    engineContext = mock(HoodieEngineContext.class);
    writeConfig = mock(HoodieWriteConfig.class);
    metadataConfig = mock(HoodieMetadataConfig.class);
    metaClient = mock(HoodieTableMetaClient.class);
    tableConfig = mock(HoodieTableConfig.class);
    recordMerger = mock(HoodieRecordMerger.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(writeConfig.getColumnStatsIndexParallelism()).thenReturn(PARALLELISM);
    when(writeConfig.getRecordMerger()).thenReturn(recordMerger);
    when(recordMerger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);
    when(metadataConfig.getColumnStatsIndexParallelism()).thenReturn(PARALLELISM);
    when(metadataConfig.getMaxReaderBufferSize()).thenReturn(MAX_READER_BUFFER_SIZE);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
  }

  @Test
  void testBuildRestoreWithEmptyInputs() {
    ColumnStatsIndexer indexer = new ColumnStatsIndexer(engineContext, writeConfig, metaClient);
    List<IndexPartitionAndRecords> result =
        indexer.buildRestore(IndexRestoreContext.of("001", Collections.emptyList(), Collections.emptyMap(), Collections.emptyMap()));
    assertTrue(result.isEmpty());
  }

  @Test
  void testBuildRestoreWithEmptyColumnsToIndex() {
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      Map<String, Object> emptyColumnsMap = new HashMap<>();
      mockedUtil.when(() -> HoodieTableMetadataUtil.getColumnsToIndex(
          any(), any(), any(), eq(false), any(), any())).thenReturn(emptyColumnsMap);

      Map<String, List<FileInfo>> filesAdded = new HashMap<>();
      filesAdded.put("partition1", Collections.singletonList(FileInfo.of("file1.parquet", 1024L)));

      ColumnStatsIndexer indexer = new ColumnStatsIndexer(engineContext, writeConfig, metaClient);
      List<IndexPartitionAndRecords> result =
          indexer.buildRestore(IndexRestoreContext.of("001", Collections.emptyList(), filesAdded, Collections.emptyMap()));
      assertTrue(result.isEmpty());
    }
  }

  @Test
  void testBuildRestoreWithValidColumns() {
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      Map<String, Object> columnsMap = new HashMap<>();
      columnsMap.put("col1", null);
      columnsMap.put("col2", null);
      mockedUtil.when(() -> HoodieTableMetadataUtil.getColumnsToIndex(
          any(), any(), any(), eq(false), any(), any())).thenReturn(columnsMap);

      HoodieData<HoodieRecord> mockHoodieData = mock(HoodieData.class);
      mockedUtil.when(() -> HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
          any(), any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(mockHoodieData);

      Map<String, List<FileInfo>> filesAdded = new HashMap<>();
      filesAdded.put("partition1", new ArrayList<>());

      ColumnStatsIndexer indexer = new ColumnStatsIndexer(engineContext, writeConfig, metaClient);
      List<IndexPartitionAndRecords> result =
          indexer.buildRestore(IndexRestoreContext.of("001", Collections.emptyList(), filesAdded, Collections.emptyMap()));

      assertEquals(1, result.size());
      assertEquals(MetadataPartitionType.COLUMN_STATS.getPartitionPath(), result.get(0).indexPartitionName());
      assertSame(mockHoodieData, result.get(0).indexRecords());

      mockedUtil.verify(() -> HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
          eq(engineContext),
          eq(Collections.emptyMap()),
          eq(filesAdded),
          eq(metaClient),
          eq(PARALLELISM),
          eq(MAX_READER_BUFFER_SIZE),
          any()));
    }
  }

  @Test
  void testBuildRestoreWithMixedInputs() {
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      Map<String, Object> columnsMap = new HashMap<>();
      columnsMap.put("col1", null);
      columnsMap.put("col2", null);
      columnsMap.put("col3", null);
      mockedUtil.when(() -> HoodieTableMetadataUtil.getColumnsToIndex(
          any(), any(), any(), eq(false), any(), any())).thenReturn(columnsMap);

      HoodieData<HoodieRecord> mockHoodieData = mock(HoodieData.class);
      mockedUtil.when(() -> HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
          any(), any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(mockHoodieData);

      Map<String, List<FileInfo>> filesAdded = new HashMap<>();
      List<FileInfo> filesToAdd = new ArrayList<>();
      filesToAdd.add(FileInfo.of("file1.parquet", 1024L));
      filesToAdd.add(FileInfo.of("file2.parquet", 2048L));
      filesAdded.put("partition1", filesToAdd);

      Map<String, List<String>> filesDeleted = new HashMap<>();
      filesDeleted.put("partition1", List.of("old_file1.parquet", "old_file2.parquet"));

      ColumnStatsIndexer indexer = new ColumnStatsIndexer(engineContext, writeConfig, metaClient);
      List<IndexPartitionAndRecords> result =
          indexer.buildRestore(IndexRestoreContext.of("001", Collections.emptyList(), filesAdded, filesDeleted));

      assertEquals(1, result.size());
      assertEquals(MetadataPartitionType.COLUMN_STATS.getPartitionPath(), result.get(0).indexPartitionName());
      assertSame(mockHoodieData, result.get(0).indexRecords());

      mockedUtil.verify(() -> HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
          eq(engineContext),
          eq(filesDeleted),
          eq(filesAdded),
          eq(metaClient),
          eq(PARALLELISM),
          eq(MAX_READER_BUFFER_SIZE),
          any()));
    }
  }

  @Test
  void testInitializeDataWithEmptyInputUsesEmptyHoodieData() throws IOException {
    HoodieData<HoodieRecord> emptyData = mock(HoodieData.class);

    when(metadataConfig.getColumnStatsIndexFileGroupCount()).thenReturn(2);
    when(engineContext.emptyHoodieData()).thenReturn((HoodieData) emptyData);

    ColumnStatsIndexer indexer = new ColumnStatsIndexer(engineContext, writeConfig, metaClient);
    List<IndexInitializationPlan> initializationList = indexer.buildInitialization(IndexInitializationContext.of(
        "001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList), Lazy.lazily(Option::empty)));
    assertEquals(1, initializationList.size());

    assertEquals(MetadataPartitionType.COLUMN_STATS.getPartitionPath(), initializationList.get(0).indexPartitionName());
    assertSame(emptyData, initializationList.get(0).dataPartitionAndRecords().get(0).indexRecords());
  }

  @SuppressWarnings("unchecked")
  @Test
  void testInitializeDataWithRealEngineContextAndIndexDataContent() throws IOException {
    HoodieEngineContext testDataEngineContext = new HoodieLocalEngineContext(getDefaultStorageConf());

    when(metadataConfig.getColumnStatsIndexFileGroupCount()).thenReturn(FILE_GROUP_COUNT);

    Map<String, List<FileInfo>> files = new HashMap<>();
    files.put("p1", Collections.singletonList(FileInfo.of("f1.parquet", 1L)));

    HoodieData<HoodieRecord> records = (HoodieData<HoodieRecord>) (HoodieData<?>) testDataEngineContext.parallelize(
        Collections.singletonList(HoodieMetadataPayload.createPartitionFilesRecord("p_col",
            Collections.singletonMap("f_col.parquet", 22L), Collections.emptyList())),
        1);

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      Map<String, Object> columns = new HashMap<>();
      columns.put("c1", new Object());
      mockedUtil.when(() -> HoodieTableMetadataUtil.getColumnsToIndex(any(), any(), any(), eq(true), eq(Option.of(HoodieRecord.HoodieRecordType.AVRO)), any()))
          .thenReturn(columns);
      mockedUtil.when(() -> HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(any(), any(), any(), any(), anyInt(), anyInt(), any()))
          .thenReturn(records);

      ColumnStatsIndexer indexer = new ColumnStatsIndexer(engineContext, writeConfig, metaClient);
      List<IndexInitializationPlan> initializationList = indexer.buildInitialization(IndexInitializationContext.of(
          "001", "002", files, Lazy.lazily(Collections::emptyList), Lazy.lazily(Option::empty)));
      assertEquals(1, initializationList.size());

      assertEquals(FILE_GROUP_COUNT, initializationList.get(0).totalFileGroups());
      List<HoodieRecord> collected = initializationList.get(0).dataPartitionAndRecords().get(0).indexRecords().collectAsList();
      assertEquals(1, collected.size());
      assertEquals("p_col", collected.get(0).getRecordKey());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testBuildUpdateWithNonEmptyCommitMetadataProducesPartitionEntry() {
    HoodieEngineContext mockedEngineContext = mock(HoodieEngineContext.class);
    HoodieEngineContext realEngineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    ColumnStatsIndexer indexer = new ColumnStatsIndexer(mockedEngineContext, writeConfig, metaClient);

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.UPSERT);
    String filePath = "p1/fileid-1_1-0-1_014.parquet";
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("p1");
    writeStat.setPath(filePath);
    writeStat.setRecordsStats(Collections.singletonMap("c1",
        HoodieColumnRangeMetadata.stub(filePath, "c1", HoodieIndexVersion.V1)));
    commitMetadata.getPartitionToWriteStats().put("p1", Collections.singletonList(writeStat));

    HoodieData<HoodieWriteStat> writeStatsData = (HoodieData<HoodieWriteStat>) mock(HoodieData.class);
    HoodieData<HoodieRecord> columnStatsData = (HoodieData<HoodieRecord>) (HoodieData<?>) realEngineContext.parallelize(
        HoodieMetadataPayload.createColumnStatsRecords("p1",
            Collections.singletonList(HoodieColumnRangeMetadata.stub(filePath, "c1", HoodieIndexVersion.V1)),
            false).collect(java.util.stream.Collectors.toList()),
        1);
    when(mockedEngineContext.parallelize(any(List.class), anyInt())).thenReturn((HoodieData) writeStatsData);
    when(writeStatsData.flatMap(any())).thenReturn((HoodieData) columnStatsData);

    List<IndexPartitionAndRecords> result;
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      Map<String, HoodieSchema> columnsToIndex = new HashMap<>();
      columnsToIndex.put("c1", mock(HoodieSchema.class));
      mockedUtil.when(() -> HoodieTableMetadataUtil.getColumnsToIndex(any(HoodieCommitMetadata.class), any(HoodieTableMetaClient.class), any(HoodieMetadataConfig.class), any()))
          .thenReturn(columnsToIndex);

      result = indexer.buildUpdate(IndexUpdateContext.of(
          "014",
          mock(HoodieBackedTableMetadata.class),
          Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
          commitMetadata));
    }

    assertEquals(1, result.size());
    assertEquals(MetadataPartitionType.COLUMN_STATS.getPartitionPath(), result.get(0).indexPartitionName());
    List<HoodieRecord> indexRecords = result.get(0).indexRecords().collectAsList();
    assertEquals(1, indexRecords.size());
    HoodieMetadataPayload payload = (HoodieMetadataPayload) indexRecords.get(0).getData();
    assertTrue(payload.getColumnStatMetadata().isPresent());
    assertEquals("c1", payload.getColumnStatMetadata().get().getColumnName());
    assertFalse(payload.getColumnStatMetadata().get().getIsDeleted());
  }

  @Test
  void testBuildCleanWithNoDeletedFilesProducesEmptyRecords() {
    Map<String, HoodieCleanPartitionMetadata> partitionMetadata = new HashMap<>();
    partitionMetadata.put("p1", new HoodieCleanPartitionMetadata(
        "p1", "KEEP_LATEST_COMMITS", Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), false));
    HoodieCleanMetadata cleanMetadata = new HoodieCleanMetadata(
        "014", 100L, 0, "013", "013", partitionMetadata, 2, Collections.emptyMap(), Collections.emptyMap());

    HoodieEngineContext localEngineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    ColumnStatsIndexer indexer = new ColumnStatsIndexer(localEngineContext, writeConfig, metaClient);
    List<IndexPartitionAndRecords> result = indexer.buildClean(IndexCleanContext.of("015", cleanMetadata));

    assertEquals(1, result.size());
    assertEquals(MetadataPartitionType.COLUMN_STATS.getPartitionPath(), result.get(0).indexPartitionName());
    assertEquals(0, result.get(0).indexRecords().collectAsList().size());
  }
}
