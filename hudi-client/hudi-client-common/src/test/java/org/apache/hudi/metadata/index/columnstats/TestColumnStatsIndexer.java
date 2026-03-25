/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.hudi.metadata.index.columnstats;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.model.IndexPartitionInitialization;
import org.apache.hudi.metadata.model.FileInfo;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.util.Lazy;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TestColumnStatsIndexer {

  @Test
  void testInitializeDataWithEmptyInputUsesEmptyHoodieData() throws IOException {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieData<HoodieRecord> emptyData = mock(HoodieData.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getColumnStatsIndexFileGroupCount()).thenReturn(2);
    when(engineContext.emptyHoodieData()).thenReturn((HoodieData) emptyData);

    ExposedColumnStatsIndexer indexer = new ExposedColumnStatsIndexer(engineContext, writeConfig, metaClient);
    List<IndexPartitionInitialization> initializationList = indexer.callGetData("001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList));
    assertEquals(1, initializationList.size());

    assertEquals(MetadataPartitionType.COLUMN_STATS.getPartitionPath(), initializationList.get(0).indexPartitionName());
    assertSame(emptyData, initializationList.get(0).dataPartitionAndRecords().get(0).indexRecords());
  }

  @SuppressWarnings("unchecked")
  @Test
  void testInitializeDataWithRealEngineContextAndIndexDataContent() throws IOException {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieRecordMerger recordMerger = mock(HoodieRecordMerger.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(writeConfig.getColumnStatsIndexParallelism()).thenReturn(4);
    when(writeConfig.getRecordMerger()).thenReturn(recordMerger);
    when(recordMerger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);
    when(metadataConfig.getColumnStatsIndexFileGroupCount()).thenReturn(5);
    when(metadataConfig.getMaxReaderBufferSize()).thenReturn(4096);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);

    Map<String, List<FileInfo>> files = new HashMap<>();
    files.put("p1", Collections.singletonList(FileInfo.of("f1.parquet", 1L)));

    HoodieData<HoodieRecord> records = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.parallelize(
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

      ExposedColumnStatsIndexer indexer = new ExposedColumnStatsIndexer(engineContext, writeConfig, metaClient);
      List<IndexPartitionInitialization> initializationList = indexer.callGetData("001", "002", files, Lazy.lazily(Collections::emptyList));
      assertEquals(1, initializationList.size());

      assertEquals(5, initializationList.get(0).totalFileGroups());
      List<HoodieRecord> collected = initializationList.get(0).dataPartitionAndRecords().get(0).indexRecords().collectAsList();
      assertEquals(1, collected.size());
      assertEquals("p_col", collected.get(0).getRecordKey());
    }
  }

  private static class ExposedColumnStatsIndexer extends ColumnStatsIndexer {
    ExposedColumnStatsIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig, HoodieTableMetaClient dataTableMetaClient) {
      super(engineContext, dataTableWriteConfig, dataTableMetaClient);
    }

    List<IndexPartitionInitialization> callGetData(String dataTableInstantTime, String instantTimeForPartition,
                                                   Map<String, List<FileInfo>> partitionIdToAllFilesMap,
                                                   Lazy<List<FileSliceAndPartition>> lazyLatestMergedPartitionFileSliceList) throws IOException {
      return buildInitialization(dataTableInstantTime, instantTimeForPartition, partitionIdToAllFilesMap, lazyLatestMergedPartitionFileSliceList);
    }
  }
}
