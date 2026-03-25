/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.hudi.metadata.index.partitionstats;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.model.IndexPartitionInitialization;
import org.apache.hudi.metadata.model.FileInfo;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.util.Lazy;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TestPartitionStatsIndexer {

  @Test
  void testSkipForNonPartitionedTable() throws IOException {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isTablePartitioned()).thenReturn(false);

    ExposedPartitionStatsIndexer indexer = new ExposedPartitionStatsIndexer(engineContext, writeConfig, metaClient);
    List<IndexPartitionInitialization> result = indexer.callGetData("001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList));
    assertTrue(result.isEmpty());
  }

  @Test
  void testSkipWhenColumnStatsDisabled() throws IOException {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isTablePartitioned()).thenReturn(true);
    when(writeConfig.isMetadataColumnStatsIndexEnabled()).thenReturn(false);

    ExposedPartitionStatsIndexer indexer = new ExposedPartitionStatsIndexer(engineContext, writeConfig, metaClient);
    List<IndexPartitionInitialization> result = indexer.callGetData("001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList));
    assertTrue(result.isEmpty());
  }

  @SuppressWarnings("unchecked")
  @Test
  void testInitializeWithRealEngineContextAndIndexDataContent() throws IOException {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieRecordMerger recordMerger = mock(HoodieRecordMerger.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isTablePartitioned()).thenReturn(true);
    when(writeConfig.isMetadataColumnStatsIndexEnabled()).thenReturn(true);
    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(writeConfig.getRecordMerger()).thenReturn(recordMerger);
    when(recordMerger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);
    when(metadataConfig.getPartitionStatsIndexFileGroupCount()).thenReturn(4);

    HoodieData<HoodieRecord> records = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.parallelize(
        Collections.singletonList(HoodieMetadataPayload.createPartitionFilesRecord("p_part",
            Collections.singletonMap("f_part.parquet", 33L), Collections.emptyList())),
        1);

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedUtil.when(() -> HoodieTableMetadataUtil.convertFilesToPartitionStatsRecords(any(), any(), any(), any(), any(), any()))
          .thenReturn(records);

      ExposedPartitionStatsIndexer indexer = new ExposedPartitionStatsIndexer(engineContext, writeConfig, metaClient);
      List<IndexPartitionInitialization> initializationList = indexer.callGetData("001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList));
      assertEquals(1, initializationList.size());

      assertEquals(4, initializationList.get(0).totalFileGroups());
      List<HoodieRecord> collected = initializationList.get(0).dataPartitionAndRecords().get(0).indexRecords().collectAsList();
      assertEquals(1, collected.size());
      assertEquals("p_part", collected.get(0).getRecordKey());
    }
  }

  private static class ExposedPartitionStatsIndexer extends PartitionStatsIndexer {
    ExposedPartitionStatsIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig, HoodieTableMetaClient dataTableMetaClient) {
      super(engineContext, dataTableWriteConfig, dataTableMetaClient);
    }

    List<IndexPartitionInitialization> callGetData(String dataTableInstantTime, String instantTimeForPartition,
                                                   Map<String, List<FileInfo>> partitionIdToAllFilesMap,
                                                   Lazy<List<FileSliceAndPartition>> lazyLatestMergedPartitionFileSliceList) throws IOException {
      return buildInitialization(dataTableInstantTime, instantTimeForPartition, partitionIdToAllFilesMap, lazyLatestMergedPartitionFileSliceList);
    }
  }
}
