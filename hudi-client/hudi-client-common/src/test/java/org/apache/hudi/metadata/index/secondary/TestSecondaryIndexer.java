/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.hudi.metadata.index.secondary;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
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
import java.util.Set;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TestSecondaryIndexer {

  @Test
  void testSkipWhenMultipleSecondaryPartitions() throws IOException {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedUtil.when(() -> HoodieTableMetadataUtil.getSecondaryIndexPartitionsToInit(any(), any(), any()))
          .thenReturn(Set.of("sec1", "sec2"));

      ExposedSecondaryIndexer indexer = new ExposedSecondaryIndexer(engineContext, writeConfig, metaClient);
      List<IndexPartitionInitialization> result = indexer.callGetData("001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList));
      assertTrue(result.isEmpty());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void testInitializeWithRealEngineContextAndIndexDataContent() throws IOException {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieIndexDefinition definition = mock(HoodieIndexDefinition.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getSecondaryIndexParallelism()).thenReturn(8);
    when(writeConfig.getProps()).thenReturn(new TypedProperties());

    HoodieData<HoodieRecord> records = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.parallelize(
        Collections.singletonList(HoodieMetadataPayload.createPartitionFilesRecord("p_sec",
            Collections.singletonMap("f_sec.parquet", 44L), Collections.emptyList())),
        1);

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class);
         MockedStatic<org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils> mockedSecondaryUtil = mockStatic(org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils.class)) {
      mockedUtil.when(() -> HoodieTableMetadataUtil.getSecondaryIndexPartitionsToInit(any(), any(), any()))
          .thenReturn(Collections.singleton("sec_idx"));
      mockedUtil.when(() -> HoodieTableMetadataUtil.getHoodieIndexDefinition("sec_idx", metaClient)).thenReturn(definition);
      mockedSecondaryUtil.when(() -> org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils.readSecondaryKeysFromFileSlices(any(), any(), anyInt(), any(), any(), any(), any()))
          .thenReturn(records);
      mockedUtil.when(() -> HoodieTableMetadataUtil.estimateFileGroupCount(any(), any(), anyInt(), anyInt(), anyInt(), anyFloat(), anyLong()))
          .thenReturn(7);

      ExposedSecondaryIndexer indexer = new ExposedSecondaryIndexer(engineContext, writeConfig, metaClient);
      List<IndexPartitionInitialization> initializationList = indexer.callGetData("001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList));
      assertEquals(1, initializationList.size());

      assertEquals("sec_idx", initializationList.get(0).indexPartitionName());
      assertEquals(7, initializationList.get(0).totalFileGroups());
      List<HoodieRecord> collected = initializationList.get(0).dataPartitionAndRecords().get(0).indexRecords().collectAsList();
      assertEquals(1, collected.size());
      assertEquals("p_sec", collected.get(0).getRecordKey());
    }
  }

  private static class ExposedSecondaryIndexer extends SecondaryIndexer {
    ExposedSecondaryIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig, HoodieTableMetaClient dataTableMetaClient) {
      super(engineContext, dataTableWriteConfig, dataTableMetaClient);
    }

    List<IndexPartitionInitialization> callGetData(String dataTableInstantTime, String instantTimeForPartition,
                                                   Map<String, List<FileInfo>> partitionIdToAllFilesMap,
                                                   Lazy<List<FileSliceAndPartition>> lazyLatestMergedPartitionFileSliceList) throws IOException {
      return buildInitialization(dataTableInstantTime, instantTimeForPartition, partitionIdToAllFilesMap, lazyLatestMergedPartitionFileSliceList);
    }
  }
}
