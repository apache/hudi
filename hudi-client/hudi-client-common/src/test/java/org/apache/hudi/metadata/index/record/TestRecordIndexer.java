/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.hudi.metadata.index.record;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.model.DataPartitionAndRecords;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
      List<IndexPartitionInitialization> result = indexer.callGetData("001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList));
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
    indexer.callPost(metaClient, IndexPartitionInitialization.of(1, "record_index", records), "record_index");
    assertFalse(indexer.validateCalled);
    verify(records, times(1)).unpersist();

    when(metadataConfig.isRecordIndexInitializationValidationEnabled()).thenReturn(true);
    indexer.callPost(metaClient, IndexPartitionInitialization.of(1, "record_index", records), "record_index");
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
      List<IndexPartitionInitialization> result = indexer.callGetData("001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList));
      assertEquals(1, result.size());
      assertEquals(1, result.get(0).dataPartitionAndRecords().get(0).indexRecords().collectAsList().size());
      assertEquals("p_record", result.get(0).dataPartitionAndRecords().get(0).indexRecords().collectAsList().get(0).getRecordKey());
      mockedUtil.verify(() -> HoodieTableMetadataUtil.createRecordIndexDefinition(any(), any()), times(1));
    }
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

    List<IndexPartitionInitialization> callGetData(String dataTableInstantTime, String instantTimeForPartition,
                                                   Map<String, List<FileInfo>> partitionIdToAllFilesMap,
                                                   Lazy<List<FileSliceAndPartition>> lazyLatestMergedPartitionFileSliceList) throws IOException {
      return buildInitialization(dataTableInstantTime, instantTimeForPartition, partitionIdToAllFilesMap, lazyLatestMergedPartitionFileSliceList);
    }

    void callPost(HoodieTableMetaClient metadataMetaClient, IndexPartitionInitialization indexPartitionInitialization, String relativePartitionPath) {
      postInitialization(metadataMetaClient, indexPartitionInitialization.dataPartitionAndRecords().get(0).indexRecords(),
          indexPartitionInitialization.totalFileGroups(), relativePartitionPath);
    }
  }
}
