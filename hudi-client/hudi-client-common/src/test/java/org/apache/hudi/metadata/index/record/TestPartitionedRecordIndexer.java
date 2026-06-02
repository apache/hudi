/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.hudi.metadata.index.record;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.model.DataPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexPartitionInitialization;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.util.Lazy;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
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
      List<IndexPartitionInitialization> initializationList = indexer.buildInitialization("001", "002", Map.of(), Lazy.lazily(() -> input));

      mockedUtil.verify(() -> HoodieTableMetadataUtil.createRecordIndexDefinition(any(), any()));
      assertEquals(1, initializationList.size());
      assertEquals(2, initializationList.get(0).dataPartitionAndRecords().size());
      assertEquals(2, indexer.initializePartitionCalls);
    }
  }

  private static class ExposedPartitionedRecordIndexer extends PartitionedRecordIndexer {
    private final DataPartitionAndRecords p1;
    private final DataPartitionAndRecords p2;
    private int initializePartitionCalls;

    ExposedPartitionedRecordIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig,
                                    HoodieTableMetaClient dataTableMetaClient,
                                    DataPartitionAndRecords p1,
                                    DataPartitionAndRecords p2) {
      super(engineContext, dataTableWriteConfig, dataTableMetaClient);
      this.p1 = p1;
      this.p2 = p2;
    }

    @Override
    protected DataPartitionAndRecords initializeRecordIndexPartition(String dataPartition, List<FileSliceAndPartition> latestMergedPartitionFileSliceList,
                                                                     int recordIndexMaxParallelism) {
      initializePartitionCalls++;
      if ("p1".equals(dataPartition)) {
        return p1;
      }
      return p2;
    }
  }
}
