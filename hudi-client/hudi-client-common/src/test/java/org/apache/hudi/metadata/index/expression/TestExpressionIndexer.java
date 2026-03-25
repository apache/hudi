/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.hudi.metadata.index.expression;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.ExpressionIndexRecordGenerator;
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
import java.util.Set;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TestExpressionIndexer {

  @Test
  void testSkipWhenMultipleExpressionPartitions() throws IOException {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedUtil.when(() -> HoodieTableMetadataUtil.getExpressionIndexPartitionsToInit(any(), any(), any()))
          .thenReturn(Set.of("expr1", "expr2"));

      ExposedExpressionIndexer indexer = new ExposedExpressionIndexer(engineContext, writeConfig, metaClient, mock(ExpressionIndexRecordGenerator.class));
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
    ExpressionIndexRecordGenerator generator = mock(ExpressionIndexRecordGenerator.class);
    HoodieIndexDefinition definition = mock(HoodieIndexDefinition.class);
    HoodieSchema tableSchema = mock(HoodieSchema.class);
    HoodieSchema projectedSchema = mock(HoodieSchema.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getExpressionIndexFileGroupCount()).thenReturn(6);
    when(metadataConfig.getExpressionIndexParallelism()).thenReturn(10);

    HoodieData<HoodieRecord> records = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.parallelize(
        Collections.singletonList(HoodieMetadataPayload.createPartitionFilesRecord("p_expr",
            Collections.singletonMap("f_expr.parquet", 55L), Collections.emptyList())),
        1);
    when(generator.generate(any(), any(), any(), anyInt(), any(), any(), any(), any())).thenReturn(records);

    FileSlice fileSlice = new FileSlice("p1", "001", "f1");
    fileSlice.setBaseFile(new HoodieBaseFile("file:///tmp/p1/f1.parquet"));
    List<FileSliceAndPartition> fileSlices = Collections.singletonList(FileSliceAndPartition.of("p1", fileSlice));

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedUtil.when(() -> HoodieTableMetadataUtil.getExpressionIndexPartitionsToInit(any(), any(), any()))
          .thenReturn(Collections.singleton("expr_idx"));
      mockedUtil.when(() -> HoodieTableMetadataUtil.getHoodieIndexDefinition("expr_idx", metaClient)).thenReturn(definition);
      mockedUtil.when(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(metaClient)).thenReturn(Option.of(tableSchema));
      mockedUtil.when(() -> HoodieTableMetadataUtil.getProjectedSchemaForExpressionIndex(definition, metaClient, tableSchema)).thenReturn(projectedSchema);

      ExposedExpressionIndexer indexer = new ExposedExpressionIndexer(engineContext, writeConfig, metaClient, generator);
      List<IndexPartitionInitialization> initializationList = indexer.callGetData("001", "002", new HashMap<>(), Lazy.lazily(() -> fileSlices));
      assertEquals(1, initializationList.size());

      assertEquals("expr_idx", initializationList.get(0).indexPartitionName());
      List<HoodieRecord> collected = initializationList.get(0).dataPartitionAndRecords().get(0).indexRecords().collectAsList();
      assertEquals(1, collected.size());
      assertEquals("p_expr", collected.get(0).getRecordKey());
    }
  }

  private static class ExposedExpressionIndexer extends ExpressionIndexer {
    ExposedExpressionIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig,
                             HoodieTableMetaClient dataTableMetaClient, ExpressionIndexRecordGenerator expressionIndexRecordGenerator) {
      super(engineContext, dataTableWriteConfig, dataTableMetaClient, expressionIndexRecordGenerator);
    }

    List<IndexPartitionInitialization> callGetData(String dataTableInstantTime, String instantTimeForPartition,
                                                   Map<String, List<FileInfo>> partitionIdToAllFilesMap,
                                                   Lazy<List<FileSliceAndPartition>> lazyLatestMergedPartitionFileSliceList) throws IOException {
      return buildInitialization(dataTableInstantTime, instantTimeForPartition, partitionIdToAllFilesMap, lazyLatestMergedPartitionFileSliceList);
    }
  }
}
