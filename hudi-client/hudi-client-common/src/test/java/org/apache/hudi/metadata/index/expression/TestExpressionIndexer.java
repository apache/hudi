/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.hudi.metadata.index.expression;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.EngineIndexerSupport;
import org.apache.hudi.metadata.index.bloomfilters.BloomFiltersIndexer;
import org.apache.hudi.metadata.index.columnstats.ColumnStatsIndexer;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.model.FileSliceAndPartition;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
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

      ExpressionIndexer indexer = new ExpressionIndexer(engineContext, writeConfig, metaClient, mock(EngineIndexerSupport.class));
      List<IndexInitializationPlan> result = indexer.buildInitialization(IndexInitializationContext.of(
          "001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList), Lazy.lazily(Option::empty)));
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
    EngineIndexerSupport engineIndexerSupport = mock(EngineIndexerSupport.class);
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
    when(engineIndexerSupport.generateExpressionIndexRecords(any(), any(), any(), anyInt(), any(), any(), any(), any(), any())).thenReturn(records);

    String fileId = UUID.randomUUID().toString();
    FileSlice fileSlice = new FileSlice("p1", "001", fileId);
    String baseFileName = FSUtils.makeBaseFileName("001", "1-0-1", fileId, ".parquet");
    fileSlice.setBaseFile(new HoodieBaseFile("file:///tmp/p1/" + baseFileName));
    List<FileSliceAndPartition> fileSlices = Collections.singletonList(FileSliceAndPartition.of("p1", fileSlice));

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedUtil.when(() -> HoodieTableMetadataUtil.getExpressionIndexPartitionsToInit(any(), any(), any()))
          .thenReturn(Collections.singleton("expr_idx"));
      mockedUtil.when(() -> HoodieTableMetadataUtil.getHoodieIndexDefinition("expr_idx", metaClient)).thenReturn(definition);
      mockedUtil.when(() -> HoodieTableMetadataUtil.getProjectedSchemaForExpressionIndex(definition, metaClient, tableSchema)).thenReturn(projectedSchema);

      ExpressionIndexer indexer = new ExpressionIndexer(engineContext, writeConfig, metaClient, engineIndexerSupport);
      List<IndexInitializationPlan> initializationList = indexer.buildInitialization(IndexInitializationContext.of(
          "001", "002", new HashMap<>(), Lazy.lazily(() -> fileSlices), Lazy.lazily(() -> Option.of(tableSchema))));
      assertEquals(1, initializationList.size());

      assertEquals("expr_idx", initializationList.get(0).indexPartitionName());
      List<HoodieRecord> collected = initializationList.get(0).dataPartitionAndRecords().get(0).indexRecords().collectAsList();
      assertEquals(1, collected.size());
      assertEquals("p_expr", collected.get(0).getRecordKey());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void testBuildUpdateForAvailableExpressionPartitions() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    EngineIndexerSupport generator = mock(EngineIndexerSupport.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexDefinition indexDefinition = mock(HoodieIndexDefinition.class);
    HoodieSchema tableSchema = mock(HoodieSchema.class);
    HoodieSchema projectedSchema = mock(HoodieSchema.class);
    HoodieData<HoodieRecord> records = mock(HoodieData.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getExpressionIndexParallelism()).thenReturn(10);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isMetadataPartitionAvailable(any())).thenReturn(true);
    when(tableConfig.getMetadataPartitions()).thenReturn(Set.of("expr_index_idx"));
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(Collections.singletonMap("expr_index_idx", indexDefinition));
    when(indexDefinition.getIndexName()).thenReturn("expr_index_idx");
    when(indexDefinition.getIndexType()).thenReturn(PARTITION_NAME_BLOOM_FILTERS);
    when(generator.generateExpressionIndexRecords(any(), any(), any(), anyInt(), any(), any(), any(), any(), any())).thenReturn(records);

    List<IndexPartitionAndRecords> result;
    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class);
         MockedConstruction<TableSchemaResolver> mockedResolver =
             mockConstruction(TableSchemaResolver.class,
                 (resolver, ctx) -> when(resolver.getTableSchema()).thenReturn(tableSchema))) {
      mockedUtil.when(() -> HoodieTableMetadataUtil.getHoodieIndexDefinition("expr_index_idx", metaClient)).thenReturn(indexDefinition);
      mockedUtil.when(() -> HoodieTableMetadataUtil.getProjectedSchemaForExpressionIndex(indexDefinition, metaClient, tableSchema)).thenReturn(projectedSchema);

      ExpressionIndexer indexer = new ExpressionIndexer(engineContext, writeConfig, metaClient, generator);
      result = indexer.buildUpdate(IndexUpdateContext.of(
          "007",
          mock(HoodieBackedTableMetadata.class),
          Lazy.lazily(() -> null),
          new HoodieCommitMetadata()));
    }

    assertEquals(1, result.size());
    assertEquals("expr_index_idx", result.get(0).indexPartitionName());
    assertSame(records, result.get(0).indexRecords());
  }

  @Test
  void testBuildCleanThrowsWhenIndexMetadataMissing() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());

    ExpressionIndexer indexer = new ExpressionIndexer(engineContext, writeConfig, metaClient, mock(EngineIndexerSupport.class));
    assertThrows(RuntimeException.class, () -> indexer.buildClean(IndexCleanContext.of(
        "008", org.apache.hudi.avro.model.HoodieCleanMetadata.newBuilder().setPartitionMetadata(Collections.emptyMap()).build())));
  }

  @Test
  void testBuildCleanThrowsWhenIndexDefinitionsEmpty() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(Collections.emptyMap());

    ExpressionIndexer indexer = new ExpressionIndexer(engineContext, writeConfig, metaClient, mock(EngineIndexerSupport.class));
    assertThrows(RuntimeException.class, () -> indexer.buildClean(IndexCleanContext.of(
        "009", org.apache.hudi.avro.model.HoodieCleanMetadata.newBuilder().setPartitionMetadata(Collections.emptyMap()).build())));
  }

  @SuppressWarnings("unchecked")
  @Test
  void testBuildCleanForBloomExpressionIndex() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexDefinition indexDefinition = mock(HoodieIndexDefinition.class);
    org.apache.hudi.avro.model.HoodieCleanMetadata cleanMetadata = mock(org.apache.hudi.avro.model.HoodieCleanMetadata.class);
    HoodieData<HoodieRecord> records = mock(HoodieData.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(writeConfig.getBloomIndexParallelism()).thenReturn(3);
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(Collections.singletonMap("expr_index_bloom", indexDefinition));
    when(indexDefinition.getIndexName()).thenReturn("expr_index_bloom");
    when(indexDefinition.getIndexType()).thenReturn(PARTITION_NAME_BLOOM_FILTERS);

    try (MockedStatic<BloomFiltersIndexer> mockedBloom = mockStatic(BloomFiltersIndexer.class)) {
      mockedBloom.when(() -> BloomFiltersIndexer.convertMetadataToBloomFilterRecords(cleanMetadata, engineContext, "010", 3))
          .thenReturn(records);

      ExpressionIndexer indexer = new ExpressionIndexer(engineContext, writeConfig, metaClient, mock(EngineIndexerSupport.class));
      List<IndexPartitionAndRecords> result = indexer.buildClean(IndexCleanContext.of("010", cleanMetadata));

      assertEquals(1, result.size());
      assertEquals("expr_index_bloom", result.get(0).indexPartitionName());
      assertSame(records, result.get(0).indexRecords());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void testBuildCleanForColumnStatsExpressionIndex() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieRecordMerger recordMerger = mock(HoodieRecordMerger.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexDefinition indexDefinition = mock(HoodieIndexDefinition.class);
    org.apache.hudi.avro.model.HoodieCleanMetadata cleanMetadata = mock(org.apache.hudi.avro.model.HoodieCleanMetadata.class);
    HoodieData<HoodieRecord> records = mock(HoodieData.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getProps()).thenReturn(new TypedProperties());
    when(writeConfig.getRecordMerger()).thenReturn(recordMerger);
    when(recordMerger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(Collections.singletonMap("expr_index_col_stats", indexDefinition));
    when(indexDefinition.getIndexName()).thenReturn("expr_index_col_stats");
    when(indexDefinition.getIndexType()).thenReturn(PARTITION_NAME_COLUMN_STATS);
    when(indexDefinition.getSourceFields()).thenReturn(Collections.singletonList("c1"));

    try (MockedStatic<ColumnStatsIndexer> mockedColumnStats = mockStatic(ColumnStatsIndexer.class)) {
      mockedColumnStats.when(() -> ColumnStatsIndexer.convertMetadataToColumnStatsRecords(any(), any(), any(), any(), any()))
          .thenReturn(records);

      ExpressionIndexer indexer = new ExpressionIndexer(engineContext, writeConfig, metaClient, mock(EngineIndexerSupport.class));
      List<IndexPartitionAndRecords> result = indexer.buildClean(IndexCleanContext.of("011", cleanMetadata));

      assertEquals(1, result.size());
      assertEquals("expr_index_col_stats", result.get(0).indexPartitionName());
      assertSame(records, result.get(0).indexRecords());
    }
  }
}
