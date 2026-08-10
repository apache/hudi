/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.hudi.metadata.index.partitionstats;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
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
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
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

    PartitionStatsIndexer indexer = new PartitionStatsIndexer(engineContext, writeConfig, metaClient);
    List<IndexInitializationPlan> result = indexer.buildInitialization(IndexInitializationContext.of(
        "001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList), Lazy.lazily(Option::empty)));
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

    PartitionStatsIndexer indexer = new PartitionStatsIndexer(engineContext, writeConfig, metaClient);
    List<IndexInitializationPlan> result = indexer.buildInitialization(IndexInitializationContext.of(
        "001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList), Lazy.lazily(Option::empty)));
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

      PartitionStatsIndexer indexer = new PartitionStatsIndexer(engineContext, writeConfig, metaClient);
      List<IndexInitializationPlan> initializationList = indexer.buildInitialization(IndexInitializationContext.of(
          "001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList), Lazy.lazily(Option::empty)));
      assertEquals(1, initializationList.size());

      assertEquals(4, initializationList.get(0).totalFileGroups());
      List<HoodieRecord> collected = initializationList.get(0).dataPartitionAndRecords().get(0).indexRecords().collectAsList();
      assertEquals(1, collected.size());
      assertEquals("p_part", collected.get(0).getRecordKey());
    }
  }

  @Test
  void testBuildUpdateThrowsWhenColumnStatsPartitionNotAvailable() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieRecordMerger recordMerger = mock(HoodieRecordMerger.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isMetadataPartitionAvailable(any(MetadataPartitionType.class))).thenReturn(false);
    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(writeConfig.getRecordMerger()).thenReturn(recordMerger);
    when(recordMerger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);

    PartitionStatsIndexer indexer = new PartitionStatsIndexer(engineContext, writeConfig, metaClient);
    assertThrows(IllegalStateException.class, () -> indexer.buildUpdate(IndexUpdateContext.of(
        "010",
        mock(HoodieBackedTableMetadata.class),
        Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
        new HoodieCommitMetadata())));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testBuildUpdateWithNonEmptyCommitMetadataProducesPartitionEntry() {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieEngineContext testDataEngineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieRecordMerger recordMerger = mock(HoodieRecordMerger.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isMetadataPartitionAvailable(any(MetadataPartitionType.class))).thenReturn(true);
    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(writeConfig.getRecordMerger()).thenReturn(recordMerger);
    when(recordMerger.getRecordType()).thenReturn(HoodieRecord.HoodieRecordType.AVRO);

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.UPSERT);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("p1");
    writeStat.setPath("p1/fileid-1_1-0-1_012.parquet");
    commitMetadata.getPartitionToWriteStats().put("p1", Collections.singletonList(writeStat));

    HoodieData<HoodieRecord> partitionStatsData = (HoodieData<HoodieRecord>) (HoodieData<?>) testDataEngineContext.parallelize(
        HoodieMetadataPayload.createPartitionStatsRecords(
            "p1",
            Collections.singletonList(HoodieColumnRangeMetadata.stub("p1/fileid-1_1-0-1_012.parquet", "c1", HoodieIndexVersion.V1)),
            false,
            true,
            Option.empty()).collect(java.util.stream.Collectors.toList()),
        1);

    PartitionStatsIndexer indexer = new PartitionStatsIndexer(engineContext, writeConfig, metaClient);
    List<IndexPartitionAndRecords> result;
    try (MockedStatic<PartitionStatsIndexer> mockedPartitionStatsIndexer = mockStatic(PartitionStatsIndexer.class)) {
      mockedPartitionStatsIndexer.when(() -> PartitionStatsIndexer.convertMetadataToPartitionStatsRecords(
              any(), any(), any(), any(), any(), any(), any(), any(), anyBoolean()))
          .thenReturn(partitionStatsData);

      result = indexer.buildUpdate(IndexUpdateContext.of(
          "012",
          mock(HoodieBackedTableMetadata.class),
          Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
          commitMetadata));
    }
    assertEquals(1, result.size());
    assertEquals(MetadataPartitionType.PARTITION_STATS.getPartitionPath(), result.get(0).indexPartitionName());
    List<HoodieRecord> indexRecords = result.get(0).indexRecords().collectAsList();
    assertEquals(1, indexRecords.size());
    HoodieMetadataPayload payload = (HoodieMetadataPayload) indexRecords.get(0).getData();
    assertTrue(payload.getColumnStatMetadata().isPresent());
    assertEquals("c1", payload.getColumnStatMetadata().get().getColumnName());
    assertFalse(payload.getColumnStatMetadata().get().getIsDeleted());
  }
}
