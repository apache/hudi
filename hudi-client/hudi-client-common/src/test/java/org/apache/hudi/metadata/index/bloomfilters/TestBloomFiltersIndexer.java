/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.hudi.metadata.index.bloomfilters;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TestBloomFiltersIndexer {

  @SuppressWarnings("unchecked")
  @Test
  void testInitializeDataWithRealEngineContextAndIndexDataContent() throws IOException {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(writeConfig.getBloomIndexParallelism()).thenReturn(8);
    when(writeConfig.getBloomFilterType()).thenReturn("DYNAMIC_V0");
    when(metadataConfig.getBloomFilterIndexFileGroupCount()).thenReturn(3);

    HoodieData<HoodieRecord> records = (HoodieData<HoodieRecord>) (HoodieData<?>) engineContext.parallelize(
        Collections.singletonList(HoodieMetadataPayload.createPartitionFilesRecord("p_bloom",
            Collections.singletonMap("f1.parquet", 11L), Collections.emptyList())),
        1);

    try (MockedStatic<HoodieTableMetadataUtil> mockedUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      mockedUtil.when(() -> HoodieTableMetadataUtil.convertFilesToBloomFilterRecords(any(), any(), any(), any(), any(), anyInt(), any()))
          .thenReturn(records);

      BloomFiltersIndexer indexer = new BloomFiltersIndexer(engineContext, writeConfig, metaClient);
      List<IndexInitializationPlan> initializationList =
          indexer.buildInitialization(IndexInitializationContext.of(
              "001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList), Lazy.lazily(Option::empty)));
      assertEquals(1, initializationList.size());

      assertEquals(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath(), initializationList.get(0).indexPartitionName());
      assertEquals(3, initializationList.get(0).totalFileGroups());
      List<HoodieRecord> collected = initializationList.get(0).dataPartitionAndRecords().get(0).indexRecords().collectAsList();
      assertEquals(1, collected.size());
      assertEquals("p_bloom", collected.get(0).getRecordKey());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testBuildUpdateWithNonEmptyCommitMetadataAndWriteStat() throws Exception {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieEngineContext realEngineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(writeConfig.getBloomIndexParallelism()).thenReturn(4);
    when(writeConfig.getBloomFilterType()).thenReturn("DYNAMIC_V0");

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("p1");
    writeStat.setPath("p1/f1.parquet");
    commitMetadata.addWriteStat("p1", writeStat);

    HoodieData<HoodieWriteStat> writeStatsData = (HoodieData<HoodieWriteStat>) mock(HoodieData.class);
    String baseFileName = FSUtils.makeBaseFileName("004", "1-0-1", "fileid-1", ".parquet");
    HoodieData<HoodieRecord> bloomIndexData = (HoodieData<HoodieRecord>) (HoodieData<?>) realEngineContext.parallelize(
        Collections.singletonList(HoodieMetadataPayload.createBloomFilterMetadataRecord(
            "p1", baseFileName, "004", "DYNAMIC_V0",
            ByteBuffer.wrap("bf".getBytes(StandardCharsets.UTF_8)), false)),
        1);
    when(engineContext.parallelize(any(List.class), anyInt())).thenReturn((HoodieData) writeStatsData);
    when(writeStatsData.flatMap(any())).thenReturn((HoodieData) bloomIndexData);

    BloomFiltersIndexer indexer = new BloomFiltersIndexer(engineContext, writeConfig, metaClient);
    List<IndexPartitionAndRecords> result = indexer.buildUpdate(IndexUpdateContext.of(
        "004",
        mock(HoodieBackedTableMetadata.class),
        Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
        commitMetadata));

    assertEquals(1, result.size());
    assertEquals(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath(), result.get(0).indexPartitionName());
    List<HoodieRecord> indexRecords = result.get(0).indexRecords().collectAsList();
    assertEquals(1, indexRecords.size());
    HoodieMetadataPayload payload = (HoodieMetadataPayload) indexRecords.get(0).getData();
    assertTrue(payload.getBloomFilterMetadata().isPresent());
    assertEquals("DYNAMIC_V0", payload.getBloomFilterMetadata().get().getType());
    assertEquals("004", payload.getBloomFilterMetadata().get().getTimestamp());
    assertFalse(payload.getBloomFilterMetadata().get().getIsDeleted());
    assertEquals("bf", new String(payload.getBloomFilterMetadata().get().getBloomFilter().array(), StandardCharsets.UTF_8));
  }

  @Test
  void testBuildCleanProducesDeleteRecordsForBaseFilesOnly() {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(writeConfig.getBloomIndexParallelism()).thenReturn(4);

    HoodieCleanMetadata cleanMetadata = mock(HoodieCleanMetadata.class);
    org.apache.hudi.avro.model.HoodieCleanPartitionMetadata partitionMetadata = mock(org.apache.hudi.avro.model.HoodieCleanPartitionMetadata.class);
    when(cleanMetadata.getPartitionMetadata()).thenReturn(Collections.singletonMap("p1", partitionMetadata));
    when(partitionMetadata.getDeletePathPatterns()).thenReturn(Collections.singletonList("f1_1-0-1_000.parquet"));

    BloomFiltersIndexer indexer = new BloomFiltersIndexer(engineContext, writeConfig, metaClient);
    List<IndexPartitionAndRecords> result = indexer.buildClean(IndexCleanContext.of("005", cleanMetadata));

    assertEquals(1, result.size());
    assertEquals(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath(), result.get(0).indexPartitionName());
    assertEquals(1, result.get(0).indexRecords().collectAsList().size());
  }
}
