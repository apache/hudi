/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.hudi.metadata.index.secondary;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
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

      SecondaryIndexer indexer = new SecondaryIndexer(engineContext, writeConfig, metaClient);
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

      SecondaryIndexer indexer = new SecondaryIndexer(engineContext, writeConfig, metaClient);
      List<IndexInitializationPlan> initializationList = indexer.buildInitialization(IndexInitializationContext.of(
          "001", "002", Collections.emptyMap(), Lazy.lazily(Collections::emptyList), Lazy.lazily(Option::empty)));
      assertEquals(1, initializationList.size());

      assertEquals("sec_idx", initializationList.get(0).indexPartitionName());
      assertEquals(7, initializationList.get(0).totalFileGroups());
      List<HoodieRecord> collected = initializationList.get(0).dataPartitionAndRecords().get(0).indexRecords().collectAsList();
      assertEquals(1, collected.size());
      assertEquals("p_sec", collected.get(0).getRecordKey());
    }
  }

  @Test
  void testBuildUpdateReturnsEmptyWhenSecondaryIndexUnavailable() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getIndexMetadata()).thenReturn(org.apache.hudi.common.util.Option.empty());
    SecondaryIndexer indexer = new SecondaryIndexer(
        mock(HoodieEngineContext.class), mock(HoodieWriteConfig.class), metaClient);
    assertTrue(indexer.buildUpdate(IndexUpdateContext.of(
        "013",
        mock(HoodieBackedTableMetadata.class),
        Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
        new HoodieCommitMetadata())).isEmpty());
  }

  @Test
  void testBuildUpdateThrowsForDeletePartitionOperation() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexDefinition indexDefinition = mock(HoodieIndexDefinition.class);

    when(metaClient.getIndexMetadata()).thenReturn(org.apache.hudi.common.util.Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(Collections.singletonMap("secondary_index_idx", indexDefinition));
    when(indexDefinition.getIndexName()).thenReturn("secondary_index_idx");

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.DELETE_PARTITION);

    SecondaryIndexer indexer = new SecondaryIndexer(engineContext, writeConfig, metaClient);
    assertThrows(RuntimeException.class, () -> indexer.buildUpdate(IndexUpdateContext.of(
        "014",
        mock(HoodieBackedTableMetadata.class),
        Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
        commitMetadata)));
  }

  @Test
  void testBuildUpdateForCompactReturnsEmpty() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexDefinition indexDefinition = mock(HoodieIndexDefinition.class);

    when(metaClient.getIndexMetadata()).thenReturn(org.apache.hudi.common.util.Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(Collections.singletonMap("secondary_index_idx", indexDefinition));
    when(indexDefinition.getIndexName()).thenReturn("secondary_index_idx");

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.COMPACT);

    SecondaryIndexer indexer = new SecondaryIndexer(engineContext, writeConfig, metaClient);
    assertTrue(indexer.buildUpdate(IndexUpdateContext.of(
        "015",
        mock(HoodieBackedTableMetadata.class),
        Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
        commitMetadata)).isEmpty());
  }

  /** Mocks a table with one secondary index partition, {@code secondary_index_idx}, on top of the given write config. */
  private static HoodieTableMetaClient mockMetaClientWithSecondaryIndex(HoodieWriteConfig writeConfig, HoodieMetadataConfig metadataConfig,
                                                                       HoodieIndexDefinition indexDefinition) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/table"));
    when(metaClient.getIndexForMetadataPartition("secondary_index_idx")).thenReturn(Option.of(indexDefinition));
    when(tableConfig.getMetadataPartitions()).thenReturn(Collections.singleton("secondary_index_idx"));
    when(indexMetadata.getIndexDefinitions()).thenReturn(Collections.singletonMap("secondary_index_idx", indexDefinition));
    when(indexDefinition.getIndexName()).thenReturn("secondary_index_idx");
    return metaClient;
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testBuildUpdateForReplaceCommitFromExternalWriterWithoutWriteStats(boolean dropsFileGroups) {
    // files written outside Hudi are registered through replace commits without a known operation type. A fresh
    // HoodieCommitMetadata carries UNKNOWN; a writer may also leave the type null. A commit that only drops files has
    // no write stats but still removes the records of the replaced file groups from the index; one that drops nothing
    // and writes nothing leaves the index alone.
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieIndexDefinition indexDefinition = mock(HoodieIndexDefinition.class);
    HoodieTableMetaClient metaClient = mockMetaClientWithSecondaryIndex(writeConfig, metadataConfig, indexDefinition);

    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();
    commitMetadata.setOperationType(null);
    if (dropsFileGroups) {
      commitMetadata.addReplaceFileId("p1", "file_1.parquet");
    }

    HoodieData<HoodieRecord> deletes = engineContext.parallelize(Collections.singletonList(
        HoodieMetadataPayload.createSecondaryIndexRecord("p1/file_1.parquet_0", "alice", "secondary_index_idx", true)), 1);
    try (MockedStatic<SecondaryIndexRecordGenerationUtils> mockedGenerationUtils = mockStatic(SecondaryIndexRecordGenerationUtils.class)) {
      mockedGenerationUtils.when(() -> SecondaryIndexRecordGenerationUtils.convertWriteStatsToSecondaryIndexRecords(
              eq(Collections.emptyList()), eq("016"), eq(indexDefinition), eq(metadataConfig), eq(metaClient), eq(engineContext), eq(writeConfig), eq(commitMetadata)))
          .thenReturn(deletes);

      SecondaryIndexer indexer = new SecondaryIndexer(engineContext, writeConfig, metaClient);
      List<IndexPartitionAndRecords> result = indexer.buildUpdate(IndexUpdateContext.of(
          "016",
          mock(HoodieBackedTableMetadata.class),
          Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
          commitMetadata));

      assertEquals(1, result.size());
      assertEquals("secondary_index_idx", result.get(0).indexPartitionName());
      assertEquals(dropsFileGroups ? deletes.collectAsList() : Collections.emptyList(), result.get(0).indexRecords().collectAsList());
    }
  }

  @Test
  void testBuildCleanReturnsEmpty() {
    SecondaryIndexer indexer = new SecondaryIndexer(
        mock(HoodieEngineContext.class), mock(HoodieWriteConfig.class), mock(HoodieTableMetaClient.class));
    assertTrue(indexer.buildClean(IndexCleanContext.of("017", mock(HoodieCleanMetadata.class))).isEmpty());
  }
}
