/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.hudi.metadata.index;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.record.PartitionedRecordIndexer;
import org.apache.hudi.metadata.index.record.RecordIndexer;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestIndexerFactory {

  @Test
  void testMetadataDisabledReturnsEmptyMap() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.isEnabled()).thenReturn(false);

    Map<MetadataPartitionType, Indexer> enabled =
        IndexerFactory.getEnabledIndexerMap(engineContext, writeConfig, mock(HoodieTableMetaClient.class), mock(ExpressionIndexRecordGenerator.class));
    assertTrue(enabled.isEmpty());
  }

  @Test
  void testRecordIndexerSelectionByConfig() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.isEnabled()).thenReturn(true);
    when(writeConfig.getRecordMerger()).thenReturn(mock(HoodieRecordMerger.class));

    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.EIGHT);
    when(tableConfig.isTablePartitioned()).thenReturn(true);
    when(tableConfig.isMetadataPartitionAvailable(any(MetadataPartitionType.class))).thenReturn(false);
    when(tableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)).thenReturn(true);

    when(writeConfig.isRecordLevelIndexEnabled()).thenReturn(false);
    Map<MetadataPartitionType, Indexer> globalIndexers =
        IndexerFactory.getEnabledIndexerMap(engineContext, writeConfig, metaClient, mock(ExpressionIndexRecordGenerator.class));
    assertInstanceOf(RecordIndexer.class, globalIndexers.get(MetadataPartitionType.RECORD_INDEX));

    when(writeConfig.isRecordLevelIndexEnabled()).thenReturn(true);
    Map<MetadataPartitionType, Indexer> partitionedIndexers =
        IndexerFactory.getEnabledIndexerMap(engineContext, writeConfig, metaClient, mock(ExpressionIndexRecordGenerator.class));
    assertInstanceOf(PartitionedRecordIndexer.class, partitionedIndexers.get(MetadataPartitionType.RECORD_INDEX));
  }

  @Test
  void testTableVersionSixGatesExpressionAndSecondary() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("file:///tmp")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withExpressionIndexEnabled(true)
            .withSecondaryIndexEnabled(true)
            .withSecondaryIndexForColumn("col1")
            .build())
        .build();

    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.SIX);
    when(tableConfig.isTablePartitioned()).thenReturn(true);
    when(tableConfig.isMetadataPartitionAvailable(any(MetadataPartitionType.class))).thenReturn(false);

    Map<MetadataPartitionType, Indexer> enabled =
        IndexerFactory.getEnabledIndexerMap(engineContext, writeConfig, metaClient, mock(ExpressionIndexRecordGenerator.class));

    assertFalse(enabled.containsKey(MetadataPartitionType.EXPRESSION_INDEX));
    assertFalse(enabled.containsKey(MetadataPartitionType.SECONDARY_INDEX));
  }
}
