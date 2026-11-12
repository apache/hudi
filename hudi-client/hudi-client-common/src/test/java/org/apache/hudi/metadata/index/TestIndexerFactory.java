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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.record.PartitionedRecordIndexer;
import org.apache.hudi.metadata.index.record.RecordIndexer;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        IndexerFactory.getEnabledIndexerMap(engineContext, writeConfig, mock(HoodieTableMetaClient.class), mock(EngineIndexerSupport.class), Option.empty(), false);
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
        IndexerFactory.getEnabledIndexerMap(engineContext, writeConfig, metaClient, mock(EngineIndexerSupport.class), Option.empty(), false);
    assertInstanceOf(RecordIndexer.class, globalIndexers.get(MetadataPartitionType.RECORD_INDEX));

    when(writeConfig.isRecordLevelIndexEnabled()).thenReturn(true);
    Map<MetadataPartitionType, Indexer> partitionedIndexers =
        IndexerFactory.getEnabledIndexerMap(engineContext, writeConfig, metaClient, mock(EngineIndexerSupport.class), Option.empty(), false);
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
        IndexerFactory.getEnabledIndexerMap(engineContext, writeConfig, metaClient, mock(EngineIndexerSupport.class), Option.empty(), false);

    assertFalse(enabled.containsKey(MetadataPartitionType.EXPRESSION_INDEX));
    assertFalse(enabled.containsKey(MetadataPartitionType.SECONDARY_INDEX));
  }

  @Test
  void testRecordCountIsReusedAtTheSameCompletionTime() {
    AtomicInteger scans = new AtomicInteger();
    Supplier<Long> recordCounter = () -> {
      scans.incrementAndGet();
      return 42L;
    };
    String basePath = "/tmp/record_count_cache_" + UUID.randomUUID();
    assertEquals(42L, IndexerFactory.getOrComputeRecordCount(basePath, "001", recordCounter));
    assertEquals(42L, IndexerFactory.getOrComputeRecordCount(basePath, "001", recordCounter));
    assertEquals(1, scans.get());
    assertEquals(42L, IndexerFactory.getOrComputeRecordCount(basePath, "002", recordCounter));
    assertEquals(2, scans.get());
  }

  /**
   * Once the table is over the threshold, the record index is still deferred while an instant other than the one the
   * metadata writer is created for is pending. Pending indexing instants do not hold it back.
   */
  @ParameterizedTest
  @CsvSource({
      // table version, pending instant, its action, expect deferred
      "SIX, 002, commit, true",
      "SIX, 002, compaction, true",
      "SIX, 002, indexing, false",
      "SIX, 003, commit, false",
      "TEN, 002, commit, true",
      "TEN, 002, compaction, true",
      "TEN, 002, indexing, false",
      "TEN, 003, commit, false"})
  void testRecordIndexDeferredWhileOtherInstantsArePending(HoodieTableVersion tableVersion, String pendingInstantTime, String pendingAction,
                                                            boolean expectDeferred) {
    HoodieInstant pendingInstant = mock(HoodieInstant.class);
    when(pendingInstant.isCompleted()).thenReturn(false);
    when(pendingInstant.getAction()).thenReturn(pendingAction);
    when(pendingInstant.requestedTime()).thenReturn(pendingInstantTime);
    assertEquals(!expectDeferred, isRecordIndexEnabledOverThreshold(tableVersion, Collections.singletonList(pendingInstant), false));
  }

  /**
   * A metadata writer of streaming writes, which is created outside the table lock, never builds the record index, even
   * when the table is over the threshold and no other instant is pending.
   */
  @ParameterizedTest
  @CsvSource({"true, true", "false, false"})
  void testRecordIndexNotBuiltByStreamingWrites(boolean streamingWrites, boolean expectDeferred) {
    assertEquals(!expectDeferred, isRecordIndexEnabledOverThreshold(HoodieTableVersion.TEN, Collections.emptyList(), streamingWrites));
  }

  /**
   * Returns whether the record index is enabled for a metadata writer of instant 003, on a table whose latest base files
   * hold 5000 records, over the threshold of 1000, and has the given pending instants.
   */
  private static boolean isRecordIndexEnabledOverThreshold(HoodieTableVersion tableVersion, List<HoodieInstant> pendingInstants,
                                                           boolean streamingWrites) {
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(writeConfig.getRecordMerger()).thenReturn(mock(HoodieRecordMerger.class));
    when(metadataConfig.isEnabled()).thenReturn(true);
    when(metadataConfig.isGlobalRecordLevelIndexEnabled()).thenReturn(true);
    when(metadataConfig.shouldDeferRliInitForFreshTable()).thenReturn(true);
    when(metadataConfig.getDeferRliInitMinRecords()).thenReturn(1000L);

    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    when(tableConfig.getTableVersion()).thenReturn(tableVersion);
    when(tableConfig.getMetadataPartitions()).thenReturn(Collections.emptySet());
    when(tableConfig.getMetadataPartitionsInflight()).thenReturn(Collections.emptySet());
    String basePath = "/tmp/over_threshold_" + UUID.randomUUID();
    when(metaClient.getBasePath()).thenReturn(new StoragePath(basePath));

    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.getInstantsAsStream()).thenAnswer(invocation -> pendingInstants.stream());
    when(activeTimeline.filterCompletedInstants()).thenReturn(completedTimeline);
    when(completedTimeline.countInstants()).thenReturn(1);
    when(completedTimeline.getLatestCompletionTime()).thenReturn(Option.of("001"));
    IndexerFactory.getOrComputeRecordCount(basePath, "001", () -> 5000L);

    return IndexerFactory.getEnabledIndexerMap(mock(HoodieEngineContext.class), writeConfig, metaClient, mock(EngineIndexerSupport.class),
        Option.of("003"), streamingWrites).containsKey(MetadataPartitionType.RECORD_INDEX);
  }

  /**
   * Deferral cases that are decided without counting records. The threshold is 1000 records in every case.
   */
  @ParameterizedTest
  @CsvSource({
      // table version, completed commits, async index, record index state, expect deferred
      "SIX, 0, false, ABSENT, true",
      "SIX, 0, true, ABSENT, true",
      "SIX, 1, true, ABSENT, false",
      "SIX, 3, false, AVAILABLE, false",
      "SIX, 3, false, INFLIGHT, false",
      "TEN, 0, false, ABSENT, true",
      "TEN, 0, true, ABSENT, true",
      "TEN, 1, true, ABSENT, false",
      "TEN, 3, false, AVAILABLE, false",
      "TEN, 3, false, INFLIGHT, false"})
  void testRecordIndexDeferral(HoodieTableVersion tableVersion, int completedCommits, boolean asyncIndex, String recordIndexState,
                               boolean expectDeferred) {
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    when(writeConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(writeConfig.getRecordMerger()).thenReturn(mock(HoodieRecordMerger.class));
    when(writeConfig.isMetadataAsyncIndex()).thenReturn(asyncIndex);
    when(metadataConfig.isEnabled()).thenReturn(true);
    when(metadataConfig.isGlobalRecordLevelIndexEnabled()).thenReturn(true);
    when(metadataConfig.shouldDeferRliInitForFreshTable()).thenReturn(true);
    when(metadataConfig.getDeferRliInitMinRecords()).thenReturn(1000L);

    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());
    when(tableConfig.getTableVersion()).thenReturn(tableVersion);
    Set<String> recordIndexPartition = Collections.singleton(MetadataPartitionType.RECORD_INDEX.getPartitionPath());
    when(tableConfig.getMetadataPartitions()).thenReturn(recordIndexState.equals("AVAILABLE") ? recordIndexPartition : Collections.emptySet());
    when(tableConfig.getMetadataPartitionsInflight()).thenReturn(recordIndexState.equals("INFLIGHT") ? recordIndexPartition : Collections.emptySet());
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filterCompletedInstants()).thenReturn(completedTimeline);
    when(completedTimeline.countInstants()).thenReturn(completedCommits);

    Map<MetadataPartitionType, Indexer> enabled =
        IndexerFactory.getEnabledIndexerMap(mock(HoodieEngineContext.class), writeConfig, metaClient, mock(EngineIndexerSupport.class), Option.empty(), false);
    assertEquals(!expectDeferred, enabled.containsKey(MetadataPartitionType.RECORD_INDEX));
  }
}
