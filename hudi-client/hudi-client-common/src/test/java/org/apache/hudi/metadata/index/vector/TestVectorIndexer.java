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
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metadata.index.vector;

import org.apache.hudi.avro.model.HoodieVectorIndexManifest;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.VectorIndexMetadataKey;
import org.apache.hudi.metadata.index.EngineIndexerSupport;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestVectorIndexer {

  private static final String INDEX_PARTITION = "vector_index_embedding";
  private static final String INSTANT = "002";
  private static final int GENERATION = 3;

  @Test
  void testNoOpCommitStillDispatchesUpdateAndEmitsSourceMarker() {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexDefinition indexDefinition = mock(HoodieIndexDefinition.class);
    HoodieBackedTableMetadata tableMetadata = mock(HoodieBackedTableMetadata.class);
    EngineIndexerSupport engineSupport = mock(EngineIndexerSupport.class);
    HoodieSchema tableSchema = mock(HoodieSchema.class);

    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getMetadataPartitions()).thenReturn(Collections.singleton(INDEX_PARTITION));
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(
        Collections.singletonMap(INDEX_PARTITION, indexDefinition));
    when(indexDefinition.getIndexName()).thenReturn(INDEX_PARTITION);
    when(tableMetadata.getRecordsByKeyPrefixes(any(), any(), anyBoolean()))
        .thenReturn(HoodieListData.eager(List.of(
            HoodieMetadataPayload.createVectorIndexActiveManifestRecord(
                GENERATION, INDEX_PARTITION),
            currentManifestRecord())));
    when(engineSupport.generateVectorIndexUpdateRecords(
        any(), any(), any(), any(), any(), anyInt(), any()))
        .thenReturn(HoodieListData.eager(Collections.emptyList()));

    try (MockedStatic<HoodieTableMetadataUtil> metadataUtil = mockStatic(HoodieTableMetadataUtil.class);
         MockedConstruction<TableSchemaResolver> ignored = mockConstruction(
             TableSchemaResolver.class,
             (resolver, context) -> when(resolver.getTableSchema()).thenReturn(tableSchema))) {
      metadataUtil.when(() -> HoodieTableMetadataUtil.getHoodieIndexDefinition(
          INDEX_PARTITION, metaClient)).thenReturn(indexDefinition);

      VectorIndexer indexer = new VectorIndexer(
          engineContext, writeConfig, metaClient, engineSupport);
      List<IndexPartitionAndRecords> updates = indexer.buildUpdate(IndexUpdateContext.of(
          INSTANT,
          tableMetadata,
          Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
          new HoodieCommitMetadata()));

      assertEquals(1, updates.size());
      assertEquals(INDEX_PARTITION, updates.get(0).indexPartitionName());
      List<HoodieRecord> records = updates.get(0).indexRecords().collectAsList();
      assertEquals(1, records.size());
      assertEquals(
          VectorIndexMetadataKey.sourceInstantMarker(GENERATION, INSTANT),
          records.get(0).getRecordKey());
      verify(engineSupport).generateVectorIndexUpdateRecords(
          indexDefinition, metaClient, tableMetadata, Collections.emptyList(),
          tableSchema, GENERATION, INSTANT);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testArchivedMarkersControlContiguousCoverage(boolean markerPresent) {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexDefinition indexDefinition = mock(HoodieIndexDefinition.class);
    HoodieBackedTableMetadata tableMetadata = mock(HoodieBackedTableMetadata.class);
    EngineIndexerSupport engineSupport = mock(EngineIndexerSupport.class);
    HoodieSchema tableSchema = mock(HoodieSchema.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieArchivedTimeline archivedTimeline = mock(HoodieArchivedTimeline.class);
    HoodieTimeline activeWrites = mock(HoodieTimeline.class);
    HoodieTimeline mergedTimeline = mock(HoodieTimeline.class);
    HoodieTimeline mergedWrites = mock(HoodieTimeline.class);
    HoodieTimeline requiredWrites = mock(HoodieTimeline.class);
    HoodieInstant archivedWrite = mock(HoodieInstant.class);
    HoodieInstant currentWrite = mock(HoodieInstant.class);

    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getMetadataPartitions()).thenReturn(Collections.singleton(INDEX_PARTITION));
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(
        Collections.singletonMap(INDEX_PARTITION, indexDefinition));
    when(indexDefinition.getIndexName()).thenReturn(INDEX_PARTITION);
    List<HoodieRecord<HoodieMetadataPayload>> persistedMetadata = markerPresent
        ? List.of(
            HoodieMetadataPayload.createVectorIndexActiveManifestRecord(
                GENERATION, INDEX_PARTITION),
            manifestRecord("001"),
            HoodieMetadataPayload.createVectorIndexSourceInstantMarkerRecord(
                GENERATION, "002", 0L, INDEX_PARTITION))
        : List.of(
            HoodieMetadataPayload.createVectorIndexActiveManifestRecord(
                GENERATION, INDEX_PARTITION),
            manifestRecord("001"));
    when(tableMetadata.getRecordsByKeyPrefixes(any(), any(), anyBoolean()))
        .thenReturn(HoodieListData.eager(persistedMetadata));
    when(engineSupport.generateVectorIndexUpdateRecords(
        any(), any(), any(), any(), any(), anyInt(), any()))
        .thenReturn(HoodieListData.eager(Collections.emptyList()));
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.getWriteTimeline()).thenReturn(activeWrites);
    when(activeWrites.filterCompletedInstants()).thenReturn(activeWrites);
    when(activeWrites.isBeforeTimelineStarts("001")).thenReturn(true);
    when(metaClient.getArchivedTimeline("001")).thenReturn(archivedTimeline);
    when(archivedTimeline.mergeTimeline(activeWrites)).thenReturn(mergedTimeline);
    when(mergedTimeline.getWriteTimeline()).thenReturn(mergedWrites);
    when(mergedWrites.filterCompletedInstants()).thenReturn(mergedWrites);
    when(mergedWrites.findInstantsAfter("001")).thenReturn(requiredWrites);
    when(archivedWrite.requestedTime()).thenReturn("002");
    when(currentWrite.requestedTime()).thenReturn("003");
    when(requiredWrites.getInstantsAsStream()).thenReturn(
        Stream.of(archivedWrite, currentWrite));

    try (MockedStatic<HoodieTableMetadataUtil> metadataUtil = mockStatic(HoodieTableMetadataUtil.class);
         MockedConstruction<TableSchemaResolver> ignored = mockConstruction(
             TableSchemaResolver.class,
             (resolver, context) -> when(resolver.getTableSchema()).thenReturn(tableSchema))) {
      metadataUtil.when(() -> HoodieTableMetadataUtil.getHoodieIndexDefinition(
          INDEX_PARTITION, metaClient)).thenReturn(indexDefinition);

      List<HoodieRecord> records = new VectorIndexer(engineContext, writeConfig, metaClient, engineSupport)
          .buildUpdate(IndexUpdateContext.of(
              "003", tableMetadata,
              Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
              new HoodieCommitMetadata()))
          .get(0).indexRecords().collectAsList();

      assertEquals(markerPresent ? 2 : 1, records.size());
      Optional<HoodieVectorIndexManifest> advancedManifest = records.stream()
          .map(record -> ((HoodieMetadataPayload) record.getData()).getVectorIndexMetadata())
          .filter(Option::isPresent)
          .map(Option::get)
          .filter(HoodieVectorIndexManifest.class::isInstance)
          .map(HoodieVectorIndexManifest.class::cast)
          .findFirst();
      assertEquals(markerPresent, advancedManifest.isPresent());
      advancedManifest.ifPresent(manifest ->
          assertEquals("003", manifest.getLastContiguousSourceInstant()));
      assertTrue(records.stream().anyMatch(record -> VectorIndexMetadataKey
          .sourceInstantMarker(GENERATION, "003").equals(record.getRecordKey())));
      verify(metaClient).getArchivedTimeline("001");
    }
  }

  private static HoodieRecord currentManifestRecord() {
    return manifestRecord(INSTANT);
  }

  private static HoodieRecord manifestRecord(String lastCoveredInstant) {
    return HoodieMetadataPayload.createVectorIndexManifestRecord(
        GENERATION, Integer.toString(GENERATION), "ACTIVE",
        2, 2, 1, 1, 0, 1, 1,
        ByteBuffer.allocate(0), ByteBuffer.allocate(0), 1.1f,
        1, 1, "L2", false, false, "embedding",
        65536, 100, 1, 1, 0.0, 0.0, 0.0, 0.0,
        1, "checksum", 2, 1, lastCoveredInstant, 0L, INDEX_PARTITION);
  }
}
