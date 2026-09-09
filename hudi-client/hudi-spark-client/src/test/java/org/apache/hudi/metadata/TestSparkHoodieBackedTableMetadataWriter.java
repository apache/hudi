/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDMetadataWriteClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieSparkIndexClient;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.Collections;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests Spark-specific behavior shared by current and table-version-six metadata writers.
 */
class TestSparkHoodieBackedTableMetadataWriter {

  @Test
  void exposesSparkEngineAndRejectsV6StreamingConversion() {
    // Both implementations use Spark, but version 6 has no streaming conversion.
    SparkHoodieBackedTableMetadataWriter currentWriter =
        mock(SparkHoodieBackedTableMetadataWriter.class, CALLS_REAL_METHODS);
    SparkHoodieBackedTableMetadataWriterTableVersionSix versionSixWriter =
        mock(SparkHoodieBackedTableMetadataWriterTableVersionSix.class, CALLS_REAL_METHODS);

    assertEquals(EngineType.SPARK, currentWriter.getEngineType());
    assertEquals(EngineType.SPARK, versionSixWriter.getEngineType());
    assertThrows(HoodieNotSupportedException.class,
        () -> versionSixWriter.convertEngineSpecificDataToHoodieData(null));
  }

  @Test
  void versionSixFileGroupUpsertCommitsWriteStatuses() {
    // Version 6 file-group writes use a prepped-record upsert and delta commit.
    SparkHoodieBackedTableMetadataWriterTableVersionSix writer =
        mock(SparkHoodieBackedTableMetadataWriterTableVersionSix.class, CALLS_REAL_METHODS);
    BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> writeClient =
        mock(BaseHoodieWriteClient.class);
    JavaRDD<HoodieRecord> records = mock(JavaRDD.class);
    JavaRDD<WriteStatus> writeStatuses = mock(JavaRDD.class);
    when(writeClient.upsertPreppedRecords(records, "001")).thenReturn(writeStatuses);

    writer.upsertAndCommit(writeClient, "001", records, Collections.emptyList());

    verify(writeClient).commit(
        "001", writeStatuses, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
  }

  @Test
  void currentFileGroupUpsertCommitsFirstUpsertStatuses() {
    // Current writer uses first-upsert semantics when file groups are supplied.
    SparkHoodieBackedTableMetadataWriter writer =
        mock(SparkHoodieBackedTableMetadataWriter.class, CALLS_REAL_METHODS);
    SparkRDDMetadataWriteClient writeClient = mock(SparkRDDMetadataWriteClient.class);
    JavaRDD<HoodieRecord> records = mock(JavaRDD.class);
    JavaRDD<WriteStatus> writeStatuses = mock(JavaRDD.class);
    java.util.List<HoodieFileGroupId> fileGroups = Collections.emptyList();
    // The writer resolves its internal client even when one is passed in.
    doReturn(writeClient).when(writer).getWriteClient();
    when(writeClient.firstUpsertPreppedRecords(records, "003", fileGroups)).thenReturn(writeStatuses);

    writer.upsertAndCommit(writeClient, "003", records, fileGroups);

    verify(writeClient).commit(
        "003", writeStatuses, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
  }

  @Test
  void currentUpsertCoalescesRecordPreparationInput() {
    // Record preparation honors its configured parallelism before writing.
    SparkHoodieBackedTableMetadataWriter writer =
        mock(SparkHoodieBackedTableMetadataWriter.class, CALLS_REAL_METHODS);
    HoodieMetadataConfig metadataConfig = mock(HoodieMetadataConfig.class);
    HoodieWriteConfig dataWriteConfig = mock(HoodieWriteConfig.class);
    when(dataWriteConfig.getMetadataConfig()).thenReturn(metadataConfig);
    when(metadataConfig.getRecordPreparationParallelism()).thenReturn(1);
    writer.dataWriteConfig = dataWriteConfig;

    BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> writeClient =
        mock(BaseHoodieWriteClient.class);
    JavaRDD<HoodieRecord> records = mock(JavaRDD.class);
    JavaRDD<HoodieRecord> coalescedRecords = mock(JavaRDD.class);
    JavaRDD<WriteStatus> writeStatuses = mock(JavaRDD.class);
    when(records.getNumPartitions()).thenReturn(2);
    when(records.coalesce(1)).thenReturn(coalescedRecords);
    when(writeClient.upsertPreppedRecords(coalescedRecords, "004")).thenReturn(writeStatuses);

    writer.upsertAndCommit(writeClient, "004", records);

    verify(writeClient).commit(
        "004", writeStatuses, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
  }

  @Test
  void bothSparkWritersUpdateColumnStatsDefinition() {
    // Both writer versions delegate column-stat definitions to the Spark index client.
    SparkHoodieBackedTableMetadataWriter currentWriter =
        mock(SparkHoodieBackedTableMetadataWriter.class, CALLS_REAL_METHODS);
    SparkHoodieBackedTableMetadataWriterTableVersionSix versionSixWriter =
        mock(SparkHoodieBackedTableMetadataWriterTableVersionSix.class, CALLS_REAL_METHODS);
    java.util.List<String> columns = Collections.singletonList("rider");

    // Capture index clients created internally by both writer versions.
    try (MockedConstruction<HoodieSparkIndexClient> construction =
             mockConstruction(HoodieSparkIndexClient.class)) {
      currentWriter.updateColumnsToIndexWithColStats(columns);
      versionSixWriter.updateColumnsToIndexWithColStats(columns);

      assertEquals(2, construction.constructed().size());
      verify(construction.constructed().get(0))
          .createOrUpdateColumnStatsIndexDefinition(null, columns);
      verify(construction.constructed().get(1))
          .createOrUpdateColumnStatsIndexDefinition(null, columns);
    }
  }

  @Test
  void versionSixDeletePartitionsCommitsReplaceCommit() {
    // Deleting an MDT partition is committed as a replace commit.
    SparkHoodieBackedTableMetadataWriterTableVersionSix writer =
        mock(SparkHoodieBackedTableMetadataWriterTableVersionSix.class, CALLS_REAL_METHODS);
    SparkRDDWriteClient writeClient = mock(SparkRDDWriteClient.class);
    HoodieWriteResult writeResult = mock(HoodieWriteResult.class);
    JavaRDD<WriteStatus> writeStatuses = mock(JavaRDD.class);
    HoodieTableMetaClient metadataMetaClient = mock(HoodieTableMetaClient.class);
    writer.metadataMetaClient = metadataMetaClient;
    doReturn(writeClient).when(writer).getWriteClient();
    when(writeResult.getWriteStatuses()).thenReturn(writeStatuses);
    when(writeResult.getPartitionToReplaceFileIds()).thenReturn(Collections.emptyMap());
    when(writeClient.deletePartitions(
        Collections.singletonList(MetadataPartitionType.RECORD_INDEX.getPartitionPath()), "002"))
        .thenReturn(writeResult);

    writer.deletePartitions("002", Collections.singletonList(MetadataPartitionType.RECORD_INDEX));

    verify(writeClient).startCommitForMetadataTable(eq(metadataMetaClient), eq("002"), any());
    verify(writeClient).commit(
        "002", writeStatuses, Option.empty(), REPLACE_COMMIT_ACTION, Collections.emptyMap());
  }
}
