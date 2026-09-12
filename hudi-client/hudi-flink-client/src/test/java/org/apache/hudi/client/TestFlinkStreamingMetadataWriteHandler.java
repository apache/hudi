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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestFlinkStreamingMetadataWriteHandler {

  @Test
  void abandonedStreamingWriteDoesNotLeakIntoLaterCompletion() {
    String instantTime = "001";
    HoodieTableMetadataWriter metadataWriter = mock(HoodieTableMetadataWriter.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTable table = mockTable(context);
    HoodieData<HoodieRecord> indexRecords = mock(HoodieData.class);
    HoodieData<WriteStatus> writeStatuses = mock(HoodieData.class);
    HoodieCommitMetadata commitMetadata = mock(HoodieCommitMetadata.class);
    when(metadataWriter.streamWriteToMetadataPartitions(indexRecords, Collections.emptySet(), instantTime))
        .thenReturn(writeStatuses);
    FlinkStreamingMetadataWriteHandler handler = new TestHandler(metadataWriter);

    handler.streamWriteToMetadataPartitions(table, indexRecords, Collections.emptySet(), instantTime);
    handler.cleanResources(instantTime);
    handler.commitToMetadataTable(table, instantTime, commitMetadata, Collections.emptyList());

    verify(metadataWriter).completeStreamingCommit(
        instantTime, context, Collections.emptyList(), commitMetadata, false);
  }

  @Test
  void updateOnlyStreamingWriteIsExplicitWhenNoMetadataStatsAreProduced() {
    String instantTime = "001";
    HoodieTableMetadataWriter metadataWriter = mock(HoodieTableMetadataWriter.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    HoodieTable table = mockTable(context);
    HoodieData<HoodieRecord> indexRecords = mock(HoodieData.class);
    HoodieData<WriteStatus> writeStatuses = mock(HoodieData.class);
    HoodieCommitMetadata commitMetadata = mock(HoodieCommitMetadata.class);
    when(metadataWriter.streamWriteToMetadataPartitions(indexRecords, Collections.emptySet(), instantTime))
        .thenReturn(writeStatuses);
    FlinkStreamingMetadataWriteHandler handler = new TestHandler(metadataWriter);

    assertSame(writeStatuses,
        handler.streamWriteToMetadataPartitions(table, indexRecords, Collections.emptySet(), instantTime));
    // The coordinator receives an index-write event even when its status list is empty.
    handler.markMetadataPartitionsWereStreamed(instantTime);
    handler.commitToMetadataTable(table, instantTime, commitMetadata, Collections.emptyList());

    verify(metadataWriter).completeStreamingCommit(
        instantTime, context, Collections.emptyList(), commitMetadata, true);
  }

  private HoodieTable mockTable(HoodieEngineContext context) {
    HoodieTable table = mock(HoodieTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(table.getContext()).thenReturn(context);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/"));
    return table;
  }

  private static class TestHandler extends FlinkStreamingMetadataWriteHandler {
    private final HoodieTableMetadataWriter metadataWriter;

    private TestHandler(HoodieTableMetadataWriter metadataWriter) {
      this.metadataWriter = metadataWriter;
    }

    @Override
    protected synchronized Option<HoodieTableMetadataWriter> getMetadataWriter(
        String triggeringInstant, HoodieTable table) {
      return Option.of(metadataWriter);
    }
  }
}
