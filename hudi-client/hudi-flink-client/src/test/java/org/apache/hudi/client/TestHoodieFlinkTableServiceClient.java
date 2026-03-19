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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.FlinkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieFlinkClientTestHarness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHoodieFlinkTableServiceClient extends HoodieFlinkClientTestHarness {

  @BeforeEach
  void setUp() throws IOException {
    initPath();
    initFileSystem();
    initMetaClient();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testInitMetadataTableRespectsStreamingWriteFlag(boolean metadataStreamingWriteEnabled) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withStreamingWriteEnabled(metadataStreamingWriteEnabled)
            .build())
        .build();

    HoodieFlinkTable<?> table = mock(HoodieFlinkTable.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline inflightAndRequestedTimeline = mock(HoodieTimeline.class);
    when(table.getActiveTimeline()).thenReturn(activeTimeline);
    when(table.getTxnManager()).thenReturn(Option.of(mock(TransactionManager.class)));
    when(activeTimeline.filterInflightsAndRequested()).thenReturn(inflightAndRequestedTimeline);
    when(inflightAndRequestedTimeline.lastInstant()).thenReturn(Option.empty());

    FlinkHoodieBackedTableMetadataWriter metadataWriter = mock(FlinkHoodieBackedTableMetadataWriter.class);
    when(metadataWriter.isInitialized()).thenReturn(true);

    TestableHoodieFlinkTableServiceClient tableServiceClient =
        new TestableHoodieFlinkTableServiceClient(context, writeConfig, Option.empty(), table);
    try (MockedStatic<FlinkHoodieBackedTableMetadataWriter> writerFactory =
             Mockito.mockStatic(FlinkHoodieBackedTableMetadataWriter.class)) {
      writerFactory.when(() -> FlinkHoodieBackedTableMetadataWriter.create(any(), any(), any(), any()))
          .thenReturn(metadataWriter);

      tableServiceClient.initMetadataTable();
    } finally {
      tableServiceClient.close();
    }

    verify(metadataWriter, times(metadataStreamingWriteEnabled ? 0 : 1)).performTableServices(any());
    verify(table).deleteMetadataIndexIfNecessary();
    verify(table, never()).maybeDeleteMetadataTable();
  }

  private static class TestableHoodieFlinkTableServiceClient extends HoodieFlinkTableServiceClient<Object> {
    private final HoodieTable mockedTable;

    protected TestableHoodieFlinkTableServiceClient(HoodieFlinkEngineContext context,
                                                    HoodieWriteConfig clientConfig,
                                                    Option<EmbeddedTimelineService> timelineService,
                                                    HoodieTable mockedTable) {
      super(context, clientConfig, timelineService, (TransactionManager) mockedTable.getTxnManager().get());
      this.mockedTable = mockedTable;
    }

    @Override
    protected HoodieTable createTable(HoodieWriteConfig config, StorageConfiguration<?> storageConf, boolean skipValidation) {
      return mockedTable;
    }
  }
}
