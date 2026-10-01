/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.compact.handler;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests write-client ownership in the compaction commit handler factory. */
class TestTableServiceHandlerFactory {
  private Configuration conf;
  private RuntimeContext runtimeContext;
  private HoodieFlinkWriteClient dataClient;
  private HoodieFlinkWriteClient metadataClient;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    conf = new Configuration();
    runtimeContext = mock(RuntimeContext.class);
    dataClient = mock(HoodieFlinkWriteClient.class);
    metadataClient = mock(HoodieFlinkWriteClient.class);
    when(dataClient.getHoodieTable()).thenReturn(mock(HoodieFlinkTable.class));
    when(metadataClient.getHoodieTable()).thenReturn(mock(HoodieFlinkTable.class));
  }

  @ParameterizedTest
  @CsvSource({"false,false", "true,false", "false,true", "true,true"})
  void testReturnedHandlerOwnsCreatedClients(boolean dataCompaction, boolean metadataCompaction) {
    configureCompaction(dataCompaction, metadataCompaction);
    try (CompactionCommitHandler handler = createHandler()) {
      if (dataCompaction && metadataCompaction) {
        assertInstanceOf(CompositeCompactionCommitHandler.class, handler);
      } else if (metadataCompaction) {
        assertInstanceOf(MetadataTableCompactionCommitHandler.class, handler);
      } else {
        assertInstanceOf(DataTableCompactionCommitHandler.class, handler);
      }
      verify(dataClient, never()).close();
      verify(metadataClient, never()).close();
    }
    verify(dataClient).close();
    if (metadataCompaction) {
      verify(metadataClient).close();
    } else {
      verifyNoInteractions(metadataClient);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testMetadataClientCreationFailureClosesDataClient(boolean dataCompaction) {
    configureCompaction(dataCompaction, true);
    RuntimeException failure = new RuntimeException("metadata client creation failed");
    RuntimeException closeFailure = new RuntimeException("data client close failed");
    doThrow(closeFailure).when(dataClient).close();
    try (MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class);
         MockedStatic<StreamerUtil> streamerUtil = mockStatic(StreamerUtil.class)) {
      writeClients.when(() -> FlinkWriteClients.createWriteClient(conf, runtimeContext)).thenReturn(dataClient);
      streamerUtil.when(() -> StreamerUtil.createMetadataWriteClient(dataClient)).thenThrow(failure);

      assertSame(failure, assertThrows(RuntimeException.class,
          () -> TableServiceHandlerFactory.createCompactionCommitHandler(conf, runtimeContext)));
    }
    verify(dataClient).close();
    verifyNoInteractions(metadataClient);
    assertArrayEquals(new Throwable[] {closeFailure}, failure.getSuppressed());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testMetadataHandlerConstructionFailureClosesBothClients(boolean dataCompaction) {
    configureCompaction(dataCompaction, true);
    RuntimeException failure = new RuntimeException("metadata handler construction failed");
    RuntimeException metadataCloseFailure = new RuntimeException("metadata client close failed");
    RuntimeException dataCloseFailure = new RuntimeException("data client close failed");
    when(metadataClient.getHoodieTable()).thenThrow(failure);
    doThrow(metadataCloseFailure).when(metadataClient).close();
    doThrow(dataCloseFailure).when(dataClient).close();

    assertSame(failure, assertThrows(RuntimeException.class, this::createHandler));

    verify(metadataClient).close();
    verify(dataClient).close();
    assertArrayEquals(new Throwable[] {metadataCloseFailure, dataCloseFailure}, failure.getSuppressed());
  }

  @Test
  void testDataHandlerConstructionFailureClosesDataClient() {
    configureCompaction(true, false);
    RuntimeException failure = new RuntimeException("data handler construction failed");
    when(dataClient.getHoodieTable()).thenThrow(failure);

    assertSame(failure, assertThrows(RuntimeException.class, this::createHandler));

    verify(dataClient).close();
    verifyNoInteractions(metadataClient);
  }

  @Test
  void testMetadataOnlyCloseFailureStillClosesDataClient() {
    configureCompaction(false, true);
    CompactionCommitHandler handler = createHandler();
    RuntimeException failure = new RuntimeException("metadata client close failed");
    RuntimeException closeFailure = new RuntimeException("data client close failed");
    doThrow(failure).when(metadataClient).close();
    doThrow(closeFailure).when(dataClient).close();

    assertSame(failure, assertThrows(RuntimeException.class, handler::close));

    verify(metadataClient).close();
    verify(dataClient).close();
    assertArrayEquals(new Throwable[] {closeFailure}, failure.getSuppressed());
  }

  private CompactionCommitHandler createHandler() {
    try (MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class);
         MockedStatic<StreamerUtil> streamerUtil = mockStatic(StreamerUtil.class)) {
      writeClients.when(() -> FlinkWriteClients.createWriteClient(conf, runtimeContext)).thenReturn(dataClient);
      streamerUtil.when(() -> StreamerUtil.createMetadataWriteClient(dataClient)).thenReturn(metadataClient);
      return TableServiceHandlerFactory.createCompactionCommitHandler(conf, runtimeContext);
    }
  }

  private void configureCompaction(boolean dataCompaction, boolean metadataCompaction) {
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, dataCompaction);
    conf.set(FlinkOptions.METADATA_ENABLED, metadataCompaction);
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name());
    conf.set(FlinkOptions.METADATA_COMPACTION_ASYNC_ENABLED, metadataCompaction);
  }
}
