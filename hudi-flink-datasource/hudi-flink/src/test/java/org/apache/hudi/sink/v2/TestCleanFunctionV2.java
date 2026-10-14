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

package org.apache.hudi.sink.v2;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.compact.handler.CleanHandler;
import org.apache.hudi.sink.compact.handler.DefaultCleanHandler;
import org.apache.hudi.sink.compact.handler.TableServiceHandlerFactory;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/** Tests checkpoint-driven cleaning in {@link CleanFunctionV2}. */
class TestCleanFunctionV2 {

  @Test
  void testAsyncCleaningLifecycle() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, true);
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    CleanHandler cleanHandler = mock(CleanHandler.class);

    try (MockedStatic<TableServiceHandlerFactory> factory = mockStatic(TableServiceHandlerFactory.class)) {
      factory.when(() -> TableServiceHandlerFactory.createCleanHandler(conf, writeClient)).thenReturn(cleanHandler);
      try (OneInputStreamOperatorTestHarness<String, RowData> harness = openHarness(conf, writeClient)) {
        harness.processElement(new StreamRecord<>("ignored"));
        assertTrue(harness.getOutput().isEmpty());
        harness.snapshot(1, 1);
        harness.notifyOfCompletedCheckpoint(1);
      }
    }

    InOrder order = inOrder(cleanHandler);
    order.verify(cleanHandler).clean();
    order.verify(cleanHandler).startAsyncCleaning();
    order.verify(cleanHandler).waitForCleaningFinish();
    order.verify(cleanHandler).close();
    verifyNoMoreInteractions(cleanHandler);
    // The handler owns the write client and closes it itself.
    verify(writeClient, never()).close();
  }

  @Test
  void testCleaningIncludesMetadataTable() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, true);
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name());
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    HoodieFlinkWriteClient metadataClient = mock(HoodieFlinkWriteClient.class);

    try (MockedStatic<StreamerUtil> streamerUtil = mockStatic(StreamerUtil.class);
         MockedConstruction<DefaultCleanHandler> handlers = mockConstruction(DefaultCleanHandler.class)) {
      streamerUtil.when(() -> StreamerUtil.createMetadataWriteClient(writeClient)).thenReturn(metadataClient);
      try (OneInputStreamOperatorTestHarness<String, RowData> harness = openHarness(conf, writeClient)) {
        harness.snapshot(1, 1);
        harness.notifyOfCompletedCheckpoint(1);
      }

      assertEquals(2, handlers.constructed().size());
      for (CleanHandler handler : handlers.constructed()) {
        InOrder order = inOrder(handler);
        order.verify(handler).clean();
        order.verify(handler).startAsyncCleaning();
        order.verify(handler).waitForCleaningFinish();
        order.verify(handler).close();
        verifyNoMoreInteractions(handler);
      }
    }
  }

  @Test
  void testCleaningDisabled() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, false);
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);

    try (MockedStatic<TableServiceHandlerFactory> factory = mockStatic(TableServiceHandlerFactory.class)) {
      try (OneInputStreamOperatorTestHarness<String, RowData> harness = openHarness(conf, writeClient)) {
        harness.snapshot(1, 1);
        harness.notifyOfCompletedCheckpoint(1);
        harness.processElement(new StreamRecord<>("ignored"));
        assertTrue(harness.getOutput().isEmpty());
      }
      factory.verifyNoInteractions();
    }
    verifyNoInteractions(writeClient);
  }

  private OneInputStreamOperatorTestHarness<String, RowData> openHarness(
      Configuration conf,
      HoodieFlinkWriteClient writeClient) throws Exception {
    OneInputStreamOperatorTestHarness<String, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(new CleanFunctionV2<String>(conf)), 1, 1, 0);
    try (MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class)) {
      writeClients.when(() -> FlinkWriteClients.createWriteClient(
          eq(conf), any())).thenReturn(writeClient);
      harness.open();
      writeClients.verify(() -> FlinkWriteClients.createWriteClient(eq(conf), any()),
          times(conf.get(FlinkOptions.CLEAN_ASYNC_ENABLED) ? 1 : 0));
    }
    return harness;
  }
}
