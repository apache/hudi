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

package org.apache.hudi.sink.compact;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.compact.handler.CleanHandler;
import org.apache.hudi.sink.compact.handler.CompactionCommitHandler;
import org.apache.hudi.sink.compact.handler.TableServiceHandlerFactory;
import org.apache.hudi.sink.utils.MockStreamingRuntimeContext;
import org.apache.hudi.sink.v2.compact.CompactionCommitSinkV2;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/** Tests cleanup failure handling across both compaction sink APIs. */
class TestCompactionCommitSink {

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testClosePreservesCommitFailureAndSuppressesCleaningFailure(boolean useSinkV2) throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, true);
    MockStreamingRuntimeContext runtimeContext = new MockStreamingRuntimeContext(false, 1, 0);
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    CompactionCommitHandler commitHandler = mock(CompactionCommitHandler.class);
    CleanHandler cleanHandler = mock(CleanHandler.class);
    RuntimeException commitFailure = new RuntimeException("commit handler close failed");
    RuntimeException cleanFailure = new RuntimeException("clean handler close failed");
    doThrow(commitFailure).when(commitHandler).close();
    doThrow(cleanFailure).when(cleanHandler).close();

    try (MockedStatic<FlinkWriteClients> clients = mockStatic(FlinkWriteClients.class);
         MockedStatic<TableServiceHandlerFactory> factory = mockStatic(TableServiceHandlerFactory.class)) {
      clients.when(() -> FlinkWriteClients.createWriteClient(conf, runtimeContext)).thenReturn(writeClient);
      factory.when(() -> TableServiceHandlerFactory.createCleanHandler(conf, writeClient)).thenReturn(cleanHandler);
      factory.when(() -> TableServiceHandlerFactory.createCompactionCommitHandler(conf, runtimeContext)).thenReturn(commitHandler);

      AutoCloseable sink;
      if (useSinkV2) {
        CompactionCommitSinkV2 function = new CompactionCommitSinkV2(conf);
        function.setRuntimeContext(runtimeContext);
        function.open(conf);
        sink = function::close;
      } else {
        CompactionCommitSink function = new CompactionCommitSink(conf);
        function.setRuntimeContext(runtimeContext);
        function.open(conf);
        sink = function::close;
      }

      assertSame(commitFailure, assertThrows(RuntimeException.class, sink::close));
      assertArrayEquals(new Throwable[] {cleanFailure}, commitFailure.getSuppressed());
      InOrder order = inOrder(commitHandler, cleanHandler);
      order.verify(commitHandler).close();
      order.verify(cleanHandler).close();
    }
  }
}
