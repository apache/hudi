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

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class TestDefaultCleanHandler {

  @Test
  void testCloseWaitsForInitialCleaning() {
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    try (DefaultCleanHandler handler = new DefaultCleanHandler(writeClient)) {
      handler.clean();
    }
    InOrder order = inOrder(writeClient);
    order.verify(writeClient).clean();
    order.verify(writeClient).close();
    verifyNoMoreInteractions(writeClient);
  }

  @Test
  void testCheckpointCleaningDoesNotStartTwice() {
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    try (DefaultCleanHandler handler = new DefaultCleanHandler(writeClient)) {
      handler.startAsyncCleaning();
      handler.startAsyncCleaning();
      handler.waitForCleaningFinish();
    }
    InOrder order = inOrder(writeClient);
    order.verify(writeClient).startAsyncCleaning();
    order.verify(writeClient).waitForCleaningFinish();
    order.verify(writeClient).close();
    verifyNoMoreInteractions(writeClient);
  }

  @Test
  void testCleaningStartFailureAllowsRetry() {
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    doThrow(new RuntimeException("expected")).doNothing().when(writeClient).startAsyncCleaning();
    try (DefaultCleanHandler handler = new DefaultCleanHandler(writeClient)) {
      assertDoesNotThrow(handler::startAsyncCleaning);
      handler.startAsyncCleaning();
      handler.waitForCleaningFinish();
    }
    verify(writeClient, times(2)).startAsyncCleaning();
    verify(writeClient).waitForCleaningFinish();
    verify(writeClient).close();
    verifyNoMoreInteractions(writeClient);
  }

  @Test
  void testCloseClosesWriteClientWithoutTriggeringClean() {
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    DefaultCleanHandler handler = new DefaultCleanHandler(writeClient);

    handler.close();

    verify(writeClient).close();
    verifyNoMoreInteractions(writeClient);
  }
}
