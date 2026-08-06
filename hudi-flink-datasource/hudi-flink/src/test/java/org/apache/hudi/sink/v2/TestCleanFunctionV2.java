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
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/** Tests checkpoint-driven cleaning in {@link CleanFunctionV2}. */
class TestCleanFunctionV2 {

  @Test
  void testAsyncCleaningLifecycle() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, true);
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    CleanFunctionV2<String> function = new CleanFunctionV2<>(conf);

    try (OneInputStreamOperatorTestHarness<String, RowData> harness =
             openHarness(conf, function, writeClient)) {
      // Opening waits for any cleaning left by a previous job attempt.
      verify(writeClient, timeout(5000)).clean();
      assertTrue(waitUntil(() -> !function.isCleaning));

      harness.processElement(new StreamRecord<>("ignored"));
      assertTrue(harness.getOutput().isEmpty());

      harness.snapshot(1, 1);
      verify(writeClient).startAsyncCleaning();
      assertTrue(function.isCleaning);

      harness.notifyOfCompletedCheckpoint(1);
      verify(writeClient, timeout(5000)).waitForCleaningFinish();
      assertTrue(waitUntil(() -> !function.isCleaning));
    }

    verify(writeClient).close();
  }

  @Test
  void testSnapshotDoesNotPropagateCleaningFailure() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, true);
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    CleanFunctionV2<String> function = new CleanFunctionV2<>(conf);
    doThrow(new RuntimeException("expected")).when(writeClient).startAsyncCleaning();

    try (OneInputStreamOperatorTestHarness<String, RowData> harness =
             openHarness(conf, function, writeClient)) {
      verify(writeClient, timeout(5000)).clean();
      assertTrue(waitUntil(() -> !function.isCleaning));

      harness.snapshot(1, 1);

      assertFalse(function.isCleaning);
      verify(writeClient).startAsyncCleaning();
    }
  }

  @Test
  void testCleaningDisabled() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, false);
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    CleanFunctionV2<String> function = new CleanFunctionV2<>(conf);

    try (OneInputStreamOperatorTestHarness<String, RowData> harness =
             openHarness(conf, function, writeClient)) {
      harness.snapshot(1, 1);
      harness.notifyOfCompletedCheckpoint(1);
      harness.processElement(new StreamRecord<>("ignored"));

      verify(writeClient, never()).clean();
      verify(writeClient, never()).startAsyncCleaning();
      verify(writeClient, never()).waitForCleaningFinish();
      assertTrue(harness.getOutput().isEmpty());
    }
  }

  private OneInputStreamOperatorTestHarness<String, RowData> openHarness(
      Configuration conf,
      CleanFunctionV2<String> function,
      HoodieFlinkWriteClient writeClient) throws Exception {
    OneInputStreamOperatorTestHarness<String, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(function), 1, 1, 0);
    try (MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class)) {
      writeClients.when(() -> FlinkWriteClients.createWriteClient(
          eq(conf), any())).thenReturn(writeClient);
      harness.open();
    }
    return harness;
  }

  private boolean waitUntil(Condition condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      if (condition.evaluate()) {
        return true;
      }
      Thread.sleep(10);
    }
    return condition.evaluate();
  }

  private interface Condition {
    boolean evaluate() throws Exception;
  }
}
