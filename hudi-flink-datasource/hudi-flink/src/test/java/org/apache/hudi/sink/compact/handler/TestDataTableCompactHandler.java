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
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.table.HoodieFlinkTable;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingRunnable;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests that a compaction failure occurring on the async {@link NonThrownExecutor} path is
 * reflected in the {@code compactionErrorCount} metric instead of being swallowed silently.
 */
class TestDataTableCompactHandler {

  @Test
  @SuppressWarnings("unchecked")
  void testAsyncCompactionFailureIncrementsErrorMetric() throws Exception {
    HoodieFlinkWriteClient<?> writeClient = mock(HoodieFlinkWriteClient.class);
    HoodieFlinkTable<?> table = mock(HoodieFlinkTable.class);
    when(writeClient.getHoodieTable()).thenReturn((HoodieFlinkTable) table);

    RuntimeException compactionFailure = new RuntimeException("compaction boom");
    DataTableCompactHandler handler = new DataTableCompactHandler(writeClient, 0) {
      @Override
      protected void doCompaction(CompactionPlanEvent event, org.apache.flink.util.Collector<CompactionCommitEvent> collector, boolean needReloadMetaClient) throws Exception {
        throw compactionFailure;
      }
    };

    MetricGroup metricGroup = mock(MetricGroup.class);
    ArgumentCaptor<Counter> counterCaptor = ArgumentCaptor.forClass(Counter.class);
    handler.registerMetrics(metricGroup);
    verify(metricGroup).counter(anyString(), counterCaptor.capture());
    Counter errorCounter = counterCaptor.getValue();
    assertEquals(0, errorCounter.getCount());

    CompactionOperation operation = mock(CompactionOperation.class);
    when(operation.getFileId()).thenReturn("file-1");
    CompactionPlanEvent event = new CompactionPlanEvent("001", operation, 0, false, false);

    AtomicReference<CompactionCommitEvent> collected = new AtomicReference<>();
    org.apache.flink.util.Collector<CompactionCommitEvent> collector = new org.apache.flink.util.Collector<CompactionCommitEvent>() {
      @Override
      public void collect(CompactionCommitEvent record) {
        collected.set(record);
      }

      @Override
      public void close() {
        // no-op
      }
    };

    NonThrownExecutor executor = new SyncFailFastExecutor();
    handler.compact(executor, event, collector, false);

    assertEquals(1, errorCounter.getCount(), "compaction failure on the async path must be reflected in the error metric");
    assertTrue(collected.get().isFailed(), "a failed commit event must still be emitted downstream");
  }

  /**
   * Executes actions synchronously so the async failure hook runs on the calling test thread.
   */
  private static class SyncFailFastExecutor extends NonThrownExecutor {
    SyncFailFastExecutor() {
      super(org.slf4j.LoggerFactory.getLogger(SyncFailFastExecutor.class), null,
          (errMsg, t) -> {
            throw new HoodieException(errMsg, t);
          }, true);
    }

    @Override
    public void execute(ThrowingRunnable<Throwable> action, ExceptionHook hook, String actionName, Object... actionParams) {
      try {
        action.run();
      } catch (Throwable t) {
        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
        if (hook != null) {
          hook.apply("error", t);
        }
      }
    }
  }
}
