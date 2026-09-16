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

package org.apache.hudi.sink.common;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.utils.MockOperatorStateStore;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.RuntimeContextUtils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockFunctionSnapshotContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests checkpoint identity and bootstrap events independently of the writer implementation.
 */
class TestAbstractStreamWriteFunction {
  private final Configuration conf = new Configuration();
  private final JobID jobId = new JobID();
  private final List<OperatorEvent> events = new ArrayList<>();

  private final MockOperatorStateStore stateStore = new MockOperatorStateStore();
  private final HoodieTimeline pendingTimeline = mock(HoodieTimeline.class);
  private final Correspondent correspondent = mock(Correspondent.class);
  private final TestWriteFunction function = new TestWriteFunction(conf);

  @AfterEach
  void tearDown() throws Exception {
    function.close();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  void testFreshStartUsesInitialCheckpointId(int attempt) throws Exception {
    initialize(-1L, attempt);

    assertEquals("002", function.instantToWrite(true));
    verify(correspondent).requestInstantTime(-1L);
    if (attempt == 0) {
      assertTrue(events.isEmpty());
    } else {
      assertCleanupEvent(-1L);
    }
  }

  @ParameterizedTest
  @CsvSource({"0, SAME", "0, DIFFERENT", "0, MISSING", "1, SAME", "1, DIFFERENT", "1, MISSING"})
  void testRestoredCheckpointId(int attempt, SavedJobState savedJob) throws Exception {
    // Old savepoints may contain this state; new scale-up subtasks may have no operator state at all.
    if (savedJob != SavedJobState.MISSING) {
      stateStore.getListState(new ListStateDescriptor<>("job-id-state", TypeInformation.of(JobID.class)))
          .add(savedJob == SavedJobState.SAME ? jobId : new JobID());
    }
    initialize(42L, attempt);

    assertEquals("002", function.instantToWrite(true));
    verify(correspondent).requestInstantTime(42L);
    if (attempt == 0) {
      assertTrue(events.isEmpty());
    } else {
      assertCleanupEvent(42L);
    }

    function.snapshotState(new MockFunctionSnapshotContext(43L));
    WriteMetadataEvent snapshot = writeMetadataState().get().iterator().next();
    assertEquals(42L, snapshot.getCheckpointId(), "Saved metadata belongs to the batch before checkpoint 43");
    assertEquals("002", snapshot.getInstantTime());
    assertTrue(snapshot.isBootstrap());
    function.instantToWrite(true);
    verify(correspondent).requestInstantTime(43L);
  }

  @Test
  void testRestoredMetadataKeepsOriginalCheckpointId() throws Exception {
    WriteMetadataEvent restored = WriteMetadataEvent.builder()
        .taskID(0)
        .checkpointId(41L)
        .instantTime("001")
        .writeStatus(Collections.emptyList())
        .bootstrap(true)
        .lastBatch(true)
        .build();
    writeMetadataState().add(restored);
    when(pendingTimeline.containsInstant("001")).thenReturn(true);

    initialize(42L, 0);

    assertEquals(1, events.size());
    WriteMetadataEvent bootstrap = (WriteMetadataEvent) events.get(0);
    assertEquals(2, bootstrap.getTaskID(), "Restore must use the new subtask ID");
    assertEquals(41L, bootstrap.getCheckpointId());
    assertEquals("001", bootstrap.getInstantTime());
    function.instantToWrite(true);
    verify(correspondent).requestInstantTime(42L);
  }

  private void initialize(long checkpointId, int attempt) throws Exception {
    RuntimeContext runtimeContext = mock(RuntimeContext.class);
    FunctionInitializationContext context = mock(FunctionInitializationContext.class);
    when(context.getOperatorStateStore()).thenReturn(stateStore);
    when(context.isRestored()).thenReturn(checkpointId >= 0);
    when(context.getRestoredCheckpointId()).thenReturn(checkpointId >= 0 ? OptionalLong.of(checkpointId) : OptionalLong.empty());
    when(correspondent.requestInstantTime(anyLong())).thenReturn("002");
    function.setRuntimeContext(runtimeContext);
    function.setCorrespondent(correspondent);
    function.setOperatorEventGateway(events::add);

    try (MockedStatic<StreamerUtil> streamerUtil = mockStatic(StreamerUtil.class);
         MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class);
         MockedStatic<RuntimeContextUtils> runtimeContextUtils = mockStatic(RuntimeContextUtils.class)) {
      HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
      when(metaClient.getActiveTimeline().filterPendingExcludingCompaction()).thenReturn(pendingTimeline);
      streamerUtil.when(() -> StreamerUtil.createMetaClient(conf)).thenReturn(metaClient);
      writeClients.when(() -> FlinkWriteClients.createWriteClient(conf, runtimeContext)).thenReturn(mock(HoodieFlinkWriteClient.class));
      runtimeContextUtils.when(() -> RuntimeContextUtils.getJobId(runtimeContext)).thenReturn(jobId);
      runtimeContextUtils.when(() -> RuntimeContextUtils.getIndexOfThisSubtask(runtimeContext)).thenReturn(2);
      runtimeContextUtils.when(() -> RuntimeContextUtils.getAttemptNumber(runtimeContext)).thenReturn(attempt);
      function.initializeState(context);
    }
  }

  private void assertCleanupEvent(long checkpointId) {
    assertEquals(1, events.size());
    WriteMetadataEvent bootstrap = (WriteMetadataEvent) events.get(0);
    assertEquals(2, bootstrap.getTaskID());
    assertEquals(checkpointId, bootstrap.getCheckpointId());
    assertEquals(WriteMetadataEvent.BOOTSTRAP_INSTANT, bootstrap.getInstantTime());
    assertTrue(bootstrap.isBootstrap());
    assertTrue(bootstrap.getWriteStatuses().isEmpty());
  }

  private ListState<WriteMetadataEvent> writeMetadataState() throws Exception {
    return stateStore.getListState(new ListStateDescriptor<>("write-metadata-state", TypeInformation.of(WriteMetadataEvent.class)));
  }

  private enum SavedJobState {
    SAME, DIFFERENT, MISSING
  }

  private static class TestWriteFunction extends AbstractStreamWriteFunction<RowData> {
    TestWriteFunction(Configuration conf) {
      super(conf);
    }

    @Override
    public void snapshotState() {
      currentInstant = instantToWrite(true);
    }

    @Override
    public void processElement(RowData value, Context ctx, Collector<RowData> out) {
      // No record processing is needed to exercise the base checkpoint lifecycle.
    }
  }
}
