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

import org.apache.hudi.adapter.SourceFunctionAdapter;
import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link CompactionPlanSourceFunction}.
 */
class TestCompactionPlanSourceFunction {

  @Test
  @SuppressWarnings("unchecked")
  void testEmitOperationsOnlyForPendingCompactionInstants() throws Exception {
    Configuration conf = new Configuration();
    HoodieCompactionPlan missingPlan = mock(HoodieCompactionPlan.class);
    HoodieCompactionPlan pendingPlan = mock(HoodieCompactionPlan.class);
    HoodieCompactionOperation operation = mock(HoodieCompactionOperation.class);
    when(operation.getBaseInstantTime()).thenReturn("000");
    when(operation.getDataFilePath()).thenReturn(null);
    when(operation.getDeltaFilePaths()).thenReturn(Collections.singletonList("file.log.1"));
    when(operation.getPartitionPath()).thenReturn("partition");
    when(operation.getFileId()).thenReturn("file-id");
    when(operation.getMetrics()).thenReturn(Collections.emptyMap());
    when(operation.getBootstrapFilePath()).thenReturn(null);
    when(pendingPlan.getOperations()).thenReturn(Collections.singletonList(operation));

    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline pendingTimeline = mock(HoodieTimeline.class);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.filterPendingCompactionTimeline()).thenReturn(pendingTimeline);
    when(pendingTimeline.containsInstant("001")).thenReturn(true);

    SourceFunctionAdapter.SourceContext<CompactionPlanEvent> sourceContext =
        mock(SourceFunctionAdapter.SourceContext.class);
    CompactionPlanSourceFunction function = new CompactionPlanSourceFunction(
        Arrays.asList(Pair.of("000", missingPlan), Pair.of("001", pendingPlan)), conf);

    try (MockedStatic<StreamerUtil> streamerUtil = mockStatic(StreamerUtil.class)) {
      streamerUtil.when(() -> StreamerUtil.createMetaClient(conf)).thenReturn(metaClient);

      function.open(conf);
      function.run(sourceContext);
      function.cancel();
      function.close();
    }

    ArgumentCaptor<CompactionPlanEvent> eventCaptor =
        ArgumentCaptor.forClass(CompactionPlanEvent.class);
    verify(sourceContext).collect(eventCaptor.capture());
    CompactionPlanEvent event = eventCaptor.getValue();
    assertEquals("001", event.getCompactionInstantTime());
    assertEquals("partition", event.getOperation().getPartitionPath());
    assertEquals("file-id", event.getOperation().getFileId());
  }
}
