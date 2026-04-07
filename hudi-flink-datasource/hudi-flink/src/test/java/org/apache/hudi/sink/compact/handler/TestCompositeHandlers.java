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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.util.Lazy;

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Collections;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.same;

class TestCompositeHandlers {

  @Test
  void testCommitHandlerRoutesEventsToMatchingTableServiceHandler() {
    CompactCommitHandler dataHandler = mock(CompactCommitHandler.class);
    CompactCommitHandler metadataHandler = mock(CompactCommitHandler.class);
    CompositeCompactCommitHandler handler = new CompositeCompactCommitHandler(
        Option.of(Lazy.eagerly(dataHandler)),
        Option.of(Lazy.eagerly(metadataHandler)));
    FlinkCompactionMetrics metrics = mock(FlinkCompactionMetrics.class);
    CompactionCommitEvent dataEvent =
        new CompactionCommitEvent("001", "file-1", Collections.<WriteStatus>emptyList(), 0, false, false);
    CompactionCommitEvent metadataEvent =
        new CompactionCommitEvent("002", "file-2", Collections.<WriteStatus>emptyList(), 0, true, false);

    handler.commitIfNecessary(dataEvent, metrics);
    handler.commitIfNecessary(metadataEvent, metrics);

    verify(dataHandler).commitIfNecessary(same(dataEvent), same(metrics));
    verify(metadataHandler).commitIfNecessary(same(metadataEvent), same(metrics));
    verifyNoMoreInteractions(dataHandler, metadataHandler);
  }

  @Test
  void testCleanHandlerBroadcastsLifecycleCallsToAvailableHandlersOnly() {
    CleanHandler dataHandler = mock(CleanHandler.class);
    CompositeCleanHandler handler = new CompositeCleanHandler(Option.of(dataHandler), Option.empty());

    handler.clean();
    handler.startAsyncCleaning();
    handler.waitForCleaningFinish();
    handler.close();

    verify(dataHandler).clean();
    verify(dataHandler).startAsyncCleaning();
    verify(dataHandler).waitForCleaningFinish();
    verify(dataHandler).close();
    verifyNoMoreInteractions(dataHandler);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testCompactionPlanHandlerPreservesDataThenMetadataBroadcastOrder() {
    CompactionPlanHandler dataHandler = mock(CompactionPlanHandler.class);
    CompactionPlanHandler metadataHandler = mock(MetadataCompactionPlanHandler.class);
    CompositeCompactionPlanHandler handler =
        new CompositeCompactionPlanHandler(Option.of(dataHandler), Option.of(metadataHandler));
    FlinkCompactionMetrics metrics = mock(FlinkCompactionMetrics.class);
    Output<StreamRecord<CompactionPlanEvent>> output = mock(Output.class);

    handler.collectCompactionOperations(100L, metrics, output);

    InOrder inOrder = inOrder(dataHandler, metadataHandler);
    inOrder.verify(dataHandler).collectCompactionOperations(100L, metrics, output);
    inOrder.verify(metadataHandler).collectCompactionOperations(100L, metrics, output);
    verifyNoMoreInteractions(dataHandler, metadataHandler);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testCompactionPlanHandlerSkipsMissingMetadataServiceHandler() {
    CompactionPlanHandler dataHandler = mock(CompactionPlanHandler.class);
    CompactionPlanHandler metadataHandler = mock(MetadataCompactionPlanHandler.class);
    CompositeCompactionPlanHandler handler =
        new CompositeCompactionPlanHandler(Option.of(dataHandler), Option.empty());
    FlinkCompactionMetrics metrics = mock(FlinkCompactionMetrics.class);
    Output<StreamRecord<CompactionPlanEvent>> output = mock(Output.class);

    handler.collectCompactionOperations(101L, metrics, output);

    verify(dataHandler).collectCompactionOperations(101L, metrics, output);
    verify(metadataHandler, never()).collectCompactionOperations(101L, metrics, output);
    verifyNoMoreInteractions(dataHandler, metadataHandler);
  }
}
