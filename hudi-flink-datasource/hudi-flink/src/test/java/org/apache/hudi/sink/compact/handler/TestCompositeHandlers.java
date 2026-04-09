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
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestUtils;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class TestCompositeHandlers {

  @TempDir
  Path tempDir;

  @Test
  void testCommitHandlerRoutesEventsToMatchingTableServiceHandler() {
    CompactionCommitHandler dataHandler = mock(CompactionCommitHandler.class);
    CompactionCommitHandler metadataHandler = mock(CompactionCommitHandler.class);
    CompositeCompactionCommitHandler handler = new CompositeCompactionCommitHandler(dataHandler, metadataHandler);
    CompactionCommitEvent dataEvent =
        new CompactionCommitEvent("001", "file-1", Collections.<WriteStatus>emptyList(), 0, false, false);
    CompactionCommitEvent metadataEvent =
        new CompactionCommitEvent("002", "file-2", Collections.<WriteStatus>emptyList(), 0, true, false);

    handler.commitIfNecessary(dataEvent);
    handler.commitIfNecessary(metadataEvent);

    verify(dataHandler).commitIfNecessary(same(dataEvent));
    verify(metadataHandler).commitIfNecessary(same(metadataEvent));
    verifyNoMoreInteractions(dataHandler, metadataHandler);
  }

  @Test
  void testCleanHandlerBroadcastsLifecycleCallsToBothHandlers() {
    CleanHandler dataHandler = mock(CleanHandler.class);
    CleanHandler metadataHandler = mock(CleanHandler.class);
    CompositeCleanHandler handler = new CompositeCleanHandler(dataHandler, metadataHandler);

    handler.clean();
    handler.startAsyncCleaning();
    handler.waitForCleaningFinish();
    handler.close();

    verify(dataHandler).clean();
    verify(dataHandler).startAsyncCleaning();
    verify(dataHandler).waitForCleaningFinish();
    verify(dataHandler).close();
    verify(metadataHandler).clean();
    verify(metadataHandler).startAsyncCleaning();
    verify(metadataHandler).waitForCleaningFinish();
    verify(metadataHandler).close();
    verifyNoMoreInteractions(dataHandler, metadataHandler);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testCompactionPlanHandlerPreservesDataThenMetadataBroadcastOrder() {
    CompactionPlanHandler dataHandler = mock(CompactionPlanHandler.class);
    CompactionPlanHandler metadataHandler = mock(CompactionPlanHandler.class);
    CompositeCompactionPlanHandler handler = new CompositeCompactionPlanHandler(dataHandler, metadataHandler);
    Output<StreamRecord<CompactionPlanEvent>> output = mock(Output.class);

    handler.collectCompactionOperations(100L, output);

    InOrder inOrder = inOrder(dataHandler, metadataHandler);
    inOrder.verify(dataHandler).collectCompactionOperations(100L, output);
    inOrder.verify(metadataHandler).collectCompactionOperations(100L, output);
    verifyNoMoreInteractions(dataHandler, metadataHandler);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testCompactHandlerRoutesEventsToMatchingTableServiceHandler() throws Exception {
    CompactHandler dataHandler = mock(CompactHandler.class);
    CompactHandler metadataHandler = mock(CompactHandler.class);
    CompositeCompactHandler handler = new CompositeCompactHandler(dataHandler, metadataHandler);
    org.apache.flink.util.Collector<CompactionCommitEvent> collector = mock(org.apache.flink.util.Collector.class);
    CompactionPlanEvent dataEvent = new CompactionPlanEvent("003", null, 0, false, false);
    CompactionPlanEvent metadataEvent = new CompactionPlanEvent("004", null, 0, true, false);

    handler.compact(null, dataEvent, collector, false);
    handler.compact(null, metadataEvent, collector, false);

    verify(dataHandler).compact(null, dataEvent, collector, false);
    verify(metadataHandler).compact(null, metadataEvent, collector, false);
    verifyNoMoreInteractions(dataHandler, metadataHandler, collector);
  }

  @Test
  void testCompactionPlanHandlerRegistersMetricsForBothHandlers() {
    CompactionPlanHandler dataHandler = mock(CompactionPlanHandler.class);
    CompactionPlanHandler metadataHandler = mock(CompactionPlanHandler.class);
    CompositeCompactionPlanHandler handler = new CompositeCompactionPlanHandler(dataHandler, metadataHandler);
    MetricGroup metricGroup = mock(MetricGroup.class);

    handler.registerMetrics(metricGroup);

    InOrder inOrder = inOrder(dataHandler, metadataHandler);
    inOrder.verify(dataHandler).registerMetrics(same(metricGroup));
    inOrder.verify(metadataHandler).registerMetrics(same(metricGroup));
    verifyNoMoreInteractions(dataHandler, metadataHandler);
  }

  @Test
  void testCompactHandlerRegistersMetricsForBothHandlers() {
    CompactHandler dataHandler = mock(CompactHandler.class);
    CompactHandler metadataHandler = mock(CompactHandler.class);
    CompositeCompactHandler handler = new CompositeCompactHandler(dataHandler, metadataHandler);
    MetricGroup metricGroup = mock(MetricGroup.class);

    handler.registerMetrics(metricGroup);

    InOrder inOrder = inOrder(dataHandler, metadataHandler);
    inOrder.verify(dataHandler).registerMetrics(same(metricGroup));
    inOrder.verify(metadataHandler).registerMetrics(same(metricGroup));
    verifyNoMoreInteractions(dataHandler, metadataHandler);
  }

  @Test
  void testCommitHandlerRegistersMetricsForBothHandlers() {
    CompactionCommitHandler dataHandler = mock(CompactionCommitHandler.class);
    CompactionCommitHandler metadataHandler = mock(CompactionCommitHandler.class);
    CompositeCompactionCommitHandler handler = new CompositeCompactionCommitHandler(dataHandler, metadataHandler);
    MetricGroup metricGroup = mock(MetricGroup.class);

    handler.registerMetrics(metricGroup);

    InOrder inOrder = inOrder(dataHandler, metadataHandler);
    inOrder.verify(dataHandler).registerMetrics(same(metricGroup));
    inOrder.verify(metadataHandler).registerMetrics(same(metricGroup));
    verifyNoMoreInteractions(dataHandler, metadataHandler);
  }

  @Test
  void testCompactionPlanHandlerFactoryReturnsSingleDataTableHandler() {
    Configuration conf = newInitializedConf();
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);

    try (CompactionPlanHandler handler = TableServiceHandlerFactory.createCompactionPlanHandler(conf, TestUtils.getMockRuntimeContext())) {
      assertInstanceOf(DataTableCompactionPlanHandler.class, handler);
    }
  }

  @Test
  void testCompactionPlanHandlerFactoryReturnsCompositeHandlerWhenMetadataCompactionEnabled() {
    Configuration conf = newInitializedConf();
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    enableMetadataCompaction(conf);

    try (CompactionPlanHandler handler = TableServiceHandlerFactory.createCompactionPlanHandler(conf, TestUtils.getMockRuntimeContext())) {
      assertInstanceOf(CompositeCompactionPlanHandler.class, handler);
    }
  }

  @Test
  void testCompactionCommitHandlerFactoryReturnsSingleMetadataTableHandler() {
    Configuration conf = newInitializedConf();
    enableMetadataCompaction(conf);

    try (CompactionCommitHandler handler = TableServiceHandlerFactory.createCompactionCommitHandler(conf, TestUtils.getMockRuntimeContext())) {
      assertInstanceOf(MetadataTableCompactionCommitHandler.class, handler);
    }
  }

  @Test
  void testCleanHandlerFactoryReturnsCompositeHandlerWhenStreamingIndexEnabled() {
    Configuration conf = newInitializedConf();
    enableMetadataCompaction(conf);

    try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, TestUtils.getMockRuntimeContext())) {
      CleanHandler handler = TableServiceHandlerFactory.createCleanHandler(conf, writeClient);
      try {
        assertInstanceOf(CompositeCleanHandler.class, handler);
      } finally {
        handler.close();
      }
    }
  }

  private Configuration newConf() {
    return TestConfigurations.getDefaultConf(tempDir.toAbsolutePath().toString());
  }

  private Configuration newInitializedConf() {
    Configuration conf = newConf();
    try {
      StreamerUtil.initTableIfNotExists(conf);
      try (HoodieFlinkWriteClient ignored = FlinkWriteClients.createWriteClient(conf)) {
        // creates view storage properties for subsequent client-side write clients
      }
    } catch (Exception e) {
      throw new HoodieException(e);
    }
    return conf;
  }

  private void enableMetadataCompaction(Configuration conf) {
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name());
    conf.set(FlinkOptions.METADATA_COMPACTION_ASYNC_ENABLED, true);
  }
}
