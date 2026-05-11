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

package org.apache.hudi.sink.bootstrap;

import org.apache.hudi.adapter.CollectOutputAdapter;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.metrics.FlinkRLIBootstrapMetrics;
import org.apache.hudi.sink.utils.MockStreamingRuntimeContext;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.metrics.FlinkRLIBootstrapMetrics.NUM_INDEX_RECORDS_EMITTED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for {@link RLIBootstrapOperator}.
 */
class TestRLIBootstrapOperator {

  private RLIBootstrapOperator operator;
  private CollectOutputAdapter<HoodieFlinkInternalRow> outputAdapter;

  @BeforeEach
  void setUp() throws Exception {
    operator = spy(new RLIBootstrapOperator(new Configuration()));
    MockStreamingRuntimeContext runtimeContext = new MockStreamingRuntimeContext(false, 4, 0);
    doReturn(runtimeContext).when(operator).getRuntimeContext();

    outputAdapter = new CollectOutputAdapter<>();
    injectOutput(outputAdapter);
    operator.open();
  }

  // -------------------------------------------------------------------------
  //  shouldLoadBucket: round-robin partition assignment
  // -------------------------------------------------------------------------

  @ParameterizedTest
  @CsvSource({
      // Single-task: always loads every file group
      "0, 1, 0, true",
      "5, 1, 0, true",
      "99, 1, 0, true",
      // 4-way parallelism, correct task assignment
      "0, 4, 0, true",
      "1, 4, 1, true",
      "2, 4, 2, true",
      "3, 4, 3, true",
      // Round-robin wraps correctly
      "4, 4, 0, true",
      "5, 4, 1, true",
      "8, 4, 0, true",
      // Wrong task does NOT load those file groups
      "1, 4, 0, false",
      "0, 4, 1, false",
      "4, 4, 1, false",
      // 3-way parallelism
      "0, 3, 0, true",
      "3, 3, 0, true",
      "4, 3, 1, true",
      "2, 3, 2, true",
      "3, 3, 1, false",
  })
  void testShouldLoadBucket(int fileGroupIdx, int parallelism, int taskId, boolean expected) throws Exception {
    Method m = RLIBootstrapOperator.class.getDeclaredMethod("shouldLoadBucket", int.class, int.class, int.class);
    m.setAccessible(true);
    assertEquals(expected, m.invoke(operator, fileGroupIdx, parallelism, taskId));
  }

  // -------------------------------------------------------------------------
  //  emitIndexRecord: output and counter
  // -------------------------------------------------------------------------

  @Test
  void testEmitIndexRecordPopulatesAllFields() throws Exception {
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("2024/01", "20240101000000", "file-uuid-1");
    invokeEmitIndexRecord("record-key-1", location);

    List<HoodieFlinkInternalRow> records = outputAdapter.getRecords();
    assertEquals(1, records.size());
    HoodieFlinkInternalRow emitted = records.get(0);
    assertEquals("record-key-1", emitted.getRecordKey());
    assertEquals("2024/01", emitted.getPartitionPath());
    assertEquals("file-uuid-1", emitted.getFileId());
    assertEquals("20240101000000", emitted.getInstantTime());
  }

  @Test
  void testEmitIndexRecordIncrementsOutputPerCall() throws Exception {
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("p1", "20240101", "f1");
    invokeEmitIndexRecord("k1", location);
    invokeEmitIndexRecord("k2", location);
    invokeEmitIndexRecord("k3", location);

    assertEquals(3, outputAdapter.getRecords().size());
  }

  @Test
  void testEmitIndexRecordDoesNotThrowAt1000Boundary() throws Exception {
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("p1", "20240101", "f1");
    // The 1000th record triggers a metrics update — verify it does not throw.
    for (int i = 0; i < 1000; i++) {
      invokeEmitIndexRecord("key-" + i, location);
    }
    assertEquals(1000, outputAdapter.getRecords().size());
  }

  @Test
  void testEmitIndexRecordPreservesInsertionOrder() throws Exception {
    HoodieRecordGlobalLocation loc1 = new HoodieRecordGlobalLocation("p1", "t1", "f1");
    HoodieRecordGlobalLocation loc2 = new HoodieRecordGlobalLocation("p2", "t2", "f2");

    invokeEmitIndexRecord("alpha", loc1);
    invokeEmitIndexRecord("beta", loc2);

    List<HoodieFlinkInternalRow> records = outputAdapter.getRecords();
    assertEquals("alpha", records.get(0).getRecordKey());
    assertEquals("f1", records.get(0).getFileId());
    assertEquals("beta", records.get(1).getRecordKey());
    assertEquals("f2", records.get(1).getFileId());
  }

  // -------------------------------------------------------------------------
  //  processElement: pass-through
  // -------------------------------------------------------------------------

  @Test
  void testProcessElementPassesThrough() throws Exception {
    HoodieFlinkInternalRow row = new HoodieFlinkInternalRow("k1", "p1", "f1", "t1");
    operator.processElement(new StreamRecord<>(row));

    assertEquals(1, outputAdapter.getRecords().size());
    assertEquals("k1", outputAdapter.getRecords().get(0).getRecordKey());
  }

  @Test
  void testProcessElementPreservesAllFields() throws Exception {
    HoodieFlinkInternalRow row = new HoodieFlinkInternalRow("myKey", "myPartition", "myFile", "myInstant");
    operator.processElement(new StreamRecord<>(row));

    HoodieFlinkInternalRow out = outputAdapter.getRecords().get(0);
    assertEquals("myKey", out.getRecordKey());
    assertEquals("myPartition", out.getPartitionPath());
    assertEquals("myFile", out.getFileId());
    assertEquals("myInstant", out.getInstantTime());
  }

  // -------------------------------------------------------------------------
  //  closeMetadataTable: null safety
  // -------------------------------------------------------------------------

  @Test
  void testCloseMetadataTableIsNullSafe() throws Exception {
    // initializeState() was never called, so metadataTable is null.
    Method m = RLIBootstrapOperator.class.getDeclaredMethod("closeMetadataTable");
    m.setAccessible(true);
    assertDoesNotThrow(() -> m.invoke(operator));
  }

  @Test
  void testCloseMetadataTableIsIdempotent() throws Exception {
    Method m = RLIBootstrapOperator.class.getDeclaredMethod("closeMetadataTable");
    m.setAccessible(true);
    assertDoesNotThrow(() -> {
      m.invoke(operator);
      m.invoke(operator);
    });
  }

  // -------------------------------------------------------------------------
  //  Metrics registration
  // -------------------------------------------------------------------------

  @Test
  void testMetricsFieldIsInitializedAfterOpen() throws Exception {
    Field metricsField = RLIBootstrapOperator.class.getDeclaredField("metrics");
    metricsField.setAccessible(true);
    assertNotNull(metricsField.get(operator));
  }

  @Test
  void testMetricsNotUpdatedBefore1000Records() throws Exception {
    CapturingMetricGroup group = injectCapturingMetrics();
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("p", "t", "f");
    for (int i = 0; i < 999; i++) {
      invokeEmitIndexRecord("k-" + i, location);
    }
    assertEquals(0L, (Long) gaugeValue(group, NUM_INDEX_RECORDS_EMITTED));
  }

  @Test
  void testMetricsUpdatedAt1000RecordBoundary() throws Exception {
    CapturingMetricGroup group = injectCapturingMetrics();
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("p", "t", "f");
    for (int i = 0; i < 1000; i++) {
      invokeEmitIndexRecord("k-" + i, location);
    }
    assertEquals(1000L, (Long) gaugeValue(group, NUM_INDEX_RECORDS_EMITTED));
  }

  @Test
  void testMetricsUpdatedAt2000RecordBoundary() throws Exception {
    CapturingMetricGroup group = injectCapturingMetrics();
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("p", "t", "f");
    for (int i = 0; i < 2000; i++) {
      invokeEmitIndexRecord("k-" + i, location);
    }
    assertEquals(2000L, (Long) gaugeValue(group, NUM_INDEX_RECORDS_EMITTED));
  }

  @Test
  void testMetricsGaugeReflectsLastMultipleOf1000() throws Exception {
    CapturingMetricGroup group = injectCapturingMetrics();
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("p", "t", "f");
    for (int i = 0; i < 1500; i++) {
      invokeEmitIndexRecord("k-" + i, location);
    }
    // Updated at 1000, not again at 1500
    assertEquals(1000L, (Long) gaugeValue(group, NUM_INDEX_RECORDS_EMITTED));
  }

  // -------------------------------------------------------------------------
  //  Helpers
  // -------------------------------------------------------------------------

  private void invokeEmitIndexRecord(String recordKey, HoodieRecordGlobalLocation location) throws Exception {
    Method m = RLIBootstrapOperator.class.getDeclaredMethod(
        "emitIndexRecord", String.class, HoodieRecordGlobalLocation.class);
    m.setAccessible(true);
    m.invoke(operator, recordKey, location);
  }

  private void injectOutput(CollectOutputAdapter<HoodieFlinkInternalRow> output) throws Exception {
    Field outputField = AbstractStreamOperator.class.getDeclaredField("output");
    outputField.setAccessible(true);
    outputField.set(operator, output);
  }

  private CapturingMetricGroup injectCapturingMetrics() throws Exception {
    CapturingMetricGroup group = new CapturingMetricGroup();
    FlinkRLIBootstrapMetrics capturingMetrics = new FlinkRLIBootstrapMetrics(group);
    capturingMetrics.registerMetrics();
    Field metricsField = RLIBootstrapOperator.class.getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(operator, capturingMetrics);
    return group;
  }

  @SuppressWarnings("unchecked")
  private static <T> T gaugeValue(CapturingMetricGroup group, String name) {
    return (T) group.gauges.get(name).getValue();
  }

  private static class CapturingMetricGroup extends UnregisteredMetricsGroup {
    final Map<String, Gauge<?>> gauges = new HashMap<>();

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
      gauges.put(name, gauge);
      return gauge;
    }
  }
}
