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

package org.apache.hudi.metrics;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.storage.StoragePath;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link FlinkRLIBootstrapMetrics}.
 */
class TestFlinkRLIBootstrapMetrics {

  @Test
  void testMetricsRegistrationAndSnapshot() {
    MetricGroup metricGroup = mock(MetricGroup.class);
    Map<String, Gauge<?>> gauges = new HashMap<>();
    doAnswer(invocation -> {
      String name = invocation.getArgument(0);
      Gauge<?> gauge = invocation.getArgument(1);
      gauges.put(name, gauge);
      return gauge;
    }).when(metricGroup).gauge(anyString(), any(Gauge.class));

    FlinkRLIBootstrapMetrics metrics = new FlinkRLIBootstrapMetrics(metricGroup);
    metrics.registerMetrics();

    assertEquals(4, gauges.size());
    assertEquals(0L, ((Number) gauges.get(FlinkRLIBootstrapMetrics.RLI_SHARDS).getValue()).longValue());
    assertEquals(0L, ((Number) gauges.get(FlinkRLIBootstrapMetrics.SCAN_LATENCY).getValue()).longValue());
    assertEquals(0L, ((Number) gauges.get(FlinkRLIBootstrapMetrics.SCAN_BYTES).getValue()).longValue());
    assertEquals(0L, ((Number) gauges.get(FlinkRLIBootstrapMetrics.SCAN_RECORDS).getValue()).longValue());

    metrics.updateFileSliceMetrics(Arrays.asList(fileSlice("file1", 100L, 20L), fileSlice("file2", 200L, 30L)));
    metrics.startScan();
    metrics.markRecordScanned();
    metrics.markRecordScanned();
    metrics.endScan();

    assertEquals(2L, ((Number) gauges.get(FlinkRLIBootstrapMetrics.RLI_SHARDS).getValue()).longValue());
    assertEquals(350L, ((Number) gauges.get(FlinkRLIBootstrapMetrics.SCAN_BYTES).getValue()).longValue());
    assertEquals(2L, ((Number) gauges.get(FlinkRLIBootstrapMetrics.SCAN_RECORDS).getValue()).longValue());
    assertTrue(((Number) gauges.get(FlinkRLIBootstrapMetrics.SCAN_LATENCY).getValue()).longValue() >= 0L);
  }

  private static FileSlice fileSlice(String fileId, long baseFileSize, long logFileSize) {
    HoodieBaseFile baseFile = new HoodieBaseFile("/tmp/partition/" + fileId + "_0-0-0_000.parquet", fileId, "000", null);
    baseFile.setFileSize(baseFileSize);

    FileSlice fileSlice = new FileSlice("partition", "000", fileId);
    fileSlice.setBaseFile(baseFile);
    fileSlice.addLogFile(new HoodieLogFile(new StoragePath("file:///tmp/partition/." + fileId + "_000.log.1_0-0-0"), logFileSize));
    return fileSlice;
  }
}
