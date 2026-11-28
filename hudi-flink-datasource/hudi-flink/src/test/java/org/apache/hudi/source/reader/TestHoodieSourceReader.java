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

package org.apache.hudi.source.reader;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.metrics.FlinkStreamReadMetrics;
import org.apache.hudi.source.HoodieScanContext;
import org.apache.hudi.source.reader.function.SplitReaderFunction;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.SplitRequestEvent;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link HoodieSourceReader}.
 */
public class TestHoodieSourceReader {

  @Test
  @SuppressWarnings("unchecked")
  public void testMetricsFinishSplitsOnceTheirRecordsAreEmitted() throws Exception {
    FlinkStreamReadMetrics metrics = Mockito.mock(FlinkStreamReadMetrics.class);
    SourceReaderContext context = Mockito.mock(SourceReaderContext.class);
    when(context.metricGroup()).thenReturn(UnregisteredMetricsGroup.createSourceReaderMetricGroup());
    HoodieScanContext scanContext = HoodieScanContext.builder()
        .conf(new Configuration())
        .limit(RecordLimiter.NO_LIMIT)
        .build();
    HoodieSourceReader<String> reader = new HoodieSourceReader<>(
        metrics,
        Mockito.mock(RecordEmitter.class),
        scanContext,
        context,
        () -> Mockito.mock(SplitReaderFunction.class),
        null);
    Map<String, HoodieSourceSplit> finishedSplits = new LinkedHashMap<>();
    finishedSplits.put("split-1", createSplit(1));
    finishedSplits.put("split-2", createSplit(2));

    reader.onSplitFinished(finishedSplits);

    verify(metrics).finishSplit("split-1");
    verify(metrics).finishSplit("split-2");
    verify(context).sendSourceEventToCoordinator(any(SplitRequestEvent.class));
    reader.close();
  }

  private static HoodieSourceSplit createSplit(int splitNum) {
    return new HoodieSourceSplit(splitNum, "base-path-" + splitNum, Option.of(Collections.emptyList()),
        "/test/table", "/test/partition", "read_optimized", "20261109100000000", "file" + splitNum,
        Option.empty());
  }
}
