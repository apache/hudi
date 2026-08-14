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

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestFlinkCompactionMetrics {

  @Test
  void testDefaultCompactionMetricName() {
    assertEquals("compaction.pendingCompactionCount",
        new ExposedFlinkCompactionMetrics().metricsName("compaction", "pendingCompactionCount"));
  }

  @Test
  void testMetadataCompactionMetricName() {
    assertEquals("mdt.compaction.pendingCompactionCount",
        new ExposedFlinkMdtCompactionMetrics().metricsName("compaction", "pendingCompactionCount"));
  }

  private static class ExposedFlinkCompactionMetrics extends FlinkCompactionMetrics {
    private ExposedFlinkCompactionMetrics() {
      super(new UnregisteredMetricsGroup());
    }

    private String metricsName(String action, String metric) {
      return getMetricsName(action, metric);
    }
  }

  private static class ExposedFlinkMdtCompactionMetrics extends FlinkMdtCompactionMetrics {
    private ExposedFlinkMdtCompactionMetrics() {
      super(new UnregisteredMetricsGroup());
    }

    private String metricsName(String action, String metric) {
      return getMetricsName(action, metric);
    }
  }
}
