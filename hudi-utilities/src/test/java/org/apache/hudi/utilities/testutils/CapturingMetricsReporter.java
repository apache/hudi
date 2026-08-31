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

package org.apache.hudi.utilities.testutils;

import org.apache.hudi.metrics.custom.CustomizableMetricsReporter;

import com.codahale.metrics.MetricRegistry;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records gauge values so a test can read them without having to guess which {@code Metrics} instance the
 * write published into -- that is keyed by base path, and reconstructing the key from outside is fragile.
 */
public class CapturingMetricsReporter extends CustomizableMetricsReporter {

  private static final Map<String, Long> CAPTURED = new ConcurrentHashMap<>();
  private static final List<CapturingMetricsReporter> ATTACHED = new CopyOnWriteArrayList<>();

  public CapturingMetricsReporter(Properties props, MetricRegistry registry) {
    super(props, registry);
    ATTACHED.add(this);
  }

  /** Polls every still-attached registry first: nothing forces a report on the DeltaStreamer path. */
  public static Map<String, Long> captured() {
    ATTACHED.forEach(CapturingMetricsReporter::report);
    return CAPTURED;
  }

  public static void reset() {
    CAPTURED.clear();
  }

  @Override
  public void start() {
  }

  @Override
  public void report() {
    getRegistry().getGauges().forEach((name, gauge) -> {
      Object value = gauge.getValue();
      if (value instanceof Number) {
        CAPTURED.put(name, ((Number) value).longValue());
      }
    });
  }

  @Override
  public void stop() {
    ATTACHED.remove(this);
  }
}
