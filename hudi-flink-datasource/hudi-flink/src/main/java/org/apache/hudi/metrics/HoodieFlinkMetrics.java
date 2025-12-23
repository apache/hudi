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

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for flink read/write metrics.
 */
@Slf4j
public abstract class HoodieFlinkMetrics {

  protected Map<String, Long> timers;
  protected final MetricGroup metricGroup;

  protected HoodieFlinkMetrics(MetricGroup metricGroup) {
    this.timers = new HashMap<>();
    this.metricGroup = metricGroup;
  }

  public abstract void registerMetrics();

  protected void startTimer(String name) {
    if (timers.containsKey(name)) {
      log.info("Restarting timer for name: {}, overriding the existing value", name);
    }
    timers.put(name, System.currentTimeMillis());
  }

  protected long stopTimer(String name) {
    if (!timers.containsKey(name)) {
      log.warn("Cannot find name {} in timer, potentially caused by inconsistent call", name);
      return 0;
    }
    long costs = System.currentTimeMillis() - timers.get(name);
    timers.remove(name);
    return costs;
  }

}
