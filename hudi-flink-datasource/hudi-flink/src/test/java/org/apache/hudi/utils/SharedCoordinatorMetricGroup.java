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

package org.apache.hudi.utils;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A coordinator metric group that behaves like the one Flink gives each coordinator of an operator.
 *
 * <p>It identifies its job and operator, and {@link #newView()} returns another group over the same
 * registrations, as Flink gives every coordinator it creates for the operator during the job. Like
 * Flink's, it keeps the first metric registered under a name and ignores later ones.
 */
public class SharedCoordinatorMetricGroup extends UnregisteredMetricsGroup
    implements OperatorCoordinatorMetricGroup {
  private final Map<String, Metric> registered;
  private final Map<String, String> variables;

  public SharedCoordinatorMetricGroup(String jobId, String operatorId) {
    this(new ConcurrentHashMap<>(), variables(jobId, operatorId));
  }

  private SharedCoordinatorMetricGroup(Map<String, Metric> registered, Map<String, String> variables) {
    this.registered = registered;
    this.variables = variables;
  }

  /** Returns another group over the same registrations, as a coordinator created by a restart gets. */
  public SharedCoordinatorMetricGroup newView() {
    return new SharedCoordinatorMetricGroup(registered, variables);
  }

  @Override
  public <C extends Counter> C counter(String name, C counter) {
    registered.putIfAbsent(name, counter);
    return counter;
  }

  @Override
  public <H extends Histogram> H histogram(String name, H histogram) {
    registered.putIfAbsent(name, histogram);
    return histogram;
  }

  @Override
  public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
    registered.putIfAbsent(name, gauge);
    return gauge;
  }

  @Override
  public Map<String, String> getAllVariables() {
    return variables;
  }

  public Counter getCounter(String name) {
    return (Counter) registered.get(name);
  }

  public Histogram getHistogram(String name) {
    return (Histogram) registered.get(name);
  }

  public Gauge<?> getGauge(String name) {
    return (Gauge<?>) registered.get(name);
  }

  private static Map<String, String> variables(String jobId, String operatorId) {
    Map<String, String> variables = new HashMap<>();
    variables.put(ScopeFormat.SCOPE_JOB_ID, jobId);
    variables.put(ScopeFormat.SCOPE_OPERATOR_ID, operatorId);
    return variables;
  }
}
