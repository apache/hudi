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

package org.apache.hudi.sink;

import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

public class ContextAdapter implements OperatorCoordinator.Context {

  public OperatorCoordinator.Context context;

  public ContextAdapter(OperatorCoordinator.Context context) {
    this.context = context;
  }

  public OperatorID getOperatorId() {
    return this.context.getOperatorId();
  }

  public OperatorCoordinatorMetricGroup metricGroup() {
    return null;
  }

  public void failJob(Throwable throwable) {

  }

  public int currentParallelism() {
    return this.context.currentParallelism();
  }

  public ClassLoader getUserCodeClassloader() {
    return this.context.getUserCodeClassloader();
  }

  public CoordinatorStore getCoordinatorStore() {
    return this.context.getCoordinatorStore();
  }

  public boolean isConcurrentExecutionAttemptsSupported() {
    return this.context.isConcurrentExecutionAttemptsSupported();
  }

  public CheckpointCoordinator getCheckpointCoordinator() {
    return null;
  }
}
