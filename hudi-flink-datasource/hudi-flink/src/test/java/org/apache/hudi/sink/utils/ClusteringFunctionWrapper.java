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

package org.apache.hudi.sink.utils;

import org.apache.hudi.sink.clustering.ClusteringCommitEvent;
import org.apache.hudi.sink.clustering.ClusteringCommitSink;
import org.apache.hudi.sink.clustering.ClusteringFunction;
import org.apache.hudi.sink.clustering.ClusteringPlanEvent;
import org.apache.hudi.sink.clustering.ClusteringPlanOperator;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A wrapper class to manipulate the {@link ClusteringFunction} instance for testing.
 */
public class ClusteringFunctionWrapper {
  private final Configuration conf;

  private final IOManager ioManager;
  private final StreamingRuntimeContext runtimeContext;

  private final MockEnvironment environment;

  /**
   * Function that generates the {@code HoodieClusteringPlan}.
   */
  private ClusteringPlanOperator clusteringPlanOperator;
  /**
   * Test harness for clustering function.
   */
  private OneInputStreamOperatorTestHarness<ClusteringPlanEvent, ClusteringCommitEvent> harness;
  /**
   * Function that executes the clustering task.
   */
  private ClusteringFunction clusteringFunction;
  /**
   * Stream sink to handle clustering commits.
   */
  private ClusteringCommitSink commitSink;

  public ClusteringFunctionWrapper(Configuration conf) {
    this.ioManager = new IOManagerAsync();
    this.environment = new MockEnvironmentBuilder()
        .setTaskName("mockTask")
        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
        .setIOManager(ioManager)
        .build();
    this.runtimeContext = new MockStreamingRuntimeContext(false, 1, 0, environment);
    this.conf = conf;
  }

  public void openFunction() throws Exception {
    clusteringPlanOperator = new ClusteringPlanOperator(conf);
    clusteringPlanOperator.open();

    ClusteringFunction.ClusteringOperatorFactory clusteringOperatorFactory =
        new ClusteringFunction.ClusteringOperatorFactory(conf, TestConfigurations.ROW_TYPE);
    harness = new OneInputStreamOperatorTestHarness<>(clusteringOperatorFactory,
        TypeInformation.of(ClusteringPlanEvent.class).createSerializer(environment.getExecutionConfig()),
        environment);
    harness.open();
    clusteringFunction = (ClusteringFunction)((AbstractUdfStreamOperator<?,?>) harness.getOperator()).getUserFunction();
    final NonThrownExecutor syncExecutor = new MockCoordinatorExecutor(
        new MockOperatorCoordinatorContext(new OperatorID(), 1));
    clusteringFunction.setExecutor(syncExecutor);

    commitSink = new ClusteringCommitSink(conf);
    commitSink.setRuntimeContext(runtimeContext);
    commitSink.open(conf);
  }

  public void cluster(long checkpointID) throws Exception {
    // collect the ClusteringPlanEvents.
    CollectorOutput<ClusteringPlanEvent> planOutput = new CollectorOutput<>();
    clusteringPlanOperator.setOutput(planOutput);
    clusteringPlanOperator.notifyCheckpointComplete(checkpointID);
    // collect the ClusteringCommitEvents
    List<ClusteringCommitEvent> compactCommitEvents = new ArrayList<>();
    for (ClusteringPlanEvent event : planOutput.getRecords()) {
      clusteringFunction.asyncInvoke(event, new ResultFuture<ClusteringCommitEvent>() {
        @Override
        public void complete(Collection<ClusteringCommitEvent> events) {
          compactCommitEvents.addAll(events);
        }

        @Override
        public void completeExceptionally(Throwable throwable) {
        }
      });
    }
    // handle and commit the clustering
    for (ClusteringCommitEvent event : compactCommitEvents) {
      commitSink.invoke(event, null);
    }
  }

  public void close() throws Exception {
    harness.close();
    ioManager.close();
  }
}
