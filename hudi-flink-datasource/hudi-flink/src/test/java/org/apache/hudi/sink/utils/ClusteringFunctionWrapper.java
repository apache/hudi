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

import org.apache.hudi.adapter.CollectOutputAdapter;
import org.apache.hudi.sink.clustering.ClusteringCommitEvent;
import org.apache.hudi.sink.clustering.ClusteringCommitSink;
import org.apache.hudi.sink.clustering.ClusteringOperator;
import org.apache.hudi.sink.clustering.ClusteringPlanEvent;
import org.apache.hudi.sink.clustering.ClusteringPlanOperator;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

/**
 * A wrapper class to manipulate the {@link ClusteringOperator} instance for testing.
 */
public class ClusteringFunctionWrapper {
  private final Configuration conf;

  private final IOManager ioManager;
  private final StreamingRuntimeContext runtimeContext;

  private final StreamTask<?, ?> streamTask;
  private final StreamConfig streamConfig;

  /**
   * Function that generates the {@code HoodieClusteringPlan}.
   */
  private ClusteringPlanOperator clusteringPlanOperator;
  /**
   * Output to collect the clustering plan events.
   */
  private CollectOutputAdapter<ClusteringPlanEvent> planEventOutput;
  /**
   * Output to collect the clustering commit events.
   */
  private CollectOutputAdapter<ClusteringCommitEvent> commitEventOutput;
  /**
   * Function that executes the clustering task.
   */
  private ClusteringOperator clusteringOperator;
  /**
   * Stream sink to handle clustering commits.
   */
  private ClusteringCommitSink commitSink;

  public ClusteringFunctionWrapper(Configuration conf, StreamTask<?, ?> streamTask, StreamConfig streamConfig) {
    this.conf = conf;
    this.ioManager = new IOManagerAsync();
    MockEnvironment environment = new MockEnvironmentBuilder()
        .setTaskName("mockTask")
        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
        .setIOManager(ioManager)
        .build();
    this.runtimeContext = new MockStreamingRuntimeContext(false, 1, 0, environment);
    this.streamTask = streamTask;
    this.streamConfig = streamConfig;
  }

  public void openFunction() throws Exception {
    clusteringPlanOperator = new ClusteringPlanOperator(conf);
    planEventOutput =  new CollectOutputAdapter<>();
    clusteringPlanOperator.setup(streamTask, streamConfig, planEventOutput);
    clusteringPlanOperator.open();

    clusteringOperator = new ClusteringOperator(conf, TestConfigurations.ROW_TYPE);
    // CAUTION: deprecated API used.
    clusteringOperator.setProcessingTimeService(new TestProcessingTimeService());
    commitEventOutput = new CollectOutputAdapter<>();
    clusteringOperator.setup(streamTask, streamConfig, commitEventOutput);
    clusteringOperator.open();
    final NonThrownExecutor syncExecutor = new MockCoordinatorExecutor(
        new MockOperatorCoordinatorContext(new OperatorID(), 1));
    clusteringOperator.setExecutor(syncExecutor);

    commitSink = new ClusteringCommitSink(conf);
    commitSink.setRuntimeContext(runtimeContext);
    commitSink.open(conf);
  }

  public void cluster(long checkpointID) throws Exception {
    // collect the ClusteringPlanEvents.
    CollectOutputAdapter<ClusteringPlanEvent> planOutput = new CollectOutputAdapter<>();
    clusteringPlanOperator.setOutput(planOutput);
    clusteringPlanOperator.notifyCheckpointComplete(checkpointID);
    // collect the ClusteringCommitEvents
    for (ClusteringPlanEvent event : planOutput.getRecords()) {
      clusteringOperator.processElement(new StreamRecord<>(event));
    }
    // handle and commit the clustering
    for (ClusteringCommitEvent event : commitEventOutput.getRecords()) {
      commitSink.invoke(event, null);
    }
  }

  public void close() throws Exception {
    ioManager.close();
  }
}
