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
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.sink.compact.CompactOperator;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionCommitSink;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.compact.CompactionPlanOperator;

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
 * A wrapper class to manipulate the {@link CompactOperator} instance for testing.
 */
public class CompactFunctionWrapper {
  private final Configuration conf;

  private final IOManager ioManager;
  private final StreamingRuntimeContext runtimeContext;

  private final StreamTask<?, ?> streamTask;
  private final StreamConfig streamConfig;

  /**
   * Function that generates the {@link HoodieCompactionPlan}.
   */
  private CompactionPlanOperator compactionPlanOperator;
  /**
   * Output to collect the compaction plan events.
   */
  private CollectOutputAdapter<CompactionPlanEvent> planEventOutput;
  /**
   * Output to collect the compaction commit events.
   */
  private CollectOutputAdapter<CompactionCommitEvent> commitEventOutput;
  /**
   * Function that executes the compaction task.
   */
  private CompactOperator compactOperator;
  /**
   * Stream sink to handle compaction commits.
   */
  private CompactionCommitSink commitSink;

  public CompactFunctionWrapper(Configuration conf, StreamTask<?, ?> streamTask, StreamConfig streamConfig) {
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
    compactionPlanOperator = new CompactionPlanOperator(conf);
    planEventOutput =  new CollectOutputAdapter<>();
    compactionPlanOperator.setup(streamTask, streamConfig, planEventOutput);
    compactionPlanOperator.open();

    compactOperator = new CompactOperator(conf);
    // CAUTION: deprecated API used.
    compactOperator.setProcessingTimeService(new TestProcessingTimeService());
    commitEventOutput = new CollectOutputAdapter<>();
    compactOperator.setup(streamTask, streamConfig, commitEventOutput);
    compactOperator.open();
    final NonThrownExecutor syncExecutor = new MockCoordinatorExecutor(
        new MockOperatorCoordinatorContext(new OperatorID(), 1));
    compactOperator.setExecutor(syncExecutor);

    commitSink = new CompactionCommitSink(conf);
    commitSink.setRuntimeContext(runtimeContext);
    commitSink.open(conf);
  }

  public void compact(long checkpointID) throws Exception {
    // collect the CompactEvents.
    compactionPlanOperator.setOutput(planEventOutput);
    compactionPlanOperator.notifyCheckpointComplete(checkpointID);
    // collect the CompactCommitEvents
    for (CompactionPlanEvent event : planEventOutput.getRecords()) {
      compactOperator.processElement(new StreamRecord<>(event));
    }
    // handle and commit the compaction
    for (CompactionCommitEvent event : commitEventOutput.getRecords()) {
      commitSink.invoke(event, null);
    }
  }

  public void close() throws Exception {
    ioManager.close();
    if (compactionPlanOperator != null) {
      compactionPlanOperator.close();
    }
    if (compactOperator != null) {
      compactOperator.close();
    }
    if (commitSink != null) {
      commitSink.close();
    }
  }
}
