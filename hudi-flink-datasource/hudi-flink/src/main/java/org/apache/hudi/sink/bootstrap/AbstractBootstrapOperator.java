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

package org.apache.hudi.sink.bootstrap;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.bootstrap.aggregate.BootstrapAggFunction;
import org.apache.hudi.utils.RuntimeContextUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.concurrent.TimeUnit;

/**
 * Base operator for bootstrap stages that emit preloaded index records.
 */
@Slf4j
public abstract class AbstractBootstrapOperator
    extends AbstractStreamOperator<HoodieFlinkInternalRow>
    implements OneInputStreamOperator<HoodieFlinkInternalRow, HoodieFlinkInternalRow> {

  protected final Configuration conf;

  protected AbstractBootstrapOperator(Configuration conf) {
    this.conf = conf;
  }

  /**
   * The modifier of this method is updated to `protected` sink Flink 2.0, here we overwrite the method
   * with `public` modifier to make it compatible considering usage in hudi-flink module.
   */
  @Override
  public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<HoodieFlinkInternalRow>> output) {
    super.setup(containingTask, config, output);
  }

  @Override
  public void processElement(StreamRecord<HoodieFlinkInternalRow> element) throws Exception {
    output.collect(element);
  }

  protected void waitForBootstrapReady(int taskID) {
    GlobalAggregateManager aggregateManager = getRuntimeContext().getGlobalAggregateManager();
    int taskNum = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());
    int readyTaskNum = 1;
    while (taskNum != readyTaskNum) {
      try {
        readyTaskNum = aggregateManager.updateGlobalAggregate(
            BootstrapAggFunction.NAME + conf.get(FlinkOptions.TABLE_NAME),
            taskID,
            new BootstrapAggFunction());
        log.info("Waiting for other bootstrap tasks to complete, taskId = {}, ready = {}/{}", taskID, readyTaskNum, taskNum);
        TimeUnit.SECONDS.sleep(5);
      } catch (Exception e) {
        log.error("Updating global task bootstrap summary failed", e);
      }
    }
  }
}
