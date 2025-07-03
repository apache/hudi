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

package org.apache.hudi.sink.v2;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Clean function that cleans the old commits.
 *
 * <p>It starts a cleaning task on new checkpoints, there is only one cleaning task
 * at a time, a new task can not be scheduled until the last task finished(fails or normally succeed).
 * The cleaning task never expects to throw but only log.
 *
 * <p>Note: The difference with {@code CleanFunction} is {@code CleanFunctionV2} extends {@code ProcessFunction},
 * while {@code CleanFunction} is a {@code SinkFunction}.
 */
public class CleanFunctionV2<T> extends ProcessFunction<T, RowData>
    implements CheckpointedFunction, CheckpointListener {
  private static final Logger LOG = LoggerFactory.getLogger(CleanFunctionV2.class);

  private final Configuration conf;

  protected HoodieFlinkWriteClient writeClient;

  private NonThrownExecutor executor;

  protected volatile boolean isCleaning;

  public CleanFunctionV2(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.writeClient = FlinkWriteClients.createWriteClient(conf, getRuntimeContext());
    this.executor = NonThrownExecutor.builder(LOG).waitForTasksFinish(true).build();
    if (conf.getBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED)) {
      executor.execute(() -> {
        this.isCleaning = true;
        try {
          this.writeClient.clean();
        } finally {
          this.isCleaning = false;
        }
      }, "wait for cleaning finish");
    }
  }

  @Override
  public void notifyCheckpointComplete(long l) throws Exception {
    if (conf.getBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED) && isCleaning) {
      executor.execute(() -> {
        try {
          this.writeClient.waitForCleaningFinish();
        } finally {
          // ensure to switch the isCleaning flag
          this.isCleaning = false;
        }
      }, "wait for cleaning finish");
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    if (conf.getBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED) && !isCleaning) {
      try {
        this.writeClient.startAsyncCleaning();
        this.isCleaning = true;
      } catch (Throwable throwable) {
        // catch the exception to not affect the normal checkpointing
        LOG.warn("Error while start async cleaning", throwable);
      }
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    // no operation
  }

  @Override
  public void close() throws Exception {
    if (executor != null) {
      executor.close();
    }

    if (this.writeClient != null) {
      this.writeClient.close();
    }
  }

  @Override
  public void processElement(T t, ProcessFunction<T, RowData>.Context context, Collector<RowData> collector) throws Exception {
    // do nothing.
  }
}
