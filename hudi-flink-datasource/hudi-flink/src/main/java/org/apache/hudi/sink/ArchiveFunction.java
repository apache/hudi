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

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveFunction<T> extends RichMapFunction<Object, Object> implements CheckpointedFunction, CheckpointListener {
  private static final Logger LOG = LoggerFactory.getLogger(ArchiveFunction.class);

  private final Configuration conf;

  protected volatile boolean isArchiving;

  private NonThrownExecutor executor;

  private transient HoodieFlinkWriteClient writeClient;

  public ArchiveFunction(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.executor = NonThrownExecutor.builder(LOG).waitForTasksFinish(true).build();
    this.writeClient = FlinkWriteClients.createWriteClient(conf, getRuntimeContext());
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    if (OptionsResolver.needsAsyncArchive(conf) && !isArchiving) {
      try {
        this.writeClient.startAsyncArchiving();
        this.isArchiving = true;
        LOG.warn("start async archiving for checkpoint " + context.getCheckpointId());
      } catch (Throwable throwable) {
        // catch the exception to not affect the normal checkpointing
        LOG.warn("Error while start async archiving", throwable);
      }
    }
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    if (OptionsResolver.needsAsyncArchive(conf) && isArchiving) {
      executor.execute(() -> {
        try {
          this.writeClient.waitForArchivingFinish();
          LOG.info("wait for archiving finish for checkpoint " + checkpointId);
        } finally {
          // ensure to switch the isCleaning flag
          this.isArchiving = false;
        }
      }, "wait for archiving finish");
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    // no operation
  }

  @Override
  public Object map(Object value) throws Exception {
    return value;
  }

  public boolean isArchiving() {
    return isArchiving;
  }
}