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

import org.apache.hudi.adapter.AbstractRichFunctionAdapter;
import org.apache.hudi.adapter.SinkFunctionAdapter;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.compact.handler.CleanHandler;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * Sink function that cleans the old commits.
 *
 * <p>It starts a cleaning task on new checkpoints, there is only one cleaning task
 * at a time, a new task can not be scheduled until the last task finished(fails or normally succeed).
 * The cleaning task never expects to throw but only log.
 */
@Slf4j
public class CleanFunction<T> extends AbstractRichFunctionAdapter
    implements SinkFunctionAdapter<T>, CheckpointedFunction, CheckpointListener {

  private final Configuration conf;

  protected HoodieFlinkWriteClient writeClient;

  // clean handler for data table
  private transient Option<CleanHandler> cleanHandlerOpt;

  // clean handler for metadata table
  private transient Option<CleanHandler> mdtCleanHandlerOpt;

  public CleanFunction(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.writeClient = FlinkWriteClients.createWriteClient(conf, getRuntimeContext());
    if (conf.get(FlinkOptions.CLEAN_ASYNC_ENABLED)) {
      this.cleanHandlerOpt = Option.of(new CleanHandler(writeClient));
      this.mdtCleanHandlerOpt = OptionsResolver.isStreamingIndexWriteEnabled(conf)
          ? Option.of(new CleanHandler(StreamerUtil.createMetadataWriteClient(writeClient))) : Option.empty();
    } else {
      this.cleanHandlerOpt = Option.empty();
      this.mdtCleanHandlerOpt = Option.empty();
    }

    cleanHandlerOpt.ifPresent(CleanHandler::clean);
    mdtCleanHandlerOpt.ifPresent(CleanHandler::clean);
  }

  @Override
  public void notifyCheckpointComplete(long l) throws Exception {
    cleanHandlerOpt.ifPresent(CleanHandler::waitForCleaningFinish);
    mdtCleanHandlerOpt.ifPresent(CleanHandler::waitForCleaningFinish);
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    cleanHandlerOpt.ifPresent(CleanHandler::startAsyncCleaning);
    mdtCleanHandlerOpt.ifPresent(CleanHandler::startAsyncCleaning);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    // no operation
  }

  @Override
  public void close() throws Exception {
    cleanHandlerOpt.ifPresent(CleanHandler::close);
    mdtCleanHandlerOpt.ifPresent(CleanHandler::close);
  }
}
