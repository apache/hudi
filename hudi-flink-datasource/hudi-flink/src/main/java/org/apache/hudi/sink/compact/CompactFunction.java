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

package org.apache.hudi.sink.compact;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.table.HoodieFlinkCopyOnWriteTable;
import org.apache.hudi.table.action.compact.HoodieFlinkMergeOnReadTableCompactor;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Function to execute the actual compaction task assigned by the compaction plan task.
 * In order to execute scalable, the input should shuffle by the compact event {@link CompactionPlanEvent}.
 */
public class CompactFunction extends ProcessFunction<CompactionPlanEvent, CompactionCommitEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(CompactFunction.class);

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Write Client.
   */
  private transient HoodieFlinkWriteClient<?> writeClient;

  /**
   * Whether to execute compaction asynchronously.
   */
  private final boolean asyncCompaction;

  /**
   * Id of current subtask.
   */
  private int taskID;

  /**
   * Executor service to execute the compaction task.
   */
  private transient NonThrownExecutor executor;

  public CompactFunction(Configuration conf) {
    this.conf = conf;
    this.asyncCompaction = OptionsResolver.needsAsyncCompaction(conf);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.writeClient = StreamerUtil.createWriteClient(conf, getRuntimeContext());
    if (this.asyncCompaction) {
      this.executor = NonThrownExecutor.builder(LOG).build();
    }
  }

  @Override
  public void processElement(CompactionPlanEvent event, Context context, Collector<CompactionCommitEvent> collector) throws Exception {
    final String instantTime = event.getCompactionInstantTime();
    final CompactionOperation compactionOperation = event.getOperation();
    if (asyncCompaction) {
      // executes the compaction task asynchronously to not block the checkpoint barrier propagate.
      executor.execute(
          () -> doCompaction(instantTime, compactionOperation, collector, reloadWriteConfig()),
          (errMsg, t) -> collector.collect(new CompactionCommitEvent(instantTime, compactionOperation.getFileId(), taskID)),
          "Execute compaction for instant %s from task %d", instantTime, taskID);
    } else {
      // executes the compaction task synchronously for batch mode.
      LOG.info("Execute compaction for instant {} from task {}", instantTime, taskID);
      doCompaction(instantTime, compactionOperation, collector, writeClient.getConfig());
    }
  }

  private void doCompaction(String instantTime,
                            CompactionOperation compactionOperation,
                            Collector<CompactionCommitEvent> collector,
                            HoodieWriteConfig writeConfig) throws IOException {
    HoodieFlinkMergeOnReadTableCompactor<?> compactor = new HoodieFlinkMergeOnReadTableCompactor<>();
    List<WriteStatus> writeStatuses = compactor.compact(
        new HoodieFlinkCopyOnWriteTable<>(
            writeConfig,
            writeClient.getEngineContext(),
            writeClient.getHoodieTable().getMetaClient()),
        writeClient.getHoodieTable().getMetaClient(),
        writeClient.getConfig(),
        compactionOperation,
        instantTime,
        writeClient.getHoodieTable().getTaskContextSupplier());
    collector.collect(new CompactionCommitEvent(instantTime, compactionOperation.getFileId(), writeStatuses, taskID));
  }

  private HoodieWriteConfig reloadWriteConfig() throws Exception {
    HoodieWriteConfig writeConfig = writeClient.getConfig();
    CompactionUtil.setAvroSchema(writeConfig, writeClient.getHoodieTable().getMetaClient());
    return writeConfig;
  }

  @VisibleForTesting
  public void setExecutor(NonThrownExecutor executor) {
    this.executor = executor;
  }
}
