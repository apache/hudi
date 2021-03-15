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

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.table.HoodieFlinkCopyOnWriteTable;
import org.apache.hudi.table.action.compact.HoodieFlinkMergeOnReadTableCompactor;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Function to execute the actual compaction task assigned by the compaction plan task.
 * In order to execute scalable, the input should shuffle by the compact event {@link CompactionPlanEvent}.
 */
public class CompactFunction extends KeyedProcessFunction<Long, CompactionPlanEvent, CompactionCommitEvent> {

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Write Client.
   */
  private transient HoodieFlinkWriteClient writeClient;

  /**
   * Id of current subtask.
   */
  private int taskID;

  public CompactFunction(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    initWriteClient();
  }

  @Override
  public void processElement(CompactionPlanEvent event, Context context, Collector<CompactionCommitEvent> collector) throws Exception {
    final String instantTime = event.getCompactionInstantTime();
    final CompactionOperation compactionOperation = event.getOperation();

    HoodieFlinkMergeOnReadTableCompactor compactor = new HoodieFlinkMergeOnReadTableCompactor();
    List<WriteStatus> writeStatuses = compactor.compact(
        new HoodieFlinkCopyOnWriteTable<>(
            this.writeClient.getConfig(),
            this.writeClient.getEngineContext(),
            this.writeClient.getHoodieTable().getMetaClient()),
        this.writeClient.getHoodieTable().getMetaClient(),
        this.writeClient.getConfig(),
        compactionOperation,
        instantTime);
    collector.collect(new CompactionCommitEvent(instantTime, writeStatuses, taskID));
  }

  private void initWriteClient() {
    HoodieFlinkEngineContext context =
        new HoodieFlinkEngineContext(
            new SerializableConfiguration(StreamerUtil.getHadoopConf()),
            new FlinkTaskContextSupplier(getRuntimeContext()));

    writeClient = new HoodieFlinkWriteClient<>(context, StreamerUtil.getHoodieClientConfig(conf));
  }
}
