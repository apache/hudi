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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.compact.handler.CompactionPlanHandler;
import org.apache.hudi.sink.compact.handler.MetadataCompactionPlanHandler;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.RowData;

/**
 * Operator that generates the compaction plan with pluggable strategies on finished checkpoints.
 *
 * <p>It should be singleton to avoid conflicts.
 */
@Slf4j
public class CompactionPlanOperator extends AbstractStreamOperator<CompactionPlanEvent>
    implements OneInputStreamOperator<RowData, CompactionPlanEvent>, BoundedOneInput {

  /**
   * Config options.
   */
  private final Configuration conf;

  private transient FlinkCompactionMetrics compactionMetrics;

  private transient Option<CompactionPlanHandler> compactionPlanHandler;

  private transient Option<CompactionPlanHandler> mdtCompactionPlanHandler;

  public CompactionPlanOperator(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void open() throws Exception {
    super.open();
    registerMetrics();
    HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, getRuntimeContext());
    this.compactionPlanHandler = OptionsResolver.needsAsyncCompaction(conf)
        ? Option.of(new CompactionPlanHandler(writeClient)) : Option.empty();
    this.mdtCompactionPlanHandler = OptionsResolver.needsAsyncMetadataCompaction(conf)
        ? Option.of(new MetadataCompactionPlanHandler(StreamerUtil.createMetadataWriteClient(writeClient))) : Option.empty();

    // when starting up, rolls back all the inflight compaction instants if there exists,
    // these instants are in priority for scheduling task because the compaction instants are
    // scheduled from earliest(FIFO sequence).
    this.compactionPlanHandler.ifPresent(CompactionPlanHandler::rollbackCompaction);
    this.mdtCompactionPlanHandler.ifPresent(CompactionPlanHandler::rollbackCompaction);
  }

  /**
   * The modifier of this method is updated to `protected` sink Flink 2.0, here we overwrite the method
   * with `public` modifier to make it compatible considering usage in hudi-flink module.
   */
  @Override
  public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<CompactionPlanEvent>> output) {
    super.setup(containingTask, config, output);
  }

  /**
   * The modifier of this method is updated to `protected` sink Flink 2.0, here we overwrite the method
   * with `public` modifier to make it compatible considering usage in hudi-flink module.
   */
  @Override
  public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
    super.setProcessingTimeService(processingTimeService);
  }

  @Override
  public void processElement(StreamRecord<RowData> streamRecord) {
    // no operation
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    // comment out: do we really need the timeout rollback ?
    // CompactionUtil.rollbackEarliestCompaction(table, conf);
    // schedule data table compaction if enabled
    this.compactionPlanHandler.ifPresent(handler -> handler.collectCompactionOperations(checkpointId, compactionMetrics, output));
    // Also schedule metadata table compaction if enabled
    this.mdtCompactionPlanHandler.ifPresent(handler -> handler.collectCompactionOperations(checkpointId, compactionMetrics, output));
  }

  @VisibleForTesting
  public void setOutput(Output<StreamRecord<CompactionPlanEvent>> output) {
    this.output = output;
  }

  @Override
  public void close() throws Exception {
    this.compactionPlanHandler.ifPresent(CompactionPlanHandler::close);
    this.mdtCompactionPlanHandler.ifPresent(CompactionPlanHandler::close);
    super.close();
  }

  @Override
  public void endInput() throws Exception {
    // Called when the input data ends, only used in batch mode.
    notifyCheckpointComplete(-1);
  }

  private void registerMetrics() {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    compactionMetrics = new FlinkCompactionMetrics(metrics);
    compactionMetrics.registerMetrics();
  }
}
