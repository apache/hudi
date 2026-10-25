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

import org.apache.hudi.sink.compact.handler.CompactionPlanHandler;
import org.apache.hudi.sink.compact.handler.TableServiceHandlerFactory;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
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

  private transient CompactionPlanHandler compactionPlanHandler;

  public CompactionPlanOperator(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.compactionPlanHandler = TableServiceHandlerFactory.createCompactionPlanHandler(conf, getRuntimeContext());
    this.compactionPlanHandler.registerMetrics(getRuntimeContext().getMetricGroup());

    // when starting up, rolls back all the inflight compaction instants if there exists,
    // these instants are in priority for scheduling task because the compaction instants are
    // scheduled from earliest(FIFO sequence).
    this.compactionPlanHandler.rollbackCompaction();
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
    this.compactionPlanHandler.collectCompactionOperations(checkpointId, output);
  }

  @VisibleForTesting
  public void setOutput(Output<StreamRecord<CompactionPlanEvent>> output) {
    this.output = output;
  }

  @Override
  public void close() throws Exception {
    this.compactionPlanHandler.close();
    super.close();
  }

  @Override
  public void endInput() throws Exception {
    // Called when the input data ends, only used in batch mode.
    notifyCheckpointComplete(-1);
  }
}
