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

import org.apache.hudi.adapter.MaskingOutputAdapter;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.compact.handler.CompactHandler;
import org.apache.hudi.sink.compact.handler.MetadataCompactHandler;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.Lazy;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.RuntimeContextUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;

/**
 * Operator to execute the actual compaction task assigned by the compaction plan task.
 * In order to execute scalable, the input should shuffle by the compact event {@link CompactionPlanEvent}.
 */
@Slf4j
public class CompactOperator extends TableStreamOperator<CompactionCommitEvent>
    implements OneInputStreamOperator<CompactionPlanEvent, CompactionCommitEvent> {

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Executor service to execute the compaction task.
   */
  private transient NonThrownExecutor executor;

  /**
   * Output records collector.
   */
  private transient StreamRecordCollector<CompactionCommitEvent> collector;

  /**
   * Compaction metrics.
   */
  private transient FlinkCompactionMetrics compactionMetrics;

  /**
   * Previous compact instant time.
   */
  private transient String prevCompactInstant = "";

  private transient Lazy<CompactHandler> compactHandler;

  private transient Lazy<CompactHandler> mdtCompactHandler;

  public CompactOperator(Configuration conf) {
    this.conf = conf;
  }

  /**
   * The modifier of this method is updated to `protected` sink Flink 2.0, here we overwrite the method
   * with `public` modifier to make it compatible considering usage in hudi-flink module.
   */
  @Override
  public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<CompactionCommitEvent>> output) {
    super.setup(containingTask, config, new MaskingOutputAdapter<>(output));
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
  public void open() throws Exception {
    // ID of current subtask
    int taskID = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
    HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, getRuntimeContext());
    if (conf.get(FlinkOptions.COMPACTION_OPERATION_EXECUTE_ASYNC_ENABLED)) {
      // executes compaction asynchronously.
      this.executor = NonThrownExecutor.builder(log).build();
    }
    this.collector = new StreamRecordCollector<>(output);
    this.compactHandler = Lazy.lazily(() -> new CompactHandler(writeClient, taskID));
    this.mdtCompactHandler = Lazy.lazily(() -> new MetadataCompactHandler(StreamerUtil.createMetadataWriteClient(writeClient), taskID));
    registerMetrics();
  }

  @Override
  public void processElement(StreamRecord<CompactionPlanEvent> record) throws Exception {
    final CompactionPlanEvent event = record.getValue();
    final String instantTime = event.getCompactionInstantTime();
    boolean needReloadMetaClient = !instantTime.equals(prevCompactInstant);
    prevCompactInstant = instantTime;

    if (event.isMetadataTable()) {
      mdtCompactHandler.get().compact(executor, event, collector, needReloadMetaClient, compactionMetrics);
    } else {
      compactHandler.get().compact(executor, event, collector, needReloadMetaClient, compactionMetrics);
    }
  }

  @VisibleForTesting
  public void setExecutor(NonThrownExecutor executor) {
    this.executor = executor;
  }

  @VisibleForTesting
  public void setOutput(Output<StreamRecord<CompactionCommitEvent>> output) {
    this.collector = new StreamRecordCollector<>(output);
  }

  @Override
  public void close() throws Exception {
    if (null != this.executor) {
      this.executor.close();
    }
    if (compactHandler.isInitialized()) {
      compactHandler.get().close();
    }
    if (mdtCompactHandler.isInitialized()) {
      mdtCompactHandler.get().close();
    }
  }

  private void registerMetrics() {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    compactionMetrics = new FlinkCompactionMetrics(metrics);
    compactionMetrics.registerMetrics();
  }
}
