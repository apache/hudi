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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.format.FlinkRowDataReaderContext;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.table.action.compact.HoodieFlinkMergeOnReadTableCompactor;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Operator to execute the actual compaction task assigned by the compaction plan task.
 * In order to execute scalable, the input should shuffle by the compact event {@link CompactionPlanEvent}.
 */
public class CompactOperator extends TableStreamOperator<CompactionCommitEvent>
    implements OneInputStreamOperator<CompactionPlanEvent, CompactionCommitEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(CompactOperator.class);

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Write Client.
   */
  private transient HoodieFlinkWriteClient<?> writeClient;

  /**
   * Hoodie Flink table.
   */
  private transient HoodieFlinkTable<?> flinkTable;

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

  /**
   * Output records collector.
   */
  private transient StreamRecordCollector<CompactionCommitEvent> collector;

  /**
   * Reader context.
   */
  private transient Option<HoodieReaderContext<?>> readerContextOpt;

  /**
   * Compaction metrics.
   */
  private transient FlinkCompactionMetrics compactionMetrics;

  public CompactOperator(Configuration conf) {
    this.conf = conf;
    this.asyncCompaction = OptionsResolver.needsAsyncCompaction(conf);
  }

  @Override
  public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<CompactionCommitEvent>> output) {
    super.setup(containingTask, config, new MaskingOutputAdapter<>(output));
  }

  @Override
  public void open() throws Exception {
    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.writeClient = FlinkWriteClients.createWriteClient(conf, getRuntimeContext());
    this.flinkTable = this.writeClient.getHoodieTable();
    this.readerContextOpt = initReaderContext(this.writeClient);
    if (this.asyncCompaction) {
      this.executor = NonThrownExecutor.builder(LOG).build();
    }
    this.collector = new StreamRecordCollector<>(output);
    registerMetrics();
  }

  @Override
  public void processElement(StreamRecord<CompactionPlanEvent> record) throws Exception {
    final CompactionPlanEvent event = record.getValue();
    final String instantTime = event.getCompactionInstantTime();
    final CompactionOperation compactionOperation = event.getOperation();
    if (asyncCompaction) {
      // executes the compaction task asynchronously to not block the checkpoint barrier propagate.
      executor.execute(
          () -> doCompaction(instantTime, compactionOperation, collector, writeClient.getConfig()),
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
                            HoodieWriteConfig writeConfig) throws Exception {
    compactionMetrics.startCompaction();
    HoodieFlinkMergeOnReadTableCompactor<?> compactor = new HoodieFlinkMergeOnReadTableCompactor<>();
    HoodieTableMetaClient metaClient = flinkTable.getMetaClient();
    // reload the timeline
    metaClient.reload();
    // schema evolution
    CompactionUtil.setAvroSchema(writeConfig, metaClient);
    List<WriteStatus> writeStatuses = compactor.compact(
        flinkTable,
        writeConfig,
        compactionOperation,
        instantTime,
        flinkTable.getTaskContextSupplier(),
        readerContextOpt,
        flinkTable);
    compactionMetrics.endCompaction();
    collector.collect(new CompactionCommitEvent(instantTime, compactionOperation.getFileId(), writeStatuses, taskID));
  }

  private Option<HoodieReaderContext<?>> initReaderContext(HoodieFlinkWriteClient<?> writeClient) {
    HoodieTableMetaClient metaClient = flinkTable.getMetaClient();
    boolean useFileGroupReaderBasedCompaction = !metaClient.isMetadataTable()
        && writeClient.getConfig().getBooleanOrDefault(HoodieReaderConfig.FILE_GROUP_READER_ENABLED)
        && writeClient.getConfig().populateMetaFields()                                                         // Virtual key support by fg reader is not ready
        && !(metaClient.getTableConfig().isCDCEnabled() && writeClient.getConfig().isYieldingPureLogForMor());  // do not support produce cdc log during fg reader
    if (useFileGroupReaderBasedCompaction) {
      // CAUTION: reuse the meta client so that the timeline is updated
      Supplier<InternalSchemaManager> internalSchemaManager = () -> InternalSchemaManager.get(conf, metaClient);
      // initialize storage conf lazily.
      StorageConfiguration<?> readerConf = writeClient.getEngineContext().getStorageConf();
      return Option.of(new FlinkRowDataReaderContext(readerConf, internalSchemaManager, Collections.emptyList(), metaClient.getTableConfig(), Option.empty()));
    } else {
      // always using avro record merger for legacy compaction since log scanner do not support rowdata reading yet.
      writeClient.getConfig().setRecordMergerClass(HoodieAvroRecordMerger.class.getName());
    }
    return Option.empty();
  }

  @VisibleForTesting
  public void setExecutor(NonThrownExecutor executor) {
    this.executor = executor;
  }

  @Override
  public void close() throws Exception {
    if (null != this.executor) {
      this.executor.close();
    }
    if (null != this.writeClient) {
      this.writeClient.close();
      this.writeClient = null;
    }
  }

  private void registerMetrics() {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    compactionMetrics = new FlinkCompactionMetrics(metrics);
    compactionMetrics.registerMetrics();
  }
}
