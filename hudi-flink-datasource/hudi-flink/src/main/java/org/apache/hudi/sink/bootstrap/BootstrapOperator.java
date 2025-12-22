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

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.bootstrap.aggregate.BootstrapAggFunction;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.FlinkTables;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.RuntimeContextUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.util.StreamerUtil.isValidFile;
import static org.apache.hudi.util.StreamerUtil.metadataConfig;

/**
 * The operator to load index from existing hoodieTable.
 *
 * <p>Each subtask of the function triggers the index bootstrap when the first element came in,
 * the record cannot be sent until all the index records have been sent.
 *
 * <p>The output records should then shuffle by the recordKey and thus do scalable write.
 */
@Slf4j
public class BootstrapOperator
    extends AbstractStreamOperator<HoodieFlinkInternalRow>
    implements OneInputStreamOperator<HoodieFlinkInternalRow, HoodieFlinkInternalRow> {

  protected HoodieTable<?, ?, ?, ?> hoodieTable;

  protected final Configuration conf;

  protected transient org.apache.hadoop.conf.Configuration hadoopConf;
  protected transient HoodieWriteConfig writeConfig;

  private transient GlobalAggregateManager aggregateManager;

  private transient ListState<String> instantState;
  private transient HoodieTableMetaClient metaClient;
  private transient InternalSchemaManager internalSchemaManager;

  private final Pattern pattern;
  private String lastInstantTime;

  public BootstrapOperator(Configuration conf) {
    this.conf = conf;
    this.pattern = Pattern.compile(conf.get(FlinkOptions.INDEX_PARTITION_REGEX));
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
  public void snapshotState(StateSnapshotContext context) throws Exception {
    lastInstantTime = StreamerUtil.getLastCompletedInstant(StreamerUtil.createMetaClient(this.conf));
    if (null != lastInstantTime) {
      instantState.update(Collections.singletonList(lastInstantTime));
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    ListStateDescriptor<String> instantStateDescriptor = new ListStateDescriptor<>(
        "instantStateDescriptor",
        Types.STRING
    );
    instantState = context.getOperatorStateStore().getListState(instantStateDescriptor);

    if (context.isRestored()) {
      Iterator<String> instantIterator = instantState.get().iterator();
      if (instantIterator.hasNext()) {
        lastInstantTime = instantIterator.next();
      }
    }

    this.hadoopConf = HadoopConfigurations.getHadoopConf(this.conf);
    // not load fs view storage config for incremental job graph, since embedded timeline server
    // is started in write coordinator which is started after bootstrap.
    this.writeConfig = FlinkWriteClients.getHoodieClientConfig(
        this.conf, false, !OptionsResolver.isIncrementalJobGraph(conf));
    this.hoodieTable = FlinkTables.createTable(writeConfig, hadoopConf, getRuntimeContext());
    this.aggregateManager = getRuntimeContext().getGlobalAggregateManager();
    this.metaClient = StreamerUtil.createMetaClient(conf, hadoopConf);
    this.internalSchemaManager = InternalSchemaManager.get(hoodieTable.getStorageConf(), metaClient);

    preLoadIndexRecords();
  }

  /**
   * Load the index records before {@link #processElement}.
   */
  protected void preLoadIndexRecords() throws Exception {
    StoragePath basePath = hoodieTable.getMetaClient().getBasePath();
    int taskID = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
    log.info("Start loading records in table {} into the index state, taskId = {}", basePath, taskID);
    for (String partitionPath : FSUtils.getAllPartitionPaths(new HoodieFlinkEngineContext(hadoopConf), hoodieTable.getMetaClient(), metadataConfig(conf))) {
      if (pattern.matcher(partitionPath).matches()) {
        loadRecords(partitionPath);
      }
    }

    log.info("Finish sending index records, taskId = {}.", taskID);

    // wait for the other bootstrap tasks finish bootstrapping.
    waitForBootstrapReady(taskID);
    hoodieTable = null;
  }

  /**
   * Wait for other bootstrap tasks to finish the index bootstrap.
   */
  private void waitForBootstrapReady(int taskID) {
    int taskNum = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());
    int readyTaskNum = 1;
    while (taskNum != readyTaskNum) {
      try {
        readyTaskNum = aggregateManager.updateGlobalAggregate(BootstrapAggFunction.NAME + conf.get(FlinkOptions.TABLE_NAME), taskID, new BootstrapAggFunction());
        log.info("Waiting for other bootstrap tasks to complete, taskId = {}.", taskID);

        TimeUnit.SECONDS.sleep(5);
      } catch (Exception e) {
        log.error("Updating global task bootstrap summary failed", e);
      }
    }
  }

  @Override
  public void processElement(StreamRecord<HoodieFlinkInternalRow> element) throws Exception {
    output.collect(element);
  }

  /**
   * Loads all the indices of give partition path into the backup state.
   *
   * @param partitionPath The partition path
   */
  protected void loadRecords(String partitionPath) throws Exception {
    long start = System.currentTimeMillis();

    final int parallelism = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());
    final int maxParallelism = RuntimeContextUtils.getMaxNumberOfParallelSubtasks(getRuntimeContext());
    final int taskID = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());

    HoodieTimeline commitsTimeline = this.hoodieTable.getMetaClient().getCommitsTimeline();
    if (!StringUtils.isNullOrEmpty(lastInstantTime)) {
      commitsTimeline = commitsTimeline.findInstantsAfter(lastInstantTime);
    }
    Option<HoodieInstant> latestCommitTime = commitsTimeline.filterCompletedAndCompactionInstants().lastInstant();

    if (latestCommitTime.isPresent()) {
      HoodieSchema schema =
          new TableSchemaResolver(this.hoodieTable.getMetaClient()).getTableSchema();

      List<FileSlice> fileSlices = this.hoodieTable.getSliceView()
          .getLatestMergedFileSlicesBeforeOrOn(partitionPath, latestCommitTime.get().requestedTime())
          .collect(toList());

      for (FileSlice fileSlice : fileSlices) {
        if (!shouldLoadFile(fileSlice.getFileId(), maxParallelism, parallelism, taskID)) {
          continue;
        }
        log.info("Load records from {}.", fileSlice);
        try (ClosableIterator<String> recordKeyIterator = getRecordKeyIterator(fileSlice, schema)) {
          while (recordKeyIterator.hasNext()) {
            String recordKey = recordKeyIterator.next();
            insertIndexStreamRecord(recordKey, partitionPath, fileSlice);
          }
        }
      }
    }

    long cost = System.currentTimeMillis() - start;
    log.info("Task [{}}:{}}] finish loading the index under partition {} and sending them to downstream, time cost: {} milliseconds.",
        this.getClass().getSimpleName(), taskID, partitionPath, cost);
  }

  /**
   * Get a record key iterator for the given {@link FileSlice} with invalid file filtered out.
   *
   * @param fileSlice   a file slice
   * @param tableSchema schema of the table
   *
   * @return A record key iterator for the file slice.
   */
  private ClosableIterator<String> getRecordKeyIterator(FileSlice fileSlice, HoodieSchema tableSchema) throws IOException {
    FileSlice scanFileSlice = new FileSlice(fileSlice.getPartitionPath(), fileSlice.getBaseInstantTime(), fileSlice.getFileId());
    // filter out crushed base file
    fileSlice.getBaseFile().map(f -> isValidFile(f.getPathInfo()) ? f : null).ifPresent(scanFileSlice::setBaseFile);
    // filter out crushed log files
    fileSlice.getLogFiles()
        .filter(logFile -> isValidFile(logFile.getPathInfo()))
        .forEach(scanFileSlice::addLogFile);

    HoodieFileGroupReader<RowData> fileGroupReader = FormatUtils.createFileGroupReader(metaClient, writeConfig, internalSchemaManager, scanFileSlice,
        tableSchema, tableSchema, scanFileSlice.getLatestInstantTime(), FlinkOptions.REALTIME_PAYLOAD_COMBINE, true, Collections.emptyList(), Option.empty());
    return fileGroupReader.getClosableKeyIterator();
  }

  protected void insertIndexStreamRecord(String recordKey, String partitionPath, FileSlice fileSlice) {
    output.collect(
        new StreamRecord<>(
            new HoodieFlinkInternalRow(
                recordKey,
                partitionPath,
                fileSlice.getFileId(),
                fileSlice.getBaseInstantTime())));
  }

  protected boolean shouldLoadFile(String fileId,
                                   int maxParallelism,
                                   int parallelism,
                                   int taskID) {
    return KeyGroupRangeAssignment.assignKeyToParallelOperator(
        fileId, maxParallelism, parallelism) == taskID;
  }

  @VisibleForTesting
  public boolean isAlreadyBootstrap() throws Exception {
    return instantState.get().iterator().hasNext();
  }
}
