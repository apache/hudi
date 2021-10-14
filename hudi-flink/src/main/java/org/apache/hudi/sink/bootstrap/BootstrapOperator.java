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

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.util.StreamerUtil.isValidFile;

/**
 * The operator to load index from existing hoodieTable.
 *
 * <p>Each subtask of the function triggers the index bootstrap when the first element came in,
 * the record cannot be sent until all the index records have been sent.
 *
 * <p>The output records should then shuffle by the recordKey and thus do scalable write.
 */
public class BootstrapOperator<I, O extends HoodieRecord>
    extends AbstractStreamOperator<O> implements OneInputStreamOperator<I, O> {

  private static final Logger LOG = LoggerFactory.getLogger(BootstrapOperator.class);

  protected HoodieTable<?, ?, ?, ?> hoodieTable;

  protected final Configuration conf;

  protected transient org.apache.hadoop.conf.Configuration hadoopConf;
  protected transient HoodieWriteConfig writeConfig;

  private transient ListState<String> instantState;
  private final Pattern pattern;
  private String lastInstantTime;
  private HoodieFlinkWriteClient writeClient;
  private String actionType;

  public BootstrapOperator(Configuration conf) {
    this.conf = conf;
    this.pattern = Pattern.compile(conf.getString(FlinkOptions.INDEX_PARTITION_REGEX));
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    lastInstantTime = this.writeClient.getLastPendingInstant(this.actionType);
    instantState.update(Collections.singletonList(lastInstantTime));
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

    this.hadoopConf = StreamerUtil.getHadoopConf();
    this.writeConfig = StreamerUtil.getHoodieClientConfig(this.conf);
    this.hoodieTable = getTable();
    this.writeClient = StreamerUtil.createWriteClient(this.conf, getRuntimeContext());
    this.actionType = CommitUtils.getCommitActionType(
        WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION)),
        HoodieTableType.valueOf(conf.getString(FlinkOptions.TABLE_TYPE)));

    preLoadIndexRecords();
  }

  /**
   * Load the index records before {@link #processElement}.
   */
  protected void preLoadIndexRecords() throws Exception {
    String basePath = hoodieTable.getMetaClient().getBasePath();
    int taskID = getRuntimeContext().getIndexOfThisSubtask();
    LOG.info("Start loading records in table {} into the index state, taskId = {}", basePath, taskID);
    for (String partitionPath : FSUtils.getAllFoldersWithPartitionMetaFile(FSUtils.getFs(basePath, hadoopConf), basePath)) {
      if (pattern.matcher(partitionPath).matches()) {
        loadRecords(partitionPath);
      }
    }

    LOG.info("Finish sending index records, taskId = {}.", getRuntimeContext().getIndexOfThisSubtask());
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processElement(StreamRecord<I> element) throws Exception {
    output.collect((StreamRecord<O>) element);
  }

  private HoodieFlinkTable getTable() {
    HoodieFlinkEngineContext context = new HoodieFlinkEngineContext(
        new SerializableConfiguration(this.hadoopConf),
        new FlinkTaskContextSupplier(getRuntimeContext()));
    return HoodieFlinkTable.create(this.writeConfig, context);
  }

  /**
   * Loads all the indices of give partition path into the backup state.
   *
   * @param partitionPath The partition path
   */
  @SuppressWarnings("unchecked")
  protected void loadRecords(String partitionPath) throws Exception {
    long start = System.currentTimeMillis();

    BaseFileUtils fileUtils = BaseFileUtils.getInstance(this.hoodieTable.getBaseFileFormat());
    Schema schema = new TableSchemaResolver(this.hoodieTable.getMetaClient()).getTableAvroSchema();

    final int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
    final int maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
    final int taskID = getRuntimeContext().getIndexOfThisSubtask();

    HoodieTimeline commitsTimeline = this.hoodieTable.getMetaClient().getCommitsTimeline();
    if (!StringUtils.isNullOrEmpty(lastInstantTime)) {
      commitsTimeline = commitsTimeline.findInstantsAfter(lastInstantTime);
    }
    Option<HoodieInstant> latestCommitTime = commitsTimeline.filterCompletedInstants().lastInstant();

    if (latestCommitTime.isPresent()) {
      List<FileSlice> fileSlices = this.hoodieTable.getSliceView()
          .getLatestFileSlicesBeforeOrOn(partitionPath, latestCommitTime.get().getTimestamp(), true)
          .collect(toList());

      for (FileSlice fileSlice : fileSlices) {
        if (!shouldLoadFile(fileSlice.getFileId(), maxParallelism, parallelism, taskID)) {
          continue;
        }
        LOG.info("Load records from {}.", fileSlice);

        // load parquet records
        fileSlice.getBaseFile().ifPresent(baseFile -> {
          // filter out crushed files
          if (!isValidFile(baseFile.getFileStatus())) {
            return;
          }

          final List<HoodieKey> hoodieKeys;
          try {
            hoodieKeys =
                fileUtils.fetchRecordKeyPartitionPath(this.hadoopConf, new Path(baseFile.getPath()));
          } catch (Exception e) {
            throw new HoodieException(String.format("Error when loading record keys from file: %s", baseFile), e);
          }

          for (HoodieKey hoodieKey : hoodieKeys) {
            output.collect(new StreamRecord(new IndexRecord(generateHoodieRecord(hoodieKey, fileSlice))));
          }
        });

        // load avro log records
        List<String> logPaths = fileSlice.getLogFiles()
            // filter out crushed files
            .filter(logFile -> isValidFile(logFile.getFileStatus()))
            .map(logFile -> logFile.getPath().toString())
            .collect(toList());
        HoodieMergedLogRecordScanner scanner = FormatUtils.logScanner(logPaths, schema, latestCommitTime.get().getTimestamp(),
            writeConfig, hadoopConf);

        try {
          for (String recordKey : scanner.getRecords().keySet()) {
            output.collect(new StreamRecord(new IndexRecord(generateHoodieRecord(new HoodieKey(recordKey, partitionPath), fileSlice))));
          }
        } catch (Exception e) {
          throw new HoodieException(String.format("Error when loading record keys from files: %s", logPaths), e);
        } finally {
          scanner.close();
        }
      }
    }

    long cost = System.currentTimeMillis() - start;
    LOG.info("Task [{}}:{}}] finish loading the index under partition {} and sending them to downstream, time cost: {} milliseconds.",
        this.getClass().getSimpleName(), taskID, partitionPath, cost);
  }

  @SuppressWarnings("unchecked")
  public static HoodieRecord generateHoodieRecord(HoodieKey hoodieKey, FileSlice fileSlice) {
    HoodieRecord hoodieRecord = new HoodieRecord(hoodieKey, null);
    hoodieRecord.setCurrentLocation(new HoodieRecordGlobalLocation(hoodieKey.getPartitionPath(), fileSlice.getBaseInstantTime(), fileSlice.getFileId()));
    hoodieRecord.seal();

    return hoodieRecord;
  }

  private static boolean shouldLoadFile(String fileId,
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
