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

package org.apache.hudi.sink.successfile;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.hudi.adapter.PartitionTimeExtractorAdapter;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;


/**
 * Sink that write a success file to partition path when it write finished.
 **/
public class SuccessFileWriteFunction<I> extends RichSinkFunction<I> implements CheckpointedFunction, CheckpointListener {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SuccessFileWriteFunction.class);
  // Success file name.
  private static final String SUCCESS_FILE_NAME = "_SUCCESS";
  // The name of active partitions state.
  private static final String ACTIVE_PARTITION_STATE_NAME = "active-partition-state";
  // The name of finished partition state.
  private static final String FINISHED_PARTITION_STATE_NAME = "finished-partition-state";
  // The global configuration of flink job.
  private Configuration conf;
  // The configured file system handle.
  private FileSystem fileSystem;
  // The extractor for partition time.
  private PartitionTimeExtractorAdapter partitionTimeExtractor;
  // The extractor for extract partition value from partition path.
  private PartitionValueExtractor partitionValueExtractor;
  // The table base path.
  private String tablePath;
  // The partition keys.
  private List<String> partitionKeys;
  // The partitions on writing currently.
  private final Set<String> activePartitions;
  // The partitions write finished.
  private final Set<String> finishedPartitions;
  // The operator state to store active partitions.
  private ListState<String> activePartitionsState;
  // The operator state to store finished partitions.
  private ListState<String> finishedPartitionsState;
  // The configured time delay to write success file.
  private Duration partitionSuccessFileDelay;
  // The operator event gateway.
  private OperatorEventGateway gateway;
  // The task id .
  private int taskID;

  public SuccessFileWriteFunction(Configuration conf) {
    this.conf = conf;
    activePartitions = new TreeSet<>();
    finishedPartitions = new TreeSet<>();
  }

  @Override
  public void open(Configuration config) throws Exception {
    tablePath = conf.get(FlinkOptions.PATH);
    fileSystem = FileSystem.get(URI.create(tablePath));
    String[] partitionFields = conf.get(FlinkOptions.PARTITION_PATH_FIELD).split(",");
    partitionKeys = Arrays.asList(partitionFields);
    partitionSuccessFileDelay = conf.get(FlinkOptions.PARTITION_WRITE_SUCCESS_FILE_DELAY);
    String partitionValueExtractorClzName = conf.get(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME);
    String partitionTimestampExtractPattern = conf.get(FlinkOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN);
    try {
      Class<?> partitionValueExtractorClz = Class.forName(partitionValueExtractorClzName);
      partitionValueExtractor = (PartitionValueExtractor) partitionValueExtractorClz.newInstance();
    } catch (ClassNotFoundException e) {
      LOG.error("class not found for: {}", partitionValueExtractorClzName, e);
      throw e;
    }
    partitionTimeExtractor = new PartitionTimeExtractorAdapter(partitionTimestampExtractPattern, null);
    taskID = getRuntimeContext().getIndexOfThisSubtask();
  }

  // Extract the partition time value from partition path, and convert them to timestamp.
  private Long convertTimestampByPartitionPath(String partitionPath) {
    List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partitionPath);
    LocalDateTime localDateTime = partitionTimeExtractor.extract(partitionKeys, partitionValues);
    return PartitionTimeExtractorAdapter.toMills(localDateTime);
  }

  @Override
  public void invoke(Object value, Context context) throws Exception {
    String partition = (String) value;
    activePartitions.add(partition);
    long watermark = context.currentWatermark();
    Iterator<String> it = activePartitions.iterator();
    while (it.hasNext()) {
      String partitionPath = it.next();
      // Convert the partition path to timestamp if the table is partitioned by time field, like day, hour
      Long partitionTimestamp = convertTimestampByPartitionPath(partitionPath);
      // If the watermark is greater than the partition timestamp plus the delay time, it represents the
      // minimum timestamp in the streaming data is beyond the partition max timestamp, so add the partition
      // path to the finished partitions set and remove it from active partitions set.
      if (partitionTimestamp + partitionSuccessFileDelay.toMillis() < watermark) {
        finishedPartitions.add(partitionPath);
        it.remove();
      }
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    // Save the partition path of active & finished partition path to state.
    Preconditions.checkNotNull(activePartitions);
    Preconditions.checkNotNull(finishedPartitions);
    activePartitionsState.update(new ArrayList<>(activePartitions));
    finishedPartitionsState.update(new ArrayList<>(finishedPartitions));

    // Send write success file event to coordinator while finished partitions is not empty.
    if (!finishedPartitions.isEmpty()) {
      List<WriteStatus> writeStatusList = new ArrayList<>();
      for (String p : finishedPartitions) {
        WriteStatus writeStatus = new WriteStatus();
        writeStatus.setPartitionPath(tablePath + "/" + p);
        writeStatusList.add(writeStatus);
      }
      final WriteMetadataEvent event = WriteMetadataEvent.builder()
          .taskID(taskID)
          .instantTime("")
          .writeStatus(writeStatusList)
          .lastBatch(true)
          .endInput(false)
          .partitionFinished(true)
          .build();
      gateway.sendEventToCoordinator(event);
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    ListStateDescriptor<String> activePartitionStateDesc =
        new ListStateDescriptor<>(ACTIVE_PARTITION_STATE_NAME, String.class);
    activePartitionsState = context.getOperatorStateStore().getListState(activePartitionStateDesc);
    ListStateDescriptor<String> finishedPartitionStateDesc =
        new ListStateDescriptor<>(FINISHED_PARTITION_STATE_NAME, String.class);
    finishedPartitionsState = context.getOperatorStateStore().getListState(finishedPartitionStateDesc);
    if (context.isRestored()) {
      for (String p : activePartitionsState.get()) {
        activePartitions.add(p);
      }
      for (String p : finishedPartitionsState.get()) {
        finishedPartitions.add(p);
      }
    }
  }

  @Override
  public void notifyCheckpointComplete(long l) throws Exception {
    Preconditions.checkNotNull(finishedPartitions);
    Iterator<String> it = finishedPartitions.iterator();
    while (it.hasNext()) {
      String successFilePath = tablePath + "/" + it.next() + "/" + SUCCESS_FILE_NAME;
      if (fileSystem.exists(new Path(successFilePath))) {
        it.remove();
      }
    }
  }

  public void setOperatorEventGateway(OperatorEventGateway gateway) {
    this.gateway = gateway;
  }

  @Override
  public void close() {
    LOG.info("close partition success file write sink.");
  }

  public void handleOperatorEvent(OperatorEvent operatorEvent) {}
}