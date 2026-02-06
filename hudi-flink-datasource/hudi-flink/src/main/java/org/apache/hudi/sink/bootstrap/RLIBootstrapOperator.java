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
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.sink.utils.OperatorIDGenerator;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.RuntimeContextUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Bootstrap operator that loads record level index (RLI) data from metadata table.
 *
 * <p>This operator reads index data from the record_index partition of the metadata table.
 * Each subtask reads one RLI partition (bucket) based on its task index, enabling parallel loading.
 *
 * <p>The loaded index records are emitted downstream to initialize the index state in
 * {@link org.apache.hudi.sink.partitioner.BucketAssignFunction}.
 */
@Slf4j
public class RLIBootstrapOperator
    extends AbstractBootstrapOperator {

  private final OperatorID dataWriteOperatorId;

  private transient HoodieTableMetaClient metaClient;
  private transient HoodieBackedTableMetadata metadataTable;
  private transient Correspondent correspondent;
  private transient long loadedCnt;

  /**
   * The last checkpoint id, starts from -1.
   */
  private long checkpointId = -1;

  /**
   * List state of the JobID.
   */
  private transient ListState<JobID> jobIdState;

  public RLIBootstrapOperator(Configuration conf) {
    super(conf);
    String writeOperatorUid = conf.get(FlinkOptions.WRITE_OPERATOR_UID);
    ValidationUtils.checkArgument(writeOperatorUid != null,
        "Write operator UID should not be null when index is Record Level Index.");
    this.dataWriteOperatorId = OperatorIDGenerator.fromUid(writeOperatorUid);
  }

  @Override
  public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<HoodieFlinkInternalRow>> output) {
    super.setup(containingTask, config, output);
    this.correspondent = Correspondent.getInstance(dataWriteOperatorId,
        getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway());
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    this.jobIdState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>(
            "job-id-state",
            TypeInformation.of(JobID.class)
        ));
    loadedCnt = 0;

    int attemptId = RuntimeContextUtils.getAttemptNumber(getRuntimeContext());
    if (context.isRestored()) {
      initCheckpointId(attemptId, context.getRestoredCheckpointId().orElse(-1L));
    }

    if (context.isRestored()) {
      // Wait for pending instants being committed successfully before loading the record index
      log.info("Waiting for pending instants committed before RLI bootstrap.");
      correspondent.awaitPendingInstantsCommitted(checkpointId);
      log.info("All pending instants are completed, continue RLI bootstrap.");
    }

    this.metaClient = StreamerUtil.createMetaClient(conf);
    this.metadataTable = (HoodieBackedTableMetadata) metaClient.getTableFormat().getMetadataFactory().create(
        HoodieFlinkEngineContext.DEFAULT,
        metaClient.getStorage(),
        StreamerUtil.metadataConfig(conf),
        conf.get(FlinkOptions.PATH));
    // Load RLI records
    preLoadRLIRecords();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    // Reload the job ID state
    reloadJobIdState();
    // Update checkpoint id
    this.checkpointId = context.getCheckpointId();
  }

  @Override
  public void close() throws Exception {
    closeMetadataTable();
    super.close();
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Reload the job id state as the current job id.
   */
  private void reloadJobIdState() throws Exception {
    this.jobIdState.clear();
    this.jobIdState.add(RuntimeContextUtils.getJobId(getRuntimeContext()));
  }

  private void initCheckpointId(int attemptId, long restoredCheckpointId) throws Exception {
    if (attemptId <= 0) {
      // returns early if the job/task is initially started.
      return;
    }
    JobID currentJobId = RuntimeContextUtils.getJobId(getRuntimeContext());
    if (StreamSupport.stream(this.jobIdState.get().spliterator(), false)
        .noneMatch(currentJobId::equals)) {
      // do not set up the checkpoint id if the state comes from the old job.
      return;
    }
    this.checkpointId = restoredCheckpointId;
  }

  private void preLoadRLIRecords() {
    int taskID = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
    int parallelism = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());

    log.info("Start loading RLI records from metadata table, taskId = {}, parallelism = {}", taskID, parallelism);

    SerializableFunctionUnchecked<List<FileSlice>, List<FileSlice>> fileSlicesFilter = fileSlices -> {
      List<FileSlice> filteredFileSlices = new ArrayList<>();
      for (int i = 0; i < fileSlices.size(); i++) {
        if (shouldLoadBucket(i, parallelism, taskID)) {
          filteredFileSlices.add(fileSlices.get(i));
        }
      }
      log.info("Subtask: {} will load record index records from file groups: {}, total file groups: {}.",
          taskID, filteredFileSlices.stream().map(FileSlice::getFileId).collect(Collectors.joining(",")), fileSlices.size());
      return filteredFileSlices;
    };

    // Each subtask loads buckets assigned to it
    long startTime = System.currentTimeMillis();
    HoodiePairData<String, HoodieRecordGlobalLocation> rliData = metadataTable.readRecordIndexLocations(fileSlicesFilter);
    rliData.forEach(locationPair -> emitIndexRecord(locationPair.getLeft(), locationPair.getRight()));
    long costMs = System.currentTimeMillis() - startTime;
    log.info("Finish loading RLI records, total records: {}, cost: {} ms, taskId = {}", loadedCnt, costMs, taskID);

    // Wait for other tasks to complete
    waitForBootstrapReady(taskID);

    // Cleanup resources
    closeMetadataTable();
  }

  /**
   * Determines if the given file group should be loaded by this task.
   * Uses round-robin assignment: file group i is assigned to task (i % parallelism).
   */
  private boolean shouldLoadBucket(int fileGroupIdx, int parallelism, int taskID) {
    return fileGroupIdx % parallelism == taskID;
  }

  private void emitIndexRecord(String recordKey, HoodieRecordGlobalLocation location) {
    output.collect(new StreamRecord<>(
        new HoodieFlinkInternalRow(
            recordKey,
            location.getPartitionPath(),
            location.getFileId(),
            String.valueOf(location.getInstantTime()))));
    loadedCnt += 1;
  }

  private void closeMetadataTable() {
    if (metadataTable != null) {
      try {
        metadataTable.close();
      } catch (Exception e) {
        log.warn("Failed to close metadata table", e);
      }
      metadataTable = null;
    }
  }
}
