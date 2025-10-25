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

package org.apache.hudi.sink.common;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.event.CommitAckEvent;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.RuntimeContextUtils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

/**
 * Base infrastructures for streaming writer function.
 *
 * @param <I> Type of the input record
 * @see StreamWriteOperatorCoordinator
 */
public abstract class AbstractStreamWriteFunction<I>
    extends AbstractWriteFunction<I>
    implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamWriteFunction.class);

  /**
   * Config options.
   */
  protected final Configuration config;

  /**
   * Id of current subtask.
   */
  protected int taskID;

  /**
   * Meta Client.
   */
  protected transient HoodieTableMetaClient metaClient;

  /**
   * Write Client.
   */
  protected transient HoodieFlinkWriteClient writeClient;

  /**
   * The REQUESTED instant we write the data.
   */
  protected volatile String currentInstant;

  /**
   * Correspondent to request the instant time.
   */
  protected transient Correspondent correspondent;

  /**
   * Gateway to send operator events to the operator coordinator.
   */
  protected transient OperatorEventGateway eventGateway;

  /**
   * List state of the write metadata events.
   */
  private transient ListState<WriteMetadataEvent> writeMetadataState;

  /**
   * List state of the JobID.
   */
  private transient ListState<JobID> jobIdState;

  /**
   * Write status list for the current checkpoint.
   */
  protected List<WriteStatus> writeStatuses;

  /**
   * Since flink 1.15, the streaming job with bounded source triggers one checkpoint
   * after calling #endInput, use this flag to avoid unnecessary data flush.
   */
  private transient boolean inputEnded;

  /**
   * The last checkpoint id, starts from -1.
   */
  protected long checkpointId = -1;

  /**
   * Constructs a StreamWriteFunctionBase.
   *
   * @param config The config options
   */
  public AbstractStreamWriteFunction(Configuration config) {
    this.config = config;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    this.taskID = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
    this.metaClient = StreamerUtil.createMetaClient(this.config);
    this.writeClient = FlinkWriteClients.createWriteClient(this.config, getRuntimeContext());
    this.writeStatuses = new ArrayList<>();
    this.writeMetadataState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>(
            "write-metadata-state",
            TypeInformation.of(WriteMetadataEvent.class)
        ));
    this.jobIdState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>(
            "job-id-state",
            TypeInformation.of(JobID.class)
        ));

    int attemptId = RuntimeContextUtils.getAttemptNumber(getRuntimeContext());
    if (context.isRestored()) {
      initCheckpointId(attemptId, context.getRestoredCheckpointId().orElse(-1L));
    }
    sendBootstrapEvent(attemptId, context.isRestored());
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    if (inputEnded) {
      return;
    }
    snapshotState();
    // Reload the snapshot state as the current state.
    reloadWriteMetaState();
    // Reload the job ID state
    reloadJobIdState();
    // Update checkpoint id
    this.checkpointId = functionSnapshotContext.getCheckpointId();
  }

  public abstract void snapshotState();

  @Override
  public void endInput() {
    this.inputEnded = true;
  }

  // -------------------------------------------------------------------------
  //  Getter/Setter
  // -------------------------------------------------------------------------

  public void setCorrespondent(Correspondent correspondent) {
    this.correspondent = correspondent;
  }

  public Correspondent getCorrespondent() {
    return correspondent;
  }

  public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
    this.eventGateway = operatorEventGateway;
  }

  public OperatorEventGateway getOperatorEventGateway() {
    return eventGateway;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

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
    // sets up the known checkpoint id as the last successful checkpoint id for purposes of:
    // 1). old events cleaning;
    // 2). instant time request for current checkpoint.
    this.checkpointId = restoredCheckpointId;
  }

  private void sendBootstrapEvent(int attemptId, boolean isRestored) throws Exception {
    if (attemptId <= 0) {
      if (isRestored) {
        HoodieTimeline pendingTimeline = this.metaClient.getActiveTimeline().filterPendingExcludingCompaction();
        // if the task is initially started, resend the pending event.
        for (WriteMetadataEvent event : this.writeMetadataState.get()) {
          // Must filter out the completed instants in case it is a partial failover,
          // the write status should not be accumulated in such case.
          if (pendingTimeline.containsInstant(event.getInstantTime())) {
            // Reset taskID for event
            event.setTaskID(taskID);
            // The checkpoint succeed but the meta does not commit,
            // re-commit the inflight instant
            this.eventGateway.sendEventToCoordinator(event);
            LOG.info("Send uncommitted write metadata event to coordinator, task[{}].", taskID);
          }
        }
      }
    } else {
      // otherwise sends an empty bootstrap event instead.
      this.eventGateway.sendEventToCoordinator(WriteMetadataEvent.emptyBootstrap(taskID, checkpointId));
      LOG.info("Send bootstrap write metadata event to coordinator, task[{}].", taskID);
    }
  }

  /**
   * Reload the write metadata state as the current checkpoint.
   */
  private void reloadWriteMetaState() throws Exception {
    this.writeMetadataState.clear();
    WriteMetadataEvent event = WriteMetadataEvent.builder()
        .taskID(taskID)
        .checkpointId(checkpointId)
        .instantTime(currentInstant)
        .writeStatus(new ArrayList<>(writeStatuses))
        .bootstrap(true)
        .build();
    this.writeMetadataState.add(event);
    writeStatuses.clear();
  }

  /**
   * Reload job id state as current job id.
   */
  private void reloadJobIdState() throws Exception {
    this.jobIdState.clear();
    this.jobIdState.add(RuntimeContextUtils.getJobId(getRuntimeContext()));
  }

  public void handleOperatorEvent(OperatorEvent event) {
    ValidationUtils.checkArgument(event instanceof CommitAckEvent,
        "The write function can only handle CommitAckEvent");
    // no-op
  }

  /**
   * Prepares the instant time to write with for next checkpoint.
   *
   * @param hasData Whether the task has buffering data
   * @return The instant time
   */
  protected String instantToWrite(boolean hasData) {
    return this.correspondent.requestInstantTime(this.checkpointId);
  }
}
