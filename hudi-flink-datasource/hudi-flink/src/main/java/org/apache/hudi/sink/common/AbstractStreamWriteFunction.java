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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.event.CommitAckEvent;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.meta.CkpMetadata;
import org.apache.hudi.sink.utils.TimeWait;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
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
import java.util.Objects;

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
   * Gateway to send operator events to the operator coordinator.
   */
  protected transient OperatorEventGateway eventGateway;

  /**
   * Flag saying whether the write task is waiting for the checkpoint success notification
   * after it finished a checkpoint.
   *
   * <p>The flag is needed because the write task does not block during the waiting time interval,
   * some data buckets still flush out with old instant time. There are two cases that the flush may produce
   * corrupted files if the old instant is committed successfully:
   * 1) the write handle was writing data but interrupted, left a corrupted parquet file;
   * 2) the write handle finished the write but was not closed, left an empty parquet file.
   *
   * <p>To solve, when this flag was set to true, we block the data flushing thus the #processElement method,
   * the flag was reset to false if the task receives the checkpoint success event or the latest inflight instant
   * time changed(the last instant committed successfully).
   */
  protected volatile boolean confirming = false;

  /**
   * List state of the write metadata events.
   */
  private transient ListState<WriteMetadataEvent> writeMetadataState;

  /**
   * Write status list for the current checkpoint.
   */
  protected List<WriteStatus> writeStatuses;

  /**
   * The checkpoint metadata.
   */
  private transient CkpMetadata ckpMetadata;

  /**
   * Since flink 1.15, the streaming job with bounded source triggers one checkpoint
   * after calling #endInput, use this flag to avoid unnecessary data flush.
   */
  private transient boolean inputEnded;

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
    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.metaClient = StreamerUtil.createMetaClient(this.config);
    this.writeClient = StreamerUtil.createWriteClient(this.config, getRuntimeContext());
    this.writeStatuses = new ArrayList<>();
    this.writeMetadataState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>(
            "write-metadata-state",
            TypeInformation.of(WriteMetadataEvent.class)
        ));

    this.ckpMetadata = CkpMetadata.getInstance(this.metaClient.getFs(), this.metaClient.getBasePath());
    this.currentInstant = lastPendingInstant();
    if (context.isRestored()) {
      restoreWriteMetadata();
    } else {
      sendBootstrapEvent();
    }
    // blocks flushing until the coordinator starts a new instant
    this.confirming = true;
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    if (inputEnded) {
      return;
    }
    snapshotState();
    // Reload the snapshot state as the current state.
    reloadWriteMetaState();
  }

  public abstract void snapshotState();

  @Override
  public void endInput() {
    this.inputEnded = true;
  }

  // -------------------------------------------------------------------------
  //  Getter/Setter
  // -------------------------------------------------------------------------
  @VisibleForTesting
  public boolean isConfirming() {
    return this.confirming;
  }

  public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
    this.eventGateway = operatorEventGateway;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void restoreWriteMetadata() throws Exception {
    String lastInflight = lastPendingInstant();
    List<WriteMetadataEvent> events = new ArrayList<>();
    for (WriteMetadataEvent event : this.writeMetadataState.get()) {
      if (Objects.equals(lastInflight, event.getInstantTime())) {
        int originTaskId = event.getTaskID();
        // Reset taskID for event
        event.setTaskID(taskID);
        // The checkpoint succeed but the meta does not commit,
        // re-commit the inflight instant
        events.add(event);
        LOG.info("Prepare sending uncommitted write metadata event to coordinator, originTaskId:[{}], task[{}].", originTaskId, taskID);
      }
    }
    if (events.isEmpty()) {
      sendBootstrapEvent();
    } else {
      WriteMetadataEvent eventToSend = events.get(0);
      for (int i = 1; i < events.size(); ++i) {
        eventToSend.mergeWith(events.get(i));
      }
      eventToSend.setNumOfMetadataState(events.size());
      this.eventGateway.sendEventToCoordinator(eventToSend);
    }
  }

  private void sendBootstrapEvent() {
    this.eventGateway.sendEventToCoordinator(WriteMetadataEvent.emptyBootstrap(taskID));
    LOG.info("Send bootstrap write metadata event to coordinator, task[{}].", taskID);
  }

  /**
   * Reload the write metadata state as the current checkpoint.
   */
  private void reloadWriteMetaState() throws Exception {
    this.writeMetadataState.clear();
    WriteMetadataEvent event = WriteMetadataEvent.builder()
        .taskID(taskID)
        .parallelism(this.config.getInteger(FlinkOptions.WRITE_TASKS))
        .instantTime(currentInstant)
        .writeStatus(new ArrayList<>(writeStatuses))
        .bootstrap(true)
        .build();
    this.writeMetadataState.add(event);
    writeStatuses.clear();
  }

  @Override
  public void handleOperatorEvent(OperatorEvent event) {
    ValidationUtils.checkArgument(event instanceof CommitAckEvent,
        "The write function can only handle CommitAckEvent");
    this.confirming = false;
  }

  /**
   * Returns the last pending instant time.
   */
  protected String lastPendingInstant() {
    return this.ckpMetadata.lastPendingInstant();
  }

  /**
   * Prepares the instant time to write with for next checkpoint.
   *
   * @param hasData Whether the task has buffering data
   * @return The instant time
   */
  protected String instantToWrite(boolean hasData) {
    String instant = lastPendingInstant();
    // if exactly-once semantics turns on,
    // waits for the checkpoint notification until the checkpoint timeout threshold hits.
    TimeWait timeWait = TimeWait.builder()
        .timeout(config.getLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT))
        .action("instant initialize")
        .build();
    while (confirming) {
      // wait condition:
      // 1. there is no inflight instant
      // 2. the inflight instant does not change and the checkpoint has buffering data
      if (instant == null || invalidInstant(instant, hasData)) {
        // sleep for a while
        timeWait.waitFor();
        // refresh the inflight instant
        instant = lastPendingInstant();
      } else {
        // the pending instant changed, that means the last instant was committed
        // successfully.
        confirming = false;
      }
    }
    return instant;
  }

  /**
   * Returns whether the pending instant is invalid to write with.
   */
  private boolean invalidInstant(String instant, boolean hasData) {
    return instant.equals(this.currentInstant) && hasData;
  }
}
