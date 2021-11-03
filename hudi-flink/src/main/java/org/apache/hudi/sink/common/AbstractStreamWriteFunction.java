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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.event.CommitAckEvent;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.message.MessageBus;
import org.apache.hudi.sink.message.MessageClient;
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
import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

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
   * Current checkpoint id.
   */
  private long checkpointId = -1;

  /**
   * The message client.
   */
  private MessageClient messageClient;

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

    if (context.isRestored()) {
      restoreWriteMetadata();
    } else {
      sendBootstrapEvent();
    }
    // blocks flushing until the coordinator starts a new instant
    this.confirming = true;
    this.messageClient = MessageBus.getClient(this.metaClient.getFs(), this.metaClient.getBasePath());
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    this.checkpointId = functionSnapshotContext.getCheckpointId();
    snapshotState();
    // Reload the snapshot state as the current state.
    reloadWriteMetaState();
  }

  public abstract void snapshotState();

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
    List<WriteMetadataEvent> events = CollectionUtil.iterableToList(this.writeMetadataState.get());
    boolean eventSent = false;
    if (events.size() > 0) {
      boolean committed = this.metaClient.getActiveTimeline()
          .filterCompletedInstants()
          .containsInstant(events.get(0).getInstantTime());
      if (!committed) {
        for (WriteMetadataEvent event : events) {
          // The checkpoint succeed but the meta does not commit,
          // re-commit the inflight instant
          this.eventGateway.sendEventToCoordinator(event);
          LOG.info("Send uncommitted write metadata event to coordinator, task[{}].", taskID);
        }
        eventSent = true;
      }
    }
    if (!eventSent) {
      sendBootstrapEvent();
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
        .instantTime(currentInstant)
        .writeStatus(new ArrayList<>(writeStatuses))
        .bootstrap(true)
        .build();
    this.writeMetadataState.add(event);
    writeStatuses.clear();
  }

  public void handleOperatorEvent(OperatorEvent event) {
    ValidationUtils.checkArgument(event instanceof CommitAckEvent,
        "The write function can only handle CommitAckEvent");
    long checkpointId = ((CommitAckEvent) event).getCheckpointId();
    if (checkpointId == -1 || checkpointId == this.checkpointId) {
      this.confirming = false;
    }
  }

  @Override
  public void close() {
    if (this.messageClient != null) {
      this.messageClient.close();
    }
  }

  /**
   * Returns the last pending instant time.
   */
  private String lastPendingInstant() {
    return StreamerUtil.getLastPendingInstant(metaClient);
  }

  /**
   * Returns the previous committed checkpoint id.
   *
   * @param eagerFlush Whether the data flush happens before the checkpoint barrier arrives
   */
  private long prevCkp(boolean eagerFlush) {
    // Use the last checkpoint id to request for the message,
    // the time sequence of committed checkpoints and ongoing
    // checkpoints are as following:

    // 0 ------------ 1 ------------ 2 ------------ 3 ------------>   committed ckp id
    // |             /              /              /              /
    // |--- ckp-1 ----|--- ckp-2 ----|--- ckp-3 ----|--- ckp-4 ----|  ongoing ckp id

    // Use 0 as the initial committed checkpoint id, the 0th checkpoint message records the writing instant for ckp-1;
    // when ckp-1 success event is received, commits a checkpoint message with the writing instant for ckp-2;
    // that means, the checkpoint message records the writing instant of next checkpoint.
    return Math.max(0, eagerFlush ? this.checkpointId : this.checkpointId - 1);
  }

  /**
   * Returns the next instant to write from the message bus.
   *
   * <p>It returns 3 kinds of value:
   * i) normal instant time: the previous checkpoint succeed;
   * ii) 'aborted' instant time: the previous checkpoint has been aborted;
   * ii) null: the checkpoint is till ongoing without any notifications.
   */
  @Nullable
  protected String ackInstant(long checkpointId) {
    Option<MessageBus.CkpMessage> ckpMessageOption = this.messageClient.getCkpMessage(checkpointId);
    return ckpMessageOption.map(message -> message.inflightInstant).orElse(null);
  }

  /**
   * Prepares the instant time to write with for next checkpoint.
   *
   * @param eagerFlush Whether the data flush happens before the checkpoint barrier arrives
   *
   * @return The instant time
   */
  protected String instantToWrite(boolean eagerFlush) {
    final long ckpId = prevCkp(eagerFlush);
    String instant = ackInstant(ckpId);

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
      if (instant == null) {
        // sleep for a while
        boolean timeout = timeWait.waitFor();
        if (timeout && MessageBus.notInitialCkp(ckpId)) {
          // if the timeout threshold hits but the last instant still not commit,
          // and the task does not receive commit ask event(no data or aborted checkpoint),
          // assumes the checkpoint was canceled silently and unblock the data flushing
          confirming = false;
          instant = lastPendingInstant();
        } else {
          // refresh the inflight instant
          instant = ackInstant(ckpId);
        }
      } else if (MessageBus.canAbort(instant, ckpId)) {
        // the checkpoint was canceled, reuse the last instant
        confirming = false;
        instant = lastPendingInstant();
      } else {
        // the pending instant changed, that means the last instant was committed
        // successfully.
        confirming = false;
      }
    }
    return instant;
  }
}
