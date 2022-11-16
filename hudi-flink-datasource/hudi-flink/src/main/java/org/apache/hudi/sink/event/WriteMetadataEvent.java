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

package org.apache.hudi.sink.event;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An operator event to mark successful checkpoint batch write.
 */
public class WriteMetadataEvent implements OperatorEvent {
  private static final long serialVersionUID = 1L;

  public static final String BOOTSTRAP_INSTANT = "";

  private List<WriteStatus> writeStatuses;
  private int taskID;
  private int parallelism;
  private int numOfMetadataState;
  private String instantTime;
  private boolean lastBatch;

  /**
   * Flag saying whether the event comes from the end of input, e.g. the source
   * is bounded, there are two cases in which this flag should be set to true:
   * 1. batch execution mode
   * 2. bounded stream source such as VALUES
   */
  private boolean endInput;

  /**
   * Flag saying whether the event comes from bootstrap of a write function.
   */
  private boolean bootstrap;

  /**
   * Creates an event.
   *
   * @param taskID        The task ID
   * @param instantTime   The instant time under which to write the data
   * @param writeStatuses The write statues list
   * @param lastBatch     Whether the event reports the last batch
   *                      within an checkpoint interval,
   *                      if true, the whole data set of the checkpoint
   *                      has been flushed successfully
   * @param bootstrap     Whether the event comes from the bootstrap
   */
  private WriteMetadataEvent(
      int taskID,
      int parallelism,
      int numOfMetadataState,
      String instantTime,
      List<WriteStatus> writeStatuses,
      boolean lastBatch,
      boolean endInput,
      boolean bootstrap) {
    this.taskID = taskID;
    this.instantTime = instantTime;
    this.numOfMetadataState = numOfMetadataState;
    this.parallelism = parallelism;
    this.writeStatuses = new ArrayList<>(writeStatuses);
    this.lastBatch = lastBatch;
    this.endInput = endInput;
    this.bootstrap = bootstrap;
  }

  // default constructor for efficient serialization
  public WriteMetadataEvent() {
  }

  /**
   * Returns the builder for {@link WriteMetadataEvent}.
   */
  public static Builder builder() {
    return new Builder();
  }

  public List<WriteStatus> getWriteStatuses() {
    return writeStatuses;
  }

  public void setWriteStatuses(List<WriteStatus> writeStatuses) {
    this.writeStatuses = writeStatuses;
  }

  public int getTaskID() {
    return taskID;
  }

  public void setTaskID(int taskID) {
    this.taskID = taskID;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public int getNumOfMetadataState() {
    return numOfMetadataState;
  }

  public void setNumOfMetadataState(int numOfMetadataState) {
    this.numOfMetadataState = numOfMetadataState;
  }

  public String getInstantTime() {
    return instantTime;
  }

  public void setInstantTime(String instantTime) {
    this.instantTime = instantTime;
  }

  public boolean isEndInput() {
    return endInput;
  }

  public void setEndInput(boolean endInput) {
    this.endInput = endInput;
  }

  public boolean isBootstrap() {
    return bootstrap;
  }

  public void setBootstrap(boolean bootstrap) {
    this.bootstrap = bootstrap;
  }

  public boolean isLastBatch() {
    return lastBatch;
  }

  public void setLastBatch(boolean lastBatch) {
    this.lastBatch = lastBatch;
  }

  /**
   * Merges this event with given {@link WriteMetadataEvent} {@code other}.
   *
   * @param other The event to be merged
   */
  public void mergeWith(WriteMetadataEvent other) {
    ValidationUtils.checkArgument(this.taskID == other.taskID);
    // the instant time could be monotonically increasing
    this.instantTime = other.instantTime;
    this.lastBatch |= other.lastBatch; // true if one of the event lastBatch is true
    List<WriteStatus> statusList = new ArrayList<>();
    statusList.addAll(this.writeStatuses);
    statusList.addAll(other.writeStatuses);
    this.writeStatuses = statusList;
  }

  /**
   * Returns whether the event is ready to commit.
   */
  public boolean isReady(String currentInstant) {
    return lastBatch && this.instantTime.equals(currentInstant);
  }

  @Override
  public String toString() {
    return "WriteMetadataEvent{"
        + "writeStatusesSize=" + writeStatuses.size()
        + ", taskID=" + taskID
        + ", instantTime='" + instantTime + '\''
        + ", lastBatch=" + lastBatch
        + ", endInput=" + endInput
        + ", bootstrap=" + bootstrap
        + '}';
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Creates empty bootstrap event for task {@code taskId}.
   *
   * <p>The event indicates that the new instant can start directly,
   * there is no old instant write statuses to recover.
   */
  public static WriteMetadataEvent emptyBootstrap(int taskId) {
    return WriteMetadataEvent.builder()
        .taskID(taskId)
        .instantTime(BOOTSTRAP_INSTANT)
        .numOfMetadataState(1)
        .writeStatus(Collections.emptyList())
        .bootstrap(true)
        .build();
  }

  // -------------------------------------------------------------------------
  //  Builder
  // -------------------------------------------------------------------------

  /**
   * Builder for {@link WriteMetadataEvent}.
   */
  public static class Builder {
    private List<WriteStatus> writeStatus;
    private Integer taskID;
    private int parallelism = 0;
    private int numOfMetadataState = 1;
    private String instantTime;
    private boolean lastBatch = false;
    private boolean endInput = false;
    private boolean bootstrap = false;

    public WriteMetadataEvent build() {
      Objects.requireNonNull(taskID);
      Objects.requireNonNull(instantTime);
      Objects.requireNonNull(writeStatus);
      return new WriteMetadataEvent(taskID, parallelism, numOfMetadataState, instantTime, writeStatus, lastBatch, endInput, bootstrap);
    }

    public Builder taskID(int taskID) {
      this.taskID = taskID;
      return this;
    }

    public Builder instantTime(String instantTime) {
      this.instantTime = instantTime;
      return this;
    }

    public Builder writeStatus(List<WriteStatus> writeStatus) {
      this.writeStatus = writeStatus;
      return this;
    }

    public Builder parallelism(int parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public Builder numOfMetadataState(int numOfMetadataState) {
      this.numOfMetadataState = numOfMetadataState;
      return this;
    }

    public Builder lastBatch(boolean lastBatch) {
      this.lastBatch = lastBatch;
      return this;
    }

    public Builder endInput(boolean endInput) {
      this.endInput = endInput;
      return this;
    }

    public Builder bootstrap(boolean bootstrap) {
      this.bootstrap = bootstrap;
      return this;
    }
  }
}
