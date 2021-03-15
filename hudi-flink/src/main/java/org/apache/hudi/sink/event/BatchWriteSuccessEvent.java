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
import java.util.List;
import java.util.Objects;

/**
 * An operator event to mark successful checkpoint batch write.
 */
public class BatchWriteSuccessEvent implements OperatorEvent {
  private static final long serialVersionUID = 1L;

  private List<WriteStatus> writeStatuses;
  private final int taskID;
  private final String instantTime;
  private boolean isLastBatch;
  /**
   * Flag saying whether the event comes from the end of input, e.g. the source
   * is bounded, there are two cases in which this flag should be set to true:
   * 1. batch execution mode
   * 2. bounded stream source such as VALUES
   */
  private final boolean isEndInput;

  /**
   * Creates an event.
   *
   * @param taskID        The task ID
   * @param instantTime   The instant time under which to write the data
   * @param writeStatuses The write statues list
   * @param isLastBatch   Whether the event reports the last batch
   *                      within an checkpoint interval,
   *                      if true, the whole data set of the checkpoint
   *                      has been flushed successfully
   */
  private BatchWriteSuccessEvent(
      int taskID,
      String instantTime,
      List<WriteStatus> writeStatuses,
      boolean isLastBatch,
      boolean isEndInput) {
    this.taskID = taskID;
    this.instantTime = instantTime;
    this.writeStatuses = new ArrayList<>(writeStatuses);
    this.isLastBatch = isLastBatch;
    this.isEndInput = isEndInput;
  }

  /**
   * Returns the builder for {@link BatchWriteSuccessEvent}.
   */
  public static Builder builder() {
    return new Builder();
  }

  public List<WriteStatus> getWriteStatuses() {
    return writeStatuses;
  }

  public int getTaskID() {
    return taskID;
  }

  public String getInstantTime() {
    return instantTime;
  }

  public boolean isLastBatch() {
    return isLastBatch;
  }

  public boolean isEndInput() {
    return isEndInput;
  }

  /**
   * Merges this event with given {@link BatchWriteSuccessEvent} {@code other}.
   *
   * @param other The event to be merged
   */
  public void mergeWith(BatchWriteSuccessEvent other) {
    ValidationUtils.checkArgument(this.instantTime.equals(other.instantTime));
    ValidationUtils.checkArgument(this.taskID == other.taskID);
    this.isLastBatch |= other.isLastBatch; // true if one of the event isLastBatch true.
    List<WriteStatus> statusList = new ArrayList<>();
    statusList.addAll(this.writeStatuses);
    statusList.addAll(other.writeStatuses);
    this.writeStatuses = statusList;
  }

  /** Returns whether the event is ready to commit. */
  public boolean isReady(String currentInstant) {
    return isLastBatch && this.instantTime.equals(currentInstant);
  }

  // -------------------------------------------------------------------------
  //  Builder
  // -------------------------------------------------------------------------

  /**
   * Builder for {@link BatchWriteSuccessEvent}.
   */
  public static class Builder {
    private List<WriteStatus> writeStatus;
    private Integer taskID;
    private String instantTime;
    private boolean isLastBatch = false;
    private boolean isEndInput = false;

    public BatchWriteSuccessEvent build() {
      Objects.requireNonNull(taskID);
      Objects.requireNonNull(instantTime);
      Objects.requireNonNull(writeStatus);
      return new BatchWriteSuccessEvent(taskID, instantTime, writeStatus, isLastBatch, isEndInput);
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

    public Builder isLastBatch(boolean isLastBatch) {
      this.isLastBatch = isLastBatch;
      return this;
    }

    public Builder isEndInput(boolean isEndInput) {
      this.isEndInput = isEndInput;
      return this;
    }
  }
}
