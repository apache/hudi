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

package org.apache.hudi.operator.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.util.ValidationUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * An operator event to mark successful checkpoint batch write.
 */
public class BatchWriteSuccessEvent implements OperatorEvent {
  private static final long serialVersionUID = 1L;

  private List<WriteStatus> writeStatuses;
  private final int taskID;
  private final String instantTime;
  private boolean isLastBatch;

  public BatchWriteSuccessEvent(
      int taskID,
      String instantTime,
      List<WriteStatus> writeStatuses) {
    this(taskID, instantTime, writeStatuses, false);
  }

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
  public BatchWriteSuccessEvent(
      int taskID,
      String instantTime,
      List<WriteStatus> writeStatuses,
      boolean isLastBatch) {
    this.taskID = taskID;
    this.instantTime = instantTime;
    this.writeStatuses = new ArrayList<>(writeStatuses);
    this.isLastBatch = isLastBatch;
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
}
