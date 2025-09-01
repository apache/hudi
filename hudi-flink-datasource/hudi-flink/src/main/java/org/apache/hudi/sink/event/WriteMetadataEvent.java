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
import org.apache.hudi.util.WriteStatusMerger;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An operator event to mark successful checkpoint batch write.
 */
public class WriteMetadataEvent implements OperatorEvent {
  private static final long serialVersionUID = 1L;

  public static final String BOOTSTRAP_INSTANT = "";

  private List<WriteStatus> writeStatuses;
  private int taskID;
  private long checkpointId;
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
      long checkpointId,
      String instantTime,
      List<WriteStatus> writeStatuses,
      boolean lastBatch,
      boolean endInput,
      boolean bootstrap) {
    this.taskID = taskID;
    this.checkpointId = checkpointId;
    this.instantTime = instantTime;
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

  public Long getCheckpointId() {
    return checkpointId;
  }

  public void setCheckpointId(long checkpointId) {
    this.checkpointId = checkpointId;
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
    // true if one of the event lastBatch is true
    this.lastBatch |= other.lastBatch;
    this.writeStatuses = mergeWriteStatuses(this.writeStatuses, other.writeStatuses);
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
        + ", checkpointId=" + checkpointId
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
   * Creates empty bootstrap event for task {@code taskId} with checkpoint ID {@code checkpointId}.
   *
   * <p>The event indicates that the new instant can start directly,
   * there is no old instant write statuses to recover.
   */
  public static WriteMetadataEvent emptyBootstrap(int taskId, long checkpointId) {
    return WriteMetadataEvent.builder()
        .taskID(taskId)
        .checkpointId(checkpointId)
        .instantTime(BOOTSTRAP_INSTANT)
        .writeStatus(Collections.emptyList())
        .bootstrap(true)
        .build();
  }

  private static List<WriteStatus> mergeWriteStatuses(List<WriteStatus> oldStatuses, List<WriteStatus> newStatuses) {
    List<WriteStatus> mergedStatuses = new ArrayList<>();
    mergedStatuses.addAll(oldStatuses);
    mergedStatuses.addAll(newStatuses);
    return mergedStatuses
        .stream()
        .collect(Collectors.groupingBy(writeStatus -> writeStatus.getStat().getPartitionPath() + writeStatus.getStat().getFileId()))
        .values()
        .stream()
        .map(duplicates -> duplicates.stream().reduce(WriteStatusMerger::merge).get())
        .collect(Collectors.toList());
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
    private Long checkpointId = -1L;
    private String instantTime;
    private boolean lastBatch = false;
    private boolean endInput = false;
    private boolean bootstrap = false;

    public WriteMetadataEvent build() {
      Objects.requireNonNull(taskID);
      Objects.requireNonNull(instantTime);
      Objects.requireNonNull(writeStatus);
      return new WriteMetadataEvent(taskID, checkpointId, instantTime, writeStatus, lastBatch, endInput, bootstrap);
    }

    public Builder taskID(int taskID) {
      this.taskID = taskID;
      return this;
    }

    public Builder checkpointId(long checkpointId) {
      this.checkpointId = checkpointId;
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
