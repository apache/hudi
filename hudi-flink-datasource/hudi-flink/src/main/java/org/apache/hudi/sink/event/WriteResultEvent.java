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

import org.apache.hudi.common.table.timeline.HoodieTimeline;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * An operator event to mark successful checkpoint batch write.
 */
public class WriteResultEvent implements OperatorEvent {
  private static final long serialVersionUID = 1L;

  public static final String BOOTSTRAP_INSTANT = "";

  private WriteMetadataEvent writeMetadataEvent;
  private String curInstant;
  public WriteResultEvent(WriteMetadataEvent writeMetadataEvent, String curInstant) {
    this.writeMetadataEvent = writeMetadataEvent;
    this.curInstant = curInstant;
  }

  public WriteResultEvent(WriteMetadataEvent writeMetadataEvent) {
    this.writeMetadataEvent = writeMetadataEvent;
    this.curInstant = null;
  }

  // default constructor for efficient serialization
  public WriteResultEvent() {
  }



  /**
   * Merges this event with given {@link WriteResultEvent} {@code other}.
   *
   * @param other The event to be merged
   */
  public void mergeWith(WriteResultEvent other) {
    this.writeMetadataEvent.mergeWith(other.writeMetadataEvent);
    if (this.curInstant == null || other.curInstant != null && HoodieTimeline.compareTimestamps(this.curInstant, HoodieTimeline.LESSER_THAN, other.curInstant)) {
      this.curInstant = other.curInstant;
    }
  }

  /**
   * Returns whether the event is ready to commit.
   */
  public boolean isReady(String currentInstant) {
    return writeMetadataEvent.isReady(currentInstant);
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
  public static WriteResultEvent emptyBootstrap(int taskId, String curInstant) {
    return new WriteResultEvent(WriteMetadataEvent.emptyBootstrap(taskId), curInstant);
  }

  public static WriteResultEvent emptyBootstrap(int taskId) {
    return emptyBootstrap(taskId, null);
  }

  public WriteMetadataEvent getWriteMetadataEvent() {
    return writeMetadataEvent;
  }

  public void setWriteMetadataEvent(WriteMetadataEvent writeMetadataEvent) {
    this.writeMetadataEvent = writeMetadataEvent;
  }

  public String getCurInstant() {
    return curInstant;
  }

  public void setCurInstant(String curInstant) {
    this.curInstant = curInstant;
  }
}
