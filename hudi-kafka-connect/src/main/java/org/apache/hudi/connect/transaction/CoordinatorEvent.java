/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.connect.transaction;

import org.apache.hudi.connect.ControlMessage;

/**
 * The events within the Coordinator that trigger
 * the state changes in the state machine of
 * the Coordinator.
 */
public class CoordinatorEvent {

  private final CoordinatorEventType eventType;
  private final String topicName;
  private final String commitTime;
  private ControlMessage message;

  public CoordinatorEvent(CoordinatorEventType eventType,
                          String topicName,
                          String commitTime) {
    this.eventType = eventType;
    this.topicName = topicName;
    this.commitTime = commitTime;
  }

  public CoordinatorEventType getEventType() {
    return eventType;
  }

  public String getTopicName() {
    return topicName;
  }

  public String getCommitTime() {
    return commitTime;
  }

  public ControlMessage getMessage() {
    return message;
  }

  public void setMessage(ControlMessage message) {
    this.message = message;
  }

  /**
   * The type of Coordinator Event.
   */
  public enum CoordinatorEventType {
    START_COMMIT,
    END_COMMIT,
    WRITE_STATUS,
    ACK_COMMIT,
    WRITE_STATUS_TIMEOUT
  }
}
