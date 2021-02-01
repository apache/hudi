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

import java.util.List;

/**
 * An operator event to mark successful checkpoint batch write.
 */
public class BatchWriteSuccessEvent implements OperatorEvent {
  private static final long serialVersionUID = 1L;

  private final List<WriteStatus> writeStatuses;
  private final int taskID;
  private final String instantTime;

  public BatchWriteSuccessEvent(
      int taskID,
      String instantTime,
      List<WriteStatus> writeStatuses) {
    this.taskID = taskID;
    this.instantTime = instantTime;
    this.writeStatuses = writeStatuses;
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
}
