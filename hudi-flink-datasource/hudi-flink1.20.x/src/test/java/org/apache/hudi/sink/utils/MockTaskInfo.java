/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.utils;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.TaskInfo;

/**
 * Mock {@link TaskInfo} to use in tests.
 */
public class MockTaskInfo implements TaskInfo {

  private final int numParallelSubtasks;
  private final int subtaskIndex;
  @Getter
  @Setter
  private int attemptNumber;

  public MockTaskInfo(
      int numParallelSubtasks,
      int subtaskIndex,
      int attemptNumber) {
    this.numParallelSubtasks = numParallelSubtasks;
    this.subtaskIndex = subtaskIndex;
    this.attemptNumber = attemptNumber;
  }

  @Override
  public String getTaskName() {
    return "";
  }

  @Override
  public int getMaxNumberOfParallelSubtasks() {
    return numParallelSubtasks;
  }

  @Override
  public int getIndexOfThisSubtask() {
    return subtaskIndex;
  }

  @Override
  public int getNumberOfParallelSubtasks() {
    return numParallelSubtasks;
  }

  @Override
  public String getTaskNameWithSubtasks() {
    return getTaskName() + "_" + subtaskIndex;
  }

  @Override
  public String getAllocationIDAsString() {
    return "";
  }
}
