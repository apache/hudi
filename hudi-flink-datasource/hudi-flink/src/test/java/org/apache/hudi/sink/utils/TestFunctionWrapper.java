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

package org.apache.hudi.sink.utils;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.common.AbstractWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;

import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.List;
import java.util.Map;

/**
 * Define the common interfaces for test function wrappers.
 */
public interface TestFunctionWrapper<I> {
  /**
   * Open all the functions within this wrapper.
   */
  void openFunction() throws Exception;

  /**
   * Process the given input record {@code record}.
   */
  void invoke(I record) throws Exception;

  /**
   * Returns the event buffer sent by the write tasks.
   */
  WriteMetadataEvent[] getEventBuffer();

  /**
   * Returns the next event sent to Coordinator.
   */
  OperatorEvent getNextEvent();

  /**
   * Returns the next event sent to subtask.
   */
  default OperatorEvent getNextSubTaskEvent() {
    throw new UnsupportedOperationException();
  }

  /**
   * Snapshot all the functions in the wrapper.
   */
  void checkpointFunction(long checkpointId) throws Exception;

  /**
   * End inputs for all the operators in the wrapper.
   */
  void endInput();

  /**
   * Mark checkpoint with id {code checkpointId} as success.
   */
  void checkpointComplete(long checkpointId);

  /**
   * Triggers the job failover, including the coordinator and the write tasks.
   */
  default void jobFailover() throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Triggers the coordinator failover separately.
   */
  default void coordinatorFails() throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Triggers Job level fail, so the coordinator need re-create a new instance.
   * @throws Exception
   */
  default void restartCoordinator() throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the operator coordinator.
   */
  StreamWriteOperatorCoordinator getCoordinator();

  /**
   * Returns the write function.
   */
  AbstractWriteFunction getWriteFunction();

  /**
   * Returns the data buffer of the write task.
   */
  default Map<String, List<HoodieRecord>> getDataBuffer() {
    throw new UnsupportedOperationException();
  }

  /**
   * Mark checkpoint with id {code checkpointId} as failed.
   */
  default void checkpointFails(long checkpointId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the context of the coordinator.
   */
  default MockOperatorCoordinatorContext getCoordinatorContext() {
    throw new UnsupportedOperationException();
  }

  /**
   * Mark sub-task with id {@code taskId} as failed.
   */
  default void subTaskFails(int taskId, int attemptNumber) throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns whether the given key {@code key} is in the state store.
   */
  default boolean isKeyInState(HoodieKey key) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns whether the bootstrap function already bootstrapped.
   */
  default boolean isAlreadyBootstrap() throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns whether the write task is confirming.
   */
  default boolean isConforming() {
    throw new UnsupportedOperationException();
  }

  /**
   * Close this function wrapper.
   */
  void close() throws Exception;
}
