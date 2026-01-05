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
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;

import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@link FunctionInitializationContext} for testing purpose.
 */
@Getter
public class MockStateInitializationContext implements StateInitializationContext {

  private final MockOperatorStateStore operatorStateStore;
  private final MockKeyedStateStore keyedStateStore;
  private long lastCheckpointId;

  public MockStateInitializationContext() {
    operatorStateStore = new MockOperatorStateStore();
    keyedStateStore = new MockKeyedStateStore();
    lastCheckpointId = -1;
  }

  @Override
  public Iterable<StatePartitionStreamProvider> getRawOperatorStateInputs() {
    return null;
  }

  @Override
  public Iterable<KeyGroupStatePartitionStreamProvider> getRawKeyedStateInputs() {
    return null;
  }

  /**
   * Override function to avoid different implementations in different Flink versions.
   * @return true if the state is restored from a checkpoint, false otherwise
   */
  @Override
  public boolean isRestored() {
    return getRestoredCheckpointId().isPresent();
  }

  @Override
  public OptionalLong getRestoredCheckpointId() {
    return this.lastCheckpointId >= 0 ? OptionalLong.of(this.lastCheckpointId) : OptionalLong.empty();
  }

  public void checkpointBegin(long checkpointId) throws Exception {
    assertTrue(checkpointId >= 0, "Checkpoint ID must be non-negative, but was: " + checkpointId);
    getOperatorStateStore().checkpointBegin(checkpointId);
    getKeyedStateStore().checkpointBegin(checkpointId);
    this.lastCheckpointId = checkpointId;
  }

  public void checkpointSuccess(long checkpointId) {
    assertTrue(checkpointId >= 0, "Checkpoint ID must be non-negative, but was: " + checkpointId);
    getOperatorStateStore().checkpointSuccess(checkpointId);
    getKeyedStateStore().checkpointSuccess(checkpointId);
    this.lastCheckpointId = checkpointId;
  }
}
