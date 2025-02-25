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

import org.apache.hudi.adapter.ExecutionAttemptUtil;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;

/**
 * A mock {@link OperatorCoordinator.SubtaskGateway} for unit tests.
 */
public class MockSubtaskGateway implements OperatorCoordinator.SubtaskGateway {

  private final LinkedList<OperatorEvent> events = new LinkedList<>();

  @Override
  public CompletableFuture<Acknowledge> sendEvent(OperatorEvent operatorEvent) {
    events.add(operatorEvent);
    return CompletableFuture.completedFuture(Acknowledge.get());
  }

  @Override
  public ExecutionAttemptID getExecution() {
    return ExecutionAttemptUtil.randomId();
  }

  @Override
  public int getSubtask() {
    return 0;
  }

  public OperatorEvent getNextEvent() {
    return this.events.isEmpty() ? null : this.events.removeFirst();
  }
}
