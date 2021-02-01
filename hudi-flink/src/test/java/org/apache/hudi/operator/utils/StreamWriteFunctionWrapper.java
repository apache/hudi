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

package org.apache.hudi.operator.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockFunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorEventGateway;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.operator.StreamWriteFunction;
import org.apache.hudi.operator.StreamWriteOperatorCoordinator;
import org.apache.hudi.operator.event.BatchWriteSuccessEvent;

import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class to manipulate the {@link StreamWriteFunction} instance for testing.
 *
 * @param <I> Input type
 */
public class StreamWriteFunctionWrapper<I> {
  private final TypeSerializer<I> serializer;
  private final Configuration conf;

  private final IOManager ioManager;
  private final StreamingRuntimeContext runtimeContext;
  private final MockOperatorEventGateway gateway;
  private final StreamWriteOperatorCoordinator coordinator;
  private final MockFunctionInitializationContext functionInitializationContext;

  private StreamWriteFunction<Object, I, Object> function;

  public StreamWriteFunctionWrapper(String tablePath, TypeSerializer<I> serializer) throws Exception {
    this.serializer = serializer;
    this.ioManager = new IOManagerAsync();
    MockEnvironment environment = new MockEnvironmentBuilder()
        .setTaskName("mockTask")
        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
        .setIOManager(ioManager)
        .build();
    this.runtimeContext = new MockStreamingRuntimeContext(false, 1, 0, environment);
    this.gateway = new MockOperatorEventGateway();
    this.conf = TestConfigurations.getDefaultConf(tablePath);
    // one function
    this.coordinator = new StreamWriteOperatorCoordinator(conf, 1);
    this.coordinator.start();
    this.functionInitializationContext = new MockFunctionInitializationContext();
  }

  public void openFunction() throws Exception {
    function = new StreamWriteFunction<>(TestConfigurations.ROW_TYPE, this.conf);
    function.setRuntimeContext(runtimeContext);
    function.setOperatorEventGateway(gateway);
    function.open(this.conf);
  }

  public void invoke(I record) throws Exception {
    function.processElement(record, null, null);
  }

  public BatchWriteSuccessEvent[] getEventBuffer() {
    return this.coordinator.getEventBuffer();
  }

  public OperatorEvent getNextEvent() {
    return this.gateway.getNextEvent();
  }

  @SuppressWarnings("rawtypes")
  public HoodieFlinkWriteClient getWriteClient() {
    return this.function.getWriteClient();
  }

  public void checkpointFunction(long checkpointId) throws Exception {
    // checkpoint the coordinator first
    this.coordinator.checkpointCoordinator(checkpointId, new CompletableFuture<>());
    function.snapshotState(new MockFunctionSnapshotContext(checkpointId));
    functionInitializationContext.getOperatorStateStore().checkpointBegin(checkpointId);
  }

  public void checkpointComplete(long checkpointId) {
    functionInitializationContext.getOperatorStateStore().checkpointSuccess(checkpointId);
    coordinator.checkpointComplete(checkpointId);
  }

  public void checkpointFails(long checkpointId) {
    coordinator.notifyCheckpointAborted(checkpointId);
  }

  public void close() throws Exception {
    coordinator.close();
    ioManager.close();
  }

  public StreamWriteOperatorCoordinator getCoordinator() {
    return coordinator;
  }
}
