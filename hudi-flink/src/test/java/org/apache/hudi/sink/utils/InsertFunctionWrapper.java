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

import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.append.AppendWriteFunction;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockFunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorEventGateway;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class to manipulate the {@link AppendWriteFunction} instance for testing.
 *
 * @param <I> Input type
 */
public class InsertFunctionWrapper<I> implements TestFunctionWrapper<I> {
  private final Configuration conf;
  private final RowType rowType;

  private final StreamingRuntimeContext runtimeContext;
  private final MockOperatorEventGateway gateway;
  private final MockOperatorCoordinatorContext coordinatorContext;
  private final StreamWriteOperatorCoordinator coordinator;
  private final MockStateInitializationContext stateInitializationContext;

  /**
   * Append write function.
   */
  private AppendWriteFunction<RowData> writeFunction;

  public InsertFunctionWrapper(String tablePath, Configuration conf) {
    IOManager ioManager = new IOManagerAsync();
    MockEnvironment environment = new MockEnvironmentBuilder()
        .setTaskName("mockTask")
        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
        .setIOManager(ioManager)
        .build();
    this.runtimeContext = new MockStreamingRuntimeContext(false, 1, 0, environment);
    this.gateway = new MockOperatorEventGateway();
    this.conf = conf;
    this.rowType = (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf)).getLogicalType();
    // one function
    this.coordinatorContext = new MockOperatorCoordinatorContext(new OperatorID(), 1);
    this.coordinator = new StreamWriteOperatorCoordinator(conf, this.coordinatorContext);
    this.stateInitializationContext = new MockStateInitializationContext();
  }

  public void openFunction() throws Exception {
    this.coordinator.start();
    this.coordinator.setExecutor(new MockCoordinatorExecutor(coordinatorContext));

    setupWriteFunction();
  }

  public void invoke(I record) throws Exception {
    writeFunction.processElement((RowData) record, null, null);
  }

  public WriteMetadataEvent[] getEventBuffer() {
    return this.coordinator.getEventBuffer();
  }

  public OperatorEvent getNextEvent() {
    return this.gateway.getNextEvent();
  }

  public void checkpointFunction(long checkpointId) throws Exception {
    // checkpoint the coordinator first
    this.coordinator.checkpointCoordinator(checkpointId, new CompletableFuture<>());

    writeFunction.snapshotState(new MockFunctionSnapshotContext(checkpointId));
    stateInitializationContext.getOperatorStateStore().checkpointBegin(checkpointId);
  }

  public void checkpointComplete(long checkpointId) {
    stateInitializationContext.getOperatorStateStore().checkpointSuccess(checkpointId);
    coordinator.notifyCheckpointComplete(checkpointId);
  }

  public StreamWriteOperatorCoordinator getCoordinator() {
    return coordinator;
  }

  @Override
  public void close() throws Exception {
    this.coordinator.close();
  }

  public BulkInsertWriterHelper getWriterHelper() {
    return this.writeFunction.getWriterHelper();
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void setupWriteFunction() throws Exception {
    writeFunction = new AppendWriteFunction<>(conf, rowType);
    writeFunction.setRuntimeContext(runtimeContext);
    writeFunction.setOperatorEventGateway(gateway);
    writeFunction.initializeState(this.stateInitializationContext);
    writeFunction.open(conf);

    // handle the bootstrap event
    coordinator.handleEventFromOperator(0, getNextEvent());
  }
}
