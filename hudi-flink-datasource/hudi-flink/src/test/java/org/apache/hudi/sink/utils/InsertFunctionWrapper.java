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

import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.append.AppendWriteFunction;
import org.apache.hudi.sink.append.AppendWriteFunctions;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
import org.apache.hudi.sink.common.AbstractWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.collect.utils.MockFunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorEventGateway;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
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

  private final MockStreamingRuntimeContext runtimeContext;
  private final MockOperatorEventGateway gateway;
  private final MockSubtaskGateway subtaskGateway;
  private final MockOperatorCoordinatorContext coordinatorContext;
  private StreamWriteOperatorCoordinator coordinator;
  private final MockStateInitializationContext stateInitializationContext;

  private final boolean asyncClustering;
  private ClusteringFunctionWrapper clusteringFunctionWrapper;

  /**
   * Append write function.
   */
  private AppendWriteFunction<RowData> writeFunction;

  public InsertFunctionWrapper(String tablePath, Configuration conf) throws Exception {
    IOManager ioManager = new IOManagerAsync();
    MockEnvironment environment = new MockEnvironmentBuilder()
        .setTaskName("mockTask")
        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
        .setIOManager(ioManager)
        .build();
    this.runtimeContext = new MockStreamingRuntimeContext(false, 1, 0, environment);
    this.gateway = new MockOperatorEventGateway();
    this.subtaskGateway = new MockSubtaskGateway();
    this.conf = conf;
    this.rowType = (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf).toAvroSchema()).getLogicalType();
    // one function
    this.coordinatorContext = new MockOperatorCoordinatorContext(new OperatorID(), 1);
    this.coordinator = new StreamWriteOperatorCoordinator(conf, this.coordinatorContext);
    this.stateInitializationContext = new MockStateInitializationContext();

    this.asyncClustering = OptionsResolver.needsAsyncClustering(conf);
    StreamConfig streamConfig = new StreamConfig(conf);
    streamConfig.setOperatorID(new OperatorID());
    StreamTask<?, ?> streamTask = new MockStreamTaskBuilder(environment)
        .setConfig(new StreamConfig(conf))
        .setExecutionConfig(new ExecutionConfig().enableObjectReuse())
        .build();
    this.clusteringFunctionWrapper = new ClusteringFunctionWrapper(this.conf, streamTask, streamConfig);
  }

  public void openFunction() throws Exception {
    this.coordinator.start();
    this.coordinator.setExecutor(new MockCoordinatorExecutor(coordinatorContext));

    setupWriteFunction();

    if (asyncClustering) {
      clusteringFunctionWrapper.openFunction();
    }
  }

  public void invoke(I record) throws Exception {
    writeFunction.processElement((RowData) record, null, null);
  }

  public WriteMetadataEvent[] getEventBuffer() {
    return this.coordinator.getEventBuffer();
  }

  @Override
  public WriteMetadataEvent[] getEventBuffer(long checkpointId) {
    return this.coordinator.getEventBuffer(checkpointId);
  }

  public OperatorEvent getNextEvent() {
    return this.gateway.getNextEvent();
  }

  @Override
  public OperatorEvent getNextSubTaskEvent() {
    return this.subtaskGateway.getNextEvent();
  }

  public void checkpointFunction(long checkpointId) throws Exception {
    // checkpoint the coordinator first
    this.coordinator.checkpointCoordinator(checkpointId, new CompletableFuture<>());

    writeFunction.snapshotState(new MockFunctionSnapshotContext(checkpointId));
    stateInitializationContext.checkpointBegin(checkpointId);
  }

  @Override
  public void endInput() {
    writeFunction.endInput();
  }

  public void checkpointComplete(long checkpointId) {
    stateInitializationContext.checkpointSuccess(checkpointId);
    coordinator.notifyCheckpointComplete(checkpointId);
    if (asyncClustering) {
      try {
        clusteringFunctionWrapper.cluster(checkpointId);
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    }
  }

  public void coordinatorFails() throws Exception {
    this.coordinator.close();
    this.coordinator.start();
    this.coordinator.setExecutor(new MockCoordinatorExecutor(coordinatorContext));
  }

  public void restartCoordinator() throws Exception {
    this.coordinator.close();
    this.coordinator = new StreamWriteOperatorCoordinator(conf, this.coordinatorContext);
    this.coordinator.start();
    this.coordinator.setExecutor(new MockCoordinatorExecutor(coordinatorContext));
  }

  public void checkpointFails(long checkpointId) {
    coordinator.notifyCheckpointAborted(checkpointId);
  }

  public void subTaskFails(int taskID, int attemptNumber) throws Exception {
    coordinator.subtaskFailed(taskID, new RuntimeException("Dummy exception"));
    // reset the attempt number to simulate the task failover/retries
    this.runtimeContext.setAttemptNumber(attemptNumber);
    setupWriteFunction();
  }

  public StreamWriteOperatorCoordinator getCoordinator() {
    return coordinator;
  }

  @Override
  public AbstractWriteFunction getWriteFunction() {
    return this.writeFunction;
  }

  @Override
  public void close() throws Exception {
    this.coordinator.close();
    if (clusteringFunctionWrapper != null) {
      clusteringFunctionWrapper.close();
    }
  }

  public BulkInsertWriterHelper getWriterHelper() {
    return this.writeFunction.getWriterHelper();
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void setupWriteFunction() throws Exception {
    writeFunction = AppendWriteFunctions.create(conf, rowType);
    writeFunction.setRuntimeContext(runtimeContext);
    writeFunction.setOperatorEventGateway(gateway);
    writeFunction.initializeState(this.stateInitializationContext);
    writeFunction.open(conf);
    writeFunction.setCorrespondent(new MockCorrespondent(this.coordinator));
    // set up subtask gateway
    coordinator.subtaskReady(0, subtaskGateway);
  }
}
