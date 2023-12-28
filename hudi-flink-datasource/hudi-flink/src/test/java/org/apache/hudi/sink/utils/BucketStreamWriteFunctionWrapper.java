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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.bucket.BucketStreamWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.transform.RowDataToHoodieFunction;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

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
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockFunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorEventGateway;
import org.apache.flink.streaming.util.MockStreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class to manipulate the {@link BucketStreamWriteFunction} instance for testing.
 *
 * @param <I> Input type
 */
public class BucketStreamWriteFunctionWrapper<I> implements TestFunctionWrapper<I> {
  protected final Configuration conf;

  private final IOManager ioManager;
  protected final StreamingRuntimeContext runtimeContext;
  private final MockOperatorEventGateway gateway;
  private final MockOperatorCoordinatorContext coordinatorContext;
  protected final StreamWriteOperatorCoordinator coordinator;
  protected final MockStateInitializationContext stateInitializationContext;

  /**
   * Function that converts row data to HoodieRecord.
   */
  protected RowDataToHoodieFunction<RowData, HoodieRecord<?>> toHoodieFunction;

  /**
   * Stream write function.
   */
  protected StreamWriteFunction<HoodieRecord<?>> writeFunction;

  private CompactFunctionWrapper compactFunctionWrapper;

  private final MockStreamTask streamTask;

  private final StreamConfig streamConfig;

  private final boolean asyncCompaction;

  public BucketStreamWriteFunctionWrapper(String tablePath) throws Exception {
    this(tablePath, TestConfigurations.getDefaultConf(tablePath));
  }

  public BucketStreamWriteFunctionWrapper(String tablePath, Configuration conf) throws Exception {
    this.ioManager = new IOManagerAsync();
    MockEnvironment environment = new MockEnvironmentBuilder()
        .setTaskName("mockTask")
        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
        .setIOManager(ioManager)
        .build();
    this.runtimeContext = new MockStreamingRuntimeContext(false, 1, 0, environment);
    this.gateway = new MockOperatorEventGateway();
    this.conf = conf;
    // one function
    this.coordinatorContext = new MockOperatorCoordinatorContext(new OperatorID(), 1);
    this.coordinator = new StreamWriteOperatorCoordinator(conf, this.coordinatorContext);
    this.stateInitializationContext = new MockStateInitializationContext();
    this.asyncCompaction = OptionsResolver.needsAsyncCompaction(conf);
    this.streamConfig = new StreamConfig(conf);
    streamConfig.setOperatorID(new OperatorID());
    this.streamTask = new MockStreamTaskBuilder(environment)
        .setConfig(new StreamConfig(conf))
        .setExecutionConfig(new ExecutionConfig().enableObjectReuse())
        .build();
    this.compactFunctionWrapper = new CompactFunctionWrapper(this.conf, this.streamTask, this.streamConfig);
  }

  public void openFunction() throws Exception {
    this.coordinator.start();
    this.coordinator.setExecutor(new MockCoordinatorExecutor(coordinatorContext));
    RowType rowType = (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf)).getLogicalType();
    toHoodieFunction = new RowDataToHoodieFunction<>(rowType, conf);
    toHoodieFunction.setRuntimeContext(runtimeContext);
    toHoodieFunction.open(conf);

    setupWriteFunction();

    if (asyncCompaction) {
      compactFunctionWrapper.openFunction();
    }
  }

  public void invoke(I record) throws Exception {
    HoodieRecord<?> hoodieRecord = toHoodieFunction.map((RowData) record);
    writeFunction.processElement(hoodieRecord, null, null);
  }

  public WriteMetadataEvent[] getEventBuffer() {
    return this.coordinator.getEventBuffer();
  }

  public OperatorEvent getNextEvent() {
    return this.gateway.getNextEvent();
  }

  public Map<String, List<HoodieRecord>> getDataBuffer() {
    return this.writeFunction.getDataBuffer();
  }

  public void checkpointFunction(long checkpointId) throws Exception {
    // checkpoint the coordinator first
    this.coordinator.checkpointCoordinator(checkpointId, new CompletableFuture<>());
    writeFunction.snapshotState(new MockFunctionSnapshotContext(checkpointId));
    stateInitializationContext.getOperatorStateStore().checkpointBegin(checkpointId);
  }

  public void endInput() {
    writeFunction.endInput();
  }

  public void checkpointComplete(long checkpointId) {
    stateInitializationContext.getOperatorStateStore().checkpointSuccess(checkpointId);
    coordinator.notifyCheckpointComplete(checkpointId);
    if (asyncCompaction) {
      try {
        compactFunctionWrapper.compact(checkpointId);
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    }
  }

  public void close() throws Exception {
    coordinator.close();
    ioManager.close();
    writeFunction.close();
    if (compactFunctionWrapper != null) {
      compactFunctionWrapper.close();
    }
  }

  public StreamWriteOperatorCoordinator getCoordinator() {
    return coordinator;
  }

  public MockOperatorCoordinatorContext getCoordinatorContext() {
    return coordinatorContext;
  }

  public boolean isConforming() {
    return this.writeFunction.isConfirming();
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void setupWriteFunction() throws Exception {
    writeFunction = createWriteFunction();
    writeFunction.setRuntimeContext(runtimeContext);
    writeFunction.setOperatorEventGateway(gateway);
    writeFunction.initializeState(this.stateInitializationContext);
    writeFunction.open(conf);

    // handle the bootstrap event
    coordinator.handleEventFromOperator(0, getNextEvent());
  }

  protected StreamWriteFunction<HoodieRecord<?>> createWriteFunction() {
    return new BucketStreamWriteFunction<>(conf);
  }
}
