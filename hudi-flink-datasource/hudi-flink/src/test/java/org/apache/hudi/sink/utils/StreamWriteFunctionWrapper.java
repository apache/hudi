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

import org.apache.hudi.adapter.CollectOutputAdapter;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.bootstrap.BootstrapOperator;
import org.apache.hudi.sink.common.AbstractWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
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
import org.apache.flink.streaming.api.operators.collect.utils.MockFunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorEventGateway;
import org.apache.flink.streaming.util.MockStreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class to manipulate the instance {@link StreamWriteFunction} for testing.
 *
 * @param <I> Input type
 */
public class StreamWriteFunctionWrapper<I> implements TestFunctionWrapper<I> {
  private final Configuration conf;
  private final RowType rowType;

  private final IOManager ioManager;
  private final MockStreamingRuntimeContext runtimeContext;
  private final MockOperatorEventGateway gateway;
  private final MockOperatorCoordinatorContext coordinatorContext;
  private StreamWriteOperatorCoordinator coordinator;
  private final MockStateInitializationContext stateInitializationContext;
  private final TreeMap<Long, byte[]> coordinatorStateStore;

  /**
   * Function that converts row data to HoodieRecord.
   */
  private RowDataToHoodieFunction<RowData, HoodieFlinkInternalRow> toHoodieFunction;
  /**
   * Function that load index in state.
   */
  private BootstrapOperator bootstrapOperator;
  /**
   * Function that assigns bucket ID.
   */
  private BucketAssignFunction bucketAssignerFunction;
  /**
   * BucketAssignOperator context.
   **/
  private final MockBucketAssignFunctionContext bucketAssignFunctionContext;
  /**
   * Stream write function.
   */
  protected StreamWriteFunction writeFunction;

  private CompactFunctionWrapper compactFunctionWrapper;

  private final MockStreamTask streamTask;

  private final StreamConfig streamConfig;

  private final boolean asyncCompaction;

  public StreamWriteFunctionWrapper(String tablePath) throws Exception {
    this(tablePath, TestConfigurations.getDefaultConf(tablePath));
  }

  public StreamWriteFunctionWrapper(String tablePath, Configuration conf) throws Exception {
    this.ioManager = new IOManagerAsync();
    MockEnvironment environment = new MockEnvironmentBuilder()
        .setTaskName("mockTask")
        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
        .setIOManager(ioManager)
        .build();
    this.runtimeContext = new MockStreamingRuntimeContext(false, 1, 0, environment);
    this.gateway = new MockOperatorEventGateway();
    this.conf = conf;
    this.rowType = (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf).getAvroSchema()).getLogicalType();
    // one function
    this.coordinatorContext = new MockOperatorCoordinatorContext(new OperatorID(), 1);
    this.coordinator = new StreamWriteOperatorCoordinator(conf, this.coordinatorContext);
    this.bucketAssignFunctionContext = new MockBucketAssignFunctionContext();
    this.stateInitializationContext = new MockStateInitializationContext();
    this.coordinatorStateStore = new TreeMap<>();
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
    resetCoordinatorToCheckpoint();
    this.coordinator.start();
    this.coordinator.setExecutor(new MockCoordinatorExecutor(coordinatorContext));
    toHoodieFunction = new RowDataToHoodieFunction<>(rowType, conf);
    toHoodieFunction.setRuntimeContext(runtimeContext);
    toHoodieFunction.open(conf);

    bucketAssignerFunction = new BucketAssignFunction(conf);
    bucketAssignerFunction.setRuntimeContext(runtimeContext);
    bucketAssignerFunction.open(conf);
    bucketAssignerFunction.initializeState(this.stateInitializationContext);

    if (conf.get(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
      bootstrapOperator = new BootstrapOperator(conf);
      CollectOutputAdapter<HoodieFlinkInternalRow> output = new CollectOutputAdapter<>();
      bootstrapOperator.setup(streamTask, streamConfig, output);
      bootstrapOperator.initializeState(this.stateInitializationContext);

      Collector<HoodieFlinkInternalRow> collector = RecordsCollector.getInstance();
      for (HoodieFlinkInternalRow bootstrapRecord : output.getRecords()) {
        stateInitializationContext.getKeyedStateStore().setCurrentKey(bootstrapRecord.getRecordKey());
        bucketAssignerFunction.processElement(bootstrapRecord, null, collector);
        bucketAssignFunctionContext.setCurrentKey(bootstrapRecord.getRecordKey());
      }
    }

    setupWriteFunction();

    if (asyncCompaction) {
      compactFunctionWrapper.openFunction();
    }
  }

  public void invoke(I record) throws Exception {
    HoodieFlinkInternalRow hoodieRecord = toHoodieFunction.map((RowData) record);
    stateInitializationContext.getKeyedStateStore().setCurrentKey(hoodieRecord.getRecordKey());
    RecordsCollector<HoodieFlinkInternalRow> collector = RecordsCollector.getInstance();
    bucketAssignerFunction.processElement(hoodieRecord, null, collector);
    bucketAssignFunctionContext.setCurrentKey(hoodieRecord.getRecordKey());
    for (HoodieFlinkInternalRow row: collector.getVal()) {
      writeFunction.processElement(row, null, null);
    }
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

  public Map<String, List<HoodieRecord>> getDataBuffer() {
    return writeFunction.getDataBuffer();
  }

  public void checkpointFunction(long checkpointId) throws Exception {
    // checkpoint the coordinator first
    checkpointCoordinator(checkpointId);
    if (conf.get(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
      bootstrapOperator.snapshotState(null);
    }
    bucketAssignerFunction.snapshotState(null);

    writeFunction.snapshotState(new MockFunctionSnapshotContext(checkpointId));
    stateInitializationContext.checkpointBegin(checkpointId);
  }

  private void checkpointCoordinator(long checkpointId) throws Exception {
    CompletableFuture<byte[]> completableFuture = new CompletableFuture<>();
    // checkpoint the coordinator first
    this.coordinator.checkpointCoordinator(checkpointId, completableFuture);
    this.coordinatorStateStore.put(checkpointId, completableFuture.get());
  }

  private void resetCoordinatorToCheckpoint() {
    if (coordinatorStateStore.isEmpty()) {
      return;
    }
    Map.Entry<Long, byte[]> latestState = this.coordinatorStateStore.lastEntry();
    this.coordinator.resetToCheckpoint(latestState.getKey(), latestState.getValue());
  }

  public void endInput() {
    writeFunction.endInput();
  }

  public void checkpointComplete(long checkpointId) {
    stateInitializationContext.checkpointSuccess(checkpointId);
    coordinator.notifyCheckpointComplete(checkpointId);
    this.bucketAssignerFunction.notifyCheckpointComplete(checkpointId);
    if (asyncCompaction) {
      try {
        compactFunctionWrapper.compact(checkpointId);
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    }
  }

  @Override
  public void inlineCompaction() {
    if (asyncCompaction) {
      try {
        compactFunctionWrapper.compact(1); // always uses a constant checkpoint ID.
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    }
  }

  public void jobFailover() throws Exception {
    coordinatorFails();
    subTaskFails(0, 0);
  }

  public void coordinatorFails() throws Exception {
    this.coordinator.close();
    resetCoordinatorToCheckpoint();
    this.coordinator.start();
    this.coordinator.setExecutor(new MockCoordinatorExecutor(coordinatorContext));
  }

  public void restartCoordinator() throws Exception {
    this.coordinator.close();
    this.coordinator = new StreamWriteOperatorCoordinator(conf, this.coordinatorContext);
    resetCoordinatorToCheckpoint();
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

  public void close() throws Exception {
    coordinator.close();
    ioManager.close();
    bucketAssignerFunction.close();
    writeFunction.close();
    if (compactFunctionWrapper != null) {
      compactFunctionWrapper.close();
    }
  }

  public StreamWriteOperatorCoordinator getCoordinator() {
    return coordinator;
  }

  @Override
  public AbstractWriteFunction getWriteFunction() {
    return this.writeFunction;
  }

  public MockOperatorCoordinatorContext getCoordinatorContext() {
    return coordinatorContext;
  }

  public boolean isKeyInState(HoodieKey hoodieKey) {
    return this.bucketAssignFunctionContext.isKeyInState(hoodieKey.getRecordKey());
  }

  public boolean isAlreadyBootstrap() throws Exception {
    return this.bootstrapOperator.isAlreadyBootstrap();
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void setupWriteFunction() throws Exception {
    writeFunction = new StreamWriteFunction(conf, rowType);
    writeFunction.setRuntimeContext(runtimeContext);
    writeFunction.setOperatorEventGateway(gateway);
    writeFunction.initializeState(this.stateInitializationContext);
    writeFunction.open(conf);
    writeFunction.setCorrespondent(new MockCorrespondent(this.coordinator));
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  private static class MockBucketAssignFunctionContext {
    private final Set<Object> updateKeys = new HashSet<>();

    public void setCurrentKey(Object key) {
      this.updateKeys.add(key);
    }

    public boolean isKeyInState(String key) {
      return this.updateKeys.contains(key);
    }
  }
}
