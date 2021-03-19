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

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.event.BatchWriteSuccessEvent;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.transform.RowDataToHoodieFunction;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorEventGateway;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class to manipulate the {@link StreamWriteFunction} instance for testing.
 *
 * @param <I> Input type
 */
public class StreamWriteFunctionWrapper<I> {
  private final Configuration conf;

  private final IOManager ioManager;
  private final StreamingRuntimeContext runtimeContext;
  private final MockOperatorEventGateway gateway;
  private final StreamWriteOperatorCoordinator coordinator;
  private final MockFunctionInitializationContext functionInitializationContext;

  /** Function that converts row data to HoodieRecord. */
  private RowDataToHoodieFunction<RowData, HoodieRecord<?>> toHoodieFunction;
  /** Function that assigns bucket ID. */
  private BucketAssignFunction<String, HoodieRecord<?>, HoodieRecord<?>> bucketAssignerFunction;
  /** Stream write function. */
  private StreamWriteFunction<Object, HoodieRecord<?>, Object> writeFunction;

  private CompactFunctionWrapper compactFunctionWrapper;

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
    // one function
    this.coordinator = new StreamWriteOperatorCoordinator(conf, 1);
    this.functionInitializationContext = new MockFunctionInitializationContext();
    this.compactFunctionWrapper = new CompactFunctionWrapper(this.conf);
  }

  public void openFunction() throws Exception {
    this.coordinator.start();
    toHoodieFunction = new RowDataToHoodieFunction<>(TestConfigurations.ROW_TYPE, conf);
    toHoodieFunction.setRuntimeContext(runtimeContext);
    toHoodieFunction.open(conf);

    bucketAssignerFunction = new BucketAssignFunction<>(conf);
    bucketAssignerFunction.setRuntimeContext(runtimeContext);
    bucketAssignerFunction.open(conf);
    bucketAssignerFunction.initializeState(this.functionInitializationContext);

    writeFunction = new StreamWriteFunction<>(conf);
    writeFunction.setRuntimeContext(runtimeContext);
    writeFunction.setOperatorEventGateway(gateway);
    writeFunction.open(conf);

    if (conf.getBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED)) {
      compactFunctionWrapper.openFunction();
    }
  }

  public void invoke(I record) throws Exception {
    HoodieRecord<?> hoodieRecord = toHoodieFunction.map((RowData) record);
    HoodieRecord<?>[] hoodieRecords = new HoodieRecord[1];
    Collector<HoodieRecord<?>> collector = new Collector<HoodieRecord<?>>() {
      @Override
      public void collect(HoodieRecord<?> record) {
        hoodieRecords[0] = record;
      }

      @Override
      public void close() {

      }
    };
    bucketAssignerFunction.processElement(hoodieRecord, null, collector);
    writeFunction.processElement(hoodieRecords[0], null, null);
  }

  public BatchWriteSuccessEvent[] getEventBuffer() {
    return this.coordinator.getEventBuffer();
  }

  public OperatorEvent getNextEvent() {
    return this.gateway.getNextEvent();
  }

  public Map<String, List<HoodieRecord>> getDataBuffer() {
    return this.writeFunction.getDataBuffer();
  }

  @SuppressWarnings("rawtypes")
  public HoodieFlinkWriteClient getWriteClient() {
    return this.writeFunction.getWriteClient();
  }

  public void checkpointFunction(long checkpointId) throws Exception {
    // checkpoint the coordinator first
    this.coordinator.checkpointCoordinator(checkpointId, new CompletableFuture<>());
    bucketAssignerFunction.snapshotState(null);

    writeFunction.snapshotState(null);
    functionInitializationContext.getOperatorStateStore().checkpointBegin(checkpointId);
  }

  public void checkpointComplete(long checkpointId) {
    functionInitializationContext.getOperatorStateStore().checkpointSuccess(checkpointId);
    coordinator.checkpointComplete(checkpointId);
    this.bucketAssignerFunction.notifyCheckpointComplete(checkpointId);
    this.writeFunction.notifyCheckpointComplete(checkpointId);
    if (conf.getBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED)) {
      try {
        compactFunctionWrapper.compact(checkpointId);
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    }
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

  public void clearIndexState() {
    this.bucketAssignerFunction.clearIndexState();
  }

  public boolean isKeyInState(HoodieKey hoodieKey) {
    return this.bucketAssignerFunction.isKeyInState(hoodieKey);
  }

  public boolean isAllPartitionsLoaded() {
    return this.bucketAssignerFunction.isAllPartitionsLoaded();
  }
}
