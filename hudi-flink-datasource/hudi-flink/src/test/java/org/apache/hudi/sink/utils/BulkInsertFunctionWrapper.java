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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.bucket.BucketBulkInsertWriterHelper;
import org.apache.hudi.sink.bulk.BulkInsertWriteFunction;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorEventGateway;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper class to manipulate the {@link BulkInsertWriteFunction} instance for testing.
 *
 * @param <I> Input type
 */
public class BulkInsertFunctionWrapper<I> implements TestFunctionWrapper<I> {
  private final Configuration conf;
  private final RowType rowType;
  private final RowType rowTypeWithFileId;

  private final MockStreamingRuntimeContext runtimeContext;
  private final MockOperatorEventGateway gateway;
  private final MockOperatorCoordinatorContext coordinatorContext;
  private final StreamWriteOperatorCoordinator coordinator;
  private final MockStateInitializationContext stateInitializationContext;

  private final boolean asyncClustering;
  private ClusteringFunctionWrapper clusteringFunctionWrapper;

  /**
   * Bulk insert write function.
   */
  private BulkInsertWriteFunction<RowData> writeFunction;
  private MapFunction<RowData, RowData> mapFunction;
  private Map<String, String> bucketIdToFileId;

  public BulkInsertFunctionWrapper(String tablePath, Configuration conf) throws Exception {
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
    this.rowTypeWithFileId = BucketBulkInsertWriterHelper.rowTypeWithFileId(rowType);
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
    RowDataKeyGen keyGen = RowDataKeyGen.instance(conf, rowType);
    String indexKeys = OptionsResolver.getIndexKeyField(conf);
    int numBuckets = conf.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);
    boolean needFixedFileIdSuffix = OptionsResolver.isNonBlockingConcurrencyControl(conf);
    this.bucketIdToFileId = new HashMap<>();
    this.mapFunction = r -> BucketBulkInsertWriterHelper.rowWithFileId(bucketIdToFileId, keyGen, r, indexKeys, numBuckets, needFixedFileIdSuffix);
    if (asyncClustering) {
      clusteringFunctionWrapper.openFunction();
    }
  }

  public void invoke(I record) throws Exception {
    RowData recordWithFileId = mapFunction.map((RowData) record);
    writeFunction.processElement(recordWithFileId, null, null);
  }

  public WriteMetadataEvent[] getEventBuffer() {
    return this.coordinator.getEventBuffer();
  }

  public OperatorEvent getNextEvent() {
    return this.gateway.getNextEvent();
  }

  public void checkpointFunction(long checkpointId) {
    // Do nothing
  }

  @Override
  public void endInput() {
    writeFunction.endInput();
    if (bucketIdToFileId != null) {
      this.bucketIdToFileId.clear();
    }
  }

  public void checkpointComplete(long checkpointId) {
    stateInitializationContext.getOperatorStateStore().checkpointSuccess(checkpointId);
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

  public void checkpointFails(long checkpointId) {
    coordinator.notifyCheckpointAborted(checkpointId);
  }

  public StreamWriteOperatorCoordinator getCoordinator() {
    return coordinator;
  }

  public MockOperatorCoordinatorContext getCoordinatorContext() {
    return coordinatorContext;
  }

  @Override
  public void close() throws Exception {
    this.coordinator.close();
    if (clusteringFunctionWrapper != null) {
      clusteringFunctionWrapper.close();
    }
    if (bucketIdToFileId != null) {
      this.bucketIdToFileId.clear();
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void setupWriteFunction() {
    ExecutorService executors = Executors.newFixedThreadPool(2);
    Runnable writeFunctionInitializer = () -> {
      try {
        writeFunction = new BulkInsertWriteFunction<>(conf, rowType);
        writeFunction.setRuntimeContext(runtimeContext);
        writeFunction.setOperatorEventGateway(gateway);
        writeFunction.open(conf);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    executors.submit(writeFunctionInitializer);
    Runnable coordinatorInitializer = () -> {
      while (true) {
        try {
          OperatorEvent initializeEvent = getNextEvent();
          // handle the bootstrap event
          coordinator.handleEventFromOperator(0, initializeEvent);
          break;
        } catch (NoSuchElementException e) {
          try {
            // sleep and retry
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
      }
    };
    executors.submit(coordinatorInitializer);
    executors.shutdown();
    try {
      executors.awaitTermination(5, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      // ignore
    }
  }
}
