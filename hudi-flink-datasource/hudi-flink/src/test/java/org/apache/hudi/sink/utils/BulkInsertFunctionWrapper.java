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
import org.apache.hudi.adapter.TestStreamConfigs;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.bucket.BucketBulkInsertWriterHelper;
import org.apache.hudi.sink.bulk.BulkInsertWriteFunction;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.sort.SortOperator;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.sink.common.AbstractWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A wrapper class to manipulate the {@link BulkInsertWriteFunction} instance for testing.
 *
 * @param <I> Input type
 */
public class BulkInsertFunctionWrapper<I> implements TestFunctionWrapper<I> {
  private final Configuration conf;
  private final RowType rowType;
  private final RowType rowTypeWithFileId;

  private final IOManager ioManager;
  private final MockStreamingRuntimeContext runtimeContext;
  private final MockOperatorEventGateway gateway;
  private final MockOperatorCoordinatorContext coordinatorContext;
  private StreamWriteOperatorCoordinator coordinator;
  private final boolean needSortInput;

  private BulkInsertWriteFunction<RowData> writeFunction;
  private MapFunction<RowData, RowData> mapFunction;
  private Map<String, String> bucketIdToFileId;
  private SortOperator sortOperator;
  private CollectOutputAdapter<RowData> output;

  public BulkInsertFunctionWrapper(String tablePath, Configuration conf) throws Exception {
    ioManager = new IOManagerAsync();
    MockEnvironment environment = new MockEnvironmentBuilder()
        .setTaskName("mockTask")
        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
        .setIOManager(ioManager)
        .build();
    this.runtimeContext = new MockStreamingRuntimeContext(false, 1, 0, environment);
    this.gateway = new MockOperatorEventGateway();
    this.conf = conf;
    this.rowType = (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf).getAvroSchema()).getLogicalType();
    this.rowTypeWithFileId = BucketBulkInsertWriterHelper.rowTypeWithFileId(rowType);
    this.coordinatorContext = new MockOperatorCoordinatorContext(new OperatorID(), 1);
    this.coordinator = new StreamWriteOperatorCoordinator(conf, this.coordinatorContext);
    this.needSortInput = conf.get(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT);
  }

  public void openFunction() throws Exception {
    this.coordinator.start();
    this.coordinator.setExecutor(new MockCoordinatorExecutor(coordinatorContext));
    setupWriteFunction();
    setupMapFunction();
    if (needSortInput) {
      setupSortOperator();
    }
  }

  public void invoke(I record) throws Exception {
    RowData recordWithFileId = mapFunction.map((RowData) record);
    if (needSortInput) {
      // Sort input first, trigger writeFunction at the #endInput
      sortOperator.processElement(new StreamRecord(recordWithFileId));
    } else {
      writeFunction.processElement(recordWithFileId, null, null);
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

  public void checkpointFunction(long checkpointId) {
    // Do nothing
  }

  @Override
  public void endInput() {
    if (needSortInput) {
      // sort all inputs of SortOperator and flush to WriteFunction
      try {
        sortOperator.endInput();
        List<RowData> sortedRecords = output.getRecords();
        for (RowData record : sortedRecords) {
          writeFunction.processElement(record, null, null);
        }
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    }
    writeFunction.endInput();
    if (bucketIdToFileId != null) {
      this.bucketIdToFileId.clear();
    }
  }

  public void checkpointComplete(long checkpointId) {
    coordinator.notifyCheckpointComplete(checkpointId);
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

  @Override
  public void close() throws Exception {
    this.coordinator.close();
    this.ioManager.close();
    this.writeFunction.close();
    if (this.bucketIdToFileId != null) {
      this.bucketIdToFileId.clear();
    }
    if (needSortInput) {
      this.sortOperator.close();
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void setupWriteFunction() throws Exception {
    writeFunction = new BulkInsertWriteFunction<>(conf, rowType);
    writeFunction.setRuntimeContext(runtimeContext);
    writeFunction.setOperatorEventGateway(gateway);
    writeFunction.open(conf);
    writeFunction.setCorrespondent(new MockCorrespondent(this.coordinator));
  }

  private void setupMapFunction() {
    RowDataKeyGen keyGen = RowDataKeyGen.instance(conf, rowType);
    String indexKeys = OptionsResolver.getIndexKeyField(conf);
    boolean needFixedFileIdSuffix = OptionsResolver.isNonBlockingConcurrencyControl(conf);
    this.bucketIdToFileId = new HashMap<>();
    this.mapFunction = r -> BucketBulkInsertWriterHelper.rowWithFileId(bucketIdToFileId, keyGen, r, indexKeys, conf, needFixedFileIdSuffix);
  }

  private void setupSortOperator() throws Exception {
    MockEnvironment environment = new MockEnvironmentBuilder()
        .setTaskName("mockTask")
        .setManagedMemorySize(12 * MemoryManager.DEFAULT_PAGE_SIZE)
        .setIOManager(ioManager)
        .build();
    StreamTask<?, ?> streamTask = new MockStreamTaskBuilder(environment)
        .setConfig(new StreamConfig(conf))
        .setExecutionConfig(new ExecutionConfig().enableObjectReuse())
        .build();
    SortOperatorGen sortOperatorGen = BucketBulkInsertWriterHelper.getFileIdSorterGen(rowTypeWithFileId);
    this.sortOperator = (SortOperator) sortOperatorGen.createSortOperator(conf);
    this.sortOperator.setProcessingTimeService(new TestProcessingTimeService());
    this.output = new CollectOutputAdapter<>();
    StreamConfig streamConfig = new StreamConfig(conf);
    streamConfig.setOperatorID(new OperatorID());
    RowDataSerializer inputSerializer = new RowDataSerializer(rowTypeWithFileId);
    TestStreamConfigs.setupNetworkInputs(streamConfig, inputSerializer);
    streamConfig.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, .99);
    this.sortOperator.setup(streamTask, streamConfig, output);
    this.sortOperator.open();
  }
}
