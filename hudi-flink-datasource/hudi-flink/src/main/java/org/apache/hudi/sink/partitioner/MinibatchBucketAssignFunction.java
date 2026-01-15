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

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.adapter.ProcessFunctionAdapter;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.sink.partitioner.index.MinibatchIndexBackend;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The function to build the write profile incrementally for records within a checkpoint,
 * it then assigns the bucket with ID using the {@link BucketAssigner}.
 *
 * <p>This implementation buffers input records and processes them in batches to improve
 * performance when using RLI index, reducing the number of individual index lookups.
 */
public class MinibatchBucketAssignFunction
    extends ProcessFunctionAdapter<HoodieFlinkInternalRow, HoodieFlinkInternalRow>
    implements BoundedOneInput, CheckpointedFunction, CheckpointListener {

  /**
   * The delegate bucket assign function that does the actual bucket assigning.
   *
   * <p>Note: we are using BucketAssignFunction as a delegate function rather than
   * extending the BucketAssignFunction here, because BucketAssignFunction is a
   * KeyedProcessFunction while MinibatchBucketAssignFunction is a ProcessFunction.
   */
  private final BucketAssignFunction delegateFunction;

  /**
   * The maximum number of input records can be buffered for MiniBatch.
   */
  private final int miniBatchSize;

  /**
   * Buffer to store the incoming record keys.
   */
  private transient List<HoodieFlinkInternalRow> recordBuffer;

  private final boolean isChangingRecords;

  private transient Collector<HoodieFlinkInternalRow> outCollector;

  public MinibatchBucketAssignFunction(Configuration conf) {
    this.delegateFunction = new BucketAssignFunction(conf);
    this.isChangingRecords = WriteOperationType.isChangingRecords(
        WriteOperationType.fromValue(conf.get(FlinkOptions.OPERATION)));
    this.miniBatchSize = conf.get(FlinkOptions.INDEX_RLI_LOOKUP_MINIBATCH_SIZE);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    delegateFunction.open(parameters);
    this.recordBuffer = new ArrayList<>();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    delegateFunction.initializeState(context);
  }

  @Override
  public void processElement(HoodieFlinkInternalRow record, Context ctx, Collector<HoodieFlinkInternalRow> outCollector) throws Exception {
    if (this.outCollector == null) {
      this.outCollector = outCollector;
    }
    if (record.isIndexRecord()) {
      // handle index records immediately, do not need buffering
      delegateFunction.processRecord(record, record.getRecordKey(), outCollector);
    } else {
      // Add data records to the buffer
      recordBuffer.add(record);
      // Process the buffer if it reaches the configured size
      if (recordBuffer.size() >= miniBatchSize) {
        processBufferedRecords(outCollector);
      }
    }
  }

  /**
   * Process all buffered records in batch.
   */
  private void processBufferedRecords(Collector<HoodieFlinkInternalRow> out) throws Exception {
    if (recordBuffer.isEmpty()) {
      return;
    }

    // process batch of records.
    if (isChangingRecords) {
      List<String> recordKeys = recordBuffer.stream().map(HoodieFlinkInternalRow::getRecordKey).collect(Collectors.toList());
      MinibatchIndexBackend minibatchIndexBackend = (MinibatchIndexBackend) delegateFunction.getIndexBackend();
      // load the record location mapping into the record index cache.
      minibatchIndexBackend.get(recordKeys);
    }
    for (HoodieFlinkInternalRow record: recordBuffer) {
      delegateFunction.processRecord(record, record.getRecordKey(), out);
    }

    // Clear the buffer after processing
    recordBuffer.clear();
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    delegateFunction.snapshotState(context);
  }

  @Override
  public void setRuntimeContext(RuntimeContext t) {
    super.setRuntimeContext(t);
    delegateFunction.setRuntimeContext(t);
  }

  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    // Process any remaining records in the buffer before checkpoint
    processBufferedRecords(outCollector);
  }

  public void setCorrespondent(Correspondent correspondent) {
    this.delegateFunction.setCorrespondent(correspondent);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // Refresh the table state when there are new commits.
    delegateFunction.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void endInput() throws Exception {
    // Process any remaining records in the buffer when input ends
    processBufferedRecords(this.outCollector);
  }

  @Override
  public void close() throws Exception {
    delegateFunction.close();
    super.close();
  }
}
