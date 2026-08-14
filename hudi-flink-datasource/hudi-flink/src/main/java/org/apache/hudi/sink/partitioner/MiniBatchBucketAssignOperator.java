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

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.sink.event.Correspondent;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * An operator that performs mini-batch bucket assignment for incoming records.
 *
 * <p>This operator wraps the {@link MinibatchBucketAssignFunction} and handles the assignment
 * of buckets to records in mini-batches to improve performance when using RLI (Remote Lookup Index)
 * or other index types. It buffers input records and processes them in batches to reduce the number
 * of individual index lookups, which can significantly improve performance compared to processing
 * each record individually.
 *
 * @see MinibatchBucketAssignFunction for the underlying bucket assignment logic
 */
public class MiniBatchBucketAssignOperator extends ProcessOperator<HoodieFlinkInternalRow, HoodieFlinkInternalRow> implements BoundedOneInput {

  /**
   * The underlying function that performs the actual bucket assignment logic.
   */
  private final MinibatchBucketAssignFunction bucketAssignFunction;

  /**
   * OperatorId for the data write operator.
   */
  private final OperatorID dataWriteOperatorId;

  /**
   * Constructs a MiniBatchBucketAssignOperator with the specified bucket assignment function.
   *
   * @param bucketAssignFunction the function responsible for performing the bucket assignment logic
   */
  public MiniBatchBucketAssignOperator(MinibatchBucketAssignFunction bucketAssignFunction, OperatorID dataWriteOperatorId) {
    super(bucketAssignFunction);
    this.bucketAssignFunction = bucketAssignFunction;
    this.dataWriteOperatorId = dataWriteOperatorId;
  }

  @Override
  public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<HoodieFlinkInternalRow>> output) {
    super.setup(containingTask, config, output);
    this.bucketAssignFunction.setCorrespondent(Correspondent.getInstance(dataWriteOperatorId,
        getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway()));
  }

  /**
   * Prepares for taking a snapshot of the operator state before a barrier arrives.
   * This method ensures that any buffered records are processed before checkpointing
   * to maintain consistency in the bucket assignment state.
   *
   * @param checkpointId the ID of the checkpoint to be taken
   */
  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
    this.bucketAssignFunction.prepareSnapshotPreBarrier(checkpointId);
  }

  /**
   * Called when the input is finished, typically in the context of bounded streams.
   * This method processes any remaining buffered records to ensure all records
   * are properly assigned to buckets before the operator finishes.
   */
  @Override
  public void endInput() throws Exception {
    this.bucketAssignFunction.endInput();
  }
}
