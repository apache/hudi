/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * Operator wrapper that wires the dynamic bucket assign function to the data write coordinator.
 */
public class DynamicBucketAssignOperator extends KeyedProcessOperator<String, HoodieFlinkInternalRow, HoodieFlinkInternalRow> {

  /**
   * The dynamic bucket assign function.
   */
  private final DynamicBucketAssignFunction bucketAssignFunction;

  /**
   * OperatorId for the data write operator.
   */
  private final OperatorID dataWriteOperatorId;

  /**
   * Creates an operator wrapper for dynamic bucket assignment.
   *
   * @param bucketAssignFunction function that performs partitioned-RLI-backed assignment
   * @param dataWriteOperatorId operator id of the downstream data write operator coordinator
   */
  public DynamicBucketAssignOperator(DynamicBucketAssignFunction bucketAssignFunction, OperatorID dataWriteOperatorId) {
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
}
