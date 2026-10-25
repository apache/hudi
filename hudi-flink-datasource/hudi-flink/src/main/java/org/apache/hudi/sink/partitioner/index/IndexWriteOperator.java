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

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.sink.common.AbstractWriteOperator;
import org.apache.hudi.sink.event.Correspondent;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.RowData;

/**
 * A Flink streaming operator responsible for writing index records to the metadata table.
 *
 * <p>This operator is part of the Flink sink topology for Hudi tables and handles the index
 * writing phase. It extends {@link AbstractWriteOperator} to leverage common write operator
 * functionality while providing specialized behavior for index record handling.
 *
 * <p>The operator works in conjunction with:
 * <ul>
 *   <li>{@link IndexWriteFunction} - performs the actual index record writing logic</li>
 *   <li>{@link Correspondent} - handles communication with the operator coordinator</li>
 * </ul>
 *
 * @see IndexWriteFunction
 * @see Correspondent
 */
public class IndexWriteOperator extends AbstractWriteOperator<RowData> {
  /**
   * The operator ID of the corresponding data write operator.
   * Used to establish communication with the coordinator for the data write task.
   */
  private final OperatorID dataWriteOperatorId;

  /**
   * Constructs an IndexWriteOperator with the specified configuration and data write operator ID.
   *
   * @param conf                  the Flink configuration for this operator
   * @param dataWriteOperatorId   the operator ID of the corresponding data write operator
   *                              used for coordinator communication
   */
  public IndexWriteOperator(Configuration conf, OperatorID dataWriteOperatorId) {
    super(new IndexWriteFunction(conf));
    this.dataWriteOperatorId = dataWriteOperatorId;
  }

  @Override
  public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<RowData>> output) {
    super.setup(containingTask, config, output);
    setCorrespondent(Correspondent.getInstance(dataWriteOperatorId,
        getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway()));
  }
}
