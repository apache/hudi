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

package org.apache.hudi.sink.bulk;

import org.apache.hudi.sink.StreamWriteOperatorCoordinator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Operator for bulk insert mode sink.
 *
 * @param <I> The input type
 */
public class BulkInsertWriteOperator<I>
    extends ProcessOperator<I, Object>
    implements OperatorEventHandler, BoundedOneInput {
  private final BulkInsertWriteFunction<I, Object> sinkFunction;

  public BulkInsertWriteOperator(Configuration conf, RowType rowType) {
    super(new BulkInsertWriteFunction<>(conf, rowType));
    this.sinkFunction = (BulkInsertWriteFunction<I, Object>) getUserFunction();
  }

  @Override
  public void handleOperatorEvent(OperatorEvent event) {
    // no operation
  }

  void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
    sinkFunction.setOperatorEventGateway(operatorEventGateway);
  }

  @Override
  public void endInput() {
    sinkFunction.endInput();
  }

  public static OperatorFactory<RowData> getFactory(Configuration conf, RowType rowType) {
    return new OperatorFactory<>(conf, rowType);
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  public static class OperatorFactory<I>
      extends SimpleUdfStreamOperatorFactory<Object>
      implements CoordinatedOperatorFactory<Object>, OneInputStreamOperatorFactory<I, Object> {
    private static final long serialVersionUID = 1L;

    private final BulkInsertWriteOperator<I> operator;
    private final Configuration conf;

    public OperatorFactory(Configuration conf, RowType rowType) {
      super(new BulkInsertWriteOperator<>(conf, rowType));
      this.operator = (BulkInsertWriteOperator<I>) getOperator();
      this.conf = conf;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Object>> T createStreamOperator(StreamOperatorParameters<Object> parameters) {
      final OperatorID operatorID = parameters.getStreamConfig().getOperatorID();
      final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();

      this.operator.setOperatorEventGateway(eventDispatcher.getOperatorEventGateway(operatorID));
      this.operator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
      this.operator.setProcessingTimeService(this.processingTimeService);
      eventDispatcher.registerEventHandler(operatorID, operator);
      return (T) operator;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String s, OperatorID operatorID) {
      return new StreamWriteOperatorCoordinator.Provider(operatorID, this.conf);
    }

    @Override
    public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
      super.setProcessingTimeService(processingTimeService);
    }
  }
}
