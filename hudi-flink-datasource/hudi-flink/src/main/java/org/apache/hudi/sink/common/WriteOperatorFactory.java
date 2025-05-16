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

package org.apache.hudi.sink.common;

import org.apache.hudi.sink.StreamWriteOperator;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.event.Correspondent;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/**
 * Factory class for {@link StreamWriteOperator}.
 */
public class WriteOperatorFactory<I>
    extends SimpleUdfStreamOperatorFactory<Object>
    implements CoordinatedOperatorFactory<Object>, OneInputStreamOperatorFactory<I, Object> {
  private static final long serialVersionUID = 1L;

  private final AbstractWriteOperator<I> operator;
  private final Configuration conf;

  public WriteOperatorFactory(Configuration conf, AbstractWriteOperator<I> operator) {
    super(operator);
    this.operator = operator;
    this.conf = conf;
  }

  public static <I> WriteOperatorFactory<I> instance(Configuration conf, AbstractWriteOperator<I> operator) {
    return new WriteOperatorFactory<>(conf, operator);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends StreamOperator<Object>> T createStreamOperator(StreamOperatorParameters<Object> parameters) {
    final OperatorID operatorID = parameters.getStreamConfig().getOperatorID();
    final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();

    this.operator.setCorrespondent(Correspondent.getInstance(operatorID,
        parameters.getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway()));
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
}
