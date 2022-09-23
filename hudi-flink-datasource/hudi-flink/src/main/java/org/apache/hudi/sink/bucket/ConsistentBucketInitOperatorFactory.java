/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.sink.bucket;

import org.apache.hudi.common.model.HoodieRecord;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

public class ConsistentBucketInitOperatorFactory
    extends SimpleUdfStreamOperatorFactory<HoodieRecord>
    implements CoordinatedOperatorFactory<HoodieRecord>, OneInputStreamOperatorFactory<HoodieRecord, HoodieRecord> {

  private final Configuration conf;
  private final ConsistentBucketInitOperator operator;

  public static ConsistentBucketInitOperatorFactory instance(Configuration conf) {
    return new ConsistentBucketInitOperatorFactory(conf, new ConsistentBucketInitOperator());
  }

  private ConsistentBucketInitOperatorFactory(Configuration conf, ConsistentBucketInitOperator operator) {
    super(operator);
    this.operator = operator;
    this.conf = conf;
  }

  @Override
  public <T extends StreamOperator<HoodieRecord>> T createStreamOperator(StreamOperatorParameters<HoodieRecord> parameters) {
    final OperatorID operatorID = parameters.getStreamConfig().getOperatorID();
    final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();

    this.operator.setOperatorEventGateway(eventDispatcher.getOperatorEventGateway(operatorID));
    this.operator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    return (T) operator;
  }

  @Override
  public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
    return new OperatorCoordinator.Provider() {
      @Override
      public OperatorID getOperatorId() {
        return operatorID;
      }

      @Override
      public OperatorCoordinator create(OperatorCoordinator.Context context) throws Exception {
        return new ConsistentBucketInitCoordinator(conf, context);
      }
    };
  }
}
