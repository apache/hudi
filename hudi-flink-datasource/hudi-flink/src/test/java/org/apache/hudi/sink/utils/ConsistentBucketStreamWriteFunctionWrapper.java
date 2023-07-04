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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.bucket.ConsistentBucketAssignFunction;
import org.apache.hudi.sink.bucket.ConsistentBucketStreamWriteFunction;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockFunctionSnapshotContext;
import org.apache.flink.table.data.RowData;

import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class to manipulate the {@link ConsistentBucketStreamWriteFunction} instance for testing.
 *
 * @param <I> Input type
 */
public class ConsistentBucketStreamWriteFunctionWrapper<I> extends BucketStreamWriteFunctionWrapper<I> {

  private ConsistentBucketAssignFunction assignFunction;

  public ConsistentBucketStreamWriteFunctionWrapper(String tablePath) throws Exception {
    this(tablePath, TestConfigurations.getDefaultConf(tablePath));
  }

  public ConsistentBucketStreamWriteFunctionWrapper(String tablePath, Configuration conf) throws Exception {
    super(tablePath, conf);
  }

  @Override
  public void openFunction() throws Exception {
    super.openFunction();
    assignFunction = new ConsistentBucketAssignFunction(conf);
    assignFunction.setRuntimeContext(runtimeContext);
    assignFunction.open(conf);
  }

  @Override
  public void invoke(I record) throws Exception {
    HoodieRecord hoodieRecord = toHoodieFunction.map((RowData) record);
    ScalaCollector<HoodieRecord> collector = ScalaCollector.getInstance();
    assignFunction.processElement(hoodieRecord, null, collector);
    writeFunction.processElement(collector.getVal(), null, null);
  }

  @Override
  protected StreamWriteFunction<HoodieRecord<?>> createWriteFunction() {
    return new ConsistentBucketStreamWriteFunction<>(conf);
  }

  @Override
  public void checkpointFunction(long checkpointId) throws Exception {
    // checkpoint the coordinator first
    FunctionSnapshotContext functionSnapshotContext = new MockFunctionSnapshotContext(checkpointId);
    this.coordinator.checkpointCoordinator(checkpointId, new CompletableFuture<>());
    writeFunction.snapshotState(functionSnapshotContext);
    assignFunction.snapshotState(functionSnapshotContext);
    stateInitializationContext.getOperatorStateStore().checkpointBegin(checkpointId);
  }
}
