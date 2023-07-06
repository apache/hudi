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

package org.apache.hudi.sink;

import org.apache.hudi.sink.append.AppendWriteFunction;
import org.apache.hudi.sink.common.AbstractStreamWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.utils.MockStateInitializationContext;
import org.apache.hudi.sink.utils.MockStreamingRuntimeContext;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.collect.utils.MockFunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorEventGateway;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for {@link AbstractStreamWriteFunction}.
 */
public class TestStreamWriteFunction {

  private Configuration conf;

  private StreamWriteOperatorCoordinator coordinator;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws Exception {
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    OperatorCoordinator.Context context = new MockOperatorCoordinatorContext(new OperatorID(), 1);
    coordinator = new StreamWriteOperatorCoordinator(
        TestConfigurations.getDefaultConf(tempFile.getAbsolutePath()), context);
    coordinator.start();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testStreamWriteProcessWatermark(boolean append) throws Exception {
    RowType rowType = (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf)).getLogicalType();
    AbstractStreamWriteFunction<RowData> writeFunction = append
        ? new AppendWriteFunction<>(conf, rowType)
        : new StreamWriteFunction<>(conf);
    writeFunction.setRuntimeContext(new MockStreamingRuntimeContext(true, 1, 0));
    MockOperatorEventGateway eventGateway = new MockOperatorEventGateway();
    writeFunction.setOperatorEventGateway(eventGateway);
    MockStateInitializationContext stateInitializationContext = new MockStateInitializationContext();
    writeFunction.initializeState(stateInitializationContext);
    writeFunction.open(conf);
    coordinator.handleEventFromOperator(0, eventGateway.getNextEvent());
    long timestamp = System.currentTimeMillis();
    writeFunction.processWatermark(new Watermark(timestamp));
    assertEquals(timestamp, writeFunction.getWatermark());
    writeFunction.snapshotState(new MockFunctionSnapshotContext(0));
    ListState<WriteMetadataEvent> metadataEventListState = stateInitializationContext.getOperatorStateStore()
        .getListState(new ListStateDescriptor<>(
            "write-metadata-state",
            TypeInformation.of(WriteMetadataEvent.class)
        ));
    for (WriteMetadataEvent next : metadataEventListState.get()) {
      assertEquals(timestamp, next.getWatermark());
    }
  }
}
