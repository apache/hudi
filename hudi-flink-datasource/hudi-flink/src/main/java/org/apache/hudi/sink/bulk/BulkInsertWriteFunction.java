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

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.common.AbstractWriteFunction;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.utils.RuntimeContextUtils;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;

/**
 * Sink function to write the data to the underneath filesystem.
 *
 * <p>The function should only be used in operation type {@link WriteOperationType#BULK_INSERT}.
 *
 * <p>Note: The function task requires the input stream be shuffled by partition path.
 *
 * @param <I> Type of the input record
 * @see StreamWriteOperatorCoordinator
 */
@Slf4j
public class BulkInsertWriteFunction<I>
    extends AbstractWriteFunction<I> {

  private static final long serialVersionUID = 1L;

  /**
   * Helper class for bulk insert mode.
   */
  private transient BulkInsertWriterHelper writerHelper;

  /**
   * Config options.
   */
  private final Configuration config;

  /**
   * Table row type.
   */
  private final RowType rowType;

  /**
   * Id of current subtask.
   */
  private int taskID;

  /**
   * Write Client.
   */
  private transient HoodieFlinkWriteClient writeClient;

  /**
   * Correspondent to request the instant time.
   */
  @Setter
  private transient Correspondent correspondent;

  /**
   * Gateway to send operator events to the operator coordinator.
   */
  @Setter
  private transient OperatorEventGateway operatorEventGateway;

  /**
   * Constructs a StreamingSinkFunction.
   *
   * @param config The config options
   */
  public BulkInsertWriteFunction(Configuration config, RowType rowType) {
    this.config = config;
    this.rowType = rowType;
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    this.taskID = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
    this.writeClient = FlinkWriteClients.createWriteClient(this.config, getRuntimeContext());
  }

  @Override
  public void processElement(I value, Context ctx, Collector<RowData> out) throws IOException {
    initWriterHelperIfNeeded();
    this.writerHelper.write((RowData) value);
  }

  @Override
  public void close() {
    if (this.writeClient != null) {
      this.writeClient.close();
    }
  }

  /**
   * End input action for batch source.
   */
  public void endInput() {
    initWriterHelperIfNeeded();
    final List<WriteStatus> writeStatus = this.writerHelper.getWriteStatuses(this.taskID);

    final WriteMetadataEvent event = WriteMetadataEvent.builder()
        .taskID(taskID)
        .instantTime(this.writerHelper.getInstantTime())
        .writeStatus(writeStatus)
        .lastBatch(true)
        .endInput(true)
        .build();
    this.operatorEventGateway.sendEventToCoordinator(event);
  }

  @Override
  public void handleOperatorEvent(OperatorEvent event) {
    // no operation
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void initWriterHelperIfNeeded() {
    if (writerHelper == null) {
      String instant = instantToWrite();
      this.writerHelper = WriterHelpers.getWriterHelper(this.config, this.writeClient.getHoodieTable(), this.writeClient.getConfig(),
          instant, this.taskID, RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext()), RuntimeContextUtils.getAttemptNumber(getRuntimeContext()),
          this.rowType);
    }
  }

  /**
   * Returns the instant to write.
   */
  private String instantToWrite() {
    return this.correspondent.requestInstantTime(-1L);
  }
}
