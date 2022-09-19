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
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.common.AbstractWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.meta.CkpMetadata;
import org.apache.hudi.sink.utils.TimeWait;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
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
public class BulkInsertWriteFunction<I>
    extends AbstractWriteFunction<I> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(BulkInsertWriteFunction.class);

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
   * The initial inflight instant when start up.
   */
  private volatile String initInstant;

  /**
   * Gateway to send operator events to the operator coordinator.
   */
  private transient OperatorEventGateway eventGateway;

  /**
   * Checkpoint metadata.
   */
  private CkpMetadata ckpMetadata;

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
    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.writeClient = StreamerUtil.createWriteClient(this.config, getRuntimeContext());
    this.ckpMetadata = CkpMetadata.getInstance(config);
    this.initInstant = lastPendingInstant();
    sendBootstrapEvent();
    initWriterHelper();
  }

  @Override
  public void processElement(I value, Context ctx, Collector<Object> out) throws IOException {
    this.writerHelper.write((RowData) value);
  }

  @Override
  public void close() {
    if (this.writeClient != null) {
      this.writeClient.cleanHandlesGracefully();
      this.writeClient.close();
    }
  }

  /**
   * End input action for batch source.
   */
  public void endInput() {
    final List<WriteStatus> writeStatus = this.writerHelper.getWriteStatuses(this.taskID);

    final WriteMetadataEvent event = WriteMetadataEvent.builder()
        .taskID(taskID)
        .instantTime(this.writerHelper.getInstantTime())
        .writeStatus(writeStatus)
        .lastBatch(true)
        .endInput(true)
        .build();
    this.eventGateway.sendEventToCoordinator(event);
  }

  @Override
  public void handleOperatorEvent(OperatorEvent event) {
    // no operation
  }

  // -------------------------------------------------------------------------
  //  Getter/Setter
  // -------------------------------------------------------------------------

  public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
    this.eventGateway = operatorEventGateway;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void initWriterHelper() {
    String instant = instantToWrite();
    this.writerHelper = WriterHelpers.getWriterHelper(this.config, this.writeClient.getHoodieTable(), this.writeClient.getConfig(),
        instant, this.taskID, getRuntimeContext().getNumberOfParallelSubtasks(), getRuntimeContext().getAttemptNumber(),
        this.rowType);
  }

  private void sendBootstrapEvent() {
    WriteMetadataEvent event = WriteMetadataEvent.builder()
        .taskID(taskID)
        .writeStatus(Collections.emptyList())
        .instantTime("")
        .bootstrap(true)
        .build();
    this.eventGateway.sendEventToCoordinator(event);
    LOG.info("Send bootstrap write metadata event to coordinator, task[{}].", taskID);
  }

  /**
   * Returns the last pending instant time.
   */
  protected String lastPendingInstant() {
    return this.ckpMetadata.lastPendingInstant();
  }

  private String instantToWrite() {
    String instant = lastPendingInstant();
    // if exactly-once semantics turns on,
    // waits for the checkpoint notification until the checkpoint timeout threshold hits.
    TimeWait timeWait = TimeWait.builder()
        .timeout(config.getLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT))
        .action("instant initialize")
        .build();
    while (instant == null || instant.equals(this.initInstant)) {
      // wait condition:
      // 1. there is no inflight instant
      // 2. the inflight instant does not change
      // sleep for a while
      timeWait.waitFor();
      // refresh the inflight instant
      instant = lastPendingInstant();
    }
    return instant;
  }
}
