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

package org.apache.hudi.sink.append;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
import org.apache.hudi.sink.common.AbstractStreamWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Sink function to write the data to the underneath filesystem.
 *
 * <p>The function writes base files directly for each checkpoint,
 * the file may roll over when it’s size hits the configured threshold.
 *
 * @param <I> Type of the input record
 * @see StreamWriteOperatorCoordinator
 */
public class AppendWriteFunction<I> extends AbstractStreamWriteFunction<I> {
  private static final Logger LOG = LoggerFactory.getLogger(AppendWriteFunction.class);

  private static final long serialVersionUID = 1L;

  /**
   * Helper class for log mode.
   */
  private transient BulkInsertWriterHelper writerHelper;

  /**
   * Table row type.
   */
  private final RowType rowType;

  /**
   * Constructs an AppendWriteFunction.
   *
   * @param config The config options
   */
  public AppendWriteFunction(Configuration config, RowType rowType) {
    super(config);
    this.rowType = rowType;
  }

  @Override
  public void snapshotState() {
    // Based on the fact that the coordinator starts the checkpoint first,
    // it would check the validity.
    // wait for the buffer data flush out and request a new instant
    flushData(false);
  }

  @Override
  public void processElement(I value, Context ctx, Collector<Object> out) throws Exception {
    if (this.writerHelper == null) {
      initWriterHelper();
    }
    this.writerHelper.write((RowData) value);
  }

  /**
   * End input action for batch source.
   */
  public void endInput() {
    super.endInput();
    flushData(true);
    this.writeStatuses.clear();
  }

  // -------------------------------------------------------------------------
  //  GetterSetter
  // -------------------------------------------------------------------------
  @VisibleForTesting
  public BulkInsertWriterHelper getWriterHelper() {
    return this.writerHelper;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  private void initWriterHelper() {
    final String instant = instantToWrite(true);
    if (instant == null) {
      // in case there are empty checkpoints that has no input data
      throw new HoodieException("No inflight instant when flushing data!");
    }
    this.writerHelper = new BulkInsertWriterHelper(this.config, this.writeClient.getHoodieTable(), this.writeClient.getConfig(),
        instant, this.taskID, getRuntimeContext().getNumberOfParallelSubtasks(), getRuntimeContext().getAttemptNumber(),
        this.rowType);
  }

  private void flushData(boolean endInput) {
    final List<WriteStatus> writeStatus;
    if (this.writerHelper != null) {
      writeStatus = this.writerHelper.getWriteStatuses(this.taskID);
      this.currentInstant = this.writerHelper.getInstantTime();
    } else {
      writeStatus = Collections.emptyList();
      this.currentInstant = instantToWrite(false);
      LOG.info("No data to write in subtask [{}] for instant [{}]", taskID, this.currentInstant);
    }
    final WriteMetadataEvent event = WriteMetadataEvent.builder()
        .taskID(taskID)
        .instantTime(this.currentInstant)
        .writeStatus(writeStatus)
        .lastBatch(true)
        .endInput(endInput)
        .build();
    this.eventGateway.sendEventToCoordinator(event);
    // nullify the write helper for next ckp
    this.writerHelper = null;
    this.writeStatuses.addAll(writeStatus);
    // blocks flushing until the coordinator starts a new instant
    this.confirming = true;
  }
}
