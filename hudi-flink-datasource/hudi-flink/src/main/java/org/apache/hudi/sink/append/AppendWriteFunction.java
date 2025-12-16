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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.FlinkStreamWriteMetrics;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
import org.apache.hudi.sink.common.AbstractStreamWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.RuntimeContextUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Sink function to write the data to the underneath filesystem.
 *
 * <p>The function writes base files directly for each checkpoint,
 * the file may roll over when it’s size hits the configured threshold.
 *
 * @param <I> Type of the input record
 * @see StreamWriteOperatorCoordinator
 */
@Slf4j
public class AppendWriteFunction<I> extends AbstractStreamWriteFunction<I> {

  private static final long serialVersionUID = 1L;

  /**
   * Helper class for log mode.
   */
  protected transient BulkInsertWriterHelper writerHelper;

  /**
   * Table row type.
   */
  protected final RowType rowType;

  /**
   * Metrics for flink stream write.
   */
  protected FlinkStreamWriteMetrics writeMetrics;

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
  public void open(Configuration parameters) throws Exception {
    registerMetrics();
  }

  @Override
  public void snapshotState() {
    // Based on the fact that the coordinator starts the checkpoint first,
    // it would check the validity.
    // wait for the buffer data flush out and request a new instant
    flushData(false);
  }

  @Override
  public void processElement(I value, Context ctx, Collector<RowData> out) throws Exception {
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
  void initWriterHelper() {
    final String instant = instantToWrite(true);
    if (instant == null) {
      // in case there are empty checkpoints that has no input data
      throw new HoodieException("No inflight instant when flushing data!");
    }
    this.writerHelper = new BulkInsertWriterHelper(this.config, this.writeClient.getHoodieTable(), this.writeClient.getConfig(),
        instant, this.taskID, RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext()), RuntimeContextUtils.getAttemptNumber(getRuntimeContext()),
        this.rowType, false, Option.of(writeMetrics));
  }

  private void flushData(boolean endInput) {
    writeMetrics.startDataFlush();
    final List<WriteStatus> writeStatus;
    if (this.writerHelper != null) {
      writeStatus = this.writerHelper.getWriteStatuses(this.taskID);
      this.currentInstant = this.writerHelper.getInstantTime();
    } else {
      writeStatus = Collections.emptyList();
      this.currentInstant = instantToWrite(false);
      log.info("No data to write in subtask [{}] for instant [{}]", taskID, this.currentInstant);
    }

    recordWriteFailure(writeMetrics, writeStatus);
    StreamerUtil.validateWriteStatus(config, currentInstant, writeStatus);
    final WriteMetadataEvent event = WriteMetadataEvent.builder()
        .taskID(taskID)
        .checkpointId(this.checkpointId)
        .instantTime(this.currentInstant)
        .writeStatus(writeStatus)
        .lastBatch(true)
        .endInput(endInput)
        .build();
    this.eventGateway.sendEventToCoordinator(event);
    // nullify the write helper for next ckp
    this.writerHelper = null;
    this.writeStatuses.addAll(writeStatus);
    writeMetrics.endDataFlush();
    writeMetrics.resetAfterCommit();
  }

  private void registerMetrics() {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    writeMetrics = new FlinkStreamWriteMetrics(metrics);
    writeMetrics.registerMetrics();
  }

  /**
   * Update metrics and log for errors in write status.
   *
   * @param writeMetrics FlinkStreamWriteMetrics
   * @param writeStatus write status from write handler
   */
  @VisibleForTesting
  static void recordWriteFailure(FlinkStreamWriteMetrics writeMetrics, List<WriteStatus> writeStatus) {
    Map.Entry<HoodieKey, Throwable> firstFailure = null;
    for (WriteStatus status : writeStatus) {
      writeMetrics.increaseNumOfRecordWriteFailure(status.getTotalErrorRecords());
      if (firstFailure == null && status.getErrors().size() > 0) {
        firstFailure = status.getErrors().entrySet().stream().findFirst().get();
      }
    }

    // Only print the first record failure to prevent logs occupy too much disk in worst case.
    if (firstFailure != null) {
      LOG.error("The first record with written failure {}", firstFailure.getKey().getRecordKey(), firstFailure.getValue());
    }
  }
}
