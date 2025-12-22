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

package org.apache.hudi.metrics;

import org.apache.hudi.sink.common.AbstractStreamWriteFunction;

import com.codahale.metrics.SlidingWindowReservoir;
import lombok.Setter;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;

/**
 * Metrics for flink stream write (including append write, normal/bucket stream write etc.).
 * Used in subclasses of {@link AbstractStreamWriteFunction}.
 */
public class FlinkStreamWriteMetrics extends HoodieFlinkMetrics {
  private static final String DATA_FLUSH_KEY = "data_flush";
  private static final String FILE_FLUSH_KEY = "file_flush";
  private static final String HANDLE_CREATION_KEY = "handle_creation";

  /**
   * Flush data costs during checkpoint.
   */
  private long dataFlushCosts;

  /**
   * Number of records written in during a checkpoint window.
   */
  protected long writtenRecords;

  /**
   * Current write buffer size in StreamWriteFunction.
   */
  @Setter
  private long writeBufferedSize;

  /**
   * Total costs for flushing files during a checkpoint window.
   */
  private long fileFlushTotalCosts;

  /**
   * Number of handles opened during a checkpoint window. Increased with partition number/bucket number etc.
   */
  private long numOfOpenHandle;

  /**
   * Number of files written during a checkpoint window.
   */
  private long numOfFilesWritten;

  /**
   * Number of record write failure during a checkpoint window.
   */
  private long numOfRecordWriteFailures;

  /**
   * Number of records written per seconds.
   */
  protected final Meter recordWrittenPerSecond;

  /**
   * Number of write handle switches per seconds.
   */
  private final Meter handleSwitchPerSecond;

  /**
   * Cost of write handle creation.
   */
  private final Histogram handleCreationCosts;

  /**
   * Cost of a file flush.
   */
  private final Histogram fileFlushCost;

  public FlinkStreamWriteMetrics(MetricGroup metricGroup) {
    super(metricGroup);
    this.recordWrittenPerSecond = new DropwizardMeterWrapper(new com.codahale.metrics.Meter());
    this.handleSwitchPerSecond = new DropwizardMeterWrapper(new com.codahale.metrics.Meter());
    this.handleCreationCosts = new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingWindowReservoir(100)));
    this.fileFlushCost = new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingWindowReservoir(100)));
  }

  @Override
  public void registerMetrics() {
    metricGroup.meter("recordWrittenPerSecond", recordWrittenPerSecond);
    metricGroup.gauge("currentCommitWrittenRecords", () -> writtenRecords);
    metricGroup.gauge("dataFlushCosts", () -> dataFlushCosts);
    metricGroup.gauge("writeBufferedSize", () -> writeBufferedSize);

    metricGroup.gauge("fileFlushTotalCosts", () -> fileFlushTotalCosts);
    metricGroup.gauge("numOfFilesWritten", () -> numOfFilesWritten);
    metricGroup.gauge("numOfOpenHandle", () -> numOfOpenHandle);
    metricGroup.gauge("numOfRecordWriteFailures", () -> numOfRecordWriteFailures);


    metricGroup.meter("handleSwitchPerSecond", handleSwitchPerSecond);

    metricGroup.histogram("handleCreationCosts", handleCreationCosts);
    metricGroup.histogram("fileFlushCost", fileFlushCost);
  }

  public void startDataFlush() {
    startTimer(DATA_FLUSH_KEY);
  }

  public void endDataFlush() {
    this.dataFlushCosts = stopTimer(DATA_FLUSH_KEY);
  }

  public void markRecordIn() {
    this.writtenRecords += 1;
    recordWrittenPerSecond.markEvent();
  }

  public void increaseNumOfFilesWritten() {
    numOfFilesWritten += 1;
  }

  public void increaseNumOfRecordWriteFailure(long recordWriteFailures) {
    numOfRecordWriteFailures += recordWriteFailures;
  }

  public long getNumOfRecordWriteFailures() {
    return numOfRecordWriteFailures;
  }

  public void increaseNumOfOpenHandle() {
    numOfOpenHandle += 1;
    increaseNumOfFilesWritten();
  }

  public void markHandleSwitch() {
    handleSwitchPerSecond.markEvent();
  }

  public void startHandleCreation() {
    startTimer(HANDLE_CREATION_KEY);
  }

  public void endHandleCreation() {
    handleCreationCosts.update(stopTimer(HANDLE_CREATION_KEY));
  }

  public void startFileFlush() {
    startTimer(FILE_FLUSH_KEY);
  }

  public void endFileFlush() {
    long costs = stopTimer(FILE_FLUSH_KEY);
    fileFlushCost.update(costs);
    this.fileFlushTotalCosts += costs;
  }

  public void resetAfterCommit() {
    this.writtenRecords = 0;
    this.numOfFilesWritten = 0;
    this.numOfOpenHandle = 0;
    this.writeBufferedSize = 0;
    this.fileFlushTotalCosts = 0;
    this.numOfRecordWriteFailures = 0;
  }

}
