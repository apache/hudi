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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.ObjectSizeCalculator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.common.AbstractStreamWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.table.action.commit.FlinkWriteHelper;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Sink function to write the data to the underneath filesystem.
 *
 * <p><h2>Work Flow</h2>
 *
 * <p>The function firstly buffers the data as a batch of {@link HoodieRecord}s,
 * It flushes(write) the records batch when the batch size exceeds the configured size {@link FlinkOptions#WRITE_BATCH_SIZE}
 * or the total buffer size exceeds the configured size {@link FlinkOptions#WRITE_TASK_MAX_SIZE}
 * or a Flink checkpoint starts. After a batch has been written successfully,
 * the function notifies its operator coordinator {@link StreamWriteOperatorCoordinator} to mark a successful write.
 *
 * <p><h2>The Semantics</h2>
 *
 * <p>The task implements exactly-once semantics by buffering the data between checkpoints. The operator coordinator
 * starts a new instant on the timeline when a checkpoint triggers, the coordinator checkpoints always
 * start before its operator, so when this function starts a checkpoint, a REQUESTED instant already exists.
 *
 * <p>The function process thread blocks data buffering after the checkpoint thread finishes flushing the existing data buffer until
 * the current checkpoint succeed and the coordinator starts a new instant. Any error triggers the job failure during the metadata committing,
 * when the job recovers from a failure, the write function re-send the write metadata to the coordinator to see if these metadata
 * can re-commit, thus if unexpected error happens during the instant committing, the coordinator would retry to commit when the job
 * recovers.
 *
 * <p><h2>Fault Tolerance</h2>
 *
 * <p>The operator coordinator checks and commits the last instant then starts a new one after a checkpoint finished successfully.
 * It rolls back any inflight instant before it starts a new instant, this means one hoodie instant only span one checkpoint,
 * the write function blocks data buffer flushing for the configured checkpoint timeout
 * before it throws exception, any checkpoint failure would finally trigger the job failure.
 *
 * <p>Note: The function task requires the input stream be shuffled by the file IDs.
 *
 * @param <I> Type of the input record
 * @see StreamWriteOperatorCoordinator
 */
public class StreamWriteFunction<I> extends AbstractStreamWriteFunction<I> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(StreamWriteFunction.class);

  /**
   * Write buffer as buckets for a checkpoint. The key is bucket ID.
   */
  private transient Map<String, DataBucket> buckets;

  private transient BiFunction<List<HoodieRecord>, String, List<WriteStatus>> writeFunction;

  /**
   * Total size tracer.
   */
  private transient TotalSizeTracer tracer;

  /**
   * Constructs a StreamingSinkFunction.
   *
   * @param config The config options
   */
  public StreamWriteFunction(Configuration config) {
    super(config);
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    this.tracer = new TotalSizeTracer(this.config);
    initBuffer();
    initWriteFunction();
  }

  @Override
  public void snapshotState() {
    // Based on the fact that the coordinator starts the checkpoint first,
    // it would check the validity.
    // wait for the buffer data flush out and request a new instant
    flushRemaining(false);
  }

  @Override
  public void processElement(I value, ProcessFunction<I, Object>.Context ctx, Collector<Object> out) throws Exception {
    bufferRecord((HoodieRecord<?>) value);
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
    super.endInput();
    flushRemaining(true);
    this.writeClient.cleanHandles();
    this.writeStatuses.clear();
  }

  // -------------------------------------------------------------------------
  //  Getter/Setter
  // -------------------------------------------------------------------------
  @VisibleForTesting
  @SuppressWarnings("rawtypes")
  public Map<String, List<HoodieRecord>> getDataBuffer() {
    Map<String, List<HoodieRecord>> ret = new HashMap<>();
    for (Map.Entry<String, DataBucket> entry : buckets.entrySet()) {
      ret.put(entry.getKey(), entry.getValue().writeBuffer());
    }
    return ret;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void initBuffer() {
    this.buckets = new LinkedHashMap<>();
  }

  private void initWriteFunction() {
    final String writeOperation = this.config.get(FlinkOptions.OPERATION);
    switch (WriteOperationType.fromValue(writeOperation)) {
      case INSERT:
        this.writeFunction = (records, instantTime) -> this.writeClient.insert(records, instantTime);
        break;
      case UPSERT:
        this.writeFunction = (records, instantTime) -> this.writeClient.upsert(records, instantTime);
        break;
      case INSERT_OVERWRITE:
        this.writeFunction = (records, instantTime) -> this.writeClient.insertOverwrite(records, instantTime);
        break;
      case INSERT_OVERWRITE_TABLE:
        this.writeFunction = (records, instantTime) -> this.writeClient.insertOverwriteTable(records, instantTime);
        break;
      default:
        throw new RuntimeException("Unsupported write operation : " + writeOperation);
    }
  }

  /**
   * Represents a data item in the buffer, this is needed to reduce the
   * memory footprint.
   *
   * <p>A {@link HoodieRecord} was firstly transformed into a {@link DataItem}
   * for buffering, it then transforms back to the {@link HoodieRecord} before flushing.
   */
  private static class DataItem {
    private final String key; // record key
    private final String instant; // 'U' or 'I'
    private final HoodieRecordPayload<?> data; // record payload
    private final HoodieOperation operation; // operation

    private DataItem(String key, String instant, HoodieRecordPayload<?> data, HoodieOperation operation) {
      this.key = key;
      this.instant = instant;
      this.data = data;
      this.operation = operation;
    }

    public static DataItem fromHoodieRecord(HoodieRecord<?> record) {
      return new DataItem(
          record.getRecordKey(),
          record.getCurrentLocation().getInstantTime(),
          ((HoodieAvroRecord) record).getData(),
          record.getOperation());
    }

    public HoodieRecord<?> toHoodieRecord(String partitionPath) {
      HoodieKey hoodieKey = new HoodieKey(this.key, partitionPath);
      HoodieRecord<?> record = new HoodieAvroRecord<>(hoodieKey, data, operation);
      HoodieRecordLocation loc = new HoodieRecordLocation(instant, null);
      record.setCurrentLocation(loc);
      return record;
    }
  }

  /**
   * Data bucket.
   */
  private static class DataBucket {
    private final List<DataItem> records;
    private final BufferSizeDetector detector;
    private final String partitionPath;
    private final String fileID;

    private DataBucket(Double batchSize, HoodieRecord<?> hoodieRecord) {
      this.records = new ArrayList<>();
      this.detector = new BufferSizeDetector(batchSize);
      this.partitionPath = hoodieRecord.getPartitionPath();
      this.fileID = hoodieRecord.getCurrentLocation().getFileId();
    }

    /**
     * Prepare the write data buffer: patch up all the records with correct partition path.
     */
    public List<HoodieRecord> writeBuffer() {
      // rewrite all the records with new record key
      return records.stream()
          .map(record -> record.toHoodieRecord(partitionPath))
          .collect(Collectors.toList());
    }

    /**
     * Sets up before flush: patch up the first record with correct partition path and fileID.
     *
     * <p>Note: the method may modify the given records {@code records}.
     */
    public void preWrite(List<HoodieRecord> records) {
      // rewrite the first record with expected fileID
      HoodieRecord<?> first = records.get(0);
      HoodieRecord<?> record = new HoodieAvroRecord<>(first.getKey(), (HoodieRecordPayload) first.getData(), first.getOperation());
      HoodieRecordLocation newLoc = new HoodieRecordLocation(first.getCurrentLocation().getInstantTime(), fileID);
      record.setCurrentLocation(newLoc);

      records.set(0, record);
    }

    public void reset() {
      this.records.clear();
      this.detector.reset();
    }
  }

  /**
   * Tool to detect if to flush out the existing buffer.
   * Sampling the record to compute the size with 0.01 percentage.
   */
  private static class BufferSizeDetector {
    private final Random random = new Random(47);
    private static final int DENOMINATOR = 100;

    private final double batchSizeBytes;

    private long lastRecordSize = -1L;
    private long totalSize = 0L;

    BufferSizeDetector(double batchSizeMb) {
      this.batchSizeBytes = batchSizeMb * 1024 * 1024;
    }

    boolean detect(Object record) {
      if (lastRecordSize == -1 || sampling()) {
        lastRecordSize = ObjectSizeCalculator.getObjectSize(record);
      }
      totalSize += lastRecordSize;
      return totalSize > this.batchSizeBytes;
    }

    boolean sampling() {
      // 0.01 sampling percentage
      return random.nextInt(DENOMINATOR) == 1;
    }

    void reset() {
      this.lastRecordSize = -1L;
      this.totalSize = 0L;
    }
  }

  /**
   * Tool to trace the total buffer size. It computes the maximum buffer size,
   * if current buffer size is greater than the maximum buffer size, the data bucket
   * flush triggers.
   */
  private static class TotalSizeTracer {
    private long bufferSize = 0L;
    private final double maxBufferSize;

    TotalSizeTracer(Configuration conf) {
      long mergeReaderMem = 100; // constant 100MB
      long mergeMapMaxMem = conf.getInteger(FlinkOptions.WRITE_MERGE_MAX_MEMORY);
      this.maxBufferSize = (conf.getDouble(FlinkOptions.WRITE_TASK_MAX_SIZE) - mergeReaderMem - mergeMapMaxMem) * 1024 * 1024;
      final String errMsg = String.format("'%s' should be at least greater than '%s' plus merge reader memory(constant 100MB now)",
          FlinkOptions.WRITE_TASK_MAX_SIZE.key(), FlinkOptions.WRITE_MERGE_MAX_MEMORY.key());
      ValidationUtils.checkState(this.maxBufferSize > 0, errMsg);
    }

    /**
     * Trace the given record size {@code recordSize}.
     *
     * @param recordSize The record size
     * @return true if the buffer size exceeds the maximum buffer size
     */
    boolean trace(long recordSize) {
      this.bufferSize += recordSize;
      return this.bufferSize > this.maxBufferSize;
    }

    void countDown(long size) {
      this.bufferSize -= size;
    }

    public void reset() {
      this.bufferSize = 0;
    }
  }

  /**
   * Returns the bucket ID with the given value {@code value}.
   */
  private String getBucketID(HoodieRecord<?> record) {
    final String fileId = record.getCurrentLocation().getFileId();
    return StreamerUtil.generateBucketKey(record.getPartitionPath(), fileId);
  }

  /**
   * Buffers the given record.
   *
   * <p>Flush the data bucket first if the bucket records size is greater than
   * the configured value {@link FlinkOptions#WRITE_BATCH_SIZE}.
   *
   * <p>Flush the max size data bucket if the total buffer size exceeds the configured
   * threshold {@link FlinkOptions#WRITE_TASK_MAX_SIZE}.
   *
   * @param value HoodieRecord
   */
  protected void bufferRecord(HoodieRecord<?> value) {
    final String bucketID = getBucketID(value);

    DataBucket bucket = this.buckets.computeIfAbsent(bucketID,
        k -> new DataBucket(this.config.getDouble(FlinkOptions.WRITE_BATCH_SIZE), value));
    final DataItem item = DataItem.fromHoodieRecord(value);

    bucket.records.add(item);

    boolean flushBucket = bucket.detector.detect(item);
    boolean flushBuffer = this.tracer.trace(bucket.detector.lastRecordSize);
    if (flushBucket) {
      if (flushBucket(bucket)) {
        this.tracer.countDown(bucket.detector.totalSize);
        bucket.reset();
      }
    } else if (flushBuffer) {
      // find the max size bucket and flush it out
      List<DataBucket> sortedBuckets = this.buckets.values().stream()
          .sorted((b1, b2) -> Long.compare(b2.detector.totalSize, b1.detector.totalSize))
          .collect(Collectors.toList());
      final DataBucket bucketToFlush = sortedBuckets.get(0);
      if (flushBucket(bucketToFlush)) {
        this.tracer.countDown(bucketToFlush.detector.totalSize);
        bucketToFlush.reset();
      } else {
        LOG.warn("The buffer size hits the threshold {}, but still flush the max size data bucket failed!", this.tracer.maxBufferSize);
      }
    }
  }

  private boolean hasData() {
    return this.buckets.size() > 0
        && this.buckets.values().stream().anyMatch(bucket -> bucket.records.size() > 0);
  }

  @SuppressWarnings("unchecked, rawtypes")
  private boolean flushBucket(DataBucket bucket) {
    String instant = instantToWrite(true);

    if (instant == null) {
      // in case there are empty checkpoints that has no input data
      LOG.info("No inflight instant when flushing data, skip.");
      return false;
    }

    List<HoodieRecord> records = bucket.writeBuffer();
    ValidationUtils.checkState(records.size() > 0, "Data bucket to flush has no buffering records");
    if (config.getBoolean(FlinkOptions.PRE_COMBINE)) {
      records = FlinkWriteHelper.newInstance().deduplicateRecords(records, (HoodieIndex) null, -1);
    }
    bucket.preWrite(records);
    final List<WriteStatus> writeStatus = new ArrayList<>(writeFunction.apply(records, instant));
    records.clear();
    final WriteMetadataEvent event = WriteMetadataEvent.builder()
        .taskID(taskID)
        .instantTime(instant) // the write instant may shift but the event still use the currentInstant.
        .writeStatus(writeStatus)
        .lastBatch(false)
        .endInput(false)
        .build();

    this.eventGateway.sendEventToCoordinator(event);
    writeStatuses.addAll(writeStatus);
    return true;
  }

  @SuppressWarnings("unchecked, rawtypes")
  private void flushRemaining(boolean endInput) {
    this.currentInstant = instantToWrite(hasData());
    if (this.currentInstant == null) {
      // in case there are empty checkpoints that has no input data
      throw new HoodieException("No inflight instant when flushing data!");
    }
    final List<WriteStatus> writeStatus;
    if (buckets.size() > 0) {
      writeStatus = new ArrayList<>();
      this.buckets.values()
          // The records are partitioned by the bucket ID and each batch sent to
          // the writer belongs to one bucket.
          .forEach(bucket -> {
            List<HoodieRecord> records = bucket.writeBuffer();
            if (records.size() > 0) {
              if (config.getBoolean(FlinkOptions.PRE_COMBINE)) {
                records = FlinkWriteHelper.newInstance().deduplicateRecords(records, (HoodieIndex) null, -1);
              }
              bucket.preWrite(records);
              writeStatus.addAll(writeFunction.apply(records, currentInstant));
              records.clear();
              bucket.reset();
            }
          });
    } else {
      LOG.info("No data to write in subtask [{}] for instant [{}]", taskID, currentInstant);
      writeStatus = Collections.emptyList();
    }
    final WriteMetadataEvent event = WriteMetadataEvent.builder()
        .taskID(taskID)
        .instantTime(currentInstant)
        .writeStatus(writeStatus)
        .lastBatch(true)
        .endInput(endInput)
        .build();

    this.eventGateway.sendEventToCoordinator(event);
    this.buckets.clear();
    this.tracer.reset();
    this.writeClient.cleanHandles();
    this.writeStatuses.addAll(writeStatus);
    // blocks flushing until the coordinator starts a new instant
    this.confirming = true;
  }
}
