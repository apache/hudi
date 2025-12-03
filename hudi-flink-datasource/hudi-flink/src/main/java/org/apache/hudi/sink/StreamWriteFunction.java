/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.FlinkStreamWriteMetrics;
import org.apache.hudi.sink.buffer.MemorySegmentPoolFactory;
import org.apache.hudi.sink.buffer.RowDataBucket;
import org.apache.hudi.sink.buffer.TotalSizeTracer;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.RowDataKeyGens;
import org.apache.hudi.sink.common.AbstractStreamWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.exception.MemoryPagesExhaustedException;
import org.apache.hudi.sink.transform.RecordConverter;
import org.apache.hudi.sink.utils.BufferUtils;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.table.action.commit.FlinkWriteHelper;
import org.apache.hudi.util.MutableIteratorWrapperIterator;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.hudi.common.util.HoodieRecordUtils.getOrderingFieldNames;

/**
 * Sink function to write the data to the underneath filesystem.
 *
 * <p><h2>Work Flow</h2>
 *
 * <p>The function firstly buffers the data (RowData) in a binary buffer based on {@code BinaryInMemorySortBuffer}.
 * It flushes(write) the records batch when the batch size exceeds the configured size {@link FlinkOptions#WRITE_BATCH_SIZE}
 * or the memory of the binary buffer is exhausted, and could not append any more data or a Flink checkpoint starts.
 * After a batch has been written successfully, the function notifies its operator coordinator {@link StreamWriteOperatorCoordinator}
 * to mark a successful write.
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
 * @see StreamWriteOperatorCoordinator
 */
public class StreamWriteFunction extends AbstractStreamWriteFunction<HoodieFlinkInternalRow> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(StreamWriteFunction.class);

  /**
   * Write buffer as buckets for a checkpoint. The key is bucket ID.
   */
  private transient Map<String, RowDataBucket> buckets;

  protected transient WriteFunction writeFunction;

  private transient BufferedRecordMerger<RowData> recordMerger;
  private transient HoodieReaderContext<RowData> readerContext;
  private transient List<String> orderingFieldNames;

  protected final RowType rowType;

  protected final RowDataKeyGen keyGen;

  /**
   * Total size tracer.
   */
  private transient TotalSizeTracer tracer;

  /**
   * Metrics for flink stream write.
   */
  protected transient FlinkStreamWriteMetrics writeMetrics;

  protected transient MemorySegmentPool memorySegmentPool;

  protected transient RecordConverter recordConverter;

  /**
   * Constructs a StreamingSinkFunction.
   *
   * @param config The config options
   */
  public StreamWriteFunction(Configuration config, RowType rowType) {
    super(config);
    this.rowType = rowType;
    this.keyGen = RowDataKeyGens.instance(config, rowType);
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    this.tracer = new TotalSizeTracer(this.config);
    initBuffer();
    initWriteFunction();
    initMergeClass();
    initRecordConverter();
    registerMetrics();
  }

  @Override
  public void snapshotState() {
    // Based on the fact that the coordinator starts the checkpoint first,
    // it would check the validity.
    // wait for the buffer data flush out and request a new instant
    flushRemaining(false);
  }

  @Override
  public void processElement(HoodieFlinkInternalRow record,
                             ProcessFunction<HoodieFlinkInternalRow, RowData>.Context ctx,
                             Collector<RowData> out) throws Exception {
    bufferRecord(record);
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
    super.endInput();
    flushRemaining(true);
    this.writeClient.cleanHandles();
    this.writeStatuses.clear();
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void initBuffer() {
    this.buckets = new LinkedHashMap<>();
    this.memorySegmentPool = MemorySegmentPoolFactory.createMemorySegmentPool(config);
  }

  private void initWriteFunction() {
    final String writeOperation = this.config.get(FlinkOptions.OPERATION);
    switch (WriteOperationType.fromValue(writeOperation)) {
      case INSERT:
        this.writeFunction = (records, bucketInfo, instantTime) -> this.writeClient.insert(records, bucketInfo, instantTime);
        break;
      case UPSERT:
      case DELETE: // shares the code path with UPSERT
      case DELETE_PREPPED:
        this.writeFunction = (records, bucketInfo, instantTime) -> this.writeClient.upsert(records, bucketInfo, instantTime);
        break;
      case INSERT_OVERWRITE:
        this.writeFunction = (records, bucketInfo, instantTime) -> this.writeClient.insertOverwrite(records, bucketInfo, instantTime);
        break;
      case INSERT_OVERWRITE_TABLE:
        this.writeFunction = (records, bucketInfo, instantTime) -> this.writeClient.insertOverwriteTable(records, bucketInfo, instantTime);
        break;
      default:
        throw new RuntimeException("Unsupported write operation : " + writeOperation);
    }
  }

  private void initRecordConverter() {
    this.recordConverter = RecordConverter.getInstance(keyGen);
  }

  private void initMergeClass() {
    readerContext = writeClient.getEngineContext().<RowData>getReaderContextFactory(metaClient).getContext();
    readerContext.initRecordMergerForIngestion(writeClient.getConfig().getProps());
    orderingFieldNames = getOrderingFieldNames(readerContext.getMergeMode(), metaClient);

    recordMerger = BufferedRecordMergerFactory.create(
        readerContext,
        readerContext.getMergeMode(),
        false,
        readerContext.getRecordMerger(),
        new Schema.Parser().parse(writeClient.getConfig().getSchema()),
        readerContext.getPayloadClasses(writeClient.getConfig().getProps()),
        writeClient.getConfig().getProps(),
        metaClient.getTableConfig().getPartialUpdateMode());
    LOG.info("init hoodie merge with class [{}]", recordMerger.getClass().getName());
  }

  /**
   * Returns the bucket ID with the given value {@code value}.
   */
  private String getBucketID(String partitionPath, String fileId) {
    return StreamerUtil.generateBucketKey(partitionPath, fileId);
  }

  /**
   * Create a data bucket if not exists and trying to insert a data row, there exists two cases that data cannot be
   * inserted successfully:
   * <p>1. Data Bucket do not exist and there is no enough memory pages to create a new binary buffer.
   * <p>2. Data Bucket exists, but fails to request new memory pages from memory pool.
   */
  private boolean doBufferRecord(String bucketID, HoodieFlinkInternalRow record) throws IOException {
    try {
      RowDataBucket bucket = this.buckets.computeIfAbsent(bucketID,
          k -> new RowDataBucket(
              bucketID,
              BufferUtils.createBuffer(rowType, memorySegmentPool),
              getBucketInfo(record),
              this.config.get(FlinkOptions.WRITE_BATCH_SIZE)));

      return bucket.writeRow(record.getRowData());
    } catch (MemoryPagesExhaustedException e) {
      LOG.info("There is no enough free pages in memory pool to create buffer, need flushing first.", e);
      return false;
    }
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
   * @param record HoodieFlinkInternalRow
   */
  protected void bufferRecord(HoodieFlinkInternalRow record) throws IOException {
    writeMetrics.markRecordIn();
    // set operation type into rowkind of row.
    record.getRowData().setRowKind(
        RowKind.fromByteValue(HoodieOperation.fromName(record.getOperationType()).getValue()));
    final String bucketID = getBucketID(record.getPartitionPath(), record.getFileId());

    // 1. try buffer the record into the memory pool
    boolean success = doBufferRecord(bucketID, record);
    if (!success) {
      // 2. flushes the bucket if the memory pool is full
      RowDataBucket bucketToFlush = this.buckets.values().stream()
          .max(Comparator.comparingLong(RowDataBucket::getBufferSize))
          .orElseThrow(NoSuchElementException::new);
      if (flushBucket(bucketToFlush)) {
        // 2.1 flushes the data bucket with maximum size
        this.tracer.countDown(bucketToFlush.getBufferSize());
        disposeBucket(bucketToFlush);
      } else {
        LOG.warn("The buffer size hits the threshold {}, but still flush the max size data bucket failed!", this.tracer.maxBufferSize);
      }
      // 2.2 try to write row again
      success = doBufferRecord(bucketID, record);
      if (!success) {
        throw new RuntimeException("Buffer is too small to hold a single record.");
      }
    }
    RowDataBucket bucket = this.buckets.get(bucketID);
    this.tracer.trace(bucket.getLastRecordSize());
    // 3. flushes the bucket if it is full
    if (bucket.isFull()) {
      if (flushBucket(bucket)) {
        this.tracer.countDown(bucket.getBufferSize());
        disposeBucket(bucket);
      }
    }
    // update buffer metrics after tracing buffer size
    writeMetrics.setWriteBufferedSize(this.tracer.bufferSize);
  }

  /**
   * RowData data bucket can not be used after disposing.
   */
  private void disposeBucket(RowDataBucket rowDataBucket) {
    rowDataBucket.dispose();
    this.buckets.remove(rowDataBucket.getBucketId());
  }

  private static BucketInfo getBucketInfo(HoodieFlinkInternalRow internalRow) {
    BucketType bucketType;
    switch (internalRow.getInstantTime()) {
      case "I":
        bucketType = BucketType.INSERT;
        break;
      case "U":
        bucketType = BucketType.UPDATE;
        break;
      default:
        throw new HoodieException("Unexpected bucket type: " + internalRow.getInstantTime());
    }
    return new BucketInfo(bucketType, internalRow.getFileId(), internalRow.getPartitionPath());
  }

  private boolean hasData() {
    return !this.buckets.isEmpty()
        && this.buckets.values().stream().anyMatch(bucket -> !bucket.isEmpty());
  }

  private boolean flushBucket(RowDataBucket bucket) {
    String instant = instantToWrite(true);

    if (instant == null) {
      // in case there are empty checkpoints that has no input data
      LOG.info("No inflight instant when flushing data, skip.");
      return false;
    }

    ValidationUtils.checkState(!bucket.isEmpty(), "Data bucket to flush has no buffering records");
    final List<WriteStatus> writeStatus = writeRecords(instant, bucket);
    final WriteMetadataEvent event = WriteMetadataEvent.builder()
        .taskID(taskID)
        .checkpointId(this.checkpointId)
        .instantTime(instant) // the write instant may shift but the event still use the currentInstant.
        .writeStatus(writeStatus)
        .lastBatch(false)
        .endInput(false)
        .build();

    this.eventGateway.sendEventToCoordinator(event);
    writeStatuses.addAll(writeStatus);
    return true;
  }

  public void flushRemaining(boolean endInput) {
    writeMetrics.startDataFlush();
    this.currentInstant = instantToWrite(hasData());
    if (this.currentInstant == null) {
      // in case there are empty checkpoints that has no input data
      throw new HoodieException("No inflight instant when flushing data!");
    }
    final List<WriteStatus> writeStatus;
    if (!buckets.isEmpty()) {
      writeStatus = new ArrayList<>();
      this.buckets.values()
          // The records are partitioned by the bucket ID and each batch sent to
          // the writer belongs to one bucket.
          .forEach(bucket -> {
            if (!bucket.isEmpty()) {
              writeStatus.addAll(writeRecords(currentInstant, bucket));
              bucket.dispose();
            }
          });
    } else {
      LOG.info("No data to write in subtask [{}] for instant [{}]", taskID, currentInstant);
      writeStatus = Collections.emptyList();
    }
    final WriteMetadataEvent event = WriteMetadataEvent.builder()
        .taskID(taskID)
        .checkpointId(checkpointId)
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

    writeMetrics.endDataFlush();
    writeMetrics.resetAfterCommit();
  }

  protected List<WriteStatus> writeRecords(
      String instant,
      RowDataBucket rowDataBucket) {
    writeMetrics.startFileFlush();

    Iterator<BinaryRowData> rowItr =
        new MutableIteratorWrapperIterator<>(
            rowDataBucket.getDataIterator(), () -> new BinaryRowData(rowType.getFieldCount()));
    Iterator<HoodieRecord> recordItr = new MappingIterator<>(
        rowItr, rowData -> recordConverter.convert(rowData, rowDataBucket.getBucketInfo()));

    List<WriteStatus> statuses = writeFunction.write(
        deduplicateRecordsIfNeeded(recordItr), rowDataBucket.getBucketInfo(), instant);
    writeMetrics.endFileFlush();
    writeMetrics.increaseNumOfFilesWritten();
    return statuses;
  }

  protected Iterator<HoodieRecord> deduplicateRecordsIfNeeded(Iterator<HoodieRecord> records) {
    if (config.get(FlinkOptions.PRE_COMBINE)) {
      return FlinkWriteHelper.newInstance().deduplicateRecords(
          records, null, -1, this.writeClient.getConfig().getSchema(),
          this.writeClient.getConfig().getProps(),
          recordMerger, readerContext, orderingFieldNames.toArray(new String[0]));
    } else {
      return records;
    }
  }

  private void registerMetrics() {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    writeMetrics = new FlinkStreamWriteMetrics(metrics);
    writeMetrics.registerMetrics();
  }

  // -------------------------------------------------------------------------
  //  Getter/Setter
  // -------------------------------------------------------------------------

  @VisibleForTesting
  @SuppressWarnings("rawtypes")
  public Map<String, List<HoodieRecord>> getDataBuffer() {
    Map<String, List<HoodieRecord>> ret = new HashMap<>();
    for (Map.Entry<String, RowDataBucket> entry : buckets.entrySet()) {
      List<HoodieRecord> records = new ArrayList<>();
      Iterator<BinaryRowData> rowItr =
          new MutableIteratorWrapperIterator<>(
              entry.getValue().getDataIterator(), () -> new BinaryRowData(rowType.getFieldCount()));
      while (rowItr.hasNext()) {
        records.add(recordConverter.convert(rowItr.next(), entry.getValue().getBucketInfo()));
      }
      ret.put(entry.getKey(), records);
    }
    return ret;
  }

  // -------------------------------------------------------------------------
  //  Inner Classes
  // -------------------------------------------------------------------------

  /**
   * Write function to trigger the actual write action.
   */
  protected interface WriteFunction extends Serializable {
    List<WriteStatus> write(Iterator<HoodieRecord> records, BucketInfo bucketInfo, String instant);
  }
}
