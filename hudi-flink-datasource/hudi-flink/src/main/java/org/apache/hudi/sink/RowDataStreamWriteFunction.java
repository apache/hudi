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

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.v2.HandleRecords;
import org.apache.hudi.metrics.FlinkStreamWriteMetrics;
import org.apache.hudi.sink.buffer.MemorySegmentPoolFactory;
import org.apache.hudi.sink.buffer.RowDataBucket;
import org.apache.hudi.sink.buffer.TotalSizeTracer;
import org.apache.hudi.sink.common.AbstractStreamWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.utils.DummyNormalizedKeyComputer;
import org.apache.hudi.sink.utils.DummyRecordComparator;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.util.MutableIteratorWrapperIterator;
import org.apache.hudi.util.PreCombineFieldExtractor;
import org.apache.hudi.util.RowDataKeyGen;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * todo
 */
public class RowDataStreamWriteFunction extends AbstractStreamWriteFunction<HoodieFlinkInternalRow> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(RowDataStreamWriteFunction.class);

  /**
   * Write buffer as buckets for a checkpoint. The key is bucket ID.
   */
  private transient Map<String, RowDataBucket> buckets;

  protected transient WriteFunction writeFunction;

  private transient HoodieRecordMerger recordMerger;

  protected final RowType rowType;

  protected final RowDataKeyGen keyGen;

  protected final PreCombineFieldExtractor preCombineFieldExtractor;

  /**
   * Total size tracer.
   */
  private transient TotalSizeTracer tracer;

  /**
   * Metrics for flink stream write.
   */
  protected transient FlinkStreamWriteMetrics writeMetrics;

  protected transient MemorySegmentPool memorySegmentPool;

  private transient FlinkTaskContextSupplier taskContextSupplier;

  private static final AtomicLong RECORD_COUNTER = new AtomicLong(1);

  /**
   * Constructs a StreamingSinkFunction.
   *
   * @param config The config options
   */
  public RowDataStreamWriteFunction(Configuration config, RowType rowType) {
    super(config);
    this.rowType = rowType;
    this.keyGen = RowDataKeyGen.instance(StreamerUtil.flinkConf2TypedProperties(config), rowType);
    this.preCombineFieldExtractor = getPreCombineFieldExtractor(config, rowType);
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    this.tracer = new TotalSizeTracer(this.config);
    this.taskContextSupplier = new FlinkTaskContextSupplier(getRuntimeContext());
    initBuffer();
    initWriteFunction();
    initMergeClass();
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
                             ProcessFunction<HoodieFlinkInternalRow, Object>.Context ctx,
                             Collector<Object> out) throws Exception {
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
  //  Getter/Setter
  // -------------------------------------------------------------------------

  @VisibleForTesting
  @SuppressWarnings("rawtypes")
  public Map<String, Iterator<BinaryRowData>> getDataBuffer() {
    Map<String, Iterator<BinaryRowData>> ret = new HashMap<>();
    for (Map.Entry<String, RowDataBucket> entry : buckets.entrySet()) {
      ret.put(entry.getKey(), new MutableIteratorWrapperIterator<>(
          entry.getValue().getDataIterator(), () -> new BinaryRowData(rowType.getFieldCount()), false));
    }
    return ret;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void initBuffer() {
    this.buckets = new LinkedHashMap<>();
    this.memorySegmentPool = MemorySegmentPoolFactory.createMemorySegmentPool(config);
  }

  protected int getTaskId() {
    return this.taskContextSupplier.getPartitionIdSupplier().get();
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

  private void initMergeClass() {
    recordMerger = HoodieRecordUtils.mergerToPreCombineMode(writeClient.getConfig().getRecordMerger());
    LOG.info("init hoodie merge with class [{}]", recordMerger.getClass().getName());
  }

  /**
   * Returns the bucket ID with the given value {@code value}.
   */
  private String getBucketID(String partitionPath, String fileId) {
    return StreamerUtil.generateBucketKey(partitionPath, fileId);
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
    final String bucketID = getBucketID(record.getPartitionPath(), record.getFileId());

    RowDataBucket bucket = this.buckets.computeIfAbsent(bucketID,
        k -> new RowDataBucket(
            createBucketBuffer(false),
            createBucketBuffer(true),
            getBucketInfo(record),
            this.config.get(FlinkOptions.WRITE_BATCH_SIZE)));

    // set operation type into rowkind of row.
    record.getRowData().setRowKind(
        RowKind.fromByteValue(HoodieOperation.fromName(record.getOperationType()).getValue()));

    boolean success = bucket.writeRow(record.getRowData());
    // buffer is full.
    if (!success) {
      RowDataBucket bucketToFlush = this.buckets.values().stream()
          .max(Comparator.comparingLong(b -> b.getBufferSize()))
          .orElseThrow(NoSuchElementException::new);
      if (flushBucket(bucketToFlush)) {
        this.tracer.countDown(bucketToFlush.getBufferSize());
        bucketToFlush.reset();
      } else {
        LOG.warn("The buffer size hits the threshold {}, but still flush the max size data bucket failed!", this.tracer.maxBufferSize);
      }
      // try write row again
      success = bucket.writeRow(record.getRowData());
      if (!success) {
        throw new RuntimeException("Buffer is too small to hold a single record.");
      }
    }
    this.tracer.trace(bucket.getLastRecordSize());
    if (bucket.isFull()) {
      if (flushBucket(bucket)) {
        this.tracer.countDown(bucket.getBufferSize());
        bucket.reset();
      }
    }
    // update buffer metrics after tracing buffer size
    writeMetrics.setWriteBufferedSize(this.tracer.bufferSize);
  }

  private BinaryInMemorySortBuffer createBucketBuffer(boolean forDelete) {
    // for this case, all kind of rowdata are keeped as it is,
    // delete rowdata will not be converted to DeleteRecord.
    if (forDelete && config.get(FlinkOptions.CHANGELOG_ENABLED)) {
      return null;
    }
    Pair<NormalizedKeyComputer, RecordComparator> sortHelper = getSortHelper();
    return BinaryInMemorySortBuffer.createBuffer(
            sortHelper.getLeft(),
            new RowDataSerializer(rowType),
            new BinaryRowDataSerializer(rowType.getFieldCount()),
            sortHelper.getRight(),
            memorySegmentPool);
  }

  private Pair<NormalizedKeyComputer, RecordComparator> getSortHelper() {
    // todo gen real ones.
    return Pair.of(new DummyNormalizedKeyComputer(), new DummyRecordComparator());
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
        .instantTime(instant) // the write instant may shift but the event still use the currentInstant.
        .writeStatus(writeStatus)
        .lastBatch(false)
        .endInput(false)
        .build();

    this.eventGateway.sendEventToCoordinator(event);
    writeStatuses.addAll(writeStatus);
    return true;
  }

  private void flushRemaining(boolean endInput) {
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

    writeMetrics.endDataFlush();
    writeMetrics.resetAfterCommit();
  }

  private void registerMetrics() {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    writeMetrics = new FlinkStreamWriteMetrics(metrics);
    writeMetrics.registerMetrics();
  }

  protected List<WriteStatus> writeRecords(
      String instant,
      RowDataBucket rowDataBucket) {
    writeMetrics.startFileFlush();

    HandleRecords.Builder builder = HandleRecords.builder();

    Iterator<BinaryRowData> rowItr =
        new MutableIteratorWrapperIterator<>(
            rowDataBucket.getDataIterator(), () -> new BinaryRowData(rowType.getFieldCount()), true);
    Iterator<HoodieRecord> recordItr = new MappingIterator<>(
        rowItr, rowData -> convertToRecord(rowData, rowDataBucket.getBucketInfo(), instant));
    builder.withRecordItr(deduplicateRecordsIfNeeded(recordItr));

    if (rowDataBucket.getDeleteDataIterator() != null) {
      Iterator<BinaryRowData> deleteRowItr =
          new MutableIteratorWrapperIterator<>(
              rowDataBucket.getDeleteDataIterator(), () -> new BinaryRowData(rowType.getFieldCount()), false);
      Iterator<DeleteRecord> deleteRecordItr = new MappingIterator<>(
          deleteRowItr, deleteRow -> convertToDeleteRecord(deleteRow, rowDataBucket.getBucketInfo()));
      builder.withDeleteRecordItr(deleteRecordItr);
    }

    List<WriteStatus> statuses = writeFunction.write(builder.build(), rowDataBucket.getBucketInfo(), instant);
    writeMetrics.endFileFlush();
    writeMetrics.increaseNumOfFilesWritten();
    return statuses;
  }

  protected DeleteRecord convertToDeleteRecord(RowData dataRow, BucketInfo bucketInfo) {
    String key = keyGen.getRecordKey(dataRow);
    HoodieKey hoodieKey = new HoodieKey(key, bucketInfo.getPartitionPath());
    Comparable<?> preCombineValue = preCombineFieldExtractor.getPreCombineField(dataRow);
    return DeleteRecord.create(hoodieKey, preCombineValue);
  }

  protected HoodieFlinkRecord convertToRecord(RowData dataRow, BucketInfo bucketInfo, String instant) {
    boolean isPopulateMetaFields = OptionsResolver.isPopulateMetaFields(config);
    boolean allowOperationMetadataField = config.get(FlinkOptions.CHANGELOG_ENABLED);
    String key = keyGen.getRecordKey(dataRow);
    Comparable<?> preCombineValue = preCombineFieldExtractor.getPreCombineField(dataRow);
    HoodieOperation operation = HoodieOperation.fromValue(dataRow.getRowKind().toByteValue());
    HoodieKey hoodieKey = new HoodieKey(key, bucketInfo.getPartitionPath());
    if (!isPopulateMetaFields && !allowOperationMetadataField) {
      return new HoodieFlinkRecord(hoodieKey, operation, preCombineValue, dataRow);
    }

    int metaArity = (isPopulateMetaFields ? 5 : 0) + (allowOperationMetadataField ? 1 : 0);
    GenericRowData metaRow = new GenericRowData(metaArity);
    if (isPopulateMetaFields) {
      String seqId = HoodieRecord.generateSequenceId(instant, getTaskId(), RECORD_COUNTER.getAndIncrement());
      metaRow.setField(0, StringData.fromString(instant));
      metaRow.setField(1, StringData.fromString(seqId));
      metaRow.setField(2, StringData.fromString(key));
      metaRow.setField(3, StringData.fromString((bucketInfo.getPartitionPath())));
      metaRow.setField(4, StringData.fromString((bucketInfo.getFileIdPrefix())));
    }
    if (allowOperationMetadataField) {
      metaRow.setField(5, StringData.fromString(HoodieOperation.fromValue(dataRow.getRowKind().toByteValue()).getName()));
    }
    return new HoodieFlinkRecord(hoodieKey, operation, preCombineValue, new JoinedRowData(dataRow.getRowKind(), metaRow, dataRow));
  }

  protected Iterator<HoodieRecord> deduplicateRecordsIfNeeded(Iterator<HoodieRecord> records) {
    if (config.get(FlinkOptions.PRE_COMBINE)) {
      // todo: sort by record key, lazy merge during iterating, default for COW.
      return records;
    } else {
      return records;
    }
  }

  private PreCombineFieldExtractor getPreCombineFieldExtractor(Configuration conf, RowType rowType) {
    String preCombineField = OptionsResolver.getPreCombineField(conf);
    if (StringUtils.isNullOrEmpty(preCombineField)) {
      // return a dummy extractor.
      return rowData -> HoodieRecord.DEFAULT_ORDERING_VALUE;
    }
    List<String> fieldNames = rowType.getFieldNames();
    List<LogicalType> fieldTypes = rowType.getChildren();
    int preCombineFieldIdx = fieldNames.indexOf(preCombineField);
    RowData.FieldGetter preCombineFieldGetter = RowData.createFieldGetter(fieldTypes.get(preCombineFieldIdx), preCombineFieldIdx);

    return rowData -> {
      Object orderVal = preCombineFieldGetter.getFieldOrNull(rowData);
      if (orderVal instanceof TimestampData) {
        return ((TimestampData) orderVal).toInstant().toEpochMilli();
      } else {
        return (Comparable<?>) orderVal;
      }
    };
  }

  private interface WriteFunction {
    List<WriteStatus> write(HandleRecords records, BucketInfo bucketInfo, String instant);
  }
}
