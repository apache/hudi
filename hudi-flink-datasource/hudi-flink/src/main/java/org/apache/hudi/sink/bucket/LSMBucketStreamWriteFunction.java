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

package org.apache.hudi.sink.bucket;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.model.HoodieRowDataCreation;
import org.apache.hudi.client.model.LSMHoodieInternalRowData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.disruptor.DisruptorData;
import org.apache.hudi.disruptor.DisruptorEventHandler;
import org.apache.hudi.disruptor.DisruptorQueue;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.io.storage.row.LSMHoodieRowDataCreateHandle;
import org.apache.hudi.sink.bucket.disruptor.FlinkDisruptorWriteAntidote;
import org.apache.hudi.sink.bucket.disruptor.FlinkDisruptorWriteData;
import org.apache.hudi.sink.bucket.disruptor.FlinkDisruptorWritePoison;
import org.apache.hudi.sink.bucket.disruptor.FlinkRowDataEventHandler;
import org.apache.hudi.sink.buffer.MemorySegmentPoolFactory;
import org.apache.hudi.sink.buffer.TotalSizeTracer;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.event.WriteResultEvent;
import org.apache.hudi.sink.utils.BufferUtils;
import org.apache.hudi.util.MutableIteratorWrapperIterator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.hudi.util.RowDataProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.FutureUtils.allOf;

/**
 * Used for LSM based bucket stream write function
 * @param <I>
 */
public class LSMBucketStreamWriteFunction<I> extends BucketStreamWriteFunction<I> {

  private static final Logger LOG = LoggerFactory.getLogger(LSMBucketStreamWriteFunction.class);
  private final RowType rowType;
  private final RowType rowTypeWithRecordKeyAndSeqId;
  private final RowDataKeyGen keyGen;
  private final Integer flushConcurrency;
  private final Integer bufferSize;
  private final String waitStrategy;
  private RowDataProjection indexKeyFieldsProjection;
  private final HashMap<String, DataBucket> buckets;
  protected transient MemorySegmentPool memorySegmentPool;
  private TotalSizeTracer tracer;
  private ArrayList<String> hashingFields;
  private int version = 0;
  private DisruptorQueue<DisruptorData<RowData>> queue;
  private String[] sortColumns;
  private int taskPartitionId;
  private static final AtomicLong SEQGEN = new AtomicLong(1);

  /**
   * Constructs a BucketStreamWriteFunction.
   *
   * @param conf The config options
   */
  public LSMBucketStreamWriteFunction(Configuration conf, RowType rowType) {
    super(conf);
    this.rowType = rowType;
    this.rowTypeWithRecordKeyAndSeqId = addRecordKeyAndSeqIdMetaFields(rowType);
    this.keyGen = RowDataKeyGen.instance(conf, rowType);
    this.buckets = new LinkedHashMap<>();
    this.flushConcurrency = conf.get(FlinkOptions.WRITE_BULK_INSERT_FLUSH_CONCURRENCY);
    this.bufferSize = conf.get(FlinkOptions.WRITE_LSM_ASYNC_BUFFER_SIZE);
    this.waitStrategy = conf.get(FlinkOptions.WRITE_LSM_ASYNC_WAITING_STRATEGY);
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    super.open(parameters);
    this.taskPartitionId = getRuntimeContext().getNumberOfParallelSubtasks();
    this.memorySegmentPool = MemorySegmentPoolFactory.createMemorySegmentPool(config);
    this.tracer = new TotalSizeTracer(this.config);
    this.sortColumns = new String[]{HoodieRecord.RECORD_KEY_METADATA_FIELD};
    List<String> fieldNames = rowType.getFieldNames();
    List<LogicalType> fieldTypes = rowType.getChildren();
    this.indexKeyFieldsProjection = getProjection(this.indexKeyFields.split(","), fieldNames, fieldTypes);
    this.hashingFields = new ArrayList<>(this.indexKeyFields.split(",").length);
    initialDisruptorQueue();
  }

  private void initialDisruptorQueue() {
    this.queue = new DisruptorQueue<>(bufferSize, waitStrategy);
    DisruptorEventHandler<DisruptorData<RowData>> handler = new FlinkRowDataEventHandler((value, partitionPath, fileID) -> {
      try {
        bufferRecord(value, partitionPath, fileID);
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });
    queue.addConsumer(handler);
    queue.start();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    super.initializeState(context);
  }

  @Override
  public void processElement(I i, ProcessFunction<I, Object>.Context context, Collector<Object> collector) throws Exception {
    RowData record = (RowData) i;
    final String partition = keyGen.getPartitionPath(record);

    Object[] keyValues = this.indexKeyFieldsProjection.projectAsValues(record);
    for (Object value: keyValues) {
      hashingFields.add(StringUtils.objToString(value));
    }
    // here use bucketID as FileID directly
    final int id = BucketIdentifier.getBucketId(hashingFields, bucketStrategist.getBucketNumber(partition));
    String fileID = String.valueOf(id);
    queue.produce(new FlinkDisruptorWriteData(record, partition, fileID));
    hashingFields.clear();
  }

  @Override
  public void snapshotState() throws IOException {
    poison();
    super.snapshotState();
    queue.produce(new FlinkDisruptorWriteAntidote());
  }

  @Override
  public void endInput() {
    poison();
    super.endInput();
    queue.close();
  }

  private void poison() {
    // produce poison
    queue.produce(new FlinkDisruptorWritePoison());
    queue.waitFor();
  }

  /**
   * Copy from RowDataKeyGen
   * Returns the row data projection for the given field names and table schema.
   */
  private static RowDataProjection getProjection(String[] fields, List<String> schemaFields, List<LogicalType> schemaTypes) {
    int[] positions = getFieldPositions(fields, schemaFields);
    LogicalType[] types = Arrays.stream(positions).mapToObj(schemaTypes::get).toArray(LogicalType[]::new);
    return RowDataProjection.instance(types, positions);
  }

  /**
   * Copy from RowDataKeyGen
   * Returns the field positions of the given fields {@code fields} among all the fields {@code allFields}.
   */
  private static int[] getFieldPositions(String[] fields, List<String> allFields) {
    return Arrays.stream(fields).mapToInt(allFields::indexOf).toArray();
  }

  protected void bufferRecord(RowData value, String partitionPath, String fileID) throws IOException {
    String bucketID = partitionPath + "_" + fileID;
    String recordKey = keyGen.getRecordKey(value);
    RowData internalHoodieRowData = HoodieRowDataCreation.createLSMHoodieInternalRowData(recordKey, String.valueOf(SEQGEN.getAndIncrement()), value);
    boolean success = doBufferRecord(bucketID, internalHoodieRowData, partitionPath, fileID);

    // 1. flushing bucket for memory pool is full.
    if (!success) {
      // flush the biggest data bucket
      DataBucket bucketToFlush = this.buckets.values().stream()
          .max(Comparator.comparingLong(DataBucket::getBufferSize))
          .orElseThrow(NoSuchElementException::new);
      if (flushBucket(bucketToFlush)) {
        this.tracer.countDown(bucketToFlush.getBufferSize());
        disposeBucket(bucketToFlush);
      } else {
        LOG.warn("The buffer size hits the threshold {}, but still flush the max size data bucket failed!", this.tracer.maxBufferSize);
      }

      // flush current data bucket to avoid conflict
      if (!bucketToFlush.getBucketId().equals(bucketID)) {
        DataBucket curBucket = this.buckets.get(bucketID);
        if (flushBucket(curBucket)) {
          this.tracer.countDown(curBucket.getBufferSize());
          disposeBucket(curBucket);
        } else {
          LOG.warn("The buffer size hits the threshold {}, flush the max size data bucket success but flush cur bucket failed!", this.tracer.maxBufferSize);
        }
      }

      // try write row again
      success = doBufferRecord(bucketID, internalHoodieRowData, partitionPath, fileID);
      if (!success) {
        throw new RuntimeException("Buffer is too small to hold a single record.");
      }
    }

    DataBucket bucket = this.buckets.get(bucketID);
    this.tracer.trace(bucket.getLastRecordSize());
    // 2. flushing bucket for bucket is full.
    if (bucket.isFull()) {
      if (flushBucket(bucket)) {
        this.tracer.countDown(bucket.getBufferSize());
        disposeBucket(bucket);
      }
    }
  }

  @Override
  protected void flushRemaining(boolean endInput) {
    this.currentInstant = instantToWrite(hasData());
    if (this.currentInstant == null) {
      // in case there are empty checkpoints that has no input data
      throw new HoodieException("No inflight instant when flushing data!");
    }
    final List<WriteStatus> writeStatus;
    if (!buckets.isEmpty()) {
      writeStatus = new ArrayList<>();
      ThreadPoolExecutor executor = new ThreadPoolExecutor(
          flushConcurrency, flushConcurrency, 0L,
          TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
      allOf(buckets.values().stream().filter(bucket -> !bucket.isEmpty())
          .map(bucket -> CompletableFuture.supplyAsync(() -> {
            WriteStatus innerStatus = writeRecords(currentInstant, bucket);
            bucket.dispose();
            return innerStatus;
          }, executor)).collect(Collectors.toList()))
          .whenComplete((result, throwable) -> {
            writeStatus.addAll(result);
          }).join();

      try {
        executor.shutdown();
        executor.awaitTermination(24, TimeUnit.DAYS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
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

    this.eventGateway.sendEventToCoordinator(new WriteResultEvent(event, currentInstant));
    this.buckets.clear();
    this.tracer.reset();
    this.writeClient.cleanHandles();
    this.writeStatuses.addAll(writeStatus);
    this.version = 0;
    // blocks flushing until the coordinator starts a new instant
    this.confirming = true;
  }

  @Override
  public void close() {
    super.close();
    if (queue != null) {
      queue.closeNow();
    }
  }

  private boolean doBufferRecord(String bucketID, RowData value, String partitionPath, String fileID) throws IOException {
    DataBucket bucket = this.buckets.computeIfAbsent(bucketID,
        k -> new DataBucket(
            this.config.getDouble(FlinkOptions.WRITE_BATCH_SIZE),
            partitionPath,
            bucketID,
            BufferUtils.createBuffer(rowTypeWithRecordKeyAndSeqId,
                memorySegmentPool,
                getRuntimeContext().getUserCodeClassLoader(),
                sortColumns),
            fileID));
    return bucket.writeRow(value);
  }

  private void disposeBucket(DataBucket dataBucket) {
    dataBucket.dispose();
    this.buckets.remove(dataBucket.getBucketId());
  }

  private boolean flushBucket(DataBucket bucket) throws IOException {
    String instant = instantToWrite(true);

    if (instant == null) {
      // in case there are empty checkpoints that has no input data
      LOG.info("No inflight instant when flushing data, skip.");
      return false;
    }

    ValidationUtils.checkState(!bucket.isEmpty(), "Data bucket to flush has no buffering records");
    // TODO zhangyue143 support pre_combine
    //    if (config.getBoolean(FlinkOptions.PRE_COMBINE)) {
    //      records = (List<HoodieRecord>) FlinkWriteHelper.newInstance()
    //          .deduplicateRecords(records, null, -1, this.writeClient.getConfig().getSchema(), this.writeClient.getConfig().getProps(), recordMerger);
    //    }

    final WriteStatus writeStatus = writeRecords(instant, bucket);
    final WriteMetadataEvent event = WriteMetadataEvent.builder()
        .taskID(taskID)
        .instantTime(instant) // the write instant may shift but the event still use the currentInstant.
        .writeStatus(Collections.singletonList(writeStatus))
        .lastBatch(false)
        .endInput(false)
        .build();

    this.eventGateway.sendEventToCoordinator(new WriteResultEvent(event, currentInstant));
    writeStatuses.add(writeStatus);
    return true;
  }

  private WriteStatus writeRecords(String instant, DataBucket bucket) {
    Iterator<BinaryRowData> rowItr =
        new MutableIteratorWrapperIterator<>(
            bucket.getSortedDataIterator(), () -> new BinaryRowData(rowTypeWithRecordKeyAndSeqId.getFieldCount()));

    LSMHoodieRowDataCreateHandle rowCreateHandle = new LSMHoodieRowDataCreateHandle(this.writeClient.getHoodieTable(),
        this.writeClient.getConfig(), bucket.getPartitionPath(), bucket.getFileID(), instant,
        getRuntimeContext().getNumberOfParallelSubtasks(), taskID, getRuntimeContext().getAttemptNumber(),
        rowType, false, 0, version++);
    rowItr.forEachRemaining(record -> {
      try {
        rowCreateHandle.write(record.getString(LSMHoodieInternalRowData.LSM_INTERNAL_RECORD_KEY_META_FIELD_ORD).toString(), bucket.getPartitionPath(), record, true);
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });

    try {
      return rowCreateHandle.close().toWriteStatus();
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  public static RowType addRecordKeyAndSeqIdMetaFields(RowType rowType) {
    List<RowType.RowField> mergedFields = new ArrayList<>();

    LogicalType metadataFieldType = DataTypes.STRING().getLogicalType();
    RowType.RowField recordKeyField =
        new RowType.RowField(HoodieRecord.RECORD_KEY_METADATA_FIELD, metadataFieldType, "record key");
    RowType.RowField commitSeqnoField =
        new RowType.RowField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, metadataFieldType, "commit seqno");

    mergedFields.add(recordKeyField);
    mergedFields.add(commitSeqnoField);
    mergedFields.addAll(rowType.getFields());

    return new RowType(false, mergedFields);
  }
}
