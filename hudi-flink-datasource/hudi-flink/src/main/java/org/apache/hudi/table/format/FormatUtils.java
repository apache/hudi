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

package org.apache.hudi.table.format;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.HoodieUnMergedLogRecordScanner;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueProducer;
import org.apache.hudi.common.util.queue.FunctionBasedQueueProducer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/**
 * Utilities for format.
 */
public class FormatUtils {
  private FormatUtils() {
  }

  /**
   * Sets up the row kind to the row data {@code rowData} from the resolved operation.
   */
  public static void setRowKind(RowData rowData, IndexedRecord record, int index) {
    if (index == -1) {
      return;
    }
    rowData.setRowKind(getRowKind(record, index));
  }

  /**
   * Returns the RowKind of the given record, never null.
   * Returns RowKind.INSERT when the given field value not found.
   */
  private static RowKind getRowKind(IndexedRecord record, int index) {
    Object val = record.get(index);
    if (val == null) {
      return RowKind.INSERT;
    }
    final HoodieOperation operation = HoodieOperation.fromName(val.toString());
    if (HoodieOperation.isInsert(operation)) {
      return RowKind.INSERT;
    } else if (HoodieOperation.isUpdateBefore(operation)) {
      return RowKind.UPDATE_BEFORE;
    } else if (HoodieOperation.isUpdateAfter(operation)) {
      return RowKind.UPDATE_AFTER;
    } else if (HoodieOperation.isDelete(operation)) {
      return RowKind.DELETE;
    } else {
      throw new AssertionError();
    }
  }

  /**
   * Returns the RowKind of the given record, never null.
   * Returns RowKind.INSERT when the given field value not found.
   */
  public static RowKind getRowKindSafely(IndexedRecord record, int index) {
    if (index == -1) {
      return RowKind.INSERT;
    }
    return getRowKind(record, index);
  }

  public static GenericRecord buildAvroRecordBySchema(
      IndexedRecord record,
      Schema requiredSchema,
      int[] requiredPos,
      GenericRecordBuilder recordBuilder) {
    List<Schema.Field> requiredFields = requiredSchema.getFields();
    assert (requiredFields.size() == requiredPos.length);
    Iterator<Integer> positionIterator = Arrays.stream(requiredPos).iterator();
    requiredFields.forEach(f -> recordBuilder.set(f, getVal(record, positionIterator.next())));
    return recordBuilder.build();
  }

  private static Object getVal(IndexedRecord record, int pos) {
    return pos == -1 ? null : record.get(pos);
  }

  public static HoodieMergedLogRecordScanner logScanner(
      MergeOnReadInputSplit split,
      Schema logSchema,
      HoodieWriteConfig writeConfig,
      Configuration hadoopConf) {
    FileSystem fs = FSUtils.getFs(split.getTablePath(), hadoopConf);
    final ExternalSpillableMap.DiskMapType diskMapType = writeConfig.getCommonConfig().getSpillableDiskMapType();
    return HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(fs)
        .withBasePath(split.getTablePath())
        .withLogFilePaths(split.getLogPaths().get())
        .withReaderSchema(logSchema)
        .withLatestInstantTime(split.getLatestCommit())
        .withReadBlocksLazily(writeConfig.getCompactionLazyBlockReadEnabled())
        .withReverseReader(false)
        .withBufferSize(writeConfig.getMaxDFSStreamBufferSize())
        .withMaxMemorySizeInBytes(diskMapType == ExternalSpillableMap.DiskMapType.ROCKS_DB ? 0 : split.getMaxCompactionMemoryInBytes())
        .withDiskMapType(diskMapType)
        .withSpillableMapBasePath(writeConfig.getSpillableMapBasePath())
        .withInstantRange(split.getInstantRange())
        .withOperationField(writeConfig.getProps().getBoolean(FlinkOptions.CHANGELOG_ENABLED.key(),
            FlinkOptions.CHANGELOG_ENABLED.defaultValue()))
        .build();
  }

  private static HoodieUnMergedLogRecordScanner unMergedLogScanner(
      MergeOnReadInputSplit split,
      Schema logSchema,
      org.apache.flink.configuration.Configuration flinkConf,
      Configuration hadoopConf,
      HoodieUnMergedLogRecordScanner.LogRecordScannerCallback callback) {
    FileSystem fs = FSUtils.getFs(split.getTablePath(), hadoopConf);
    return HoodieUnMergedLogRecordScanner.newBuilder()
        .withFileSystem(fs)
        .withBasePath(split.getTablePath())
        .withLogFilePaths(split.getLogPaths().get())
        .withReaderSchema(logSchema)
        .withLatestInstantTime(split.getLatestCommit())
        .withReadBlocksLazily(
            string2Boolean(
                flinkConf.getString(HoodieRealtimeConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP,
                    HoodieRealtimeConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED)))
        .withReverseReader(false)
        .withBufferSize(
            flinkConf.getInteger(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP,
                HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE))
        .withInstantRange(split.getInstantRange())
        .withLogRecordScannerCallback(callback)
        .build();
  }

  /**
   * Utility to read and buffer the records in the unMerged log record scanner.
   */
  public static class BoundedMemoryRecords {
    // Log Record unmerged scanner
    private final HoodieUnMergedLogRecordScanner scanner;

    // Executor that runs the above producers in parallel
    private final BoundedInMemoryExecutor<HoodieRecord<?>, HoodieRecord<?>, ?> executor;

    // Iterator for the buffer consumer
    private final Iterator<HoodieRecord<?>> iterator;

    public BoundedMemoryRecords(
        MergeOnReadInputSplit split,
        Schema logSchema,
        Configuration hadoopConf,
        org.apache.flink.configuration.Configuration flinkConf) {
      this.executor = new BoundedInMemoryExecutor<>(
          StreamerUtil.getMaxCompactionMemoryInBytes(flinkConf),
          getParallelProducers(),
          Option.empty(),
          Function.identity(),
          new DefaultSizeEstimator<>(),
          Functions.noop());
      // Consumer of this record reader
      this.iterator = this.executor.getQueue().iterator();
      this.scanner = FormatUtils.unMergedLogScanner(split, logSchema, flinkConf, hadoopConf,
          record -> executor.getQueue().insertRecord(record));
      // Start reading and buffering
      this.executor.startProducers();
    }

    public Iterator<HoodieRecord<?>> getRecordsIterator() {
      return this.iterator;
    }

    /**
     * Setup log and parquet reading in parallel. Both write to central buffer.
     */
    private List<BoundedInMemoryQueueProducer<HoodieRecord<?>>> getParallelProducers() {
      List<BoundedInMemoryQueueProducer<HoodieRecord<?>>> producers = new ArrayList<>();
      producers.add(new FunctionBasedQueueProducer<>(buffer -> {
        scanner.scan();
        return null;
      }));
      return producers;
    }

    public void close() {
      this.executor.shutdownNow();
    }
  }

  public static HoodieMergedLogRecordScanner logScanner(
      List<String> logPaths,
      Schema logSchema,
      String latestInstantTime,
      HoodieWriteConfig writeConfig,
      Configuration hadoopConf) {
    String basePath = writeConfig.getBasePath();
    final ExternalSpillableMap.DiskMapType diskMapType = writeConfig.getCommonConfig().getSpillableDiskMapType();
    return HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(FSUtils.getFs(basePath, hadoopConf))
        .withBasePath(basePath)
        .withLogFilePaths(logPaths)
        .withReaderSchema(logSchema)
        .withLatestInstantTime(latestInstantTime)
        .withReadBlocksLazily(writeConfig.getCompactionLazyBlockReadEnabled())
        .withReverseReader(false)
        .withBufferSize(writeConfig.getMaxDFSStreamBufferSize())
        .withMaxMemorySizeInBytes(diskMapType == ExternalSpillableMap.DiskMapType.ROCKS_DB ? 0 : writeConfig.getMaxMemoryPerPartitionMerge())
        .withSpillableMapBasePath(writeConfig.getSpillableMapBasePath())
        .withDiskMapType(diskMapType)
        .withBitCaskDiskMapCompressionEnabled(writeConfig.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
        .build();
  }

  private static Boolean string2Boolean(String s) {
    return "true".equals(s.toLowerCase(Locale.ROOT));
  }
}
