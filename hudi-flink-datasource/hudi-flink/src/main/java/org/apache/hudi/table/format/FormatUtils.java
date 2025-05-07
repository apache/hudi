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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.HoodieUnMergedLogRecordScanner;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.FunctionBasedQueueProducer;
import org.apache.hudi.common.util.queue.HoodieProducer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;

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

  public static ExternalSpillableMap<String, byte[]> spillableMap(
      HoodieWriteConfig writeConfig,
      long maxCompactionMemoryInBytes,
      String loggingContext) {
    try {
      return new ExternalSpillableMap<>(
          maxCompactionMemoryInBytes,
          writeConfig.getSpillableMapBasePath(),
          new DefaultSizeEstimator<>(),
          new DefaultSizeEstimator<>(),
          writeConfig.getCommonConfig().getSpillableDiskMapType(),
          new DefaultSerializer<>(),
          writeConfig.getCommonConfig().isBitCaskDiskMapCompressionEnabled(),
          loggingContext);
    } catch (IOException e) {
      throw new HoodieIOException(
          "IOException when creating ExternalSpillableMap at " + writeConfig.getSpillableMapBasePath(), e);
    }
  }

  public static HoodieMergedLogRecordScanner logScanner(
      MergeOnReadInputSplit split,
      Schema logSchema,
      InternalSchema internalSchema,
      org.apache.flink.configuration.Configuration flinkConf,
      Configuration hadoopConf) {
    HoodieWriteConfig writeConfig = FlinkWriteClients.getHoodieClientConfig(flinkConf, false, false, true);
    HoodieStorage storage = HoodieStorageUtils.getStorage(
        split.getTablePath(), HadoopFSUtils.getStorageConf(hadoopConf));
    return HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(split.getTablePath())
        .withLogFilePaths(split.getLogPaths().get())
        .withReaderSchema(logSchema)
        .withInternalSchema(internalSchema)
        .withLatestInstantTime(split.getLatestCommit())
        .withReverseReader(false)
        .withBufferSize(writeConfig.getMaxDFSStreamBufferSize())
        .withMaxMemorySizeInBytes(split.getMaxCompactionMemoryInBytes())
        .withDiskMapType(writeConfig.getCommonConfig().getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(writeConfig.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
        .withSpillableMapBasePath(writeConfig.getSpillableMapBasePath())
        .withInstantRange(split.getInstantRange())
        .withOperationField(flinkConf.getBoolean(FlinkOptions.CHANGELOG_ENABLED))
        .withRecordMerger(writeConfig.getRecordMerger())
        .build();
  }

  /**
   * Utility to read and buffer the records in the unMerged log record scanner.
   */
  public static class BoundedMemoryRecords {
    // Executor that runs the above producers in parallel
    private final BoundedInMemoryExecutor<HoodieRecord<?>, HoodieRecord<?>, ?> executor;

    // Iterator for the buffer consumer
    private final Iterator<HoodieRecord<?>> iterator;

    public BoundedMemoryRecords(
        MergeOnReadInputSplit split,
        Schema logSchema,
        InternalSchema internalSchema,
        Configuration hadoopConf,
        org.apache.flink.configuration.Configuration flinkConf) {
      List<String> mergers = Arrays.stream(flinkConf.getString(FlinkOptions.RECORD_MERGER_IMPLS).split(","))
          .map(String::trim)
          .distinct()
          .collect(Collectors.toList());
      HoodieRecordMerger merger = HoodieRecordUtils.createRecordMerger(
          split.getTablePath(), EngineType.FLINK, mergers, flinkConf.getString(FlinkOptions.RECORD_MERGER_STRATEGY_ID));
      HoodieUnMergedLogRecordScanner.Builder scannerBuilder =
          HoodieUnMergedLogRecordScanner.newBuilder()
              .withStorage(HoodieStorageUtils.getStorage(
                  split.getTablePath(), HadoopFSUtils.getStorageConf(hadoopConf)))
          .withBasePath(split.getTablePath())
          .withLogFilePaths(split.getLogPaths().get())
          .withReaderSchema(logSchema)
          .withInternalSchema(internalSchema)
          .withLatestInstantTime(split.getLatestCommit())
          .withReverseReader(false)
          .withBufferSize(
              flinkConf.getInteger(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.key(),
                  HoodieMemoryConfig.DEFAULT_MR_MAX_DFS_STREAM_BUFFER_SIZE))
          .withInstantRange(split.getInstantRange())
          .withRecordMerger(merger);

      this.executor = new BoundedInMemoryExecutor<>(
          StreamerUtil.getMaxCompactionMemoryInBytes(flinkConf),
          getParallelProducers(scannerBuilder),
          Option.empty(),
          Function.identity(),
          new DefaultSizeEstimator<>(),
          Functions.noop());
      this.iterator = this.executor.getRecordIterator();

      // Start reading and buffering
      this.executor.startProducingAsync();
    }

    public Iterator<HoodieRecord<?>> getRecordsIterator() {
      return this.iterator;
    }

    /**
     * Setup log and parquet reading in parallel. Both write to central buffer.
     */
    private List<HoodieProducer<HoodieRecord<?>>> getParallelProducers(
        HoodieUnMergedLogRecordScanner.Builder scannerBuilder
    ) {
      List<HoodieProducer<HoodieRecord<?>>> producers = new ArrayList<>();
      producers.add(new FunctionBasedQueueProducer<>(queue -> {
        HoodieUnMergedLogRecordScanner scanner =
            scannerBuilder.withLogRecordScannerCallback(queue::insertRecord).build();
        // Scan all the delta-log files, filling in the queue
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
    return HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(HoodieStorageUtils.getStorage(
            basePath, HadoopFSUtils.getStorageConf(hadoopConf)))
        .withBasePath(basePath)
        .withLogFilePaths(logPaths)
        .withReaderSchema(logSchema)
        .withLatestInstantTime(latestInstantTime)
        .withReverseReader(false)
        .withBufferSize(writeConfig.getMaxDFSStreamBufferSize())
        .withMaxMemorySizeInBytes(writeConfig.getMaxMemoryPerPartitionMerge())
        .withSpillableMapBasePath(writeConfig.getSpillableMapBasePath())
        .withDiskMapType(writeConfig.getCommonConfig().getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(writeConfig.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
        .withRecordMerger(writeConfig.getRecordMerger())
        .build();
  }

  /**
   * Gets the raw value for a {@link ConfigProperty} config from Flink configuration. The key and
   * alternative keys are used to fetch the config.
   *
   * @param flinkConf      Configs in Flink {@link org.apache.flink.configuration.Configuration}.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return {@link Option} of value if the config exists; empty {@link Option} otherwise.
   */
  public static Option<String> getRawValueWithAltKeys(org.apache.flink.configuration.Configuration flinkConf,
                                                      ConfigProperty<?> configProperty) {
    if (flinkConf.containsKey(configProperty.key())) {
      return Option.ofNullable(flinkConf.getString(configProperty.key(), ""));
    }
    for (String alternative : configProperty.getAlternatives()) {
      if (flinkConf.containsKey(alternative)) {
        return Option.ofNullable(flinkConf.getString(alternative, ""));
      }
    }
    return Option.empty();
  }

  /**
   * Gets the boolean value for a {@link ConfigProperty} config from Flink configuration. The key and
   * alternative keys are used to fetch the config. The default value of {@link ConfigProperty}
   * config, if exists, is returned if the config is not found in the configuration.
   *
   * @param conf           Configs in Flink {@link Configuration}.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return boolean value if the config exists; default boolean value if the config does not exist
   * and there is default value defined in the {@link ConfigProperty} config; {@code false} otherwise.
   */
  public static boolean getBooleanWithAltKeys(org.apache.flink.configuration.Configuration conf,
                                              ConfigProperty<?> configProperty) {
    Option<String> rawValue = getRawValueWithAltKeys(conf, configProperty);
    boolean defaultValue = configProperty.hasDefaultValue() && Boolean.parseBoolean(configProperty.defaultValue().toString());
    return rawValue.map(Boolean::parseBoolean).orElse(defaultValue);
  }

  private static Boolean string2Boolean(String s) {
    return "true".equals(s.toLowerCase(Locale.ROOT));
  }
}
