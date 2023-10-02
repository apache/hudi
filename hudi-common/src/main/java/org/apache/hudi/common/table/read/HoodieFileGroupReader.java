/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordReader;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGER_STRATEGY;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * A file group reader that iterates through the records in a single file group.
 * <p>
 * This should be used by the every engine integration, by plugging in a
 * {@link HoodieReaderContext<T>} implementation.
 *
 * @param <T> The type of engine-specific record representation, e.g.,{@code InternalRow}
 *            in Spark and {@code RowData} in Flink.
 */
public final class HoodieFileGroupReader<T> implements Closeable {
  private final HoodieReaderContext<T> readerContext;
  private final Option<HoodieBaseFile> baseFilePath;
  private final Option<List<String>> logFilePathList;
  private final Configuration hadoopConf;
  private final TypedProperties props;
  // Byte offset to start reading from the base file
  private final long start;
  // Length of bytes to read from the base file
  private final long length;
  // Key to record and metadata mapping from log files
  private final Map<String, Pair<Option<T>, Map<String, Object>>> logFileRecordMapping = new HashMap<>();
  private final FileGroupReaderState readerState = new FileGroupReaderState();
  private ClosableIterator<T> baseFileIterator;
  // This is only initialized and used after all records from the base file are iterated
  private Iterator<Pair<Option<T>, Map<String, Object>>> logRecordIterator;
  private HoodieRecordMerger recordMerger;

  T nextRecord;

  public HoodieFileGroupReader(HoodieReaderContext<T> readerContext,
                               HoodieTableMetaClient metaClient,
                               String fileGroupId,
                               TypedProperties props,
                               HoodieTimeline timeline,
                               HoodieTableQueryType queryType,
                               Option<String> instantTime,
                               Option<String> startInstantTime) {
    // This constructor is a placeholder now to allow automatically fetching the correct list of
    // base and log files for a file group.
    // Derive base and log files and call the corresponding constructor.
    this.readerContext = readerContext;
    this.hadoopConf = metaClient.getHadoopConf();
    this.baseFilePath = Option.empty();
    this.logFilePathList = Option.empty();
    this.props = props;
    this.start = 0;
    this.length = Long.MAX_VALUE;
    this.baseFileIterator = new EmptyIterator<>();
  }

  public HoodieFileGroupReader(HoodieReaderContext<T> readerContext,
                               Configuration hadoopConf,
                               String tablePath,
                               String latestCommitTime,
                               Option<HoodieBaseFile> baseFilePath,
                               Option<List<String>> logFilePathList,
                               Schema avroSchema,
                               TypedProperties props,
                               long start,
                               long length) {
    this.readerContext = readerContext;
    this.hadoopConf = hadoopConf;
    this.baseFilePath = baseFilePath;
    this.logFilePathList = logFilePathList;
    this.props = props;
    this.start = start;
    this.length = length;
    this.recordMerger = readerContext.getRecordMerger(
        getStringWithAltKeys(props, RECORD_MERGER_STRATEGY, RECORD_MERGER_STRATEGY.defaultValue()));
    this.readerState.tablePath = tablePath;
    this.readerState.latestCommitTime = latestCommitTime;
    this.readerState.baseFileAvroSchema = avroSchema;
    this.readerState.logRecordAvroSchema = avroSchema;
    this.readerState.mergeProps.putAll(props);
  }

  /**
   * Initialize internal iterators on the base and log files.
   */
  public void initRecordIterators() {
    this.baseFileIterator = baseFilePath.isPresent()
        ? readerContext.getFileRecordIterator(
        baseFilePath.get().getHadoopPath(), 0, baseFilePath.get().getFileLen(),
        readerState.baseFileAvroSchema, readerState.baseFileAvroSchema, hadoopConf)
        : new EmptyIterator<>();
    scanLogFiles();
  }

  /**
   * @return {@code true} if the next record exists; {@code false} otherwise.
   * @throws IOException on reader error.
   */
  public boolean hasNext() throws IOException {
    while (baseFileIterator.hasNext()) {
      T baseRecord = baseFileIterator.next();
      String recordKey = readerContext.getRecordKey(baseRecord, readerState.baseFileAvroSchema);
      Pair<Option<T>, Map<String, Object>> logRecordInfo = logFileRecordMapping.remove(recordKey);
      Option<T> resultRecord = logRecordInfo != null
          ? merge(Option.of(baseRecord), Collections.emptyMap(), logRecordInfo.getLeft(), logRecordInfo.getRight())
          : merge(Option.empty(), Collections.emptyMap(), Option.of(baseRecord), Collections.emptyMap());
      if (resultRecord.isPresent()) {
        nextRecord = readerContext.seal(resultRecord.get());
        return true;
      }
    }

    if (logRecordIterator == null) {
      logRecordIterator = logFileRecordMapping.values().iterator();
    }

    while (logRecordIterator.hasNext()) {
      Pair<Option<T>, Map<String, Object>> nextRecordInfo = logRecordIterator.next();
      Option<T> resultRecord = merge(Option.empty(), Collections.emptyMap(),
          nextRecordInfo.getLeft(), nextRecordInfo.getRight());
      if (resultRecord.isPresent()) {
        nextRecord = readerContext.seal(resultRecord.get());
        return true;
      }
    }

    return false;
  }

  /**
   * @return The next record after calling {@link #hasNext}.
   */
  public T next() {
    T result = nextRecord;
    nextRecord = null;
    return result;
  }

  private void scanLogFiles() {
    if (logFilePathList.isPresent()) {
      FileSystem fs = readerContext.getFs(logFilePathList.get().get(0), hadoopConf);
      HoodieMergedLogRecordReader<T> logRecordReader = HoodieMergedLogRecordReader.newBuilder()
          .withHoodieReaderContext(readerContext)
          .withFileSystem(fs)
          .withBasePath(readerState.tablePath)
          .withLogFilePaths(logFilePathList.get())
          .withLatestInstantTime(readerState.latestCommitTime)
          .withReaderSchema(readerState.logRecordAvroSchema)
          .withReadBlocksLazily(getBooleanWithAltKeys(props, HoodieReaderConfig.COMPACTION_LAZY_BLOCK_READ_ENABLE))
          .withReverseReader(false)
          .withBufferSize(getIntWithAltKeys(props, HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE))
          .withPartition(getRelativePartitionPath(
              new Path(readerState.tablePath), new Path(logFilePathList.get().get(0)).getParent()
          ))
          .withRecordMerger(recordMerger)
          .build();
      logFileRecordMapping.putAll(logRecordReader.getRecords());
      logRecordReader.close();
    }
  }

  private Option<T> merge(Option<T> older, Map<String, Object> olderInfoMap,
                          Option<T> newer, Map<String, Object> newerInfoMap) throws IOException {
    if (!older.isPresent()) {
      return newer;
    }

    Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.merge(
        readerContext.constructHoodieRecord(older, olderInfoMap, readerState.baseFileAvroSchema), readerState.baseFileAvroSchema,
        readerContext.constructHoodieRecord(newer, newerInfoMap, readerState.logRecordAvroSchema), readerState.logRecordAvroSchema, props);
    if (mergedRecord.isPresent()) {
      return Option.ofNullable((T) mergedRecord.get().getLeft().getData());
    }
    return Option.empty();
  }

  @Override
  public void close() throws IOException {
    if (baseFileIterator != null) {
      baseFileIterator.close();
    }
  }
}
