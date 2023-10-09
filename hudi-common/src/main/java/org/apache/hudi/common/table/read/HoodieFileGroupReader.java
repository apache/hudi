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
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordReader;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

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
  // Core structure to store and process records.
  private final HoodieFileGroupRecordBuffer<T> recordBuffer;
  private final HoodieFileGroupReaderState readerState = new HoodieFileGroupReaderState();
  private ClosableIterator<T> baseFileIterator;
  private HoodieRecordMerger recordMerger;

  public HoodieFileGroupReader(HoodieReaderContext<T> readerContext,
                               HoodieTableMetaClient metaClient,
                               String fileGroupId,
                               TypedProperties props,
                               HoodieTimeline timeline,
                               HoodieTableQueryType queryType,
                               Option<String> instantTime,
                               Option<String> startInstantTime,
                               HoodieFileGroupRecordBuffer<T> recordBuffer) {
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
    this.recordBuffer = recordBuffer;
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
                               long length,
                               HoodieFileGroupRecordBuffer<T> recordBuffer) {
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
    this.recordBuffer = recordBuffer;
  }

  /**
   * Initialize internal iterators on the base and log files.
   */
  public void initRecordIterators() {
    this.baseFileIterator = baseFilePath.isPresent()
        ? readerContext.getFileRecordIterator(
            baseFilePath.get().getHadoopPath(), start, length, readerState.baseFileAvroSchema, readerState.baseFileAvroSchema, hadoopConf)
        : new EmptyIterator<>();
    scanLogFiles();
    recordBuffer.setBaseFileIteraotr(baseFileIterator);
  }

  /**
   * @return {@code true} if the next record exists; {@code false} otherwise.
   * @throws IOException on reader error.
   */
  public boolean hasNext() throws IOException {
    return recordBuffer.hasNext();
  }

  /**
   * @return The next record after calling {@link #hasNext}.
   */
  public T next() {
    return recordBuffer.next();
  }

  private void scanLogFiles() {
    if (logFilePathList.isPresent()) {
      String path = baseFilePath.isPresent() ? baseFilePath.get().getPath() : logFilePathList.get().get(0);
      FileSystem fs = readerContext.getFs(path, hadoopConf);

      HoodieMergedLogRecordReader logRecordReader = HoodieMergedLogRecordReader.newBuilder()
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
              new Path(readerState.tablePath), new Path(logFilePathList.get().get(0)).getParent()))
          .withRecordMerger(recordMerger)
          .withRecordBuffer(recordBuffer)
          .build();
      logRecordReader.close();
    }
  }

  @Override
  public void close() throws IOException {
    if (baseFileIterator != null) {
      baseFileIterator.close();
    }
    if (recordBuffer != null) {
      recordBuffer.close();
    }
  }

  public HoodieFileGroupReaderIterator<T> getClosableIterator() {
    return new HoodieFileGroupReaderIterator<>(this);
  }

  public static class HoodieFileGroupReaderIterator<T> implements ClosableIterator<T> {
    private final HoodieFileGroupReader<T> reader;

    public HoodieFileGroupReaderIterator(HoodieFileGroupReader<T> reader) {
      this.reader = reader;
    }

    @Override
    public boolean hasNext() {
      try {
        return reader.hasNext();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to read record", e);
      }
    }

    @Override
    public T next() {
      return reader.next();
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to close the reader", e);
      }
    }
  }
}
