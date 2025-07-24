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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordReader;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;

import java.io.Closeable;
import java.util.List;

import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;

/**
 * This interface defines the contract for initializing a {@link FileGroupRecordBuffer} for a given file group.
 * The implementation is expected to establish the record buffer and populate it with the records from the log files.
 * @param <T> the engine specific record type
 */
public interface FileGroupRecordBufferInitializer<T> {

  Pair<FileGroupRecordBuffer<T>, List<String>> getRecordBuffer(HoodieReaderContext<T> readerContext,
                                                               HoodieStorage storage,
                                                               HoodieFileGroupReader.InputSplit inputSplit,
                                                               Option<String> orderingFieldName,
                                                               HoodieTableMetaClient hoodieTableMetaClient,
                                                               TypedProperties props,
                                                               HoodieFileGroupReader.ReaderParameters readerParameters,
                                                               HoodieReadStats readStats,
                                                               Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback);

  static <T> FileGroupRecordBufferInitializer<T> createDefault() {
    return (FileGroupRecordBufferInitializer<T>) DefaultFileGroupRecordBufferInitializer.INSTANCE;
  }

  static <T> ReusableFileGroupRecordBufferInitializer<T> createReusable(HoodieReaderContext<T> readerContextWithoutFilters) {
    return new ReusableFileGroupRecordBufferInitializer<>(readerContextWithoutFilters);
  }

  /**
   * Default implementation of {@link FileGroupRecordBufferInitializer} that initializes a buffer based on the reader parameters.
   * @param <T> the engine specific record type
   */
  class DefaultFileGroupRecordBufferInitializer<T> implements FileGroupRecordBufferInitializer<T> {
    private static final DefaultFileGroupRecordBufferInitializer INSTANCE = new DefaultFileGroupRecordBufferInitializer<>();

    @Override
    public Pair<FileGroupRecordBuffer<T>, List<String>> getRecordBuffer(HoodieReaderContext<T> readerContext,
                                                                        HoodieStorage storage,
                                                                        HoodieFileGroupReader.InputSplit inputSplit,
                                                                        Option<String> orderingFieldName,
                                                                        HoodieTableMetaClient hoodieTableMetaClient,
                                                                        TypedProperties props,
                                                                        HoodieFileGroupReader.ReaderParameters readerParameters,
                                                                        HoodieReadStats readStats,
                                                                        Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback) {

      boolean isSkipMerge = ConfigUtils.getStringWithAltKeys(props, HoodieReaderConfig.MERGE_TYPE, true).equalsIgnoreCase(HoodieReaderConfig.REALTIME_SKIP_MERGE);
      PartialUpdateMode partialUpdateMode = hoodieTableMetaClient.getTableConfig().getPartialUpdateMode();
      UpdateProcessor<T> updateProcessor = UpdateProcessor.create(readStats, readerContext, readerParameters.isEmitDelete(), fileGroupUpdateCallback);
      FileGroupRecordBuffer<T> recordBuffer;
      if (isSkipMerge) {
        recordBuffer = new UnmergedFileGroupRecordBuffer<>(
            readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, readStats);
      } else if (readerParameters.isSortOutput()) {
        recordBuffer = new SortedKeyBasedFileGroupRecordBuffer<>(
            readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, orderingFieldName, updateProcessor);
      } else if (readerParameters.isShouldUseRecordPosition() && inputSplit.getBaseFileOption().isPresent()) {
        recordBuffer = new PositionBasedFileGroupRecordBuffer<>(
            readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, inputSplit.getBaseFileOption().get().getCommitTime(), props,
            orderingFieldName, updateProcessor);
      } else {
        recordBuffer = new KeyBasedFileGroupRecordBuffer<>(
            readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, orderingFieldName, updateProcessor);
      }
      return Pair.of(recordBuffer, scanLogFiles(readerContext, storage, inputSplit, hoodieTableMetaClient, props, readerParameters, readStats, recordBuffer));
    }
  }

  /**
   * A special case for when the record buffer needs to be reused across multiple reads of the same file group.
   * @param <T> the engine specific record type
   */
  class ReusableFileGroupRecordBufferInitializer<T> implements FileGroupRecordBufferInitializer<T>, Closeable {
    private final HoodieReaderContext<T> readerContextWithoutFilters;
    private Pair<KeyBasedFileGroupRecordBuffer<T>, List<String>> cachedResults;

    public ReusableFileGroupRecordBufferInitializer(HoodieReaderContext<T> readerContextWithoutFilters) {
      this.readerContextWithoutFilters = readerContextWithoutFilters;
    }

    @Override
    public synchronized Pair<FileGroupRecordBuffer<T>, List<String>> getRecordBuffer(HoodieReaderContext<T> readerContext,
                                                                                     HoodieStorage storage,
                                                                                     HoodieFileGroupReader.InputSplit inputSplit,
                                                                                     Option<String> orderingFieldName,
                                                                                     HoodieTableMetaClient hoodieTableMetaClient,
                                                                                     TypedProperties props,
                                                                                     HoodieFileGroupReader.ReaderParameters readerParameters,
                                                                                     HoodieReadStats readStats,
                                                                                     Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback) {
      UpdateProcessor<T> updateProcessor = UpdateProcessor.create(readStats, readerContext, readerParameters.isEmitDelete(), fileGroupUpdateCallback);
      PartialUpdateMode partialUpdateMode = hoodieTableMetaClient.getTableConfig().getPartialUpdateMode();
      if (cachedResults == null) {
        // Create an initial buffer to process the log files
        KeyBasedFileGroupRecordBuffer<T> initialBuffer = new KeyBasedFileGroupRecordBuffer<>(
            readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, orderingFieldName, updateProcessor);
        List<String> validInstants = scanLogFiles(readerContextWithoutFilters, storage, inputSplit, hoodieTableMetaClient, props, readerParameters, readStats, initialBuffer);
        cachedResults = Pair.of(initialBuffer, validInstants);
      }
      // Create a reusable buffer with the results from the initial scan
      ReusableKeyBasedRecordBuffer<T> reusableBuffer = new ReusableKeyBasedRecordBuffer<>(
          readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, orderingFieldName, updateProcessor, cachedResults.getLeft().getLogRecords());
      return Pair.of(reusableBuffer, cachedResults.getRight());
    }

    @Override
    public void close() {
      if (cachedResults != null && cachedResults.getLeft() != null) {
        cachedResults.getLeft().close();
        cachedResults = null;
      }
    }
  }

  static <T> List<String> scanLogFiles(HoodieReaderContext<T> readerContext, HoodieStorage storage,
                                       HoodieFileGroupReader.InputSplit inputSplit, HoodieTableMetaClient hoodieTableMetaClient,
                                       TypedProperties props, HoodieFileGroupReader.ReaderParameters readerParameters,
                                       HoodieReadStats readStats, FileGroupRecordBuffer<T> recordBuffer) {
    try (HoodieMergedLogRecordReader<T> logRecordReader = HoodieMergedLogRecordReader.<T>newBuilder()
        .withHoodieReaderContext(readerContext)
        .withStorage(storage)
        .withLogFiles(inputSplit.getLogFiles())
        .withReverseReader(false)
        .withBufferSize(getIntWithAltKeys(props, HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE))
        .withInstantRange(readerContext.getInstantRange())
        .withPartition(inputSplit.getPartitionPath())
        .withRecordBuffer(recordBuffer)
        .withAllowInflightInstants(readerParameters.isAllowInflightInstants())
        .withMetaClient(hoodieTableMetaClient)
        .withOptimizedLogBlocksScan(readerParameters.isEnableOptimizedLogBlockScan())
        .build()) {
      readStats.setTotalLogReadTimeMs(logRecordReader.getTotalTimeTakenToReadAndMergeBlocks());
      readStats.setTotalUpdatedRecordsCompacted(logRecordReader.getNumMergedRecordsInLog());
      readStats.setTotalLogFilesCompacted(logRecordReader.getTotalLogFiles());
      readStats.setTotalLogRecords(logRecordReader.getTotalLogRecords());
      readStats.setTotalLogBlocks(logRecordReader.getTotalLogBlocks());
      readStats.setTotalCorruptLogBlock(logRecordReader.getTotalCorruptBlocks());
      readStats.setTotalRollbackBlocks(logRecordReader.getTotalRollbacks());
      return logRecordReader.getValidBlockInstants();
    }
  }
}
