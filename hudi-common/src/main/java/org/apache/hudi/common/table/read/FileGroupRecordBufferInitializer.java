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

import org.jetbrains.annotations.NotNull;

import java.util.List;

import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;

public interface FileGroupRecordBufferInitializer<T> {
  Pair<FileGroupRecordBuffer<T>, List<String>> getRecordBuffer(HoodieReaderContext<T> readerContext,
                                                                   HoodieStorage storage,
                                                                   HoodieFileGroupReader.InputSplit inputSplit,
                                                                   Option<String> orderingFieldName,
                                                                   HoodieTableMetaClient hoodieTableMetaClient,
                                                                   TypedProperties props,
                                                                   HoodieFileGroupReader.ReaderParameters readerParameters,
                                                                   HoodieReadStats readStats,
                                                                   Option<BaseFileUpdateCallback> fileGroupUpdateCallback);

  public static FileGroupRecordBufferInitializer createDefault() {
    return new DefaultFileGroupRecordBufferInitializer();
  }

  class DefaultFileGroupRecordBufferInitializer<T> implements FileGroupRecordBufferInitializer<T> {
    @Override
    public Pair<FileGroupRecordBuffer<T>, List<String>> getRecordBuffer(HoodieReaderContext<T> readerContext,
                                                                            HoodieStorage storage,
                                                                            HoodieFileGroupReader.InputSplit inputSplit,
                                                                        Option<String> orderingFieldName,
                                                                            HoodieTableMetaClient hoodieTableMetaClient,
                                                                            TypedProperties props,
                                                                            HoodieFileGroupReader.ReaderParameters readerParameters,
                                                                            HoodieReadStats readStats,
                                                                            Option<BaseFileUpdateCallback> fileGroupUpdateCallback
    ) {

      boolean isSkipMerge = ConfigUtils.getStringWithAltKeys(props, HoodieReaderConfig.MERGE_TYPE, true).equalsIgnoreCase(HoodieReaderConfig.REALTIME_SKIP_MERGE);
      PartialUpdateMode partialUpdateMode = hoodieTableMetaClient.getTableConfig().getPartialUpdateMode();
      UpdateProcessor<T> updateProcessor = UpdateProcessor.create(readStats, readerContext, readerParameters.isEmitDelete(), fileGroupUpdateCallback);
      FileGroupRecordBuffer<T> recordBuffer;
      if (isSkipMerge) {
        recordBuffer = new UnmergedFileGroupRecordBuffer<>(
            readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, readStats);
      } else if (readerParameters.isSortOutput()) {
        recordBuffer = new SortedKeyBasedFileGroupRecordBuffer<>(
            readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, readStats, orderingFieldName, updateProcessor);
      } else if (readerParameters.isShouldUseRecordPosition() && inputSplit.getBaseFileOption().isPresent()) {
        recordBuffer = new PositionBasedFileGroupRecordBuffer<>(
            readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, inputSplit.getBaseFileOption().get().getCommitTime(), props, readStats, orderingFieldName, updateProcessor);
      } else {
        recordBuffer = new KeyBasedFileGroupRecordBuffer<>(
            readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, readStats, orderingFieldName, updateProcessor);
      }
      return scanLogFiles(readerContext, storage, inputSplit, hoodieTableMetaClient, props, readerParameters, readStats, recordBuffer);
    }
  }

  class ReusableFileGroupRecordBufferInitializer<T> implements FileGroupRecordBufferInitializer<T> {
    private final HoodieReaderContext<T> readerContextWithoutFilters;
    private Pair<ReusableKeyBasedRecordBuffer<T>, List<String>> cachedResults;

    @Override
    public synchronized Pair<FileGroupRecordBuffer<T>, List<String>> getRecordBuffer(HoodieReaderContext<T> readerContext,
                                                                            HoodieStorage storage,
                                                                            HoodieFileGroupReader.InputSplit inputSplit,
                                                                                     Option<String> orderingFieldName,

                                                                                     HoodieTableMetaClient hoodieTableMetaClient,
                                                                            TypedProperties props,
                                                                            HoodieFileGroupReader.ReaderParameters readerParameters,
                                                                            HoodieReadStats readStats,
                                                                            Option<BaseFileUpdateCallback> fileGroupUpdateCallback) {
      if (cachedResults == null) {

        PartialUpdateMode partialUpdateMode = hoodieTableMetaClient.getTableConfig().getPartialUpdateMode();
        ReusableKeyBasedRecordBuffer<T> reusableKeyBasedRecordBuffer = new ReusableKeyBasedRecordBuffer<>(
            readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, readStats, orderingFieldName,
            UpdateProcessor.create(readStats, readerContext, readerParameters.isEmitDelete(), fileGroupUpdateCallback));
        Pair<FileGroupRecordBuffer<T>, List<String>> results = scanLogFiles(readerContextWithoutFilters, storage, inputSplit, hoodieTableMetaClient, props, readerParameters, readStats,
            reusableKeyBasedRecordBuffer);
        cachedResults = Pair.of((ReusableKeyBasedRecordBuffer<T>) results.getLeft(), results.getRight());
      }
      return Pair.of(cachedResults.getLeft().withKeyPredicate(readerContext.getKeyFilterOpt()), cachedResults.getRight());
    }
  }

  static <T> Pair<FileGroupRecordBuffer<T>, List<String>> scanLogFiles(HoodieReaderContext<T> readerContext, HoodieStorage storage,
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
      return Pair.of(recordBuffer, logRecordReader.getValidBlockInstants());
    }
  }
}
