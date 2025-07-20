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
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.PartitionPathParser;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordReader;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.Schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;

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
  private final HoodieTableMetaClient metaClient;
  private final InputSplit inputSplit;
  private final Option<String[]> partitionPathFields;
  private final Option<String> orderingFieldName;
  private final HoodieStorage storage;
  private final TypedProperties props;
  // Core structure to store and process records.
  private final FileGroupRecordBuffer<T> recordBuffer;
  private ClosableIterator<T> baseFileIterator;
  private final Option<UnaryOperator<T>> outputConverter;
  private final HoodieReadStats readStats;
  // Allows to consider inflight instants while merging log records using HoodieMergedLogRecordReader
  // The inflight instants need to be considered while updating RLI records. RLI needs to fetch the revived
  // and deleted keys from the log files written as part of active data commit. During the RLI update,
  // the allowInflightInstants flag would need to be set to true. This would ensure the HoodieMergedLogRecordReader
  // considers the log records which are inflight.
  private final boolean allowInflightInstants;
  // Callback to run custom logic on updates to the base files for the file group
  private final Option<BaseFileUpdateCallback> fileGroupUpdateCallback;
  private final boolean enableOptimizedLogBlockScan;
  // The list of instant times read from the log blocks, this value is used by the log-compaction to allow optimized log-block scans
  private List<String> validBlockInstants = Collections.emptyList();

  /**
   * Constructs an instance of the HoodieFileGroupReader.
   * @deprecated use {@link #newBuilder()} instead.
   */
  @Deprecated
  public HoodieFileGroupReader(HoodieReaderContext<T> readerContext, HoodieStorage storage,
      String tablePath,
      String latestCommitTime, FileSlice fileSlice, Schema dataSchema, Schema requestedSchema,
      Option<InternalSchema> internalSchemaOpt, HoodieTableMetaClient hoodieTableMetaClient,
      TypedProperties props,
      long start, long length, boolean shouldUseRecordPosition) {
    this(readerContext, storage, tablePath, latestCommitTime, dataSchema, requestedSchema, internalSchemaOpt,
        hoodieTableMetaClient, props, shouldUseRecordPosition, false, false, false,
        InputSplit.fromFileSlice(fileSlice, start, length), Option.empty(), false);
  }

  private HoodieFileGroupReader(HoodieReaderContext<T> readerContext, HoodieStorage storage, String tablePath,
                                String latestCommitTime, Schema dataSchema, Schema requestedSchema,
                                Option<InternalSchema> internalSchemaOpt, HoodieTableMetaClient hoodieTableMetaClient, TypedProperties props,
                                boolean shouldUseRecordPosition, boolean allowInflightInstants, boolean emitDelete, boolean sortOutput,
                                InputSplit inputSplit, Option<BaseFileUpdateCallback> updateCallback, boolean enableOptimizedLogBlockScan) {
    this.readerContext = readerContext;
    this.fileGroupUpdateCallback = updateCallback;
    this.metaClient = hoodieTableMetaClient;
    this.storage = storage;
    this.enableOptimizedLogBlockScan = enableOptimizedLogBlockScan;
    this.inputSplit = inputSplit;
    readerContext.setHasLogFiles(!this.inputSplit.logFiles.isEmpty());
    readerContext.setPartitionPath(inputSplit.partitionPath);
    if (readerContext.getHasLogFiles() && inputSplit.start != 0) {
      throw new IllegalArgumentException("Filegroup reader is doing log file merge but not reading from the start of the base file");
    }
    this.props = props;
    HoodieTableConfig tableConfig = hoodieTableMetaClient.getTableConfig();
    this.partitionPathFields = tableConfig.getPartitionFields();
    readerContext.initRecordMerger(props);
    readerContext.setTablePath(tablePath);
    readerContext.setLatestCommitTime(latestCommitTime);
    boolean isSkipMerge = ConfigUtils.getStringWithAltKeys(props, HoodieReaderConfig.MERGE_TYPE, true).equalsIgnoreCase(HoodieReaderConfig.REALTIME_SKIP_MERGE);
    readerContext.setShouldMergeUseRecordPosition(shouldUseRecordPosition && !isSkipMerge && readerContext.getHasLogFiles());
    readerContext.setHasBootstrapBaseFile(inputSplit.baseFileOption.flatMap(HoodieBaseFile::getBootstrapBaseFile).isPresent());
    readerContext.setSchemaHandler(readerContext.supportsParquetRowIndex()
        ? new ParquetRowIndexBasedSchemaHandler<>(readerContext, dataSchema, requestedSchema, internalSchemaOpt, tableConfig, props)
        : new FileGroupReaderSchemaHandler<>(readerContext, dataSchema, requestedSchema, internalSchemaOpt, tableConfig, props));
    this.outputConverter = readerContext.getSchemaHandler().getOutputConverter();
    this.orderingFieldName = readerContext.getMergeMode() == RecordMergeMode.COMMIT_TIME_ORDERING
        ? Option.empty()
        : Option.ofNullable(ConfigUtils.getOrderingField(props))
        .or(() -> {
          String preCombineField = hoodieTableMetaClient.getTableConfig().getPreCombineField();
          if (StringUtils.isNullOrEmpty(preCombineField)) {
            return Option.empty();
          }
          return Option.of(preCombineField);
        });
    this.readStats = new HoodieReadStats();
    this.recordBuffer = getRecordBuffer(readerContext, hoodieTableMetaClient,
        readerContext.getMergeMode(), tableConfig.getPartialUpdateMode(), props,
        isSkipMerge, shouldUseRecordPosition, readStats, emitDelete, sortOutput);
    this.allowInflightInstants = allowInflightInstants;
  }

  /**
   * Initialize correct record buffer
   */
  private FileGroupRecordBuffer<T> getRecordBuffer(HoodieReaderContext<T> readerContext,
                                                   HoodieTableMetaClient hoodieTableMetaClient,
                                                   RecordMergeMode recordMergeMode,
                                                   PartialUpdateMode partialUpdateMode,
                                                   TypedProperties props,
                                                   boolean isSkipMerge,
                                                   boolean shouldUseRecordPosition,
                                                   HoodieReadStats readStats,
                                                   boolean emitDelete,
                                                   boolean sortOutput) {
    if (inputSplit.logFiles.isEmpty()) {
      return null;
    }
    UpdateProcessor<T> updateProcessor = UpdateProcessor.create(readStats, readerContext, emitDelete, fileGroupUpdateCallback);
    if (isSkipMerge) {
      return new UnmergedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateMode, props, readStats);
    } else if (sortOutput) {
      return new SortedKeyBasedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateMode, props, readStats, orderingFieldName, updateProcessor);
    } else if (shouldUseRecordPosition && inputSplit.baseFileOption.isPresent()) {
      return new PositionBasedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateMode, inputSplit.baseFileOption.get().getCommitTime(), props, readStats, orderingFieldName, updateProcessor);
    } else {
      return new KeyBasedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateMode, props, readStats, orderingFieldName, updateProcessor);
    }
  }

  /**
   * Initialize internal iterators on the base and log files.
   */
  private void initRecordIterators() throws IOException {
    ClosableIterator<T> iter = makeBaseFileIterator();
    if (inputSplit.logFiles.isEmpty()) {
      this.baseFileIterator = new CloseableMappingIterator<>(iter, readerContext::seal);
    } else {
      this.baseFileIterator = iter;
      scanLogFiles();
      recordBuffer.setBaseFileIterator(baseFileIterator);
    }
  }

  private ClosableIterator<T> makeBaseFileIterator() throws IOException {
    if (!inputSplit.baseFileOption.isPresent()) {
      return new EmptyIterator<>();
    }

    HoodieBaseFile baseFile = inputSplit.baseFileOption.get();
    if (baseFile.getBootstrapBaseFile().isPresent()) {
      return makeBootstrapBaseFileIterator(baseFile);
    }

    StoragePathInfo baseFileStoragePathInfo = baseFile.getPathInfo();
    final ClosableIterator<T> recordIterator;
    if (baseFileStoragePathInfo != null) {
      recordIterator = readerContext.getFileRecordIterator(
          baseFileStoragePathInfo, inputSplit.start, inputSplit.length,
          readerContext.getSchemaHandler().getTableSchema(),
          readerContext.getSchemaHandler().getRequiredSchema(), storage);
    } else {
      recordIterator = readerContext.getFileRecordIterator(
          baseFile.getStoragePath(), inputSplit.start, inputSplit.length,
          readerContext.getSchemaHandler().getTableSchema(),
          readerContext.getSchemaHandler().getRequiredSchema(), storage);
    }
    return readerContext.getInstantRange().isPresent()
        ? readerContext.applyInstantRangeFilter(recordIterator)
        : recordIterator;
  }

  private ClosableIterator<T> makeBootstrapBaseFileIterator(HoodieBaseFile baseFile) throws IOException {
    BaseFile dataFile = baseFile.getBootstrapBaseFile().get();
    Pair<List<Schema.Field>, List<Schema.Field>> requiredFields = readerContext.getSchemaHandler().getBootstrapRequiredFields();
    Pair<List<Schema.Field>, List<Schema.Field>> allFields = readerContext.getSchemaHandler().getBootstrapDataFields();
    Option<Pair<ClosableIterator<T>, Schema>> dataFileIterator =
        makeBootstrapBaseFileIteratorHelper(requiredFields.getRight(), allFields.getRight(), dataFile);
    Option<Pair<ClosableIterator<T>, Schema>> skeletonFileIterator =
        makeBootstrapBaseFileIteratorHelper(requiredFields.getLeft(), allFields.getLeft(), baseFile);
    if (!dataFileIterator.isPresent() && !skeletonFileIterator.isPresent()) {
      throw new IllegalStateException("should not be here if only partition cols are required");
    } else if (!dataFileIterator.isPresent()) {
      return skeletonFileIterator.get().getLeft();
    } else if (!skeletonFileIterator.isPresent()) {
      return dataFileIterator.get().getLeft();
    } else {
      if (inputSplit.start != 0) {
        throw new IllegalArgumentException("Filegroup reader is doing bootstrap merge but we are not reading from the start of the base file");
      }
      PartitionPathParser partitionPathParser = new PartitionPathParser();
      Object[] partitionValues = partitionPathParser.getPartitionFieldVals(partitionPathFields, inputSplit.partitionPath, readerContext.getSchemaHandler().getTableSchema());
      // filter out the partition values that are not required by the data schema
      List<Pair<String, Object>> partitionPathFieldsAndValues = partitionPathFields.map(partitionFields -> {
        Schema dataSchema = dataFileIterator.get().getRight();
        List<Pair<String, Object>> filterFieldsAndValues = new ArrayList<>(partitionFields.length);
        for (int i = 0; i < partitionFields.length; i++) {
          String field = partitionFields[i];
          if (dataSchema.getField(field) != null) {
            filterFieldsAndValues.add(Pair.of(field, readerContext.convertValueToEngineType((Comparable) partitionValues[i])));
          }
        }
        return filterFieldsAndValues;
      }).orElseGet(Collections::emptyList);
      return readerContext.mergeBootstrapReaders(skeletonFileIterator.get().getLeft(), skeletonFileIterator.get().getRight(),
          dataFileIterator.get().getLeft(), dataFileIterator.get().getRight(), partitionPathFieldsAndValues);
    }
  }

  /**
   * Creates file record iterator to read bootstrap skeleton or data file
   *
   * @param requiredFields list of fields that are expected to be read from the file
   * @param allFields      list of all fields in the data file to be read
   * @param file           file to be read
   * @return pair of the record iterator of the file, and the schema of the data being read
   */
  private Option<Pair<ClosableIterator<T>, Schema>> makeBootstrapBaseFileIteratorHelper(List<Schema.Field> requiredFields,
                                                                                        List<Schema.Field> allFields,
                                                                                        BaseFile file) throws IOException {
    if (requiredFields.isEmpty()) {
      return Option.empty();
    }
    Schema requiredSchema = readerContext.getSchemaHandler().createSchemaFromFields(requiredFields);
    StoragePathInfo fileStoragePathInfo = file.getPathInfo();
    if (fileStoragePathInfo != null) {
      return Option.of(Pair.of(readerContext.getFileRecordIterator(fileStoragePathInfo, 0, file.getFileLen(),
          readerContext.getSchemaHandler().createSchemaFromFields(allFields), requiredSchema, storage), requiredSchema));
    } else {
      // If the base file length passed in is invalid, i.e., -1,
      // the file group reader fetches the length from the file system
      long fileLength = file.getFileLen() >= 0
          ? file.getFileLen() : storage.getPathInfo(file.getStoragePath()).getLength();
      return Option.of(Pair.of(readerContext.getFileRecordIterator(file.getStoragePath(), 0, fileLength,
          readerContext.getSchemaHandler().createSchemaFromFields(allFields), requiredSchema, storage), requiredSchema));
    }
  }

  /**
   * @return {@code true} if the next record exists; {@code false} otherwise.
   * @throws IOException on reader error.
   */
  boolean hasNext() throws IOException {
    if (recordBuffer == null) {
      return baseFileIterator.hasNext();
    } else {
      return recordBuffer.hasNext();
    }
  }

  /**
   * @return statistics of reading a file group.
   */
  public HoodieReadStats getStats() {
    return readStats;
  }

  /**
   * @return The next record after calling {@link #hasNext}.
   */
  T next() {
    T nextVal = recordBuffer == null ? baseFileIterator.next() : recordBuffer.next();
    if (outputConverter.isPresent()) {
      return outputConverter.get().apply(nextVal);
    }
    return nextVal;
  }

  private void scanLogFiles() {
    try (HoodieMergedLogRecordReader<T> logRecordReader = HoodieMergedLogRecordReader.<T>newBuilder()
        .withHoodieReaderContext(readerContext)
        .withStorage(storage)
        .withLogFiles(inputSplit.logFiles)
        .withReverseReader(false)
        .withBufferSize(getIntWithAltKeys(props, HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE))
        .withInstantRange(readerContext.getInstantRange())
        .withPartition(inputSplit.partitionPath)
        .withRecordBuffer(recordBuffer)
        .withAllowInflightInstants(allowInflightInstants)
        .withMetaClient(metaClient)
        .withOptimizedLogBlocksScan(enableOptimizedLogBlockScan)
        .build()) {
      this.validBlockInstants = logRecordReader.getValidBlockInstants();
      readStats.setTotalLogReadTimeMs(logRecordReader.getTotalTimeTakenToReadAndMergeBlocks());
      readStats.setTotalUpdatedRecordsCompacted(logRecordReader.getNumMergedRecordsInLog());
      readStats.setTotalLogFilesCompacted(logRecordReader.getTotalLogFiles());
      readStats.setTotalLogRecords(logRecordReader.getTotalLogRecords());
      readStats.setTotalLogBlocks(logRecordReader.getTotalLogBlocks());
      readStats.setTotalCorruptLogBlock(logRecordReader.getTotalCorruptBlocks());
      readStats.setTotalRollbackBlocks(logRecordReader.getTotalRollbacks());
    }
  }

  public List<String> getValidBlockInstants() {
    return validBlockInstants;
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

  public ClosableIterator<T> getClosableIterator() throws IOException {
    initRecordIterators();
    return new HoodieFileGroupReaderIterator<>(this);
  }

  /**
   * @return An iterator over the records that wraps the engine-specific record in a HoodieRecord.
   */
  public ClosableIterator<HoodieRecord<T>> getClosableHoodieRecordIterator() throws IOException {
    return new CloseableMappingIterator<>(getClosableIterator(), nextRecord -> {
      BufferedRecord<T> bufferedRecord = BufferedRecord.forRecordWithContext(nextRecord, readerContext.getSchemaHandler().getRequestedSchema(), readerContext, orderingFieldName, false);
      return readerContext.constructHoodieRecord(bufferedRecord);
    });
  }

  /**
   * @return A record key iterator over the records.
   */
  public ClosableIterator<String> getClosableKeyIterator() throws IOException {
    return new CloseableMappingIterator<>(getClosableIterator(),
        nextRecord -> readerContext.getRecordKey(nextRecord, readerContext.getSchemaHandler().getRequestedSchema()));
  }

  public static class HoodieFileGroupReaderIterator<T> implements ClosableIterator<T> {
    private HoodieFileGroupReader<T> reader;

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
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          throw new HoodieIOException("Failed to close the reader", e);
        } finally {
          this.reader = null;
        }
      }
    }
  }

  public static <T> Builder<T> newBuilder() {
    return new Builder<>();
  }

  public static class Builder<T> {
    private HoodieReaderContext<T> readerContext;
    private HoodieStorage storage;
    private String tablePath;
    private String latestCommitTime;
    private Schema dataSchema;
    private Schema requestedSchema;
    private Option<InternalSchema> internalSchemaOpt = Option.empty();
    private HoodieTableMetaClient hoodieTableMetaClient;
    private TypedProperties props;
    private Option<HoodieBaseFile> baseFileOption;
    private Stream<HoodieLogFile> logFiles;
    private String partitionPath;
    private long start = 0;
    private long length = Long.MAX_VALUE;
    private boolean shouldUseRecordPosition = false;
    private boolean allowInflightInstants = false;
    private boolean emitDelete;
    private boolean sortOutput = false;
    private boolean enableOptimizedLogBlockScan = false;
    private Option<BaseFileUpdateCallback> fileGroupUpdateCallback = Option.empty();

    public Builder<T> withReaderContext(HoodieReaderContext<T> readerContext) {
      this.readerContext = readerContext;
      return this;
    }

    public Builder<T> withLatestCommitTime(String latestCommitTime) {
      this.latestCommitTime = latestCommitTime;
      return this;
    }

    public Builder<T> withFileSlice(FileSlice fileSlice) {
      this.baseFileOption = fileSlice.getBaseFile();
      this.logFiles = fileSlice.getLogFiles();
      this.partitionPath = fileSlice.getPartitionPath();
      return this;
    }

    public Builder<T> withBaseFileOption(Option<HoodieBaseFile> baseFileOption) {
      this.baseFileOption = baseFileOption;
      return this;
    }

    public Builder<T> withLogFiles(Stream<HoodieLogFile> logFiles) {
      this.logFiles = logFiles;
      return this;
    }

    public Builder<T> withPartitionPath(String partitionPath) {
      this.partitionPath = partitionPath;
      return this;
    }

    public Builder<T> withDataSchema(Schema dataSchema) {
      this.dataSchema = dataSchema;
      return this;
    }

    public Builder<T> withRequestedSchema(Schema requestedSchema) {
      this.requestedSchema = requestedSchema;
      return this;
    }

    public Builder<T> withInternalSchema(Option<InternalSchema> internalSchemaOpt) {
      this.internalSchemaOpt = internalSchemaOpt;
      return this;
    }

    public Builder<T> withHoodieTableMetaClient(HoodieTableMetaClient hoodieTableMetaClient) {
      this.hoodieTableMetaClient = hoodieTableMetaClient;
      this.tablePath = hoodieTableMetaClient.getBasePath().toString();
      return this;
    }

    public Builder<T> withProps(TypedProperties props) {
      this.props = props;
      return this;
    }

    public Builder<T> withStart(long start) {
      this.start = start;
      return this;
    }

    public Builder<T> withLength(long length) {
      this.length = length;
      return this;
    }

    public Builder<T> withShouldUseRecordPosition(boolean shouldUseRecordPosition) {
      this.shouldUseRecordPosition = shouldUseRecordPosition;
      return this;
    }

    public Builder<T> withAllowInflightInstants(boolean allowInflightInstants) {
      this.allowInflightInstants = allowInflightInstants;
      return this;
    }

    public Builder<T> withEmitDelete(boolean emitDelete) {
      this.emitDelete = emitDelete;
      return this;
    }

    public Builder<T> withFileGroupUpdateCallback(Option<BaseFileUpdateCallback> fileGroupUpdateCallback) {
      this.fileGroupUpdateCallback = fileGroupUpdateCallback;
      return this;
    }

    public Builder<T> withEnableOptimizedLogBlockScan(boolean enableOptimizedLogBlockScan) {
      this.enableOptimizedLogBlockScan = enableOptimizedLogBlockScan;
      return this;
    }

    /**
     * If true, the output of the merge will be sorted instead of appending log records to end of the iterator if they do not have matching keys in the base file.
     * This assumes that the base file is already sorted by key.
     * @param sortOutput whether to sort the output iterator
     * @return this builder instance
     */
    public Builder<T> withSortOutput(boolean sortOutput) {
      this.sortOutput = sortOutput;
      return this;
    }

    public HoodieFileGroupReader<T> build() {
      ValidationUtils.checkArgument(readerContext != null, "Reader context is required");
      ValidationUtils.checkArgument(hoodieTableMetaClient != null, "Hoodie table meta client is required");
      ValidationUtils.checkArgument(tablePath != null, "Table path is required");
      // set the storage with the readerContext's storage configuration
      this.storage = hoodieTableMetaClient.getStorage().newInstance(new StoragePath(tablePath), readerContext.getStorageConfiguration());

      ValidationUtils.checkArgument(storage != null, "Storage is required");
      ValidationUtils.checkArgument(latestCommitTime != null, "Latest commit time is required");
      ValidationUtils.checkArgument(dataSchema != null, "Data schema is required");
      ValidationUtils.checkArgument(requestedSchema != null, "Requested schema is required");
      ValidationUtils.checkArgument(props != null, "Props is required");
      ValidationUtils.checkArgument(baseFileOption != null, "Base file option is required");
      ValidationUtils.checkArgument(logFiles != null, "Log files stream is required");
      ValidationUtils.checkArgument(partitionPath != null, "Partition path is required");

      InputSplit inputSplit = new InputSplit(baseFileOption, logFiles, partitionPath, start, length);
      return new HoodieFileGroupReader<>(
          readerContext, storage, tablePath, latestCommitTime, dataSchema, requestedSchema, internalSchemaOpt, hoodieTableMetaClient,
          props, shouldUseRecordPosition, allowInflightInstants, emitDelete, sortOutput, inputSplit, fileGroupUpdateCallback, enableOptimizedLogBlockScan);
    }
  }

  private static class InputSplit {
    private final Option<HoodieBaseFile> baseFileOption;
    private final List<HoodieLogFile> logFiles;
    private final String partitionPath;
    // Byte offset to start reading from the base file
    private final long start;
    // Length of bytes to read from the base file
    private final long length;

    InputSplit(Option<HoodieBaseFile> baseFileOption, Stream<HoodieLogFile> logFiles, String partitionPath, long start, long length) {
      this.baseFileOption = baseFileOption;
      this.logFiles = logFiles.sorted(HoodieLogFile.getLogFileComparator())
          .filter(logFile -> !logFile.getFileName().endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
          .collect(Collectors.toList());
      this.partitionPath = partitionPath;
      this.start = start;
      this.length = length;
    }

    static InputSplit fromFileSlice(FileSlice fileSlice, long start, long length) {
      return new InputSplit(fileSlice.getBaseFile(), fileSlice.getLogFiles(), fileSlice.getPartitionPath(),
          start, length);
    }
  }
}
