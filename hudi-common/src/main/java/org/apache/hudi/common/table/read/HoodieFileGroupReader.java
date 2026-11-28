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

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartitionPathParser;
import org.apache.hudi.common.table.log.LogReadAheadStorage;
import org.apache.hudi.common.table.read.buffer.BatchMatcher;
import org.apache.hudi.common.table.read.buffer.FileGroupRecordBufferLoader;
import org.apache.hudi.common.table.read.buffer.HoodieFileGroupRecordBuffer;
import org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * A file group reader that iterates through the records in a single file group.
 * <p>
 * This should be used by the every engine integration, by plugging in a
 * {@link HoodieReaderContext<T>} implementation.
 *
 * @param <T> The type of engine-specific record representation, e.g.,{@code InternalRow}
 *            in Spark and {@code RowData} in Flink.
 */
@AllArgsConstructor
public final class HoodieFileGroupReader<T> implements HoodieRecordReader<T> {

  // Rows per batch when the rows of a row-based merge are returned as batches
  private static final int ROWS_PER_BATCH = 4096;

  private final HoodieReaderContext<T> readerContext;
  private final TableState tableState;
  private final InputSplit inputSplit;
  private final Option<String[]> partitionPathFields;
  private final List<String> orderingFieldNames;
  private final HoodieStorage storage;
  private final TypedProperties props;
  private final ReaderParameters readerParameters;
  private final FileGroupRecordBufferLoader<T> recordBufferLoader;
  // Core structure to store and process records.
  private HoodieFileGroupRecordBuffer<T> recordBuffer;
  private ClosableIterator<T> baseFileIterator;
  private final Option<UnaryOperator<T>> outputConverter;
  @Getter
  private final HoodieReadStats readStats;
  // Callback to run custom logic on updates to the base files for the file group
  private final Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback;
  // The list of instant times read from the log blocks, this value is used by the log-compaction to allow optimized log-block scans
  @Getter
  private List<String> validBlockInstants = Collections.emptyList();
  private BufferedRecordConverter<T> bufferedRecordConverter;

  @Builder(setterPrefix = "with")
  private HoodieFileGroupReader(
      HoodieReaderContext<T> readerContext,
      String latestCommitTime,
      HoodieSchema dataSchema,
      HoodieSchema requestedSchema,
      Option<InternalSchema> internalSchemaOpt,
      @Deprecated HoodieTableMetaClient hoodieTableMetaClient,
      TableState tableState,
      HoodieStorage storage,
      TypedProperties props,
      Option<HoodieBaseFile> baseFileOption,
      Stream<HoodieLogFile> logFiles,
      String partitionPath,
      Long start,
      Long length,
      Iterator<? extends HoodieRecord> recordIterator,
      Boolean shouldUseRecordPosition,
      Boolean allowInflightInstants,
      Boolean emitDelete,
      Boolean sortOutput,
      Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback,
      FileGroupRecordBufferLoader<T> recordBufferLoader) {

    // Validations
    ValidationUtils.checkArgument(readerContext != null, "Reader context is required");
    ValidationUtils.checkArgument(tableState != null || hoodieTableMetaClient != null, "Table state or meta client is required");
    ValidationUtils.checkArgument(latestCommitTime != null, "Latest commit time is required");
    ValidationUtils.checkArgument(dataSchema != null, "Data schema is required");
    ValidationUtils.checkArgument(requestedSchema != null, "Requested schema is required");
    ValidationUtils.checkArgument(props != null, "Props is required");
    ValidationUtils.checkArgument(partitionPath != null, "Partition path is required");

    // Handle defaults
    if (internalSchemaOpt == null) {
      internalSchemaOpt = Option.empty();
    }
    if (baseFileOption == null) {
      baseFileOption = Option.empty();
    }
    if (start == null) {
      start = 0L;
    }
    if (length == null) {
      length = Long.MAX_VALUE;
    }
    if (shouldUseRecordPosition == null) {
      shouldUseRecordPosition = false;
    }
    if (allowInflightInstants == null) {
      allowInflightInstants = false;
    }
    if (emitDelete == null) {
      emitDelete = false;
    }
    if (sortOutput == null) {
      sortOutput = false;
    }
    if (fileGroupUpdateCallback == null) {
      fileGroupUpdateCallback = Option.empty();
    }

    if (tableState == null) {
      tableState = TableState.fromMetaClient(hoodieTableMetaClient);
    }
    String tablePath = tableState.getBasePath().toString();

    // Read with the readerContext's storage configuration unless the caller supplies the storage
    if (storage == null) {
      storage = hoodieTableMetaClient != null
          ? hoodieTableMetaClient.getStorage().newInstance(new StoragePath(tablePath), readerContext.getStorageConfiguration())
          : HoodieStorageUtils.getStorage(new StoragePath(tablePath), readerContext.getStorageConfiguration());
    }

    // Handle recordBufferLoader default
    if (recordBufferLoader == null) {
      if (recordIterator != null) {
        recordBufferLoader = FileGroupRecordBufferLoader.createStreamingRecordsBufferLoader();
      } else {
        recordBufferLoader = FileGroupRecordBufferLoader.createDefault();
      }
    }

    // Build composite objects using static helpers
    this.readerParameters = ReaderParameters.builder()
        .shouldUseRecordPosition(shouldUseRecordPosition)
        .emitDeletes(emitDelete)
        .sortOutputs(sortOutput)
        .inflightInstantsAllowed(allowInflightInstants)
        .build();
    this.inputSplit = InputSplit.builder()
        .baseFileOption(baseFileOption)
        .logFileStream(logFiles)
        .recordIterator((Iterator<HoodieRecord>) recordIterator)
        .partitionPath(partitionPath)
        .start(start)
        .length(length)
        .build();

    // Initialize fields
    this.readerContext = readerContext;
    this.recordBufferLoader = recordBufferLoader;
    this.fileGroupUpdateCallback = fileGroupUpdateCallback;
    this.tableState = tableState;
    this.storage = storage;

    readerContext.setHasLogFiles(this.inputSplit.hasLogFiles());
    readerContext.getRecordContext().setPartitionPath(inputSplit.getPartitionPath());
    if (readerContext.getHasLogFiles() && inputSplit.getStart() != 0) {
      throw new IllegalArgumentException("Filegroup reader is doing log file merge but not reading from the start of the base file");
    }
    HoodieTableConfig tableConfig = tableState.getTableConfig();
    props = ConfigUtils.getMergeProps(props, tableConfig);
    this.props = props;
    this.partitionPathFields = tableConfig.getPartitionFields();
    readerContext.initRecordMerger(props);
    readerContext.setTablePath(tablePath);
    readerContext.setLatestCommitTime(latestCommitTime);
    boolean isSkipMerge = ConfigUtils.getStringWithAltKeys(props, HoodieReaderConfig.MERGE_TYPE, true).equalsIgnoreCase(HoodieReaderConfig.REALTIME_SKIP_MERGE);
    readerContext.setShouldMergeUseRecordPosition(readerParameters.shouldUseRecordPosition() && !isSkipMerge && readerContext.getHasLogFiles() && inputSplit.isParquetBaseFile());
    readerContext.setHasBootstrapBaseFile(inputSplit.getBaseFileOption().flatMap(HoodieBaseFile::getBootstrapBaseFile).isPresent());
    readerContext.setSchemaHandler(readerContext.getRecordContext().supportsParquetRowIndex()
        ? new ParquetRowIndexBasedSchemaHandler<>(readerContext, dataSchema, requestedSchema, internalSchemaOpt, props, tableState)
        : new FileGroupReaderSchemaHandler<>(readerContext, dataSchema, requestedSchema, internalSchemaOpt, props, tableState));
    this.outputConverter = readerContext.getSchemaHandler().getOutputConverter();
    this.orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(readerContext.getMergeMode(), tableConfig);
    this.readStats = new HoodieReadStats();
  }

  /**
   * Initialize internal iterators on the base and log files.
   */
  private void initRecordIterators() throws IOException {
    ClosableIterator<T> iter = makeBaseFileIterator();
    if (inputSplit.hasNoRecordsToMerge()) {
      HoodieSchema requiredSchema = readerContext.getSchemaHandler().getRequiredSchema();
      this.baseFileIterator = new CloseableMappingIterator<>(iter,
          rec -> readerContext.getRecordContext().seal(requiredSchema, rec));
    } else {
      this.baseFileIterator = iter;
      loadRecordBuffer();
      recordBuffer.setBaseFileIterator(baseFileIterator);
    }
  }

  /**
   * Scans the log files into the record buffer.
   */
  private void loadRecordBuffer() throws IOException {
    Pair<HoodieFileGroupRecordBuffer<T>, List<String>> initializationResult;
    LogReadAheadStorage logReadAhead = startLogReadAhead();
    try {
      initializationResult = recordBufferLoader.getRecordBuffer(readerContext, logReadAhead != null ? logReadAhead : storage,
          inputSplit, orderingFieldNames, tableState, props, readerParameters, readStats, fileGroupUpdateCallback);
    } finally {
      // The log scan is over once the buffer is loaded
      if (logReadAhead != null) {
        logReadAhead.close();
      }
    }
    recordBuffer = initializationResult.getLeft();
    validBlockInstants = initializationResult.getRight();
  }

  /**
   * Starts reading the log files ahead of the log scan when {@link HoodieReaderConfig#LOG_READ_AHEAD_BYTES} is positive,
   * or returns null. Reads that only look up some keys (metadata table lookups, key filters) and reads that do not merge
   * read the logs in line, since they would not use most of what is read ahead.
   */
  private LogReadAheadStorage startLogReadAhead() {
    long windowBytes = props.getLong(HoodieReaderConfig.LOG_READ_AHEAD_BYTES.key(), HoodieReaderConfig.LOG_READ_AHEAD_BYTES.defaultValue());
    boolean skipMerge = ConfigUtils.getStringWithAltKeys(props, HoodieReaderConfig.MERGE_TYPE, true)
        .equalsIgnoreCase(HoodieReaderConfig.REALTIME_SKIP_MERGE);
    // A split built from a record iterator, as for compaction, has no log files to read
    if (windowBytes <= 0 || inputSplit.hasRecordIterator() || !inputSplit.hasLogFiles() || storage instanceof LogReadAheadStorage
        || skipMerge || readerContext.getKeyFilterOpt().isPresent() || HoodieTableMetadata.isMetadataTable(tableState.getBasePath())) {
      return null;
    }
    long executorLimitBytes = props.getLong(HoodieReaderConfig.LOG_READ_AHEAD_EXECUTOR_BYTES.key(), Long.MAX_VALUE);
    return LogReadAheadStorage.create(storage, inputSplit.getLogFiles(), windowBytes, executorLimitBytes);
  }

  private ClosableIterator<T> makeBaseFileIterator() throws IOException {
    if (!inputSplit.getBaseFileOption().isPresent()) {
      return new EmptyIterator<>();
    }
    this.bufferedRecordConverter = BufferedRecordConverter.createConverter(readerContext.getIteratorMode(),
        readerContext.getSchemaHandler().getRequiredSchema(), readerContext.getRecordContext(), orderingFieldNames);

    HoodieBaseFile baseFile = inputSplit.getBaseFileOption().get();
    if (baseFile.getBootstrapBaseFile().isPresent()) {
      return makeBootstrapBaseFileIterator(baseFile);
    }

    StoragePathInfo baseFileStoragePathInfo = baseFile.getPathInfo();
    final ClosableIterator<T> recordIterator;
    if (baseFileStoragePathInfo != null) {
      recordIterator = readerContext.getFileRecordIterator(
          baseFileStoragePathInfo, inputSplit.getStart(), inputSplit.getLength(),
          readerContext.getSchemaHandler().getTableSchema(),
          readerContext.getSchemaHandler().getRequiredSchema(), storage);
    } else {
      recordIterator = readerContext.getFileRecordIterator(
          baseFile.getStoragePath(), inputSplit.getStart(), inputSplit.getLength(),
          readerContext.getSchemaHandler().getTableSchema(),
          readerContext.getSchemaHandler().getRequiredSchema(), storage);
    }
    return readerContext.getInstantRange().isPresent()
        ? readerContext.applyInstantRangeFilter(recordIterator)
        : recordIterator;
  }

  private ClosableIterator<T> makeBootstrapBaseFileIterator(HoodieBaseFile baseFile) throws IOException {
    BaseFile dataFile = baseFile.getBootstrapBaseFile().get();
    Pair<List<HoodieSchemaField>, List<HoodieSchemaField>> requiredFields = readerContext.getSchemaHandler().getBootstrapRequiredFields();
    Pair<List<HoodieSchemaField>, List<HoodieSchemaField>> allFields = readerContext.getSchemaHandler().getBootstrapDataFields();
    Option<Pair<ClosableIterator<T>, HoodieSchema>> dataFileIterator =
        makeBootstrapBaseFileIteratorHelper(requiredFields.getRight(), allFields.getRight(), dataFile);
    Option<Pair<ClosableIterator<T>, HoodieSchema>> skeletonFileIterator =
        makeBootstrapBaseFileIteratorHelper(requiredFields.getLeft(), allFields.getLeft(), baseFile);
    if (!dataFileIterator.isPresent() && !skeletonFileIterator.isPresent()) {
      throw new IllegalStateException("should not be here if only partition cols are required");
    } else if (!dataFileIterator.isPresent()) {
      return skeletonFileIterator.get().getLeft();
    } else if (!skeletonFileIterator.isPresent()) {
      return dataFileIterator.get().getLeft();
    } else {
      if (inputSplit.getStart() != 0) {
        throw new IllegalArgumentException("Filegroup reader is doing bootstrap merge but we are not reading from the start of the base file");
      }
      PartitionPathParser partitionPathParser = new PartitionPathParser();
      Object[] partitionValues = partitionPathParser.getPartitionFieldVals(partitionPathFields, inputSplit.getPartitionPath(),
          readerContext.getSchemaHandler().getTableSchema());
      // filter out the partition values that are not required by the data schema
      List<Pair<String, Object>> partitionPathFieldsAndValues = partitionPathFields.map(partitionFields -> {
        HoodieSchema dataSchema = dataFileIterator.get().getRight();
        List<Pair<String, Object>> filterFieldsAndValues = new ArrayList<>(partitionFields.length);
        for (int i = 0; i < partitionFields.length; i++) {
          String field = partitionFields[i];
          if (dataSchema.getField(field).isPresent()) {
            filterFieldsAndValues.add(Pair.of(field, readerContext.getRecordContext().convertValueToEngineType((Comparable) partitionValues[i])));
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
  private Option<Pair<ClosableIterator<T>, HoodieSchema>> makeBootstrapBaseFileIteratorHelper(List<HoodieSchemaField> requiredFields,
                                                                                              List<HoodieSchemaField> allFields,
                                                                                              BaseFile file) throws IOException {
    if (requiredFields.isEmpty()) {
      return Option.empty();
    }
    HoodieSchema requiredSchema = readerContext.getSchemaHandler().createSchemaFromFields(requiredFields);
    StoragePathInfo fileStoragePathInfo = file.getPathInfo();
    if (fileStoragePathInfo != null) {
      return Option.of(Pair.of(readerContext.getFileRecordIterator(fileStoragePathInfo, 0, file.getFileSize(),
          readerContext.getSchemaHandler().createSchemaFromFields(allFields), requiredSchema, storage), requiredSchema));
    } else {
      // If the base file length passed in is invalid, i.e., -1,
      // the file group reader fetches the length from the file system
      long fileLength = file.getFileSize() >= 0
          ? file.getFileSize() : storage.getPathInfo(file.getStoragePath()).getLength();
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
   * @return The next record after calling {@link #hasNext}.
   */
  BufferedRecord<T> next() {
    BufferedRecord<T> nextVal = recordBuffer == null ? bufferedRecordConverter.convert(baseFileIterator.next()) : recordBuffer.next();
    if (outputConverter.isPresent()) {
      return nextVal.project(outputConverter.get());
    }
    return nextVal;
  }

  /**
   * Notifies a write failure with the given record key.
   */
  public void onWriteFailure(String recordKey) {
    this.fileGroupUpdateCallback.ifPresent(callback -> callback.onFailure(recordKey));
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

  private ClosableIterator<BufferedRecord<T>> getBufferedRecordIterator(IteratorMode iteratorMode) throws IOException {
    this.readerContext.setIteratorMode(iteratorMode);
    initRecordIterators();
    return new HoodieFileGroupReaderIterator<>(this);
  }

  public ClosableIterator<BufferedRecord<T>> getClosableBufferedRecordIterator() throws IOException {
    return getBufferedRecordIterator(IteratorMode.HOODIE_RECORD);
  }

  public ClosableIterator<T> getClosableIterator() throws IOException {
    return new CloseableMappingIterator<>(getBufferedRecordIterator(IteratorMode.ENGINE_RECORD), BufferedRecord::getRecord);
  }

  /**
   * Reads the file group as engine batches of the requested schema through the reader context's {@link BatchSupport}.
   *
   * <p>Under commit-time or event-time ordering with the built-in merging, the log records are loaded first and then
   * merged into the vectorized base file batches: a base row without a log record stays in its batch, matched by its
   * position in the file when the log blocks carry record positions and by its record key otherwise. Every other
   * merge, and a slice whose log records cannot be matched that way, is merged row by row and returned as batches.
   */
  @SuppressWarnings("unchecked")
  public <B> ClosableIterator<B> getClosableBatchIterator() throws IOException {
    BatchSupport<T, B> batchSupport = (BatchSupport<T, B>) readerContext.getBatchSupport()
        .orElseThrow(() -> new IllegalStateException("The reader context does not read columnar batches"));
    readerContext.setIteratorMode(IteratorMode.ENGINE_RECORD);
    if (!canMergeBatches()) {
      return rowsAsBatches(batchSupport, getClosableIterator());
    }
    HoodieSchema requestedSchema = readerContext.getSchemaHandler().getRequestedSchema();
    RecordMergeMode mergeMode = readerContext.getMergeMode();
    Option<HoodieBaseFile> baseFile = inputSplit.getBaseFileOption();
    if (inputSplit.hasNoRecordsToMerge()) {
      ClosableIterator<B> batches = baseFile.isPresent()
          ? batchSupport.readBaseFile(baseFile.get(), requestedSchema, true) : new EmptyIterator<>();
      return new BatchMergeIterator<>(selectAll(batchSupport, batches, requestedSchema.getFields().size()), this);
    }

    // With record positions, the base file read does not depend on the log records, so it starts during the log scan
    HoodieSchema earlySchema = null;
    CompletableFuture<ClosableIterator<B>> earlyBase = null;
    if (baseFile.isPresent() && readerContext.getShouldMergeUseRecordPosition() && batchSupport.supportsConcurrentOpen()) {
      earlySchema = baseReadSchema(requestedSchema, PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME, mergeMode);
      HoodieSchema schema = earlySchema;
      earlyBase = BaseBatchOpener.open(() -> batchSupport.readBaseFile(baseFile.get(), schema, mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING));
    }
    ClosableIterator<B> baseBatches = null;
    try {
      loadRecordBuffer();
      int maxLogRecords = props.getInteger(HoodieReaderConfig.VECTORIZED_MOR_READ_MAX_LOG_RECORDS.key(),
          HoodieReaderConfig.VECTORIZED_MOR_READ_MAX_LOG_RECORDS.defaultValue());
      Option<BatchMatcher<T>> matcher = recordBuffer.size() > maxLogRecords ? Option.empty() : recordBuffer.batchMatcher(batchSupport);
      if (!matcher.isPresent()) {
        BaseBatchOpener.discard(earlyBase);
        earlyBase = null;
        this.baseFileIterator = makeBaseFileIterator();
        recordBuffer.setBaseFileIterator(baseFileIterator);
        return rowsAsBatches(batchSupport,
            new CloseableMappingIterator<>(new HoodieFileGroupReaderIterator<>(this), BufferedRecord::getRecord));
      }

      HoodieSchema baseReadSchema = baseReadSchema(requestedSchema, matcher.get().columnName(), mergeMode);
      if (earlyBase != null) {
        if (baseReadSchema.equals(earlySchema)) {
          baseBatches = BaseBatchOpener.await(earlyBase);
        } else {
          BaseBatchOpener.discard(earlyBase);
        }
        earlyBase = null;
      }
      if (baseBatches == null) {
        baseBatches = baseFile.isPresent()
            ? batchSupport.readBaseFile(baseFile.get(), baseReadSchema, mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING)
            : new EmptyIterator<>();
      }
      HoodieSchema logSchema = readerContext.getSchemaHandler().getRequiredSchema();
      BufferedRecordMerger<T> merger = BufferedRecordMergerFactory.create(readerContext, mergeMode, false,
          readerContext.getRecordMerger(), logSchema, readerContext.getPayloadClasses(props), props, Option.empty());
      return new BatchMergeIterator<>(new BatchFileGroupMerger<>(batchSupport, baseBatches, matcher.get(), merger, mergeMode,
          readerContext.getRecordContext(), baseReadSchema, logSchema, requestedSchema, orderingFieldNames), this);
    } catch (IOException | RuntimeException e) {
      // Nothing returned owns the base file or the record buffer yet
      BaseBatchOpener.discard(earlyBase);
      if (baseBatches != null) {
        baseBatches.close();
      }
      close();
      throw e;
    }
  }

  /**
   * Whether the log records of this file group can be merged into base file batches: a snapshot read of a Parquet base
   * file, or no base file, under commit-time or event-time ordering with the built-in merging.
   */
  private boolean canMergeBatches() {
    if (!props.getBoolean(HoodieReaderConfig.VECTORIZED_MOR_READ_ENABLED.key(), HoodieReaderConfig.VECTORIZED_MOR_READ_ENABLED.defaultValue())) {
      return false;
    }
    boolean skipMerge = ConfigUtils.getStringWithAltKeys(props, HoodieReaderConfig.MERGE_TYPE, true)
        .equalsIgnoreCase(HoodieReaderConfig.REALTIME_SKIP_MERGE);
    Option<HoodieBaseFile> baseFile = inputSplit.getBaseFileOption();
    return !skipMerge
        && (!baseFile.isPresent() || (inputSplit.isParquetBaseFile() && !baseFile.get().getBootstrapBaseFile().isPresent()))
        && !readerContext.getInstantRange().isPresent()
        && !readerParameters.isEmitDeletes()
        && !readerParameters.isSortOutputs()
        && !fileGroupUpdateCallback.isPresent()
        // Key-based matching reads the stored record key; nested ordering fields are not columns of a base batch
        && tableState.getTableConfig().populateMetaFields()
        && orderingFieldNames.stream().noneMatch(name -> name.contains("."))
        && !BufferedRecordMergerFactory.needsFullRowMerge(readerContext.getMergeMode(),
            Option.ofNullable(tableState.getTableConfig().getPayloadClass()), tableState.getTableConfig().getPartialUpdateMode());
  }

  /**
   * The base file read of a merge: the requested columns, then the column the log records are matched by and, under
   * event-time ordering, the ordering fields the requested schema lacks.
   */
  private HoodieSchema baseReadSchema(HoodieSchema requestedSchema, String matchColumn, RecordMergeMode mergeMode) {
    List<String> extraFields = new ArrayList<>();
    extraFields.add(matchColumn);
    if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING) {
      extraFields.addAll(orderingFieldNames);
    }
    HoodieSchema requiredSchema = readerContext.getSchemaHandler().getRequiredSchema();
    List<HoodieSchemaField> fields = new ArrayList<>();
    for (HoodieSchemaField field : requestedSchema.getFields()) {
      fields.add(field);
    }
    for (String name : extraFields) {
      if (!requestedSchema.getField(name).isPresent() && fields.stream().noneMatch(f -> f.name().equals(name))) {
        fields.add(requiredSchema.getField(name)
            .orElseThrow(() -> new IllegalStateException("Field " + name + " is not in the required schema of the merge")));
      }
    }
    return readerContext.getSchemaHandler().createSchemaFromFields(fields);
  }

  private static <T, B> ClosableIterator<B> selectAll(BatchSupport<T, B> batchSupport, ClosableIterator<B> batches,
                                                      int numColumns) {
    return new CloseableMappingIterator<>(batches,
        batch -> batchSupport.select(batch, numColumns, numColumns, null, batchSupport.numRows(batch)));
  }

  /**
   * Returns the rows of a row-based read as engine batches, copying each row since the read may reuse it.
   */
  private <B> ClosableIterator<B> rowsAsBatches(BatchSupport<T, B> batchSupport, ClosableIterator<T> rows) {
    HoodieSchema rowSchema = readerContext.getSchemaHandler().getRequestedSchema();
    return new ClosableIterator<B>() {
      @Override
      public boolean hasNext() {
        return rows.hasNext();
      }

      @Override
      public B next() {
        List<T> batch = new ArrayList<>();
        while (rows.hasNext() && batch.size() < ROWS_PER_BATCH) {
          batch.add(readerContext.getRecordContext().seal(rowSchema, rows.next()));
        }
        return batchSupport.toBatch(batch);
      }

      @Override
      public void close() {
        rows.close();
      }
    };
  }

  /**
   * Batches from a merge that also close this reader, which holds the record buffer.
   */
  private static final class BatchMergeIterator<B> implements ClosableIterator<B> {
    private final ClosableIterator<B> batches;
    private final HoodieFileGroupReader<?> reader;

    private BatchMergeIterator(ClosableIterator<B> batches, HoodieFileGroupReader<?> reader) {
      this.batches = batches;
      this.reader = reader;
    }

    @Override
    public boolean hasNext() {
      return batches.hasNext();
    }

    @Override
    public B next() {
      return batches.next();
    }

    @Override
    public void close() {
      try {
        batches.close();
      } finally {
        try {
          reader.close();
        } catch (IOException e) {
          throw new HoodieIOException("Failed to close the reader", e);
        }
      }
    }
  }

  /**
   * @return An iterator over the records that wraps the engine-specific record in a HoodieRecord.
   */
  public ClosableIterator<HoodieRecord<T>> getClosableHoodieRecordIterator() throws IOException {
    return new CloseableMappingIterator<>(getBufferedRecordIterator(IteratorMode.HOODIE_RECORD),
        bufferedRecord -> readerContext.getRecordContext().constructFinalHoodieRecord(bufferedRecord));
  }

  /**
   * @return A record key iterator over the records.
   */
  public ClosableIterator<String> getClosableKeyIterator() throws IOException {
    return new CloseableMappingIterator<>(getBufferedRecordIterator(IteratorMode.RECORD_KEY), BufferedRecord::getRecordKey);
  }

  public ClosableIterator<BufferedRecord<T>> getLogRecordsOnly() throws IOException {
    initRecordIterators();
    return recordBuffer.getLogRecordIterator();
  }

  public static class HoodieFileGroupReaderIterator<T> implements ClosableIterator<BufferedRecord<T>> {
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
    public BufferedRecord<T> next() {
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
}
