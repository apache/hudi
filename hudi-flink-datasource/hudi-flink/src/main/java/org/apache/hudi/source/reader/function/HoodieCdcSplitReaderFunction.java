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

package org.apache.hudi.source.reader.function;

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.HoodieCDCLogRecordIterator;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.source.reader.BatchRecords;
import org.apache.hudi.source.reader.HoodieRecordWithPosition;
import org.apache.hudi.source.split.HoodieCdcSourceSplit;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.FlinkReaderContextFactory;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.RecordIterators;
import org.apache.hudi.table.format.cdc.CdcInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.util.RowDataProjection;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS;
import static org.apache.hudi.table.format.FormatUtils.buildAvroRecordBySchema;

/**
 * CDC reader function for source V2. Reads CDC splits ({@link HoodieCdcSourceSplit}) and
 * emits change-log {@link RowData} records tagged with the appropriate {@link RowKind}.
 *
 * <p>The implementation mirrors the logic in {@link CdcInputFormat}, adapted for the
 * {@link SplitReaderFunction} contract.
 */
@Slf4j
public class HoodieCdcSplitReaderFunction extends AbstractSplitReaderFunction {

  private final List<DataType> fieldTypes;
  private final MergeOnReadTableState tableState;
  private transient HoodieTableMetaClient metaClient;
  private transient ClosableIterator<RowData> currentIterator;
  // Fallback reader for non-CDC splits (e.g. snapshot reads when read.start-commit='earliest')
  private transient HoodieSplitReaderFunction fallbackReaderFunction;

  /**
   * Creates a CDC split reader function.
   *
   * @param conf                  Flink configuration
   * @param tableState            Merge on Read table state
   * @param internalSchemaManager Schema-evolution manager
   * @param fieldTypes            DataType list for all table fields (used for parquet reading)
   * @param predicates            Predicates for push down
   * @param emitDelete            Whether to emit delete
   */
  public HoodieCdcSplitReaderFunction(
      org.apache.flink.configuration.Configuration conf,
      MergeOnReadTableState tableState,
      InternalSchemaManager internalSchemaManager,
      List<DataType> fieldTypes,
      List<ExpressionPredicates.Predicate> predicates,
      boolean emitDelete) {
    super(conf, predicates, internalSchemaManager, emitDelete);
    ValidationUtils.checkArgument(tableState != null, "tableState can't be null");
    ValidationUtils.checkArgument(internalSchemaManager != null, "internalSchemaManager can't be null");
    this.tableState = tableState;
    this.fieldTypes = fieldTypes;
  }

  @Override
  public RecordsWithSplitIds<HoodieRecordWithPosition<RowData>> read(HoodieSourceSplit split) {
    if (!(split instanceof HoodieCdcSourceSplit)) {
      // Non-CDC splits arrive when reading from 'earliest' with no prior CDC history
      // (i.e. instantRange is empty → snapshot path). Fall back to the standard MOR reader
      // which emits all records as INSERT rows, matching the expected snapshot behaviour.
      return getFallbackReaderFunction().read(split);
    }
    HoodieCdcSourceSplit cdcSplit = (HoodieCdcSourceSplit) split;

    HoodieCDCSupplementalLoggingMode mode = OptionsResolver.getCDCSupplementalLoggingMode(conf);
    HoodieTableMetaClient client = getMetaClient();
    HoodieWriteConfig wConfig = getWriteConfig();

    ImageManager imageManager = new ImageManager(tableState.getRowType(), wConfig, this::getFileSliceIterator);

    Function<HoodieCDCFileSplit, ClosableIterator<RowData>> recordIteratorFunc =
        cdcFileSplit -> createRecordIteratorSafe(
            cdcSplit.getTablePath(),
            cdcSplit.getMaxCompactionMemoryInBytes(),
            cdcFileSplit,
            mode,
            imageManager,
            client);

    currentIterator = new CdcFileSplitsIterator(cdcSplit.getChanges(), imageManager, recordIteratorFunc);
    BatchRecords<RowData> records = BatchRecords.forRecords(
        split.splitId(), currentIterator, split.getFileOffset(), split.getConsumed());
    records.seek(split.getConsumed());
    return records;
  }

  @Override
  public void close() throws Exception {
    if (currentIterator != null) {
      currentIterator.close();
    }
    if (fallbackReaderFunction != null) {
      fallbackReaderFunction.close();
    }
  }

  // -------------------------------------------------------------------------
  //  Internal helpers
  // -------------------------------------------------------------------------

  private HoodieSplitReaderFunction getFallbackReaderFunction() {
    if (fallbackReaderFunction == null) {
      fallbackReaderFunction = new HoodieSplitReaderFunction(
          conf,
          HoodieSchema.parse(tableState.getTableSchema()),
          HoodieSchema.parse(tableState.getRequiredSchema()),
          internalSchemaManager,
          conf.get(FlinkOptions.MERGE_TYPE),
          predicates,
          emitDelete);
    }
    return fallbackReaderFunction;
  }

  private ClosableIterator<RowData> createRecordIteratorSafe(
      String tablePath,
      long maxCompactionMemoryInBytes,
      HoodieCDCFileSplit fileSplit,
      HoodieCDCSupplementalLoggingMode mode,
      ImageManager imageManager,
      HoodieTableMetaClient client) {
    try {
      return createRecordIterator(tablePath, maxCompactionMemoryInBytes, fileSplit, mode, imageManager, client);
    } catch (IOException e) {
      throw new HoodieException("Failed to create CDC record iterator for split: " + fileSplit, e);
    }
  }

  private ClosableIterator<RowData> createRecordIterator(
      String tablePath,
      long maxCompactionMemoryInBytes,
      HoodieCDCFileSplit fileSplit,
      HoodieCDCSupplementalLoggingMode mode,
      ImageManager imageManager,
      HoodieTableMetaClient client) throws IOException {

    final HoodieSchema tableSchema = HoodieSchema.parse(tableState.getTableSchema());
    final HoodieSchema requiredSchema = HoodieSchema.parse(tableState.getRequiredSchema());
    switch (fileSplit.getCdcInferCase()) {
      case BASE_FILE_INSERT: {
        ValidationUtils.checkState(fileSplit.getCdcFiles() != null && fileSplit.getCdcFiles().size() == 1,
            "CDC file path should exist and be singleton for BASE_FILE_INSERT");
        String path = new Path(tablePath, fileSplit.getCdcFiles().get(0)).toString();
        return new AddBaseFileIterator(getBaseFileIterator(path));
      }
      case BASE_FILE_DELETE: {
        ValidationUtils.checkState(fileSplit.getBeforeFileSlice().isPresent(),
            "Before file slice should exist for BASE_FILE_DELETE");
        FileSlice fileSlice = fileSplit.getBeforeFileSlice().get();
        MergeOnReadInputSplit inputSplit = CdcInputFormat.fileSlice2Split(tablePath, fileSlice, maxCompactionMemoryInBytes);
        return new RemoveBaseFileIterator(tableState.getRequiredRowType(), tableState.getRequiredPositions(), getFileSliceIterator(inputSplit));
      }
      case AS_IS: {
        HoodieSchema dataSchema = HoodieSchemaUtils.removeMetadataFields(tableSchema);
        HoodieSchema cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(mode, dataSchema);
        switch (mode) {
          case DATA_BEFORE_AFTER:
            return new BeforeAfterImageIterator(
                getHadoopConf(), tablePath, tableSchema, requiredSchema, tableState.getRequiredRowType(), cdcSchema, fileSplit);
          case DATA_BEFORE:
            return new BeforeImageIterator(
                conf, getHadoopConf(), tablePath, tableSchema, requiredSchema, tableState.getRequiredRowType(),
                maxCompactionMemoryInBytes, cdcSchema, fileSplit, imageManager);
          case OP_KEY_ONLY:
            return new RecordKeyImageIterator(
                conf, getHadoopConf(), tablePath, tableSchema, requiredSchema, tableState.getRequiredRowType(),
                maxCompactionMemoryInBytes, cdcSchema, fileSplit, imageManager);
          default:
            throw new AssertionError("Unexpected CDC supplemental logging mode: " + mode);
        }
      }
      case LOG_FILE: {
        ValidationUtils.checkState(fileSplit.getCdcFiles() != null && fileSplit.getCdcFiles().size() == 1,
            "CDC file path should exist and be singleton for LOG_FILE");
        String logFilePath = new Path(tablePath, fileSplit.getCdcFiles().get(0)).toString();
        MergeOnReadInputSplit split = CdcInputFormat.singleLogFile2Split(tablePath, logFilePath, maxCompactionMemoryInBytes);
        ClosableIterator<HoodieRecord<RowData>> recordIterator = getFileSliceHoodieRecordIterator(split);
        return new DataLogFileIterator(
            maxCompactionMemoryInBytes, imageManager, fileSplit, tableSchema, tableState.getRequiredRowType(), tableState.getRequiredPositions(),
            recordIterator, client, getWriteConfig());
      }
      case REPLACE_COMMIT: {
        return new ReplaceCommitIterator(
            conf, tablePath, tableState.getRequiredRowType(), tableState.getRequiredPositions(), maxCompactionMemoryInBytes,
            fileSplit, this::getFileSliceIterator);
      }
      default:
        throw new AssertionError("Unexpected CDC file split infer case: " + fileSplit.getCdcInferCase());
    }
  }

  /** Reads the full-schema before/after image for a file slice (emitDelete=false). */
  private ClosableIterator<RowData> getFileSliceIterator(MergeOnReadInputSplit split) {
    FileSlice fileSlice = buildFileSlice(split);
    final HoodieSchema tableSchema = HoodieSchemaCache.intern(HoodieSchema.parse(tableState.getTableSchema()));
    try {
      HoodieFileGroupReader<RowData> reader = FormatUtils.createFileGroupReader(
          getMetaClient(), getWriteConfig(), internalSchemaManager, fileSlice,
          tableSchema, tableSchema, split.getLatestCommit(),
          FlinkOptions.REALTIME_PAYLOAD_COMBINE, false,
          predicates, split.getInstantRange());
      return reader.getClosableIterator();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to create file slice iterator for split: " + split, e);
    }
  }

  /** Reads a single log file and returns a typed {@link HoodieRecord} iterator (for LOG_FILE CDC inference). */
  private ClosableIterator<HoodieRecord<RowData>> getFileSliceHoodieRecordIterator(MergeOnReadInputSplit split) {
    FileSlice fileSlice = buildFileSlice(split);
    final HoodieSchema tableSchema = HoodieSchemaCache.intern(HoodieSchema.parse(tableState.getTableSchema()));
    try {
      HoodieFileGroupReader<RowData> reader = FormatUtils.createFileGroupReader(
          getMetaClient(), getWriteConfig(), internalSchemaManager, fileSlice,
          tableSchema, tableSchema, split.getLatestCommit(),
          FlinkOptions.REALTIME_PAYLOAD_COMBINE, true,
          predicates, split.getInstantRange());
      return reader.getClosableHoodieRecordIterator();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to create Hoodie record iterator for split: " + split, e);
    }
  }

  /** Reads a parquet CDC base file returning required-schema records. */
  private ClosableIterator<RowData> getBaseFileIterator(String path) throws IOException {
    String[] fieldNames = tableState.getRowType().getFieldNames().toArray(new String[0]);
    DataType[] fieldTypesArray = fieldTypes.toArray(new DataType[0]);
    LinkedHashMap<String, Object> partObjects = FilePathUtils.generatePartitionSpecs(
            path,
            tableState.getRowType().getFieldNames(),
            fieldTypes,
            conf.get(FlinkOptions.PARTITION_DEFAULT_NAME),
            conf.get(FlinkOptions.PARTITION_PATH_FIELD),
            conf.get(FlinkOptions.HIVE_STYLE_PARTITIONING)
    );

    return RecordIterators.getParquetRecordIterator(
        internalSchemaManager,
        conf.get(FlinkOptions.READ_UTC_TIMEZONE),
        true,
        HadoopConfigurations.getParquetConf(conf, getHadoopConf()),
        fieldNames,
        fieldTypesArray,
        partObjects,
        tableState.getRequiredPositions(),
        2048,
        new org.apache.flink.core.fs.Path(path),
        0,
        Long.MAX_VALUE,
        predicates);
  }

  private static FileSlice buildFileSlice(MergeOnReadInputSplit split) {
    return new FileSlice(
        new HoodieFileGroupId("", split.getFileId()),
        "",
        split.getBasePath().map(HoodieBaseFile::new).orElse(null),
        split.getLogPaths()
            .map(lp -> lp.stream().map(HoodieLogFile::new).collect(Collectors.toList()))
            .orElse(Collections.emptyList()));
  }

  private static int[] computeRequiredPositions(RowType rowType, RowType requiredRowType) {
    List<String> allNames = rowType.getFieldNames();
    return requiredRowType.getFieldNames().stream()
        .map(allNames::indexOf)
        .mapToInt(i -> i)
        .toArray();
  }

  private HoodieTableMetaClient getMetaClient() {
    if (metaClient == null) {
      metaClient = StreamerUtil.metaClientForReader(conf, getHadoopConf());
    }
    return metaClient;
  }

  // -------------------------------------------------------------------------
  //  Inner iterators (adapted from CdcInputFormat inner classes)
  // -------------------------------------------------------------------------

  /** Iterates over an ordered list of {@link HoodieCDCFileSplit}s, delegating record reading to a factory. */
  private static class CdcFileSplitsIterator implements ClosableIterator<RowData> {
    private ImageManager imageManager;
    private final Iterator<HoodieCDCFileSplit> fileSplitIterator;
    private final Function<HoodieCDCFileSplit, ClosableIterator<RowData>> recordIteratorFunc;
    private ClosableIterator<RowData> recordIterator;

    CdcFileSplitsIterator(
        HoodieCDCFileSplit[] changes,
        ImageManager imageManager,
        Function<HoodieCDCFileSplit, ClosableIterator<RowData>> recordIteratorFunc) {
      this.fileSplitIterator = Arrays.asList(changes).iterator();
      this.imageManager = imageManager;
      this.recordIteratorFunc = recordIteratorFunc;
    }

    @Override
    public boolean hasNext() {
      if (recordIterator != null) {
        if (recordIterator.hasNext()) {
          return true;
        } else {
          recordIterator.close();
          recordIterator = null;
        }
      }
      if (fileSplitIterator.hasNext()) {
        recordIterator = recordIteratorFunc.apply(fileSplitIterator.next());
        return recordIterator.hasNext();
      }
      return false;
    }

    @Override
    public RowData next() {
      return recordIterator.next();
    }

    @Override
    public void close() {
      if (recordIterator != null) {
        recordIterator.close();
      }
      if (imageManager != null) {
        imageManager.close();
        imageManager = null;
      }
    }
  }

  /** Wraps a base-file parquet iterator and marks every record as {@link RowKind#INSERT}. */
  private static class AddBaseFileIterator implements ClosableIterator<RowData> {
    private ClosableIterator<RowData> nested;
    private RowData currentRecord;

    AddBaseFileIterator(ClosableIterator<RowData> nested) {
      this.nested = nested;
    }

    @Override
    public boolean hasNext() {
      if (nested.hasNext()) {
        currentRecord = nested.next();
        currentRecord.setRowKind(RowKind.INSERT);
        return true;
      }
      return false;
    }

    @Override
    public RowData next() {
      return currentRecord;
    }

    @Override
    public void close() {
      if (nested != null) {
        nested.close();
        nested = null;
      }
    }
  }

  /** Wraps a file-slice iterator and marks every record as {@link RowKind#DELETE}, with projection. */
  private static class RemoveBaseFileIterator implements ClosableIterator<RowData> {
    private ClosableIterator<RowData> nested;
    private final RowDataProjection projection;

    RemoveBaseFileIterator(RowType requiredRowType, int[] requiredPositions, ClosableIterator<RowData> iterator) {
      this.nested = iterator;
      this.projection = RowDataProjection.instance(requiredRowType, requiredPositions);
    }

    @Override
    public boolean hasNext() {
      return nested.hasNext();
    }

    @Override
    public RowData next() {
      RowData row = nested.next();
      row.setRowKind(RowKind.DELETE);
      return projection.project(row);
    }

    @Override
    public void close() {
      if (nested != null) {
        nested.close();
        nested = null;
      }
    }
  }

  /**
   * Handles the {@code LOG_FILE} CDC inference case: compares records from the log file
   * against before-image snapshots to emit INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE events.
   */
  private static class DataLogFileIterator implements ClosableIterator<RowData> {
    private final HoodieSchema tableSchema;
    private final long maxCompactionMemoryInBytes;
    private final ImageManager imageManager;
    private final RowDataProjection projection;
    private final BufferedRecordMerger recordMerger;
    private final ClosableIterator<HoodieRecord<RowData>> logRecordIterator;
    private final DeleteContext deleteContext;
    private final HoodieReaderContext<RowData> readerContext;
    private final String[] orderingFields;
    private final TypedProperties props;

    private ExternalSpillableMap<String, byte[]> beforeImages;
    private RowData currentImage;
    private RowData sideImage;

    DataLogFileIterator(
        long maxCompactionMemoryInBytes,
        ImageManager imageManager,
        HoodieCDCFileSplit cdcFileSplit,
        HoodieSchema tableSchema,
        RowType requiredRowType,
        int[] requiredPositions,
        ClosableIterator<HoodieRecord<RowData>> logRecordIterator,
        HoodieTableMetaClient metaClient,
        HoodieWriteConfig writeConfig) throws IOException {
      this.tableSchema = tableSchema;
      this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
      this.imageManager = imageManager;
      this.projection = HoodieSchemaConverter.convertToRowType(tableSchema).equals(requiredRowType)
              ? null : RowDataProjection.instance(requiredRowType, requiredPositions);
      this.props = writeConfig.getProps();
      this.readerContext = new FlinkReaderContextFactory(metaClient).getContext();
      readerContext.initRecordMerger(props);
      this.orderingFields = ConfigUtils.getOrderingFields(props);
      this.recordMerger = BufferedRecordMergerFactory.create(
          readerContext,
          readerContext.getMergeMode(),
          false,
          Option.of(writeConfig.getRecordMerger()),
          tableSchema,
          Option.ofNullable(Pair.of(metaClient.getTableConfig().getPayloadClass(), writeConfig.getPayloadClass())),
          props,
          metaClient.getTableConfig().getPartialUpdateMode());
      this.logRecordIterator = logRecordIterator;
      this.deleteContext = new DeleteContext(props, tableSchema).withReaderSchema(tableSchema);
      initImages(cdcFileSplit, writeConfig);
    }

    private void initImages(HoodieCDCFileSplit fileSplit, HoodieWriteConfig writeConfig) throws IOException {
      if (fileSplit.getBeforeFileSlice().isPresent() && !fileSplit.getBeforeFileSlice().get().isEmpty()) {
        this.beforeImages = this.imageManager.getOrLoadImages(
            maxCompactionMemoryInBytes, fileSplit.getBeforeFileSlice().get());
      } else {
        this.beforeImages = FormatUtils.spillableMap(writeConfig, maxCompactionMemoryInBytes, getClass().getSimpleName());
      }
    }

    @Override
    public boolean hasNext() {
      if (sideImage != null) {
        currentImage = sideImage;
        sideImage = null;
        return true;
      }
      while (logRecordIterator.hasNext()) {
        HoodieRecord<RowData> record = logRecordIterator.next();
        RowData existed = imageManager.removeImageRecord(record.getRecordKey(), beforeImages);
        if (isDelete(record)) {
          if (existed != null) {
            existed.setRowKind(RowKind.DELETE);
            currentImage = existed;
            return true;
          }
        } else {
          if (existed == null) {
            RowData newRow = record.getData();
            newRow.setRowKind(RowKind.INSERT);
            currentImage = newRow;
            return true;
          } else {
            HoodieOperation operation = HoodieOperation.fromValue(existed.getRowKind().toByteValue());
            HoodieRecord<RowData> historyRecord = new HoodieFlinkRecord(record.getKey(), operation, existed);
            HoodieRecord<RowData> merged = mergeRowWithLog(historyRecord, record).get();
            if (merged.getData() != existed) {
              existed.setRowKind(RowKind.UPDATE_BEFORE);
              currentImage = existed;
              RowData mergedRow = merged.getData();
              mergedRow.setRowKind(RowKind.UPDATE_AFTER);
              imageManager.updateImageRecord(record.getRecordKey(), beforeImages, mergedRow);
              sideImage = mergedRow;
              return true;
            }
          }
        }
      }
      return false;
    }

    @Override
    public RowData next() {
      return projection != null ? projection.project(currentImage) : currentImage;
    }

    @Override
    public void close() {
      logRecordIterator.close();
      imageManager.close();
    }

    @SuppressWarnings("unchecked")
    private Option<HoodieRecord<RowData>> mergeRowWithLog(
        HoodieRecord<RowData> historyRecord, HoodieRecord<RowData> newRecord) {
      try {
        BufferedRecord<RowData> histBuf = BufferedRecords.fromHoodieRecord(
            historyRecord, tableSchema, readerContext.getRecordContext(), props, orderingFields, deleteContext);
        BufferedRecord<RowData> newBuf = BufferedRecords.fromHoodieRecord(
            newRecord, tableSchema, readerContext.getRecordContext(), props, orderingFields, deleteContext);
        BufferedRecord<RowData> merged = recordMerger.finalMerge(histBuf, newBuf);
        return Option.ofNullable(readerContext.getRecordContext()
            .constructHoodieRecord(merged, historyRecord.getPartitionPath()));
      } catch (IOException e) {
        throw new HoodieIOException("Merge base and delta payloads exception", e);
      }
    }

    private boolean isDelete(HoodieRecord<RowData> record) {
      return record.isDelete(deleteContext, CollectionUtils.emptyProps());
    }
  }

  /**
   * Base iterator for CDC log files stored with supplemental logging (AS_IS inference case).
   * Reads {@link HoodieCDCLogRecordIterator} and resolves before/after images using
   * subclass-specific logic.
   */
  private abstract static class BaseImageIterator implements ClosableIterator<RowData> {
    private final HoodieSchema requiredSchema;
    private final int[] requiredPos;
    private final GenericRecordBuilder recordBuilder;
    private final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter;
    private HoodieCDCLogRecordIterator cdcItr;

    private GenericRecord cdcRecord;
    private RowData sideImage;
    private RowData currentImage;

    BaseImageIterator(
        org.apache.hadoop.conf.Configuration hadoopConf,
        String tablePath,
        HoodieSchema tableSchema,
        HoodieSchema requiredSchema,
        RowType requiredRowType,
        HoodieSchema cdcSchema,
        HoodieCDCFileSplit fileSplit) {
      this.requiredSchema = requiredSchema;
      this.requiredPos = computeRequiredPos(tableSchema, requiredSchema);
      this.recordBuilder = new GenericRecordBuilder(requiredSchema.getAvroSchema());
      this.avroToRowDataConverter = AvroToRowDataConverters.createRowConverter(requiredRowType);

      StoragePath hadoopTablePath = new StoragePath(tablePath);
      HoodieStorage storage = HoodieStorageUtils.getStorage(
          tablePath, HadoopFSUtils.getStorageConf(hadoopConf));
      HoodieLogFile[] cdcLogFiles = fileSplit.getCdcFiles().stream()
          .map(cdcFile -> {
            try {
              return new HoodieLogFile(storage.getPathInfo(new StoragePath(hadoopTablePath, cdcFile)));
            } catch (IOException e) {
              throw new HoodieIOException("Failed to get file status for CDC log: " + cdcFile, e);
            }
          })
          .toArray(HoodieLogFile[]::new);
      this.cdcItr = new HoodieCDCLogRecordIterator(storage, cdcLogFiles, cdcSchema);
    }

    private static int[] computeRequiredPos(HoodieSchema tableSchema, HoodieSchema requiredSchema) {
      HoodieSchema dataSchema = HoodieSchemaUtils.removeMetadataFields(tableSchema);
      List<String> fields = dataSchema.getFields().stream()
          .map(HoodieSchemaField::name)
          .collect(Collectors.toList());
      return requiredSchema.getFields().stream()
          .map(f -> fields.indexOf(f.name()))
          .mapToInt(i -> i)
          .toArray();
    }

    @Override
    public boolean hasNext() {
      if (sideImage != null) {
        currentImage = sideImage;
        sideImage = null;
        return true;
      } else if (cdcItr.hasNext()) {
        cdcRecord = (GenericRecord) cdcItr.next();
        String op = String.valueOf(cdcRecord.get(0));
        resolveImage(op);
        return true;
      }
      return false;
    }

    protected abstract RowData getAfterImage(RowKind rowKind, GenericRecord cdcRecord);

    protected abstract RowData getBeforeImage(RowKind rowKind, GenericRecord cdcRecord);

    @Override
    public RowData next() {
      return currentImage;
    }

    @Override
    public void close() {
      if (cdcItr != null) {
        cdcItr.close();
        cdcItr = null;
      }
    }

    private void resolveImage(String op) {
      switch (op) {
        case "i":
          currentImage = getAfterImage(RowKind.INSERT, cdcRecord);
          break;
        case "u":
          currentImage = getBeforeImage(RowKind.UPDATE_BEFORE, cdcRecord);
          sideImage = getAfterImage(RowKind.UPDATE_AFTER, cdcRecord);
          break;
        case "d":
          currentImage = getBeforeImage(RowKind.DELETE, cdcRecord);
          break;
        default:
          throw new AssertionError("Unexpected CDC operation: " + op);
      }
    }

    protected RowData resolveAvro(RowKind rowKind, GenericRecord avroRecord) {
      GenericRecord requiredAvroRecord = buildAvroRecordBySchema(
          avroRecord, requiredSchema, requiredPos, recordBuilder);
      RowData resolved = (RowData) avroToRowDataConverter.convert(requiredAvroRecord);
      resolved.setRowKind(rowKind);
      return resolved;
    }
  }

  /** Reads CDC log files that contain both before and after images ({@code DATA_BEFORE_AFTER} mode). */
  private static class BeforeAfterImageIterator extends BaseImageIterator {
    BeforeAfterImageIterator(
        org.apache.hadoop.conf.Configuration hadoopConf,
        String tablePath,
        HoodieSchema tableSchema,
        HoodieSchema requiredSchema,
        RowType requiredRowType,
        HoodieSchema cdcSchema,
        HoodieCDCFileSplit fileSplit) {
      super(hadoopConf, tablePath, tableSchema, requiredSchema, requiredRowType, cdcSchema, fileSplit);
    }

    @Override
    protected RowData getAfterImage(RowKind rowKind, GenericRecord cdcRecord) {
      return resolveAvro(rowKind, (GenericRecord) cdcRecord.get(3));
    }

    @Override
    protected RowData getBeforeImage(RowKind rowKind, GenericRecord cdcRecord) {
      return resolveAvro(rowKind, (GenericRecord) cdcRecord.get(2));
    }
  }

  /**
   * Reads CDC log files containing op + key + before_image ({@code DATA_BEFORE} mode).
   * The after-image is loaded from the after file-slice via the {@link ImageManager}.
   */
  private static class BeforeImageIterator extends BaseImageIterator {
    protected ExternalSpillableMap<String, byte[]> afterImages;
    protected final long maxCompactionMemoryInBytes;
    protected final RowDataProjection projection;
    protected final ImageManager imageManager;

    BeforeImageIterator(
        org.apache.flink.configuration.Configuration flinkConf,
        org.apache.hadoop.conf.Configuration hadoopConf,
        String tablePath,
        HoodieSchema tableSchema,
        HoodieSchema requiredSchema,
        RowType requiredRowType,
        long maxCompactionMemoryInBytes,
        HoodieSchema cdcSchema,
        HoodieCDCFileSplit fileSplit,
        ImageManager imageManager) throws IOException {
      super(hadoopConf, tablePath, tableSchema, requiredSchema, requiredRowType, cdcSchema, fileSplit);
      this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
      this.projection = RowDataProjection.instance(requiredRowType,
          computePositions(tableSchema, requiredRowType));
      this.imageManager = imageManager;
      initImages(fileSplit);
    }

    protected void initImages(HoodieCDCFileSplit fileSplit) throws IOException {
      ValidationUtils.checkState(fileSplit.getAfterFileSlice().isPresent(),
          "Current file slice does not exist for instant: " + fileSplit.getInstant());
      this.afterImages = imageManager.getOrLoadImages(
          maxCompactionMemoryInBytes, fileSplit.getAfterFileSlice().get());
    }

    @Override
    protected RowData getAfterImage(RowKind rowKind, GenericRecord cdcRecord) {
      String recordKey = cdcRecord.get(1).toString();
      RowData row = imageManager.getImageRecord(recordKey, afterImages, rowKind);
      row.setRowKind(rowKind);
      return projection.project(row);
    }

    @Override
    protected RowData getBeforeImage(RowKind rowKind, GenericRecord cdcRecord) {
      return resolveAvro(rowKind, (GenericRecord) cdcRecord.get(2));
    }

    private static int[] computePositions(HoodieSchema tableSchema, RowType requiredRowType) {
      List<String> allFields = tableSchema.getFields().stream()
          .map(HoodieSchemaField::name)
          .collect(Collectors.toList());
      return requiredRowType.getFieldNames().stream()
          .map(allFields::indexOf)
          .mapToInt(i -> i)
          .toArray();
    }
  }

  /**
   * Reads CDC log files containing only op + key ({@code OP_KEY_ONLY} mode).
   * Both before and after images are loaded from file-slice snapshots via {@link ImageManager}.
   */
  private static class RecordKeyImageIterator extends BeforeImageIterator {
    protected ExternalSpillableMap<String, byte[]> beforeImages;

    RecordKeyImageIterator(
        org.apache.flink.configuration.Configuration flinkConf,
        org.apache.hadoop.conf.Configuration hadoopConf,
        String tablePath,
        HoodieSchema tableSchema,
        HoodieSchema requiredSchema,
        RowType requiredRowType,
        long maxCompactionMemoryInBytes,
        HoodieSchema cdcSchema,
        HoodieCDCFileSplit fileSplit,
        ImageManager imageManager) throws IOException {
      super(flinkConf, hadoopConf, tablePath, tableSchema, requiredSchema, requiredRowType,
          maxCompactionMemoryInBytes, cdcSchema, fileSplit, imageManager);
    }

    @Override
    protected void initImages(HoodieCDCFileSplit fileSplit) throws IOException {
      super.initImages(fileSplit);
      ValidationUtils.checkState(fileSplit.getBeforeFileSlice().isPresent(),
          "Before file slice does not exist for instant: " + fileSplit.getInstant());
      this.beforeImages = imageManager.getOrLoadImages(
          maxCompactionMemoryInBytes, fileSplit.getBeforeFileSlice().get());
    }

    @Override
    protected RowData getBeforeImage(RowKind rowKind, GenericRecord cdcRecord) {
      String recordKey = cdcRecord.get(1).toString();
      RowData row = imageManager.getImageRecord(recordKey, beforeImages, rowKind);
      row.setRowKind(rowKind);
      return projection.project(row);
    }
  }

  /** Handles the {@code REPLACE_COMMIT} CDC inference case: emits all records from before-slice as DELETE. */
  private static class ReplaceCommitIterator implements ClosableIterator<RowData> {
    private final ClosableIterator<RowData> itr;
    private final RowDataProjection projection;

    ReplaceCommitIterator(
        org.apache.flink.configuration.Configuration flinkConf,
        String tablePath,
        RowType requiredRowType,
        int[] requiredPositions,
        long maxCompactionMemoryInBytes,
        HoodieCDCFileSplit fileSplit,
        Function<MergeOnReadInputSplit, ClosableIterator<RowData>> splitIteratorFunc) {
      ValidationUtils.checkState(fileSplit.getBeforeFileSlice().isPresent(),
          "Before file slice does not exist for instant: " + fileSplit.getInstant());
      MergeOnReadInputSplit inputSplit = CdcInputFormat.fileSlice2Split(
          tablePath, fileSplit.getBeforeFileSlice().get(), maxCompactionMemoryInBytes);
      this.itr = splitIteratorFunc.apply(inputSplit);
      this.projection = RowDataProjection.instance(requiredRowType, requiredPositions);
    }

    @Override
    public boolean hasNext() {
      return itr.hasNext();
    }

    @Override
    public RowData next() {
      RowData row = itr.next();
      row.setRowKind(RowKind.DELETE);
      return projection.project(row);
    }

    @Override
    public void close() {
      itr.close();
    }
  }

  // -------------------------------------------------------------------------
  //  ImageManager - caches full-schema row images keyed by record key
  // -------------------------------------------------------------------------

  /**
   * Manages serialized before/after image snapshots for a file group, cached by instant time.
   * At most two versions (before and after) are kept in memory; older entries are spilled to disk.
   */
  private static class ImageManager implements AutoCloseable {
    private final HoodieWriteConfig writeConfig;
    private final RowDataSerializer serializer;
    private final Function<MergeOnReadInputSplit, ClosableIterator<RowData>> splitIteratorFunc;
    private final Map<String, ExternalSpillableMap<String, byte[]>> cache;

    ImageManager(
        RowType rowType,
        HoodieWriteConfig writeConfig,
        Function<MergeOnReadInputSplit, ClosableIterator<RowData>> splitIteratorFunc) {
      this.serializer = new RowDataSerializer(rowType);
      this.writeConfig = writeConfig;
      this.splitIteratorFunc = splitIteratorFunc;
      this.cache = new TreeMap<>();
    }

    ExternalSpillableMap<String, byte[]> getOrLoadImages(
        long maxCompactionMemoryInBytes, FileSlice fileSlice) throws IOException {
      final String instant = fileSlice.getBaseInstantTime();
      if (cache.containsKey(instant)) {
        return cache.get(instant);
      }
      if (cache.size() > 1) {
        String oldest = cache.keySet().iterator().next();
        cache.remove(oldest).close();
      }
      ExternalSpillableMap<String, byte[]> images = loadImageRecords(maxCompactionMemoryInBytes, fileSlice);
      cache.put(instant, images);
      return images;
    }

    private ExternalSpillableMap<String, byte[]> loadImageRecords(
        long maxCompactionMemoryInBytes, FileSlice fileSlice) throws IOException {
      MergeOnReadInputSplit inputSplit = CdcInputFormat.fileSlice2Split(
          writeConfig.getBasePath(), fileSlice, maxCompactionMemoryInBytes);
      ExternalSpillableMap<String, byte[]> imageRecordsMap =
          FormatUtils.spillableMap(writeConfig, maxCompactionMemoryInBytes, getClass().getSimpleName());
      try (ClosableIterator<RowData> itr = splitIteratorFunc.apply(inputSplit)) {
        while (itr.hasNext()) {
          RowData row = itr.next();
          String recordKey = row.getString(HOODIE_RECORD_KEY_COL_POS).toString();
          ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
          serializer.serialize(row, new BytesArrayOutputView(baos));
          imageRecordsMap.put(recordKey, baos.toByteArray());
        }
      }
      return imageRecordsMap;
    }

    RowData getImageRecord(
        String recordKey, ExternalSpillableMap<String, byte[]> imageCache, RowKind rowKind) {
      byte[] bytes = imageCache.get(recordKey);
      ValidationUtils.checkState(bytes != null,
          "Key " + recordKey + " does not exist in current file group image");
      try {
        RowData row = serializer.deserialize(new BytesArrayInputView(bytes));
        row.setRowKind(rowKind);
        return row;
      } catch (IOException e) {
        throw new HoodieException("Failed to deserialize image record for key: " + recordKey, e);
      }
    }

    void updateImageRecord(
        String recordKey, ExternalSpillableMap<String, byte[]> imageCache, RowData row) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
      try {
        serializer.serialize(row, new BytesArrayOutputView(baos));
      } catch (IOException e) {
        throw new HoodieException("Failed to serialize image record for key: " + recordKey, e);
      }
      imageCache.put(recordKey, baos.toByteArray());
    }

    RowData removeImageRecord(
        String recordKey, ExternalSpillableMap<String, byte[]> imageCache) {
      byte[] bytes = imageCache.remove(recordKey);
      if (bytes == null) {
        return null;
      }
      try {
        return serializer.deserialize(new BytesArrayInputView(bytes));
      } catch (IOException e) {
        throw new HoodieException("Failed to deserialize image record for key: " + recordKey, e);
      }
    }

    @Override
    public void close() {
      cache.values().forEach(ExternalSpillableMap::close);
      cache.clear();
    }
  }

  // -------------------------------------------------------------------------
  //  I/O view adapters for RowDataSerializer
  // -------------------------------------------------------------------------

  private static final class BytesArrayInputView extends DataInputStream
      implements org.apache.flink.core.memory.DataInputView {
    BytesArrayInputView(byte[] data) {
      super(new ByteArrayInputStream(data));
    }

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
      while (numBytes > 0) {
        int skipped = skipBytes(numBytes);
        numBytes -= skipped;
      }
    }
  }

  private static final class BytesArrayOutputView extends DataOutputStream
      implements org.apache.flink.core.memory.DataOutputView {
    BytesArrayOutputView(ByteArrayOutputStream baos) {
      super(baos);
    }

    @Override
    public void skipBytesToWrite(int numBytes) throws IOException {
      for (int i = 0; i < numBytes; i++) {
        write(0);
      }
    }

    @Override
    public void write(org.apache.flink.core.memory.DataInputView source, int numBytes) throws IOException {
      byte[] buffer = new byte[numBytes];
      source.readFully(buffer);
      write(buffer);
    }
  }
}
