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

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.source.reader.BatchRecords;
import org.apache.hudi.source.reader.HoodieRecordWithPosition;
import org.apache.hudi.source.split.HoodieCdcSourceSplit;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.RecordIterators;
import org.apache.hudi.table.format.cdc.CdcImageManager;
import org.apache.hudi.table.format.cdc.CdcInputFormat;
import org.apache.hudi.table.format.cdc.CdcIterators;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.util.StreamerUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * CDC reader function for source V2. Reads CDC splits ({@link HoodieCdcSourceSplit}) and
 * emits change-log {@link RowData} records tagged with the appropriate {@link org.apache.flink.types.RowKind}.
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

    CdcImageManager imageManager = new CdcImageManager(
        tableState.getRowType(), getWriteConfig(), this::getFileSliceIterator);

    Function<HoodieCDCFileSplit, ClosableIterator<RowData>> recordIteratorFunc =
        cdcFileSplit -> createRecordIteratorSafe(
            cdcSplit.getTablePath(),
            cdcSplit.getMaxCompactionMemoryInBytes(),
            cdcFileSplit,
            mode,
            imageManager);

    currentIterator = new CdcIterators.CdcFileSplitsIterator(cdcSplit.getChanges(), imageManager, recordIteratorFunc);
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
      CdcImageManager imageManager) {
    try {
      return createRecordIterator(tablePath, maxCompactionMemoryInBytes, fileSplit, mode, imageManager);
    } catch (IOException e) {
      throw new HoodieException("Failed to create CDC record iterator for split: " + fileSplit, e);
    }
  }

  private ClosableIterator<RowData> createRecordIterator(
      String tablePath,
      long maxCompactionMemoryInBytes,
      HoodieCDCFileSplit fileSplit,
      HoodieCDCSupplementalLoggingMode mode,
      CdcImageManager imageManager) throws IOException {

    final HoodieSchema tableSchema = HoodieSchema.parse(tableState.getTableSchema());
    final HoodieSchema requiredSchema = HoodieSchema.parse(tableState.getRequiredSchema());
    switch (fileSplit.getCdcInferCase()) {
      case BASE_FILE_INSERT: {
        ValidationUtils.checkState(fileSplit.getCdcFiles() != null && fileSplit.getCdcFiles().size() == 1,
            "CDC file path should exist and be singleton for BASE_FILE_INSERT");
        String path = new Path(tablePath, fileSplit.getCdcFiles().get(0)).toString();
        return new CdcIterators.AddBaseFileIterator(getBaseFileIterator(path));
      }
      case BASE_FILE_DELETE: {
        ValidationUtils.checkState(fileSplit.getBeforeFileSlice().isPresent(),
            "Before file slice should exist for BASE_FILE_DELETE");
        MergeOnReadInputSplit inputSplit = CdcIterators.fileSlice2Split(
            tablePath, fileSplit.getBeforeFileSlice().get(), maxCompactionMemoryInBytes);
        return new CdcIterators.RemoveBaseFileIterator(
            tableState.getRequiredRowType(), tableState.getRequiredPositions(), getFileSliceIterator(inputSplit));
      }
      case AS_IS: {
        HoodieSchema dataSchema = HoodieSchemaUtils.removeMetadataFields(tableSchema);
        HoodieSchema cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(mode, dataSchema);
        switch (mode) {
          case DATA_BEFORE_AFTER:
            return new CdcIterators.BeforeAfterImageIterator(
                getHadoopConf(), tablePath, tableSchema, requiredSchema,
                tableState.getRequiredRowType(), cdcSchema, fileSplit);
          case DATA_BEFORE:
            return new CdcIterators.BeforeImageIterator(
                getHadoopConf(), tablePath, tableSchema, requiredSchema,
                tableState.getRequiredRowType(), tableState.getRequiredPositions(),
                maxCompactionMemoryInBytes, cdcSchema, fileSplit, imageManager);
          case OP_KEY_ONLY:
            return new CdcIterators.RecordKeyImageIterator(
                getHadoopConf(), tablePath, tableSchema, requiredSchema,
                tableState.getRequiredRowType(), tableState.getRequiredPositions(),
                maxCompactionMemoryInBytes, cdcSchema, fileSplit, imageManager);
          default:
            throw new AssertionError("Unexpected CDC supplemental logging mode: " + mode);
        }
      }
      case LOG_FILE: {
        ValidationUtils.checkState(fileSplit.getCdcFiles() != null && fileSplit.getCdcFiles().size() == 1,
            "CDC file path should exist and be singleton for LOG_FILE");
        String logFilePath = new Path(tablePath, fileSplit.getCdcFiles().get(0)).toString();
        MergeOnReadInputSplit split = CdcIterators.singleLogFile2Split(tablePath, logFilePath, maxCompactionMemoryInBytes);
        ClosableIterator<HoodieRecord<RowData>> recordIterator = getFileSliceHoodieRecordIterator(split);
        return new CdcIterators.DataLogFileIterator(
            maxCompactionMemoryInBytes, imageManager, fileSplit, tableSchema,
            tableState.getRequiredRowType(), tableState.getRequiredPositions(),
            recordIterator, getMetaClient(), getWriteConfig());
      }
      case REPLACE_COMMIT: {
        return new CdcIterators.ReplaceCommitIterator(
            tablePath, tableState.getRequiredRowType(), tableState.getRequiredPositions(),
            maxCompactionMemoryInBytes, fileSplit, this::getFileSliceIterator);
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

  private HoodieTableMetaClient getMetaClient() {
    if (metaClient == null) {
      metaClient = StreamerUtil.metaClientForReader(conf, getHadoopConf());
    }
    return metaClient;
  }
}
