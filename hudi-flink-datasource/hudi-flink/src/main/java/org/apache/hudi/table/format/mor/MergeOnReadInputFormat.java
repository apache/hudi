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

package org.apache.hudi.table.format.mor;

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.source.ExpressionPredicates.Predicate;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.FlinkRowDataReaderContext;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.RecordIterators;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.FlinkClientUtil;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.RowDataProjection;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.util.StringToRowDataConverter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_COMMIT_TIME_COL_POS;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS;
import static org.apache.hudi.table.format.FormatUtils.buildAvroRecordBySchema;

/**
 * The base InputFormat class to read from Hoodie data + log files.
 *
 * <P>Use {@code ParquetRecordReader} to read files instead of {@link org.apache.flink.core.fs.FSDataInputStream},
 * overrides {@link #createInputSplits(int)} and {@link #close()} to change the behaviors.
 */
public class MergeOnReadInputFormat
    extends RichInputFormat<RowData, MergeOnReadInputSplit> {

  private static final long serialVersionUID = 1L;

  protected final Configuration conf;

  protected transient org.apache.hadoop.conf.Configuration hadoopConf;

  protected final MergeOnReadTableState tableState;

  /**
   * Uniform iterator view for the underneath records.
   */
  private transient ClosableIterator<RowData> iterator;

  // for project push down
  /**
   * Full table names.
   */
  private final List<String> fieldNames;

  /**
   * Full field data types.
   */
  private final List<DataType> fieldTypes;

  /**
   * Required field positions.
   */
  private final int[] requiredPos;

  // for predicate push down
  private final List<Predicate> predicates;

  // for limit push down
  /**
   * Limit for the reader, -1 when the reading is not limited.
   */
  private final long limit;

  /**
   * Recording the current read count for limit check.
   */
  private long currentReadCount = 0;

  /**
   * Flag saying whether to emit the deletes. In streaming read mode, downstream
   * operators need the DELETE messages to retract the legacy accumulator.
   */
  private boolean emitDelete;

  /**
   * Flag saying whether the input format has been closed.
   */
  private boolean closed = true;

  protected final InternalSchemaManager internalSchemaManager;

  /**
   * The table metadata client
   */
  private transient HoodieTableMetaClient metaClient;

  /**
   * The hoodie write configuration.
   */
  private transient HoodieWriteConfig writeConfig;

  protected MergeOnReadInputFormat(
      Configuration conf,
      MergeOnReadTableState tableState,
      List<DataType> fieldTypes,
      List<Predicate> predicates,
      long limit,
      boolean emitDelete,
      InternalSchemaManager internalSchemaManager) {
    this.conf = conf;
    this.tableState = tableState;
    this.fieldNames = tableState.getRowType().getFieldNames();
    this.fieldTypes = fieldTypes;
    // Needs improvement: this requiredPos is only suitable for parquet reader,
    // because we need to
    this.requiredPos = tableState.getRequiredPositions();
    this.predicates = predicates;
    this.limit = limit;
    this.emitDelete = emitDelete;
    this.internalSchemaManager = internalSchemaManager;
  }

  /**
   * Returns the builder for {@link MergeOnReadInputFormat}.
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void open(MergeOnReadInputSplit split) throws IOException {
    this.currentReadCount = 0L;
    this.closed = false;
    this.hadoopConf = HadoopConfigurations.getHadoopConf(this.conf);
    this.metaClient = StreamerUtil.metaClientForReader(this.conf, hadoopConf);
    this.writeConfig = FlinkWriteClients.getHoodieClientConfig(this.conf);
    this.iterator = initIterator(split);
    mayShiftInputSplit(split);
  }

  protected ClosableIterator<RowData> initIterator(MergeOnReadInputSplit split) throws IOException {
    if (!(split.getLogPaths().isPresent() && split.getLogPaths().get().size() > 0)) {
      if (split.getInstantRange().isPresent()) {
        // base file only with commit time filtering
        return new BaseFileOnlyFilteringIterator(
            split.getInstantRange().get(),
            this.tableState.getRequiredRowType(),
            this.requiredPos,
            getBaseFileIterator(split.getBasePath().get(), getRequiredPosWithCommitTime(this.requiredPos)));
      } else {
        // base file only
        return getBaseFileIterator(split.getBasePath().get());
      }
    } else if (!split.getBasePath().isPresent()) {
      // log files only
      if (OptionsResolver.emitDeletes(conf)) {
        return new LogFileOnlyIterator(getUnMergedLogFileIterator(split));
      } else {
        return new LogFileOnlyIterator(getLogFileIterator(split));
      }
    } else if (split.getMergeType().equals(FlinkOptions.REALTIME_SKIP_MERGE)) {
      return new SkipMergeIterator(
          getBaseFileIterator(split.getBasePath().get()),
          getLogFileIterator(split));
    } else if (split.getMergeType().equals(FlinkOptions.REALTIME_PAYLOAD_COMBINE)) {
      return new MergeIterator(
          conf,
          hadoopConf,
          split,
          this.tableState.getRowType(),
          this.tableState.getRequiredRowType(),
          new Schema.Parser().parse(this.tableState.getAvroSchema()),
          new Schema.Parser().parse(this.tableState.getRequiredAvroSchema()),
          internalSchemaManager.getQuerySchema(),
          this.requiredPos,
          this.emitDelete,
          this.tableState.getOperationPos(),
          getBaseFileIteratorWithMetadata(split.getBasePath().get()));
    } else {
      throw new HoodieException("Unable to select an Iterator to read the Hoodie MOR File Split for "
          + "file path: " + split.getBasePath()
          + "log paths: " + split.getLogPaths()
          + "hoodie table path: " + split.getTablePath()
          + "flink partition Index: " + split.getSplitNumber()
          + "merge type: " + split.getMergeType());
    }
  }

  @Override
  public void configure(Configuration configuration) {
    // no operation
    // may support nested files in the future.
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics baseStatistics) {
    // statistics not supported yet.
    return null;
  }

  @Override
  public MergeOnReadInputSplit[] createInputSplits(int minNumSplits) {
    return this.tableState.getInputSplits().toArray(new MergeOnReadInputSplit[0]);
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(MergeOnReadInputSplit[] mergeOnReadInputSplits) {
    return new DefaultInputSplitAssigner(mergeOnReadInputSplits);
  }

  @Override
  public boolean reachedEnd() throws IOException {
    if (limit > 0 && currentReadCount >= limit) {
      return true;
    } else {
      // log file reaches end ?
      return !this.iterator.hasNext();
    }
  }

  @Override
  public RowData nextRecord(RowData o) {
    currentReadCount++;
    return this.iterator.next();
  }

  @Override
  public void close() throws IOException {
    if (this.iterator != null) {
      this.iterator.close();
    }
    this.iterator = null;
    this.closed = true;
  }

  public boolean isClosed() {
    return this.closed;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Shifts the input split by its consumed records number.
   *
   * <p>Note: This action is time-consuming.
   */
  private void mayShiftInputSplit(MergeOnReadInputSplit split) throws IOException {
    if (split.isConsumed()) {
      // if the input split has been consumed before,
      // shift the input split with consumed num of records first
      for (long i = 0; i < split.getConsumed() && !reachedEnd(); i++) {
        nextRecord(null);
      }
    }
  }

  protected ClosableIterator<RowData> getBaseFileIteratorWithMetadata(String path) {
    try {
      return getBaseFileIterator(path, IntStream.range(0, this.tableState.getRowType().getFieldCount()).toArray());
    } catch (IOException e) {
      throw new HoodieException("Get reader error for path: " + path, e);
    }
  }

  protected ClosableIterator<RowData> getBaseFileIterator(String path) throws IOException {
    return getBaseFileIterator(path, this.requiredPos);
  }

  private ClosableIterator<RowData> getBaseFileIterator(String path, int[] requiredPos) throws IOException {
    LinkedHashMap<String, Object> partObjects = FilePathUtils.generatePartitionSpecs(
        path,
        fieldNames,
        fieldTypes,
        conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME),
        conf.getString(FlinkOptions.PARTITION_PATH_FIELD),
        conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING)
    );

    return RecordIterators.getParquetRecordIterator(
        internalSchemaManager,
        this.conf.getBoolean(FlinkOptions.READ_UTC_TIMEZONE),
        true,
        HadoopConfigurations.getParquetConf(this.conf, hadoopConf),
        fieldNames.toArray(new String[0]),
        fieldTypes.toArray(new DataType[0]),
        partObjects,
        requiredPos,
        2048,
        new org.apache.flink.core.fs.Path(path),
        0,
        Long.MAX_VALUE, // read the whole file
        predicates);
  }

  private ClosableIterator<RowData> getLogFileIterator(MergeOnReadInputSplit split) {
    final Schema tableSchema = new Schema.Parser().parse(tableState.getAvroSchema());
    final Schema requiredSchema = new Schema.Parser().parse(tableState.getRequiredAvroSchema());
    final GenericRecordBuilder recordBuilder = new GenericRecordBuilder(requiredSchema);
    final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter =
        AvroToRowDataConverters.createRowConverter(tableState.getRequiredRowType(), conf.getBoolean(FlinkOptions.READ_UTC_TIMEZONE));
    final HoodieMergedLogRecordScanner scanner = FormatUtils.logScanner(split, tableSchema, internalSchemaManager.getQuerySchema(), conf, hadoopConf);
    final Iterator<String> logRecordsKeyIterator = scanner.getRecords().keySet().iterator();
    final int[] pkOffset = tableState.getPkOffsetsInRequired();
    // flag saying whether the pk semantics has been dropped by user specified
    // projections. For e.g, if the pk fields are [a, b] but user only select a,
    // then the pk semantics is lost.
    final boolean pkSemanticLost = Arrays.stream(pkOffset).anyMatch(offset -> offset == -1);
    final LogicalType[] pkTypes = pkSemanticLost ? null : tableState.getPkTypes(pkOffset);
    final StringToRowDataConverter converter = pkSemanticLost ? null : new StringToRowDataConverter(pkTypes);

    return new ClosableIterator<RowData>() {
      private RowData currentRecord;

      @Override
      public boolean hasNext() {
        while (logRecordsKeyIterator.hasNext()) {
          String curAvroKey = logRecordsKeyIterator.next();
          Option<IndexedRecord> curAvroRecord = null;
          final HoodieAvroRecord<?> hoodieRecord = (HoodieAvroRecord) scanner.getRecords().get(curAvroKey);
          try {
            curAvroRecord = hoodieRecord.getData().getInsertValue(tableSchema);
          } catch (IOException e) {
            throw new HoodieException("Get avro insert value error for key: " + curAvroKey, e);
          }
          if (!curAvroRecord.isPresent()) {
            // delete record found
            if (emitDelete && !pkSemanticLost) {
              GenericRowData delete = new GenericRowData(tableState.getRequiredRowType().getFieldCount());

              final String recordKey = hoodieRecord.getRecordKey();
              final String[] pkFields = KeyGenUtils.extractRecordKeys(recordKey);
              final Object[] converted = converter.convert(pkFields);
              for (int i = 0; i < pkOffset.length; i++) {
                delete.setField(pkOffset[i], converted[i]);
              }
              delete.setRowKind(RowKind.DELETE);

              this.currentRecord = delete;
              return true;
            }
            // skipping if the condition is unsatisfied
            // continue;
          } else {
            final IndexedRecord avroRecord = curAvroRecord.get();
            final RowKind rowKind = FormatUtils.getRowKindSafely(avroRecord, tableState.getOperationPos());
            if (rowKind == RowKind.DELETE && !emitDelete) {
              // skip the delete record
              continue;
            }
            GenericRecord requiredAvroRecord = buildAvroRecordBySchema(
                avroRecord,
                requiredSchema,
                requiredPos,
                recordBuilder);
            currentRecord = (RowData) avroToRowDataConverter.convert(requiredAvroRecord);
            currentRecord.setRowKind(rowKind);
            return true;
          }
        }
        return false;
      }

      @Override
      public RowData next() {
        return currentRecord;
      }

      @Override
      public void close() {
        scanner.close();
      }
    };
  }

  /**
   * Get record iterator using {@link HoodieFileGroupReader}.
   *
   * @param split input split
   * @return {@link RowData} iterator.
   */
  private ClosableIterator<RowData> getUnMergedLogFileIterator(MergeOnReadInputSplit split) {
    final Schema tableSchema = new Schema.Parser().parse(tableState.getAvroSchema());
    final Schema requiredSchema = new Schema.Parser().parse(tableState.getRequiredAvroSchema());

    FileSlice fileSlice = new FileSlice(
        // partitionPath is not needed for FG reader on Flink
        new HoodieFileGroupId("", split.getFileId()),
        // baseInstantTime in FileSlice is not used in FG reader
        "",
        split.getBasePath().map(HoodieBaseFile::new).orElse(null),
        split.getLogPaths().map(logFiles -> logFiles.stream().map(HoodieLogFile::new).collect(Collectors.toList())).orElse(Collections.emptyList()));

    FlinkRowDataReaderContext readerContext =
        new FlinkRowDataReaderContext(
            HadoopFSUtils.getStorageConf(hadoopConf),
            () -> internalSchemaManager,
            predicates,
            metaClient.getTableConfig());
    TypedProperties typedProps = FlinkClientUtil.getMergedTableAndWriteProps(metaClient.getTableConfig(), writeConfig);
    typedProps.put(HoodieReaderConfig.MERGE_TYPE.key(), HoodieReaderConfig.REALTIME_SKIP_MERGE);

    try (HoodieFileGroupReader<RowData> fileGroupReader = HoodieFileGroupReader.<RowData>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metaClient)
        .withLatestCommitTime(split.getLatestCommit())
        .withFileSlice(fileSlice)
        .withDataSchema(tableSchema)
        .withRequestedSchema(requiredSchema)
        .withInternalSchema(Option.ofNullable(internalSchemaManager.getQuerySchema()))
        .withProps(typedProps)
        .withShouldUseRecordPosition(false)
        .build()) {
      return fileGroupReader.getClosableIterator();
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to compact file slice: " + fileSlice, e);
    }
  }

  protected static Option<IndexedRecord> getInsertVal(HoodieAvroRecord<?> hoodieRecord, Schema tableSchema) {
    try {
      return hoodieRecord.getData().getInsertValue(tableSchema);
    } catch (IOException e) {
      throw new HoodieException("Get avro insert value error for key: " + hoodieRecord.getRecordKey(), e);
    }
  }

  protected ClosableIterator<RowData> getFullLogFileIterator(MergeOnReadInputSplit split) {
    final Schema tableSchema = new Schema.Parser().parse(tableState.getAvroSchema());
    final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter =
        AvroToRowDataConverters.createRowConverter(tableState.getRowType(), conf.getBoolean(FlinkOptions.READ_UTC_TIMEZONE));
    final HoodieMergedLogRecordScanner scanner = FormatUtils.logScanner(split, tableSchema, InternalSchema.getEmptyInternalSchema(), conf, hadoopConf);
    final Iterator<String> logRecordsKeyIterator = scanner.getRecords().keySet().iterator();

    return new ClosableIterator<RowData>() {
      private RowData currentRecord;

      @Override
      public boolean hasNext() {
        while (logRecordsKeyIterator.hasNext()) {
          String curAvroKey = logRecordsKeyIterator.next();
          Option<IndexedRecord> curAvroRecord = null;
          final HoodieAvroRecord<?> hoodieRecord = (HoodieAvroRecord) scanner.getRecords().get(curAvroKey);
          try {
            curAvroRecord = hoodieRecord.getData().getInsertValue(tableSchema);
          } catch (IOException e) {
            throw new HoodieException("Get avro insert value error for key: " + curAvroKey, e);
          }
          if (curAvroRecord.isPresent()) {
            final IndexedRecord avroRecord = curAvroRecord.get();
            final RowKind rowKind = FormatUtils.getRowKindSafely(avroRecord, tableState.getOperationPos());
            if (rowKind == RowKind.DELETE) {
              // skip the delete record
              continue;
            }
            currentRecord = (RowData) avroToRowDataConverter.convert(avroRecord);
            currentRecord.setRowKind(rowKind);
            return true;
          }
          // else:
          // delete record found
          // skipping if the condition is unsatisfied
          // continue;

        }
        return false;
      }

      @Override
      public RowData next() {
        return currentRecord;
      }

      @Override
      public void close() {
        scanner.close();
      }
    };
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------
  /**
   * Base record iterator with instant time filtering.
   */
  static class BaseFileOnlyFilteringIterator implements ClosableIterator<RowData> {
    // base file record iterator
    private final ClosableIterator<RowData> nested;
    private final InstantRange instantRange;
    private final RowDataProjection projection;

    private RowData currentRecord;

    private int commitTimePos;

    BaseFileOnlyFilteringIterator(
        InstantRange instantRange,
        RowType requiredRowType,
        int[] requiredPos,
        ClosableIterator<RowData> nested) {
      this.nested = nested;
      this.instantRange = instantRange;
      this.commitTimePos = getCommitTimePos(requiredPos);
      int[] positions;
      if (commitTimePos < 0) {
        commitTimePos = 0;
        positions = IntStream.range(1, 1 + requiredPos.length).toArray();
      } else {
        positions = IntStream.range(0, requiredPos.length).toArray();
      }
      this.projection = RowDataProjection.instance(requiredRowType, positions);
    }

    @Override
    public boolean hasNext() {
      while (this.nested.hasNext()) {
        currentRecord = this.nested.next();
        boolean isInRange = instantRange.isInRange(currentRecord.getString(commitTimePos).toString());
        if (isInRange) {
          return true;
        }
      }
      return false;
    }

    @Override
    public RowData next() {
      // can promote: no need to project with null instant range
      return projection.project(currentRecord);
    }

    @Override
    public void close() {
      if (this.nested != null) {
        this.nested.close();
      }
    }
  }

  protected static class LogFileOnlyIterator implements ClosableIterator<RowData> {
    // iterator for log files
    private final ClosableIterator<RowData> iterator;

    public LogFileOnlyIterator(ClosableIterator<RowData> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return this.iterator.hasNext();
    }

    @Override
    public RowData next() {
      return this.iterator.next();
    }

    @Override
    public void close() {
      if (this.iterator != null) {
        this.iterator.close();
      }
    }
  }

  static class SkipMergeIterator implements ClosableIterator<RowData> {
    // base file record iterator
    private final ClosableIterator<RowData> nested;
    // iterator for log files
    private final ClosableIterator<RowData> iterator;

    private RowData currentRecord;

    SkipMergeIterator(ClosableIterator<RowData> nested, ClosableIterator<RowData> iterator) {
      this.nested = nested;
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      if (this.nested.hasNext()) {
        currentRecord = this.nested.next();
        return true;
      }
      if (this.iterator.hasNext()) {
        currentRecord = this.iterator.next();
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
      if (this.nested != null) {
        this.nested.close();
      }
      if (this.iterator != null) {
        this.iterator.close();
      }
    }
  }

  protected static class MergeIterator implements ClosableIterator<RowData> {
    // base file record iterator
    private final ClosableIterator<RowData> nested;
    // log keys used for merging
    private final Iterator<String> logKeysIterator;
    // scanner
    private final HoodieMergedLogRecordScanner scanner;

    private final Schema tableSchema;
    private final boolean emitDelete;
    private final int operationPos;
    private final RowDataToAvroConverters.RowDataToAvroConverter rowDataToAvroConverter;
    private final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter;

    private final Option<RowDataProjection> projection;
    private final Option<Function<IndexedRecord, GenericRecord>> avroProjection;

    private final InstantRange instantRange;

    private final HoodieRecordMerger recordMerger;

    private final Set<String> keyToSkip = new HashSet<>();

    private final TypedProperties payloadProps;

    private RowData currentRecord;

    public MergeIterator(
        Configuration flinkConf,
        org.apache.hadoop.conf.Configuration hadoopConf,
        MergeOnReadInputSplit split,
        RowType tableRowType,
        RowType requiredRowType,
        Schema tableSchema,
        Schema requiredSchema,
        InternalSchema querySchema,
        int[] requiredPos,
        boolean emitDelete,
        int operationPos,
        ClosableIterator<RowData> nested) { // the iterator should be with full schema
      this(flinkConf, hadoopConf, split, tableRowType, requiredRowType, tableSchema,
          querySchema,
          Option.of(RowDataProjection.instance(requiredRowType, requiredPos)),
          Option.of(record -> buildAvroRecordBySchema(record, requiredSchema, requiredPos, new GenericRecordBuilder(requiredSchema))),
          emitDelete, operationPos, nested);
    }

    public MergeIterator(
        Configuration flinkConf,
        org.apache.hadoop.conf.Configuration hadoopConf,
        MergeOnReadInputSplit split,
        RowType tableRowType,
        RowType requiredRowType,
        Schema tableSchema,
        InternalSchema querySchema,
        Option<RowDataProjection> projection,
        Option<Function<IndexedRecord, GenericRecord>> avroProjection,
        boolean emitDelete,
        int operationPos,
        ClosableIterator<RowData> nested) { // the iterator should be with full schema
      this.tableSchema = tableSchema;
      this.nested = nested;
      this.scanner = FormatUtils.logScanner(split, tableSchema, querySchema, flinkConf, hadoopConf);
      this.payloadProps = StreamerUtil.getPayloadConfig(flinkConf).getProps();
      this.logKeysIterator = scanner.getRecords().keySet().iterator();
      this.emitDelete = emitDelete;
      this.operationPos = operationPos;
      this.avroProjection = avroProjection;
      this.rowDataToAvroConverter = RowDataToAvroConverters.createConverter(tableRowType, flinkConf.getBoolean(FlinkOptions.WRITE_UTC_TIMEZONE));
      this.avroToRowDataConverter = AvroToRowDataConverters.createRowConverter(requiredRowType, flinkConf.getBoolean(FlinkOptions.READ_UTC_TIMEZONE));
      this.projection = projection;
      this.instantRange = split.getInstantRange().orElse(null);
      this.recordMerger = StreamerUtil.getRecordMergerForReader(flinkConf, split.getTablePath());
    }

    @Override
    public boolean hasNext() {
      while (this.nested.hasNext()) {
        currentRecord = this.nested.next();
        if (instantRange != null) {
          boolean isInRange = instantRange.isInRange(currentRecord.getString(HOODIE_COMMIT_TIME_COL_POS).toString());
          if (!isInRange) {
            // filter base file by instant range
            continue;
          }
        }
        final String curKey = currentRecord.getString(HOODIE_RECORD_KEY_COL_POS).toString();
        if (scanner.getRecords().containsKey(curKey)) {
          keyToSkip.add(curKey);
          Option<HoodieRecord<IndexedRecord>> mergedAvroRecord = mergeRowWithLog(currentRecord, curKey);
          if (!mergedAvroRecord.isPresent()) {
            // deleted
            continue;
          } else {
            final RowKind rowKind = FormatUtils.getRowKindSafely(mergedAvroRecord.get().getData(), this.operationPos);
            if (!emitDelete && rowKind == RowKind.DELETE) {
              // deleted
              continue;
            }
            IndexedRecord avroRecord = avroProjection.isPresent()
                ? avroProjection.get().apply(mergedAvroRecord.get().getData())
                : mergedAvroRecord.get().getData();
            this.currentRecord = (RowData) avroToRowDataConverter.convert(avroRecord);
            this.currentRecord.setRowKind(rowKind);
            return true;
          }
        }
        // project the full record in base with required positions
        if (projection.isPresent()) {
          currentRecord = projection.get().project(currentRecord);
        }
        return true;
      }
      // read the logs
      while (logKeysIterator.hasNext()) {
        final String curKey = logKeysIterator.next();
        if (!keyToSkip.contains(curKey)) {
          Option<IndexedRecord> insertAvroRecord = getInsertValue(curKey);
          if (insertAvroRecord.isPresent()) {
            // the record is a DELETE if insertAvroRecord not present, skipping
            IndexedRecord avroRecord = avroProjection.isPresent()
                ? avroProjection.get().apply(insertAvroRecord.get())
                : insertAvroRecord.get();
            this.currentRecord = (RowData) avroToRowDataConverter.convert(avroRecord);
            FormatUtils.setRowKind(this.currentRecord, insertAvroRecord.get(), this.operationPos);
            return true;
          }
        }
      }
      return false;
    }

    private Option<IndexedRecord> getInsertValue(String curKey) {
      final HoodieAvroRecord<?> record = (HoodieAvroRecord) scanner.getRecords().get(curKey);
      if (!emitDelete && HoodieOperation.isDelete(record.getOperation())) {
        return Option.empty();
      }
      try {
        return record.getData().getInsertValue(tableSchema);
      } catch (IOException e) {
        throw new HoodieIOException("Get insert value from payload exception", e);
      }
    }

    @Override
    public RowData next() {
      return currentRecord;
    }

    @Override
    public void close() {
      if (this.nested != null) {
        this.nested.close();
      }
      if (this.scanner != null) {
        this.scanner.close();
      }
    }

    @SuppressWarnings("unchecked")
    private Option<HoodieRecord<IndexedRecord>> mergeRowWithLog(RowData curRow, String curKey) {
      final HoodieRecord<?> record = scanner.getRecords().get(curKey);
      GenericRecord historyAvroRecord = (GenericRecord) rowDataToAvroConverter.convert(tableSchema, curRow);
      HoodieAvroIndexedRecord hoodieAvroIndexedRecord = new HoodieAvroIndexedRecord(historyAvroRecord);
      try {
        return recordMerger.merge(hoodieAvroIndexedRecord, tableSchema, record, tableSchema, payloadProps).map(Pair::getLeft);
      } catch (IOException e) {
        throw new HoodieIOException("Merge base and delta payloads exception", e);
      }
    }
  }

  /**
   * Builder for {@link MergeOnReadInputFormat}.
   */
  public static class Builder {
    protected Configuration conf;
    protected MergeOnReadTableState tableState;
    protected List<DataType> fieldTypes;
    protected List<Predicate> predicates;
    protected long limit = -1;
    protected boolean emitDelete = false;
    protected InternalSchemaManager internalSchemaManager = InternalSchemaManager.DISABLED;

    public Builder config(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder tableState(MergeOnReadTableState tableState) {
      this.tableState = tableState;
      return this;
    }

    public Builder fieldTypes(List<DataType> fieldTypes) {
      this.fieldTypes = fieldTypes;
      return this;
    }

    public Builder predicates(List<Predicate> predicates) {
      this.predicates = predicates;
      return this;
    }

    public Builder limit(long limit) {
      this.limit = limit;
      return this;
    }

    public Builder emitDelete(boolean emitDelete) {
      this.emitDelete = emitDelete;
      return this;
    }

    public Builder internalSchemaManager(InternalSchemaManager internalSchemaManager) {
      this.internalSchemaManager = internalSchemaManager;
      return this;
    }

    public MergeOnReadInputFormat build() {
      return new MergeOnReadInputFormat(conf, tableState,
          fieldTypes, predicates, limit, emitDelete, internalSchemaManager);
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private static int[] getRequiredPosWithCommitTime(int[] requiredPos) {
    if (getCommitTimePos(requiredPos) >= 0) {
      return requiredPos;
    }
    int[] requiredPos2 = new int[requiredPos.length + 1];
    requiredPos2[0] = HOODIE_COMMIT_TIME_COL_POS;
    System.arraycopy(requiredPos, 0, requiredPos2, 1, requiredPos.length);
    return requiredPos2;
  }

  private static int getCommitTimePos(int[] requiredPos) {
    for (int i = 0; i < requiredPos.length; i++) {
      if (requiredPos[i] == HOODIE_COMMIT_TIME_COL_POS) {
        return i;
      }
    }
    return -1;
  }

  @VisibleForTesting
  public void isEmitDelete(boolean emitDelete) {
    this.emitDelete = emitDelete;
  }
}
