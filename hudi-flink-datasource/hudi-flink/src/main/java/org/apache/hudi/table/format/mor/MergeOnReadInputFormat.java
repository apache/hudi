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

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.io.lsm.RecordReader;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.metrics.CollectStatistic;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.RecordIterators;
import org.apache.hudi.table.format.mor.lsm.LsmMergeIterator;
import org.apache.hudi.table.format.mor.lsm.FlinkLsmUtils;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.DataTypeUtils;
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
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Function;
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

  private static final Logger LOG = LoggerFactory.getLogger(MergeOnReadInputFormat.class);

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
   * Default partition name when the field value is null.
   */
  private final String defaultPartName;

  /**
   * Required field positions.
   */
  protected final int[] requiredPos;

  private List<ExpressionPredicates.Predicate> predicates = new ArrayList<>();

  private Option<ExpressionPredicates.Predicate> commitTimePredicate;

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
  protected boolean emitDelete;

  /**
   * Flag saying whether the input format has been closed.
   */
  private boolean closed = true;

  protected final InternalSchemaManager internalSchemaManager;
  protected HoodieTableConfig tableConfig;

  protected IOManager ioManager;
  protected int memorySegmentSize;
  protected int spillTreshold;

  protected MergeOnReadInputFormat(
      Configuration conf,
      MergeOnReadTableState tableState,
      List<DataType> fieldTypes,
      String defaultPartName,
      List<ExpressionPredicates.Predicate> predicates,
      long limit,
      boolean emitDelete,
      InternalSchemaManager internalSchemaManager) {
    this.conf = conf;
    this.tableState = tableState;
    this.fieldNames = tableState.getRowType().getFieldNames();
    this.fieldTypes = fieldTypes;
    this.defaultPartName = defaultPartName;
    // Needs improvement: this requiredPos is only suitable for parquet reader,
    // because we need to
    this.requiredPos = tableState.getRequiredPositions();
    if (predicates != null && predicates.size() != 0) {
      this.predicates.addAll(predicates);
    }
    this.limit = limit;
    this.emitDelete = emitDelete;
    this.internalSchemaManager = internalSchemaManager;
    this.spillTreshold = conf.getInteger(FlinkOptions.LSM_SORT_MERGE_SPILL_THRESHOLD);
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
    this.tableConfig = split.getTableConfig();
    this.iterator = initIterator(split);
    mayShiftInputSplit(split);
  }

  public void setIOManager(IOManager ioManager) {
    this.ioManager = ioManager;
  }

  public void setMemorySegmentSize(int memorySegmentSize) {
    this.memorySegmentSize = memorySegmentSize;
  }

  public void setSpillTreshold(int spillTreshold) {
    this.spillTreshold = spillTreshold;
  }

  protected ClosableIterator<RowData> initIterator(MergeOnReadInputSplit split) throws IOException {
    if (tableConfig.isLSMBasedLogFormat()) {
      this.commitTimePredicate = extractCommitTimePredicate(split.getInstantRange());
      if (this.commitTimePredicate.isPresent()) {
        addCommitTimePredicate(this.commitTimePredicate.get());
      }
      TypedProperties payloadProps = StreamerUtil.getPayloadConfig(conf).getProps();
      Schema tableSchema = new Schema.Parser().parse(tableState.getAvroSchema());
      Schema requiredSchemaWithMeta = AvroSchemaCache.intern(this.tableState.getLsmReadSchema(payloadProps, tableSchema, conf));
      RowType rowTypeWithMeta = (RowType) AvroSchemaConverter.convertToDataType(requiredSchemaWithMeta).getLogicalType();
      RowDataSerializer rowDataSerializer = new RowDataSerializer(rowTypeWithMeta);
      int recordKeyIndex = FlinkLsmUtils.getRecordKeyIndex(requiredSchemaWithMeta);

      List<String> lsmLogPaths = split.getLogPaths().get();
      // 标记level 1文件的下标位置, level 1文件不spill disk
      int level1Index = -1;
      for (int i = 0; i < lsmLogPaths.size(); i++) {
        int level = FSUtils.getLevelNumFromLog(new Path(lsmLogPaths.get(i)));
        if (level == 1) {
          level1Index = i;
          break;
        }
      }

      List<ClosableIterator<RowData>> iterators = lsmLogPaths.stream().map(path -> {
        try {
          return FlinkLsmUtils.getBaseFileIterator(path, FlinkLsmUtils.getLsmRequiredPositions(requiredSchemaWithMeta, tableSchema),
              fieldNames, fieldTypes, defaultPartName, conf, hadoopConf, tableConfig, internalSchemaManager, predicates);
        } catch (IOException ioe) {
          throw new HoodieIOException(ioe.getMessage(), ioe);
        }
      }).collect(Collectors.toList());

      List<RecordReader<HoodieRecord>> readers = FlinkLsmUtils.createLsmRecordReaders(iterators, spillTreshold, rowDataSerializer, ioManager,
          memorySegmentSize, recordKeyIndex, level1Index);
      return new LsmMergeIterator(
          true,
          conf,
          payloadProps,
          requiredSchemaWithMeta,
          this.tableState.getRequiredRowType(),
          hadoopConf,
          this.requiredPos,
          split.getTablePath(),
          readers,
          false);
    }

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
      if (OptionsResolver.emitChangelog(conf)) {
        return new LogFileOnlyIterator(getUnMergedLogFileIterator(split));
      } else {
        return new LogFileOnlyIterator(getLogFileIterator(split));
      }
    } else if (split.getMergeType().equals(FlinkOptions.REALTIME_SKIP_MERGE)) {
      return new SkipMergeIterator(
          getBaseFileIterator(split.getBasePath().get()),
          getLogFileIterator(split));
    }  else if (split.getMergeType().equals(FlinkOptions.REALTIME_SKIP_COMBINE)) {
      return new SkipMergeIterator(
          getBaseFileIterator(split.getBasePath().get()),
          getUnMergedLogFileIterator(split));
    } else if (split.getMergeType().equals(FlinkOptions.REALTIME_PAYLOAD_COMBINE)) {
      // conf contains all the configs including reading runtime env
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
    } else if (split.getMergeType().equals(FlinkOptions.HISTORICAL_PAYLOAD_COMBINE)) {
      return new MergeHistoricalIterator(
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

  private Option<ExpressionPredicates.Predicate> extractCommitTimePredicate(Option<InstantRange> instantRange) {
    if (instantRange.isPresent()) {
      InstantRange range = instantRange.get();
      if (range instanceof InstantRange.CloseCloseRange) {
        CallExpression expression = FlinkLsmUtils.buildCloseCloseRangeCommitExpression(range);
        return Option.of(ExpressionPredicates.fromExpression(expression));
      } else if (range instanceof InstantRange.CloseCloseRangeNullableBoundary) {
        CallExpression expression = FlinkLsmUtils.buildCloseCloseRangeNullableBoundaryCommitExpression(range);
        return Option.of(ExpressionPredicates.fromExpression(expression));
      } else if (range instanceof InstantRange.OpenCloseRange) {
        CallExpression expression = FlinkLsmUtils.buildOpenCloseRangeCommitExpression(range);
        return Option.of(ExpressionPredicates.fromExpression(expression));
      } else if (range instanceof InstantRange.OpenCloseRangeNullableBoundary) {
        CallExpression expression = FlinkLsmUtils.buildOpenCloseRangeNullableBoundaryCommitExpression(range);
        return Option.of(ExpressionPredicates.fromExpression(expression));
      }
    }
    return Option.empty();
  }

  private void addCommitTimePredicate(ExpressionPredicates.Predicate predicate) {
    this.predicates.add(predicate);
  }

  private void removeCommitTimePredicate(ExpressionPredicates.Predicate predicate) {
    this.predicates.remove(predicate);
  }

  public Map<String, Long> getStatistic() {
    if (this.iterator instanceof CollectStatistic) {
      return ((CollectStatistic<?>) this.iterator).getStatisticMetrics();
    } else {
      return new HashMap<>();
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
    if (this.commitTimePredicate != null && this.commitTimePredicate.isPresent()) {
      removeCommitTimePredicate(this.commitTimePredicate.get());
    }
    this.commitTimePredicate = null;
    this.iterator = null;
    this.closed = true;
  }

  public boolean isClosed() {
    return this.closed;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  public Configuration getConfiguration() {
    return conf;
  }

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
      throw new HoodieException("Get reader error for path: " + path);
    }
  }

  protected ClosableIterator<RowData> getBaseFileIterator(String path) throws IOException {
    return getBaseFileIterator(path, this.requiredPos);
  }

  protected ClosableIterator<RowData> getBaseFileIterator(String path, int[] requiredPos) throws IOException {
    // generate partition specs.
    LinkedHashMap<String, String> partSpec = FilePathUtils.extractPartitionKeyValues(
        new org.apache.hadoop.fs.Path(path).getParent(),
        this.conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING),
        FilePathUtils.extractPartitionKeys(this.conf));
    LinkedHashMap<String, Object> partObjects = new LinkedHashMap<>();
    partSpec.forEach((k, v) -> {
      final int idx = fieldNames.indexOf(k);
      if (idx == -1) {
        // for any rare cases that the partition field does not exist in schema,
        // fallback to file read
        return;
      }
      DataType fieldType = fieldTypes.get(idx);
      if (!DataTypeUtils.isDatetimeType(fieldType)) {
        // date time type partition field is formatted specifically,
        // read directly from the data file to avoid format mismatch or precision loss
        partObjects.put(k, DataTypeUtils.resolvePartition(defaultPartName.equals(v) ? null : v, fieldType));
      }
    });
    org.apache.hadoop.conf.Configuration parquetConf = HadoopConfigurations.getParquetConf(this.conf, hadoopConf);
    FSUtils.addChubaoFsConfig2HadoopConf(parquetConf, tableConfig);
    return RecordIterators.getParquetRecordIterator(
        internalSchemaManager,
        this.conf.getBoolean(FlinkOptions.UTC_TIMEZONE),
        true,
        parquetConf,
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

  protected ClosableIterator<RowData> getLogFileIterator(MergeOnReadInputSplit split) {
    final Schema tableSchema = new Schema.Parser().parse(tableState.getAvroSchema());
    final Schema requiredSchema = new Schema.Parser().parse(tableState.getRequiredAvroSchema());
    final GenericRecordBuilder recordBuilder = new GenericRecordBuilder(requiredSchema);
    final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter =
        AvroToRowDataConverters.createRowConverter(tableState.getRequiredRowType());
    final HoodieMergedLogRecordScanner scanner = FormatUtils.logScanner(split, tableSchema, internalSchemaManager.getQuerySchema(), conf, hadoopConf);
    final Iterator<String> logRecordsKeyIterator = scanner.getRecords().keySet().iterator();
    final int[] pkOffset = tableState.getPkOffsetsInRequired();
    // flag saying whether the pk semantics has been dropped by user specified
    // projections. For e.g, if the pk fields are [a, b] but user only select a,
    // then the pk semantics is lost.
    final boolean pkSemanticLost = Arrays.stream(pkOffset).anyMatch(offset -> offset == -1);
    final LogicalType[] pkTypes = pkSemanticLost ? null : tableState.getPkTypes(pkOffset);
    final StringToRowDataConverter converter = pkSemanticLost ? null : new StringToRowDataConverter(pkTypes);

    return new CollectStatistic<RowData>() {
      @Override
      public Map<String, Long> getStatisticMetrics() {
        Map<String, Long> statistic = new HashMap<>();
        String finalBaseCommitTime = scanner.getFinalBaseCommitTime();
        if (StringUtils.nonEmpty(finalBaseCommitTime)) {
          String baseCommitTime = finalBaseCommitTime.length() > 14 ? finalBaseCommitTime.substring(0, 14) : finalBaseCommitTime;
          statistic.put("finalBaseCommitTime", Long.valueOf(baseCommitTime));
        }
        statistic.put("diffBetweenLogPathSizeAndScannedLogSize",
                scanner.getDiffBetweenLogPathSizeAndScannedLogSize());
        statistic.put("totalLogFiles", scanner.getTotalLogFiles());
        statistic.put("totalRollbacks",scanner.getTotalRollbacks());
        statistic.put("totalCorruptBlocks", scanner.getTotalCorruptBlocks());
        statistic.put("totalLogBlocks", scanner.getTotalLogBlocks());
        statistic.put("totalLogRecords", scanner.getTotalLogRecords());
        return statistic;
      }

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

  private ClosableIterator<RowData> getUnMergedLogFileIterator(MergeOnReadInputSplit split) {
    final Schema tableSchema = new Schema.Parser().parse(tableState.getAvroSchema());
    final Schema requiredSchema = new Schema.Parser().parse(tableState.getRequiredAvroSchema());
    final GenericRecordBuilder recordBuilder = new GenericRecordBuilder(requiredSchema);
    final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter =
        AvroToRowDataConverters.createRowConverter(tableState.getRequiredRowType());
    final FormatUtils.BoundedMemoryRecords records = new FormatUtils.BoundedMemoryRecords(split, tableSchema, internalSchemaManager.getQuerySchema(), hadoopConf, conf);
    final Iterator<HoodieRecord<?>> recordsIterator = records.getRecordsIterator();

    return new ClosableIterator<RowData>() {
      private RowData currentRecord;

      @Override
      public boolean hasNext() {
        while (recordsIterator.hasNext()) {
          Option<IndexedRecord> curAvroRecord = null;
          final HoodieAvroRecord<?> hoodieRecord = (HoodieAvroRecord) recordsIterator.next();
          try {
            curAvroRecord = hoodieRecord.getData().getInsertValue(tableSchema);
          } catch (IOException e) {
            throw new HoodieException("Get avro insert value error for key: " + hoodieRecord.getRecordKey(), e);
          }
          if (curAvroRecord.isPresent()) {
            final IndexedRecord avroRecord = curAvroRecord.get();
            GenericRecord requiredAvroRecord = buildAvroRecordBySchema(
                avroRecord,
                requiredSchema,
                requiredPos,
                recordBuilder);
            currentRecord = (RowData) avroToRowDataConverter.convert(requiredAvroRecord);
            FormatUtils.setRowKind(currentRecord, avroRecord, tableState.getOperationPos());
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
        records.close();
      }
    };
  }

  protected ClosableIterator<RowData> getFullLogFileIterator(MergeOnReadInputSplit split) {
    final Schema tableSchema = new Schema.Parser().parse(tableState.getAvroSchema());
    final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter =
        AvroToRowDataConverters.createRowConverter(tableState.getRowType());
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

  protected static class LogFileOnlyIterator implements CollectStatistic<RowData> {
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

    @Override
    public Map<String, Long> getStatisticMetrics() {
      if (iterator instanceof CollectStatistic) {
        return ((CollectStatistic<?>) iterator).getStatisticMetrics();
      } else {
        return new HashMap<>();
      }
    }
  }

  static class SkipMergeIterator implements ClosableIterator<RowData> {
    // base file record iterator
    private final ClosableIterator<RowData> nested;
    // iterator for log files
    private final ClosableIterator<RowData> iterator;

    // add the flag because the flink ParquetColumnarRowSplitReader is buggy:
    // method #reachedEnd() returns false after it returns true.
    // refactor it out once FLINK-22370 is resolved.
    private boolean readLogs = false;

    private RowData currentRecord;

    SkipMergeIterator(ClosableIterator<RowData> nested, ClosableIterator<RowData> iterator) {
      this.nested = nested;
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      if (!readLogs && this.nested.hasNext()) {
        currentRecord = this.nested.next();
        return true;
      }
      readLogs = true;
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

  protected static class MergeIterator implements CollectStatistic {
    // base file record iterator
    protected final ClosableIterator<RowData> nested;
    // log keys used for merging
    protected final Iterator<String> logKeysIterator;
    // scanner
    protected final HoodieMergedLogRecordScanner scanner;

    private final Schema tableSchema;
    protected final boolean emitDelete;
    protected final int operationPos;
    private final RowDataToAvroConverters.RowDataToAvroConverter rowDataToAvroConverter;
    protected final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter;

    protected final Option<RowDataProjection> projection;
    protected final Option<Function<IndexedRecord, GenericRecord>> avroProjection;

    protected final InstantRange instantRange;

    private final HoodieRecordMerger recordMerger;

    // add the flag because the flink ParquetColumnarRowSplitReader is buggy:
    // method #reachedEnd() returns false after it returns true.
    // refactor it out once FLINK-22370 is resolved.
    protected boolean readLogs = false;

    protected final Set<String> keyToSkip = new HashSet<>();

    private final TypedProperties payloadProps;

    protected RowData currentRecord;

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
      this.rowDataToAvroConverter = RowDataToAvroConverters.createConverter(tableRowType);
      this.avroToRowDataConverter = AvroToRowDataConverters.createRowConverter(requiredRowType);
      this.projection = projection;
      this.instantRange = split.getInstantRange().orElse(null);
      List<String> mergers = Arrays.stream(flinkConf.getString(FlinkOptions.RECORD_MERGER_IMPLS).split(","))
          .map(String::trim)
          .distinct()
          .collect(Collectors.toList());
      this.recordMerger = HoodieRecordUtils.createRecordMerger(split.getTablePath(), EngineType.FLINK, mergers, flinkConf.getString(FlinkOptions.RECORD_MERGER_STRATEGY));
    }

    @Override
    public boolean hasNext() {
      while (!readLogs && this.nested.hasNext()) {
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
      readLogs = true;
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

    protected Option<IndexedRecord> getInsertValue(String curKey) {
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
    protected Option<HoodieRecord<IndexedRecord>> mergeRowWithLog(RowData curRow, String curKey) {
      final HoodieRecord<?> record = scanner.getRecords().get(curKey);
      GenericRecord historyAvroRecord = (GenericRecord) rowDataToAvroConverter.convert(tableSchema, curRow);
      HoodieAvroIndexedRecord hoodieAvroIndexedRecord = new HoodieAvroIndexedRecord(historyAvroRecord);
      try {
        return recordMerger.merge(hoodieAvroIndexedRecord, tableSchema, record, tableSchema, payloadProps).map(Pair::getLeft);
      } catch (IOException e) {
        throw new HoodieIOException("Merge base and delta payloads exception", e);
      }
    }

    @Override
    public Map<String, Long> getStatisticMetrics() {
      Map<String, Long> statistic = new HashMap<>();
      String finalBaseCommitTime = scanner.getFinalBaseCommitTime();
      if (StringUtils.nonEmpty(finalBaseCommitTime)) {
        String baseCommitTime = finalBaseCommitTime.length() > 14 ? finalBaseCommitTime.substring(0, 14) : finalBaseCommitTime;
        statistic.put("finalBaseCommitTime", Long.valueOf(baseCommitTime));
      }
      statistic.put("diffBetweenLogPathSizeAndScannedLogSize", scanner.getDiffBetweenLogPathSizeAndScannedLogSize());
      statistic.put("totalLogFiles", scanner.getTotalLogFiles());
      statistic.put("totalRollbacks",scanner.getTotalRollbacks());
      statistic.put("totalCorruptBlocks", scanner.getTotalCorruptBlocks());
      statistic.put("totalLogBlocks", scanner.getTotalLogBlocks());
      statistic.put("totalLogRecords", scanner.getTotalLogRecords());
      return statistic;
    }
  }

  protected static class MergeHistoricalIterator extends MergeIterator {
    public MergeHistoricalIterator(
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
      super(flinkConf, hadoopConf, split, tableRowType, requiredRowType, tableSchema,
          querySchema,
          Option.of(RowDataProjection.instance(requiredRowType, requiredPos)),
          Option.of(record -> buildAvroRecordBySchema(record, requiredSchema, requiredPos, new GenericRecordBuilder(requiredSchema))),
          emitDelete, operationPos, nested);
    }

    @Override
    public boolean hasNext() {
      while (!readLogs && this.nested.hasNext()) {
        currentRecord = this.nested.next();
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

        if (instantRange != null) {
          boolean isInRange = instantRange.isInRange(currentRecord.getString(HOODIE_COMMIT_TIME_COL_POS).toString());
          if (!isInRange) {
            // filter base file by instant range
            continue;
          }
        }

        // project the full record in base with required positions
        if (projection.isPresent()) {
          currentRecord = projection.get().project(currentRecord);
        }
        return true;
      }
      // read the logs
      readLogs = true;
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
  }

  /**
   * Builder for {@link MergeOnReadInputFormat}.
   */
  public static class Builder {
    protected Configuration conf;
    protected MergeOnReadTableState tableState;
    protected List<DataType> fieldTypes;
    protected String defaultPartName;
    protected List<ExpressionPredicates.Predicate> predicates;
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

    public Builder defaultPartName(String defaultPartName) {
      this.defaultPartName = defaultPartName;
      return this;
    }

    public Builder predicates(List<ExpressionPredicates.Predicate> predicates) {
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
      if (conf.get(FlinkOptions.COMPACTION_SCANNER_TYPE).equals(FlinkOptions.SCANNER_WITH_FK)) {
        return new MergeOnReadInputFormatWithFK(conf, tableState, fieldTypes,
            defaultPartName, limit, emitDelete, internalSchemaManager);
      }
      return new MergeOnReadInputFormat(conf, tableState, fieldTypes,
          defaultPartName, predicates, limit, emitDelete, internalSchemaManager);
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  protected static int[] getRequiredPosWithCommitTime(int[] requiredPos) {
    if (getCommitTimePos(requiredPos) >= 0) {
      return requiredPos;
    }
    int[] requiredPos2 = new int[requiredPos.length + 1];
    requiredPos2[0] = HOODIE_COMMIT_TIME_COL_POS;
    System.arraycopy(requiredPos, 0, requiredPos2, 1, requiredPos.length);
    return requiredPos2;
  }

  protected static int[] getRequiredPosWithRecordKey(int[] requiredPos) {
    if (getRecordKeyPos(requiredPos) >= 0) {
      return requiredPos;
    }
    int[] requiredPos2 = new int[requiredPos.length + 1];
    requiredPos2[0] = HOODIE_RECORD_KEY_COL_POS;
    System.arraycopy(requiredPos, 0, requiredPos2, 1, requiredPos.length);
    return requiredPos2;
  }

  public static int getCommitTimePos(int[] requiredPos) {
    for (int i = 0; i < requiredPos.length; i++) {
      if (requiredPos[i] == HOODIE_COMMIT_TIME_COL_POS) {
        return i;
      }
    }
    return -1;
  }

  public static int getRecordKeyPos(int[] requiredPos) {
    for (int i = 0; i < requiredPos.length; i++) {
      if (requiredPos[i] == HOODIE_RECORD_KEY_COL_POS) {
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
