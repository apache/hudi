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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.cow.ParquetColumnarRowSplitReader;
import org.apache.hudi.table.format.cow.ParquetSplitReaderUtil;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.flink.table.data.vector.VectorizedColumnBatch.DEFAULT_SIZE;
import static org.apache.flink.table.filesystem.RowPartitionComputer.restorePartValueFromType;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS;
import static org.apache.hudi.table.format.FormatUtils.buildAvroRecordBySchema;

/**
 * The base InputFormat class to read from Hoodie data + log files.
 *
 * <P>Use {@link org.apache.flink.formats.parquet.utils.ParquetRecordReader}
 * to read files instead of {@link org.apache.flink.core.fs.FSDataInputStream},
 * overrides {@link #createInputSplits(int)} and {@link #close()} to change the behaviors.
 */
public class MergeOnReadInputFormat
    extends RichInputFormat<RowData, MergeOnReadInputSplit> {

  private static final long serialVersionUID = 1L;

  private final Configuration conf;

  private transient org.apache.hadoop.conf.Configuration hadoopConf;

  private Path[] paths;

  private final MergeOnReadTableState tableState;

  /**
   * Uniform iterator view for the underneath records.
   */
  private transient RecordIterator iterator;

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
  private final int[] requiredPos;

  // for limit push down
  /**
   * Limit for the reader, -1 when the reading is not limited.
   */
  private final long limit;

  /**
   * Recording the current read count for limit check.
   */
  private long currentReadCount = 0;

  public MergeOnReadInputFormat(
      Configuration conf,
      Path[] paths,
      MergeOnReadTableState tableState,
      List<DataType> fieldTypes,
      String defaultPartName,
      long limit) {
    this.conf = conf;
    this.paths = paths;
    this.tableState = tableState;
    this.fieldNames = tableState.getRowType().getFieldNames();
    this.fieldTypes = fieldTypes;
    this.defaultPartName = defaultPartName;
    // Needs improvement: this requiredPos is only suitable for parquet reader,
    // because we need to
    this.requiredPos = tableState.getRequiredPositions();
    this.limit = limit;
  }

  @Override
  public void open(MergeOnReadInputSplit split) throws IOException {
    this.currentReadCount = 0L;
    this.hadoopConf = StreamerUtil.getHadoopConf();
    if (!split.getLogPaths().isPresent()) {
      // base file only
      this.iterator = new BaseFileOnlyIterator(getRequiredSchemaReader(split.getBasePath().get()));
    } else if (!split.getBasePath().isPresent()) {
      // log files only
      this.iterator = new LogFileOnlyIterator(getLogFileIterator(split));
    } else if (split.getMergeType().equals(FlinkOptions.REALTIME_SKIP_MERGE)) {
      this.iterator = new SkipMergeIterator(
          getRequiredSchemaReader(split.getBasePath().get()),
          getLogFileIterator(split));
    } else if (split.getMergeType().equals(FlinkOptions.REALTIME_PAYLOAD_COMBINE)) {
      this.iterator = new MergeIterator(
          hadoopConf,
          split,
          this.tableState.getRowType(),
          this.tableState.getRequiredRowType(),
          new Schema.Parser().parse(this.tableState.getAvroSchema()),
          new Schema.Parser().parse(this.tableState.getRequiredAvroSchema()),
          this.requiredPos,
          getFullSchemaReader(split.getTablePath()));
    } else {
      throw new HoodieException("Unable to select an Iterator to read the Hoodie MOR File Split for "
          + "file path: " + split.getBasePath()
          + "log paths: " + split.getLogPaths()
          + "hoodie table path: " + split.getTablePath()
          + "spark partition Index: " + split.getSplitNumber()
          + "merge type: " + split.getMergeType());
    }
  }

  @Override
  public void configure(Configuration configuration) {
    if (this.paths.length == 0) {
      // file path was not specified yet. Try to set it from the parameters.
      String filePath = configuration.getString(FlinkOptions.PATH, null);
      if (filePath == null) {
        throw new IllegalArgumentException("File path was not specified in input format or configuration.");
      } else {
        this.paths = new Path[] { new Path(filePath) };
      }
    }
    // may supports nested files in the future.
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
      return this.iterator.reachedEnd();
    }
  }

  @Override
  public RowData nextRecord(RowData o) {
    currentReadCount++;
    return this.iterator.nextRecord();
  }

  @Override
  public void close() throws IOException {
    if (this.iterator != null) {
      this.iterator.close();
    }
    this.iterator = null;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private ParquetColumnarRowSplitReader getFullSchemaReader(String path) throws IOException {
    return getReader(path, IntStream.range(0, this.tableState.getRowType().getFieldCount()).toArray());
  }

  private ParquetColumnarRowSplitReader getRequiredSchemaReader(String path) throws IOException {
    return getReader(path, this.requiredPos);
  }

  private ParquetColumnarRowSplitReader getReader(String path, int[] requiredPos) throws IOException {
    // generate partition specs.
    LinkedHashMap<String, String> partSpec = FilePathUtils.extractPartitionKeyValues(
        new org.apache.hadoop.fs.Path(path).getParent(),
        this.conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITION),
        this.conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(","));
    LinkedHashMap<String, Object> partObjects = new LinkedHashMap<>();
    partSpec.forEach((k, v) -> partObjects.put(k, restorePartValueFromType(
        defaultPartName.equals(v) ? null : v,
        fieldTypes.get(fieldNames.indexOf(k)))));

    return ParquetSplitReaderUtil.genPartColumnarRowReader(
        this.conf.getBoolean(FlinkOptions.UTC_TIMEZONE),
        true,
        FormatUtils.getParquetConf(this.conf, hadoopConf),
        fieldNames.toArray(new String[0]),
        fieldTypes.toArray(new DataType[0]),
        partObjects,
        requiredPos,
        DEFAULT_SIZE,
        new org.apache.flink.core.fs.Path(path),
        0,
        Long.MAX_VALUE); // read the whole file
  }

  private Iterator<RowData> getLogFileIterator(MergeOnReadInputSplit split) {
    final Schema tableSchema = new Schema.Parser().parse(tableState.getAvroSchema());
    final Schema requiredSchema = new Schema.Parser().parse(tableState.getRequiredAvroSchema());
    final GenericRecordBuilder recordBuilder = new GenericRecordBuilder(requiredSchema);
    final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter =
        AvroToRowDataConverters.createRowConverter(tableState.getRequiredRowType());
    final Map<String, HoodieRecord<? extends HoodieRecordPayload>> logRecords =
        FormatUtils.scanLog(split, tableSchema, hadoopConf).getRecords();
    final Iterator<String> logRecordsKeyIterator = logRecords.keySet().iterator();

    return new Iterator<RowData>() {
      private RowData currentRecord;

      @Override
      public boolean hasNext() {
        if (logRecordsKeyIterator.hasNext()) {
          String curAvrokey = logRecordsKeyIterator.next();
          Option<IndexedRecord> curAvroRecord = null;
          try {
            curAvroRecord = logRecords.get(curAvrokey).getData().getInsertValue(tableSchema);
          } catch (IOException e) {
            throw new HoodieException("Get avro insert value error for key: " + curAvrokey, e);
          }
          if (!curAvroRecord.isPresent()) {
            // delete record found, skipping
            return hasNext();
          } else {
            // should improve the code when log scanner supports
            // seeking by log blocks with commit time which is more
            // efficient.
            if (split.getInstantRange().isPresent()) {
              // based on the fact that commit time is always the first field
              String commitTime = curAvroRecord.get().get(0).toString();
              if (!split.getInstantRange().get().isInRange(commitTime)) {
                // filter out the records that are not in range
                return hasNext();
              }
            }
            GenericRecord requiredAvroRecord = buildAvroRecordBySchema(
                curAvroRecord.get(),
                requiredSchema,
                requiredPos,
                recordBuilder);
            currentRecord = (RowData) avroToRowDataConverter.convert(requiredAvroRecord);
            return true;
          }
        } else {
          return false;
        }
      }

      @Override
      public RowData next() {
        return currentRecord;
      }
    };
  }

  private interface RecordIterator {
    boolean reachedEnd() throws IOException;

    RowData nextRecord();

    void close() throws IOException;
  }

  static class BaseFileOnlyIterator implements RecordIterator {
    // base file reader
    private final ParquetColumnarRowSplitReader reader;

    BaseFileOnlyIterator(ParquetColumnarRowSplitReader reader) {
      this.reader = reader;
    }

    @Override
    public boolean reachedEnd() throws IOException {
      return this.reader.reachedEnd();
    }

    @Override
    public RowData nextRecord() {
      return this.reader.nextRecord();
    }

    @Override
    public void close() throws IOException {
      if (this.reader != null) {
        this.reader.close();
      }
    }
  }

  static class LogFileOnlyIterator implements RecordIterator {
    // iterator for log files
    private final Iterator<RowData> iterator;

    LogFileOnlyIterator(Iterator<RowData> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean reachedEnd() {
      return !this.iterator.hasNext();
    }

    @Override
    public RowData nextRecord() {
      return this.iterator.next();
    }

    @Override
    public void close() {
      // no operation
    }
  }

  static class SkipMergeIterator implements RecordIterator {
    // base file reader
    private final ParquetColumnarRowSplitReader reader;
    // iterator for log files
    private final Iterator<RowData> iterator;

    private RowData currentRecord;

    SkipMergeIterator(ParquetColumnarRowSplitReader reader, Iterator<RowData> iterator) {
      this.reader = reader;
      this.iterator = iterator;
    }

    @Override
    public boolean reachedEnd() throws IOException {
      if (!this.reader.reachedEnd()) {
        currentRecord = this.reader.nextRecord();
        return false;
      }
      if (this.iterator.hasNext()) {
        currentRecord = this.iterator.next();
        return false;
      }
      return true;
    }

    @Override
    public RowData nextRecord() {
      return currentRecord;
    }

    @Override
    public void close() throws IOException {
      if (this.reader != null) {
        this.reader.close();
      }
    }
  }

  static class MergeIterator implements RecordIterator {
    // base file reader
    private final ParquetColumnarRowSplitReader reader;
    // log keys used for merging
    private final Iterator<String> logKeysIterator;
    // log records
    private final Map<String, HoodieRecord<? extends HoodieRecordPayload>> logRecords;

    private final Schema tableSchema;
    private final Schema requiredSchema;
    private final int[] requiredPos;
    private final RowDataToAvroConverters.RowDataToAvroConverter rowDataToAvroConverter;
    private final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter;
    private final GenericRecordBuilder recordBuilder;

    private Set<String> keyToSkip = new HashSet<>();

    private RowData currentRecord;

    MergeIterator(
        org.apache.hadoop.conf.Configuration hadoopConf,
        MergeOnReadInputSplit split,
        RowType tableRowType,
        RowType requiredRowType,
        Schema tableSchema,
        Schema requiredSchema,
        int[] requiredPos,
        ParquetColumnarRowSplitReader reader) { // the reader should be with full schema
      this.tableSchema = tableSchema;
      this.reader = reader;
      this.logRecords = FormatUtils.scanLog(split, tableSchema, hadoopConf).getRecords();
      this.logKeysIterator = this.logRecords.keySet().iterator();
      this.requiredSchema = requiredSchema;
      this.requiredPos = requiredPos;
      this.recordBuilder = new GenericRecordBuilder(requiredSchema);
      this.rowDataToAvroConverter = RowDataToAvroConverters.createConverter(tableRowType);
      this.avroToRowDataConverter = AvroToRowDataConverters.createRowConverter(requiredRowType);
    }

    @Override
    public boolean reachedEnd() throws IOException {
      if (!this.reader.reachedEnd()) {
        currentRecord = this.reader.nextRecord();
        final String curKey = currentRecord.getString(HOODIE_RECORD_KEY_COL_POS).toString();
        if (logRecords.containsKey(curKey)) {
          keyToSkip.add(curKey);
          Option<IndexedRecord> mergedAvroRecord = mergeRowWithLog(currentRecord, curKey);
          if (!mergedAvroRecord.isPresent()) {
            // deleted
            return reachedEnd();
          } else {
            GenericRecord record = buildAvroRecordBySchema(
                mergedAvroRecord.get(),
                requiredSchema,
                requiredPos,
                recordBuilder);
            this.currentRecord = (RowData) avroToRowDataConverter.convert(record);
            return false;
          }
        }
        return false;
      } else {
        if (logKeysIterator.hasNext()) {
          final String curKey = logKeysIterator.next();
          if (keyToSkip.contains(curKey)) {
            return reachedEnd();
          } else {
            Option<IndexedRecord> insertAvroRecord =
                logRecords.get(curKey).getData().getInsertValue(tableSchema);
            if (!insertAvroRecord.isPresent()) {
              // stand alone delete record, skipping
              return reachedEnd();
            } else {
              GenericRecord requiredAvroRecord = buildAvroRecordBySchema(
                  insertAvroRecord.get(),
                  requiredSchema,
                  requiredPos,
                  recordBuilder);
              this.currentRecord = (RowData) avroToRowDataConverter.convert(requiredAvroRecord);
              return false;
            }
          }
        }
        return true;
      }
    }

    @Override
    public RowData nextRecord() {
      return currentRecord;
    }

    @Override
    public void close() throws IOException {
      if (this.reader != null) {
        this.reader.close();
      }
    }

    private Option<IndexedRecord> mergeRowWithLog(
        RowData curRow,
        String curKey) throws IOException {
      GenericRecord historyAvroRecord = (GenericRecord) rowDataToAvroConverter.convert(tableSchema, curRow);
      return logRecords.get(curKey).getData().combineAndGetUpdateValue(historyAvroRecord, tableSchema);
    }
  }
}
