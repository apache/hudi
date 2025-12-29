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

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.source.ExpressionPredicates.Predicate;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.RecordIterators;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import lombok.Getter;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

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
  protected boolean emitDelete;

  /**
   * Flag saying whether the input format has been closed.
   */
  @Getter
  private boolean closed = true;

  protected final InternalSchemaManager internalSchemaManager;

  /**
   * The table metadata client
   */
  protected transient HoodieTableMetaClient metaClient;

  /**
   * The hoodie write configuration.
   */
  protected transient HoodieWriteConfig writeConfig;

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
    String mergeType = split.getMergeType();
    if (!split.getBasePath().isPresent()) {
      if (OptionsResolver.emitDeletes(conf)) {
        mergeType = FlinkOptions.REALTIME_SKIP_MERGE;
      } else {
        // always merge records in log files if there is no base file (aligned with legacy behaviour)
        mergeType = FlinkOptions.REALTIME_PAYLOAD_COMBINE;
      }
    }
    ValidationUtils.checkArgument(
        mergeType.equals(FlinkOptions.REALTIME_SKIP_MERGE) || mergeType.equals(FlinkOptions.REALTIME_PAYLOAD_COMBINE),
        "Unable to select an Iterator to read the Hoodie MOR File Split for "
            + "file path: " + split.getBasePath()
            + "log paths: " + split.getLogPaths()
            + "hoodie table path: " + split.getTablePath()
            + "flink partition Index: " + split.getSplitNumber()
            + "merge type: " + split.getMergeType());
    final HoodieSchema tableSchema = HoodieSchemaCache.intern(
        HoodieSchema.parse(tableState.getTableSchema()));
    final HoodieSchema requiredSchema = HoodieSchemaCache.intern(
        HoodieSchema.parse(tableState.getRequiredSchema()));
    return getSplitRowIterator(split, tableSchema, requiredSchema, mergeType, emitDelete);
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

  protected ClosableIterator<RowData> getBaseFileIterator(String path) throws IOException {
    LinkedHashMap<String, Object> partObjects = FilePathUtils.generatePartitionSpecs(
        path,
        fieldNames,
        fieldTypes,
        conf.get(FlinkOptions.PARTITION_DEFAULT_NAME),
        conf.get(FlinkOptions.PARTITION_PATH_FIELD),
        conf.get(FlinkOptions.HIVE_STYLE_PARTITIONING)
    );

    return RecordIterators.getParquetRecordIterator(
        internalSchemaManager,
        this.conf.get(FlinkOptions.READ_UTC_TIMEZONE),
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

  /**
   * Get a {@link RowData} iterator using {@link HoodieFileGroupReader}.
   *
   * @param split          input split
   * @param tableSchema    schema of the table
   * @param requiredSchema required query schema
   * @param mergeType      merge type for FileGroup reader
   * @param emitDelete     flag saying whether DELETE record should be emitted
   *
   * @return {@link RowData} iterator for the given split.
   */
  protected ClosableIterator<RowData> getSplitRowIterator(
      MergeOnReadInputSplit split,
      HoodieSchema tableSchema,
      HoodieSchema requiredSchema,
      String mergeType,
      boolean emitDelete) throws IOException {
    HoodieFileGroupReader<RowData> fileGroupReader = createFileGroupReader(split, tableSchema, requiredSchema, mergeType, emitDelete);
    return fileGroupReader.getClosableIterator();
  }

  /**
   * Create a {@link HoodieFileGroupReader}.
   *
   * @param split          input split
   * @param tableSchema    schema of the table
   * @param requiredSchema required query schema
   * @param mergeType      merge type for FileGroup reader
   * @param emitDelete     flag saying whether DELETE record should be emitted
   *
   * @return A {@link HoodieFileGroupReader}.
   */
  protected HoodieFileGroupReader<RowData> createFileGroupReader(
      MergeOnReadInputSplit split,
      HoodieSchema tableSchema,
      HoodieSchema requiredSchema,
      String mergeType,
      boolean emitDelete) {
    FileSlice fileSlice = new FileSlice(
        // partitionPath is not needed for FG reader on Flink
        new HoodieFileGroupId("", split.getFileId()),
        // baseInstantTime in FileSlice is not used in FG reader
        "",
        split.getBasePath().map(HoodieBaseFile::new).orElse(null),
        split.getLogPaths().map(logFiles -> logFiles.stream().map(HoodieLogFile::new).collect(Collectors.toList())).orElse(Collections.emptyList()));
    return FormatUtils.createFileGroupReader(metaClient, writeConfig, internalSchemaManager, fileSlice,
        tableSchema, requiredSchema, split.getLatestCommit(), mergeType, emitDelete, predicates, split.getInstantRange());
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

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

  @VisibleForTesting
  public void isEmitDelete(boolean emitDelete) {
    this.emitDelete = emitDelete;
  }
}
