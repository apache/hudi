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

package org.apache.hudi.table;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.HoodieROTablePathFilter;
import org.apache.hudi.source.FileIndex;
import org.apache.hudi.source.IncrementalInputSplits;
import org.apache.hudi.source.StreamReadMonitoringFunction;
import org.apache.hudi.source.StreamReadOperator;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.cow.CopyOnWriteInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.ChangelogModes;
import org.apache.hudi.util.InputFormats;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.configuration.HadoopConfigurations.getParquetConf;

/**
 * Hoodie batch table source that always read the latest snapshot of the underneath table.
 */
public class HoodieTableSource implements
    ScanTableSource,
    SupportsPartitionPushDown,
    SupportsProjectionPushDown,
    SupportsLimitPushDown,
    SupportsFilterPushDown {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableSource.class);

  private static final int NO_LIMIT_CONSTANT = -1;

  private final transient org.apache.hadoop.conf.Configuration hadoopConf;
  private final transient HoodieTableMetaClient metaClient;
  private final long maxCompactionMemoryInBytes;

  private final ResolvedSchema schema;
  private final Path path;
  private final List<String> partitionKeys;
  private final String defaultPartName;
  private final Configuration conf;
  private final FileIndex fileIndex;

  private int[] requiredPos;
  private long limit;
  private List<Expression> filters;

  private List<Map<String, String>> requiredPartitions;

  public HoodieTableSource(
      ResolvedSchema schema,
      Path path,
      List<String> partitionKeys,
      String defaultPartName,
      Configuration conf) {
    this(schema, path, partitionKeys, defaultPartName, conf, null, null, null, null);
  }

  public HoodieTableSource(
      ResolvedSchema schema,
      Path path,
      List<String> partitionKeys,
      String defaultPartName,
      Configuration conf,
      @Nullable List<Map<String, String>> requiredPartitions,
      @Nullable int[] requiredPos,
      @Nullable Long limit,
      @Nullable List<Expression> filters) {
    this.schema = schema;
    this.path = path;
    this.partitionKeys = partitionKeys;
    this.defaultPartName = defaultPartName;
    this.conf = conf;
    this.fileIndex = FileIndex.instance(this.path, this.conf);
    this.requiredPartitions = requiredPartitions;
    this.requiredPos = requiredPos == null
        ? IntStream.range(0, schema.getColumnCount()).toArray()
        : requiredPos;
    this.limit = limit == null ? NO_LIMIT_CONSTANT : limit;
    this.filters = filters == null ? Collections.emptyList() : filters;
    this.hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    this.metaClient = StreamerUtil.metaClientForReader(conf, hadoopConf);
    this.maxCompactionMemoryInBytes = StreamerUtil.getMaxCompactionMemoryInBytes(conf);
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    return new DataStreamScanProvider() {

      @Override
      public boolean isBounded() {
        return !conf.getBoolean(FlinkOptions.READ_AS_STREAMING);
      }

      @Override
      public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
        @SuppressWarnings("unchecked")
        TypeInformation<RowData> typeInfo =
            (TypeInformation<RowData>) TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(getProducedDataType());
        if (conf.getBoolean(FlinkOptions.READ_AS_STREAMING)) {
          StreamReadMonitoringFunction monitoringFunction = new StreamReadMonitoringFunction(
              conf, FilePathUtils.toFlinkPath(path), maxCompactionMemoryInBytes, getRequiredPartitionPaths());
          InputFormat<RowData, ?> inputFormat = getInputFormat(true);
          OneInputStreamOperatorFactory<MergeOnReadInputSplit, RowData> factory = StreamReadOperator.factory((MergeOnReadInputFormat) inputFormat);
          SingleOutputStreamOperator<RowData> source = execEnv.addSource(monitoringFunction, getSourceOperatorName("split_monitor"))
              .setParallelism(1)
              .transform("split_reader", typeInfo, factory)
              .setParallelism(conf.getInteger(FlinkOptions.READ_TASKS));
          return new DataStreamSource<>(source);
        } else {
          InputFormatSourceFunction<RowData> func = new InputFormatSourceFunction<>(getInputFormat(), typeInfo);
          DataStreamSource<RowData> source = execEnv.addSource(func, asSummaryString(), typeInfo);
          return source.name(getSourceOperatorName("bounded_source")).setParallelism(conf.getInteger(FlinkOptions.READ_TASKS));
        }
      }
    };
  }

  @Override
  public ChangelogMode getChangelogMode() {
    // when read as streaming and changelog mode is enabled, emit as FULL mode;
    // when all the changes are compacted or read as batch, emit as INSERT mode.
    return OptionsResolver.emitChangelog(conf) ? ChangelogModes.FULL : ChangelogMode.insertOnly();
  }

  @Override
  public DynamicTableSource copy() {
    return new HoodieTableSource(schema, path, partitionKeys, defaultPartName,
        conf, requiredPartitions, requiredPos, limit, filters);
  }

  @Override
  public String asSummaryString() {
    return "HudiTableSource";
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> filters) {
    this.filters = new ArrayList<>(filters);
    // refuse all the filters now
    return SupportsFilterPushDown.Result.of(Collections.emptyList(), new ArrayList<>(filters));
  }

  @Override
  public Optional<List<Map<String, String>>> listPartitions() {
    List<Map<String, String>> partitions = this.fileIndex.getPartitions(
        this.partitionKeys, defaultPartName, conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING));
    return Optional.of(partitions);
  }

  @Override
  public void applyPartitions(List<Map<String, String>> partitions) {
    this.requiredPartitions = partitions;
  }

  @Override
  public boolean supportsNestedProjection() {
    return false;
  }

  @Override
  public void applyProjection(int[][] projections) {
    // nested projection is not supported.
    this.requiredPos = Arrays.stream(projections).mapToInt(array -> array[0]).toArray();
  }

  @Override
  public void applyLimit(long limit) {
    this.limit = limit;
  }

  private DataType getProducedDataType() {
    String[] schemaFieldNames = this.schema.getColumnNames().toArray(new String[0]);
    DataType[] schemaTypes = this.schema.getColumnDataTypes().toArray(new DataType[0]);

    return DataTypes.ROW(Arrays.stream(this.requiredPos)
            .mapToObj(i -> DataTypes.FIELD(schemaFieldNames[i], schemaTypes[i]))
            .toArray(DataTypes.Field[]::new))
        .bridgedTo(RowData.class);
  }

  private List<Map<String, String>> getOrFetchPartitions() {
    if (requiredPartitions == null) {
      requiredPartitions = listPartitions().orElse(Collections.emptyList());
    }
    return requiredPartitions;
  }

  private String getSourceOperatorName(String operatorName) {
    String[] schemaFieldNames = this.schema.getColumnNames().toArray(new String[0]);
    List<String> fields = Arrays.stream(this.requiredPos)
        .mapToObj(i -> schemaFieldNames[i])
        .collect(Collectors.toList());
    StringBuilder sb = new StringBuilder();
    sb.append(operatorName)
        .append("(")
        .append("table=").append(Collections.singletonList(conf.getString(FlinkOptions.TABLE_NAME)))
        .append(", ")
        .append("fields=").append(fields)
        .append(")");
    return sb.toString();
  }

  @Nullable
  private Set<String> getRequiredPartitionPaths() {
    if (this.requiredPartitions == null) {
      // returns null for non partition pruning
      return null;
    }
    return FilePathUtils.toRelativePartitionPaths(this.partitionKeys, this.requiredPartitions,
        conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING));
  }

  private List<MergeOnReadInputSplit> buildFileIndex() {
    Set<String> requiredPartitionPaths = getRequiredPartitionPaths();
    fileIndex.setPartitionPaths(requiredPartitionPaths);
    List<String> relPartitionPaths = fileIndex.getOrBuildPartitionPaths();
    if (relPartitionPaths.size() == 0) {
      return Collections.emptyList();
    }
    FileStatus[] fileStatuses = fileIndex.getFilesInPartitions();
    if (fileStatuses.length == 0) {
      throw new HoodieException("No files found for reading in user provided path.");
    }

    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        // file-slice after pending compaction-requested instant-time is also considered valid
        metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants(), fileStatuses);
    String latestCommit = fsView.getLastInstant().get().getTimestamp();
    final String mergeType = this.conf.getString(FlinkOptions.MERGE_TYPE);
    final AtomicInteger cnt = new AtomicInteger(0);
    // generates one input split for each file group
    return relPartitionPaths.stream()
        .map(relPartitionPath -> fsView.getLatestMergedFileSlicesBeforeOrOn(relPartitionPath, latestCommit)
            .map(fileSlice -> {
              String basePath = fileSlice.getBaseFile().map(BaseFile::getPath).orElse(null);
              Option<List<String>> logPaths = Option.ofNullable(fileSlice.getLogFiles()
                  .sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString())
                  .collect(Collectors.toList()));
              return new MergeOnReadInputSplit(cnt.getAndAdd(1), basePath, logPaths, latestCommit,
                  metaClient.getBasePath(), maxCompactionMemoryInBytes, mergeType, null);
            }).collect(Collectors.toList()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  public InputFormat<RowData, ?> getInputFormat() {
    return getInputFormat(false);
  }

  @VisibleForTesting
  public InputFormat<RowData, ?> getInputFormat(boolean isStreaming) {
    return isStreaming ? getStreamInputFormat() : getBatchInputFormat();
  }

  private InputFormat<RowData, ?> getBatchInputFormat() {
    final Schema tableAvroSchema = getTableAvroSchema();
    final DataType rowDataType = AvroSchemaConverter.convertToDataType(tableAvroSchema);
    final RowType rowType = (RowType) rowDataType.getLogicalType();
    final RowType requiredRowType = (RowType) getProducedDataType().notNull().getLogicalType();

    final String queryType = this.conf.getString(FlinkOptions.QUERY_TYPE);
    switch (queryType) {
      case FlinkOptions.QUERY_TYPE_SNAPSHOT:
        final HoodieTableType tableType = HoodieTableType.valueOf(this.conf.getString(FlinkOptions.TABLE_TYPE));
        switch (tableType) {
          case MERGE_ON_READ:
            final List<MergeOnReadInputSplit> inputSplits = buildFileIndex();
            if (inputSplits.size() == 0) {
              // When there is no input splits, just return an empty source.
              LOG.warn("No input splits generate for MERGE_ON_READ input format, returns empty collection instead");
              return InputFormats.EMPTY_INPUT_FORMAT;
            }
            return mergeOnReadInputFormat(rowType, requiredRowType, tableAvroSchema,
                rowDataType, inputSplits, false);
          case COPY_ON_WRITE:
            return baseFileOnlyInputFormat();
          default:
            throw new HoodieException("Unexpected table type: " + this.conf.getString(FlinkOptions.TABLE_TYPE));
        }
      case FlinkOptions.QUERY_TYPE_READ_OPTIMIZED:
        return baseFileOnlyInputFormat();
      case FlinkOptions.QUERY_TYPE_INCREMENTAL:
        IncrementalInputSplits incrementalInputSplits = IncrementalInputSplits.builder()
            .conf(conf).path(FilePathUtils.toFlinkPath(path))
            .maxCompactionMemoryInBytes(maxCompactionMemoryInBytes)
            .requiredPartitions(getRequiredPartitionPaths()).build();
        final IncrementalInputSplits.Result result = incrementalInputSplits.inputSplits(metaClient, hadoopConf);
        if (result.isEmpty()) {
          // When there is no input splits, just return an empty source.
          LOG.warn("No input splits generate for incremental read, returns empty collection instead");
          return InputFormats.EMPTY_INPUT_FORMAT;
        }
        return mergeOnReadInputFormat(rowType, requiredRowType, tableAvroSchema,
            rowDataType, result.getInputSplits(), false);
      default:
        String errMsg = String.format("Invalid query type : '%s', options ['%s', '%s', '%s'] are supported now", queryType,
            FlinkOptions.QUERY_TYPE_SNAPSHOT, FlinkOptions.QUERY_TYPE_READ_OPTIMIZED, FlinkOptions.QUERY_TYPE_INCREMENTAL);
        throw new HoodieException(errMsg);
    }
  }

  private InputFormat<RowData, ?> getStreamInputFormat() {
    // if table does not exist, use schema from the DDL
    Schema tableAvroSchema = this.metaClient == null ? inferSchemaFromDdl() : getTableAvroSchema();
    final DataType rowDataType = AvroSchemaConverter.convertToDataType(tableAvroSchema);
    final RowType rowType = (RowType) rowDataType.getLogicalType();
    final RowType requiredRowType = (RowType) getProducedDataType().notNull().getLogicalType();

    final String queryType = this.conf.getString(FlinkOptions.QUERY_TYPE);
    if (FlinkOptions.QUERY_TYPE_SNAPSHOT.equals(queryType)) {
      final HoodieTableType tableType = HoodieTableType.valueOf(this.conf.getString(FlinkOptions.TABLE_TYPE));
      boolean emitDelete = tableType == HoodieTableType.MERGE_ON_READ;
      return mergeOnReadInputFormat(rowType, requiredRowType, tableAvroSchema,
          rowDataType, Collections.emptyList(), emitDelete);
    }
    String errMsg = String.format("Invalid query type : '%s', options ['%s'] are supported now", queryType,
        FlinkOptions.QUERY_TYPE_SNAPSHOT);
    throw new HoodieException(errMsg);
  }

  private MergeOnReadInputFormat mergeOnReadInputFormat(
      RowType rowType,
      RowType requiredRowType,
      Schema tableAvroSchema,
      DataType rowDataType,
      List<MergeOnReadInputSplit> inputSplits,
      boolean emitDelete) {
    final MergeOnReadTableState hoodieTableState = new MergeOnReadTableState(
        rowType,
        requiredRowType,
        tableAvroSchema.toString(),
        AvroSchemaConverter.convertToSchema(requiredRowType).toString(),
        inputSplits,
        conf.getString(FlinkOptions.RECORD_KEY_FIELD).split(","));
    return MergeOnReadInputFormat.builder()
        .config(this.conf)
        .tableState(hoodieTableState)
        // use the explicit fields' data type because the AvroSchemaConverter
        // is not very stable.
        .fieldTypes(rowDataType.getChildren())
        .defaultPartName(conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME))
        .limit(this.limit)
        .emitDelete(emitDelete)
        .build();
  }

  private InputFormat<RowData, ?> baseFileOnlyInputFormat() {
    final Path[] paths = getReadPaths();
    if (paths.length == 0) {
      return InputFormats.EMPTY_INPUT_FORMAT;
    }
    FileInputFormat<RowData> format = new CopyOnWriteInputFormat(
        FilePathUtils.toFlinkPaths(paths),
        this.schema.getColumnNames().toArray(new String[0]),
        this.schema.getColumnDataTypes().toArray(new DataType[0]),
        this.requiredPos,
        this.conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME),
        this.limit == NO_LIMIT_CONSTANT ? Long.MAX_VALUE : this.limit, // ParquetInputFormat always uses the limit value
        getParquetConf(this.conf, this.hadoopConf),
        this.conf.getBoolean(FlinkOptions.UTC_TIMEZONE)
    );
    format.setFilesFilter(new LatestFileFilter(this.hadoopConf));
    return format;
  }

  private Schema inferSchemaFromDdl() {
    Schema schema = AvroSchemaConverter.convertToSchema(this.schema.toPhysicalRowDataType().getLogicalType());
    return HoodieAvroUtils.addMetadataFields(schema, conf.getBoolean(FlinkOptions.CHANGELOG_ENABLED));
  }

  @VisibleForTesting
  public Schema getTableAvroSchema() {
    try {
      TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
      return schemaResolver.getTableAvroSchema();
    } catch (Throwable e) {
      // table exists but has no written data
      LOG.warn("Get table avro schema error, use schema from the DDL instead", e);
      return inferSchemaFromDdl();
    }
  }

  @VisibleForTesting
  public HoodieTableMetaClient getMetaClient() {
    return this.metaClient;
  }

  @VisibleForTesting
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Reset the state of the table source.
   */
  @VisibleForTesting
  public void reset() {
    this.metaClient.reloadActiveTimeline();
    this.requiredPartitions = null;
    this.fileIndex.reset();
  }

  /**
   * Get the reader paths with partition path expanded.
   */
  @VisibleForTesting
  public Path[] getReadPaths() {
    return partitionKeys.isEmpty()
        ? new Path[] {path}
        : FilePathUtils.partitionPath2ReadPath(path, partitionKeys, getOrFetchPartitions(),
        conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING));
  }

  private static class LatestFileFilter extends FilePathFilter {
    private final HoodieROTablePathFilter hoodieFilter;

    public LatestFileFilter(org.apache.hadoop.conf.Configuration hadoopConf) {
      this.hoodieFilter = new HoodieROTablePathFilter(hadoopConf);
    }

    @Override
    public boolean filterPath(org.apache.flink.core.fs.Path filePath) {
      return !this.hoodieFilter.accept(new Path(filePath.toUri()));
    }
  }
}
