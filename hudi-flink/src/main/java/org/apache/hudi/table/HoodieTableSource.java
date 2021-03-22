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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.HoodieROTablePathFilter;
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils;
import org.apache.hudi.source.StreamReadMonitoringFunction;
import org.apache.hudi.source.StreamReadOperator;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.cow.CopyOnWriteInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.sources.FilterableTableSource;
import org.apache.flink.table.sources.LimitableTableSource;
import org.apache.flink.table.sources.PartitionableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes;
import static org.apache.hudi.table.format.FormatUtils.getParquetConf;

/**
 * Hoodie batch table source that always read the latest snapshot of the underneath table.
 */
public class HoodieTableSource implements
    StreamTableSource<RowData>,
    PartitionableTableSource,
    ProjectableTableSource<RowData>,
    LimitableTableSource<RowData>,
    FilterableTableSource<RowData> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableSource.class);

  private static final int NO_LIMIT_CONSTANT = -1;

  private final transient org.apache.hadoop.conf.Configuration hadoopConf;
  private final transient HoodieTableMetaClient metaClient;
  private final long maxCompactionMemoryInBytes;

  private final TableSchema schema;
  private final Path path;
  private final List<String> partitionKeys;
  private final String defaultPartName;
  private final Configuration conf;

  private final int[] requiredPos;
  private final long limit;
  private final List<Expression> filters;

  private List<Map<String, String>> requiredPartitions;

  public HoodieTableSource(
      TableSchema schema,
      Path path,
      List<String> partitionKeys,
      String defaultPartName,
      Configuration conf) {
    this(schema, path, partitionKeys, defaultPartName, conf, null, null, null, null);
  }

  public HoodieTableSource(
      TableSchema schema,
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
    this.requiredPartitions = requiredPartitions;
    this.requiredPos = requiredPos == null
        ? IntStream.range(0, schema.getFieldCount()).toArray()
        : requiredPos;
    this.limit = limit == null ? NO_LIMIT_CONSTANT : limit;
    this.filters = filters == null ? Collections.emptyList() : filters;
    final String basePath = this.conf.getString(FlinkOptions.PATH);
    this.hadoopConf = StreamerUtil.getHadoopConf();
    this.metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).build();
    this.maxCompactionMemoryInBytes = getMaxCompactionMemoryInBytes(new JobConf(this.hadoopConf));
    if (conf.getBoolean(FlinkOptions.READ_AS_STREAMING)) {
      ValidationUtils.checkArgument(
          conf.getString(FlinkOptions.TABLE_TYPE).equalsIgnoreCase(FlinkOptions.TABLE_TYPE_MERGE_ON_READ),
          "Streaming read is only supported for table type: " + FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    }
  }

  @Override
  public DataStream<RowData> getDataStream(StreamExecutionEnvironment execEnv) {
    @SuppressWarnings("unchecked")
    TypeInformation<RowData> typeInfo =
        (TypeInformation<RowData>) TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(getProducedDataType());
    if (conf.getBoolean(FlinkOptions.READ_AS_STREAMING)) {
      StreamReadMonitoringFunction monitoringFunction = new StreamReadMonitoringFunction(
          conf, FilePathUtils.toFlinkPath(path), metaClient, maxCompactionMemoryInBytes);
      OneInputStreamOperatorFactory<MergeOnReadInputSplit, RowData> factory = StreamReadOperator.factory((MergeOnReadInputFormat) getInputFormat(true));
      SingleOutputStreamOperator<RowData> source = execEnv.addSource(monitoringFunction, "streaming_source")
          .setParallelism(1)
          .uid("uid_streaming_source")
          .transform("split_reader", typeInfo, factory)
          .setParallelism(conf.getInteger(FlinkOptions.READ_TASKS))
          .uid("uid_split_reader");
      return new DataStreamSource<>(source);
    } else {
      InputFormatSourceFunction<RowData> func = new InputFormatSourceFunction<>(getInputFormat(), typeInfo);
      DataStreamSource<RowData> source = execEnv.addSource(func, explainSource(), typeInfo);
      return source.name("streaming_source")
          .setParallelism(conf.getInteger(FlinkOptions.READ_TASKS))
          .uid("uid_streaming_source");
    }
  }

  @Override
  public boolean isBounded() {
    return !conf.getBoolean(FlinkOptions.READ_AS_STREAMING);
  }

  @Override
  public TableSource<RowData> applyPredicate(List<Expression> predicates) {
    return new HoodieTableSource(schema, path, partitionKeys, defaultPartName, conf,
        requiredPartitions, requiredPos, limit, new ArrayList<>(predicates));
  }

  @Override
  public boolean isFilterPushedDown() {
    return this.filters != null && this.filters.size() > 0;
  }

  @Override
  public boolean isLimitPushedDown() {
    return this.limit != NO_LIMIT_CONSTANT;
  }

  @Override
  public TableSource<RowData> applyLimit(long limit) {
    return new HoodieTableSource(schema, path, partitionKeys, defaultPartName, conf,
        requiredPartitions, requiredPos, limit, filters);
  }

  @Override
  public List<Map<String, String>> getPartitions() {
    return FilePathUtils.getPartitions(path, hadoopConf, partitionKeys, defaultPartName,
        conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITION));
  }

  @Override
  public TableSource applyPartitionPruning(List<Map<String, String>> requiredPartitions) {
    return new HoodieTableSource(schema, path, partitionKeys, defaultPartName, conf,
        requiredPartitions, requiredPos, limit, filters);
  }

  @Override
  public TableSource<RowData> projectFields(int[] requiredPos) {
    return new HoodieTableSource(schema, path, partitionKeys, defaultPartName, conf,
        requiredPartitions, requiredPos, limit, filters);
  }

  @Override
  public TableSchema getTableSchema() {
    return schema;
  }

  @Override
  public String explainSource() {
    final String filterString = filters.stream()
        .map(Expression::asSummaryString)
        .collect(Collectors.joining(","));
    return TableConnectorUtils.generateRuntimeName(getClass(), getTableSchema().getFieldNames())
        + (requiredPartitions == null ? "" : ", requiredPartition=" + requiredPartitions)
        + (requiredPos == null ? "" : ", requiredPos=" + Arrays.toString(requiredPos))
        + (limit == -1 ? "" : ", limit=" + limit)
        + (filters.size() == 0 ? "" : ", filters=" + filterString);
  }

  @Override
  public DataType getProducedDataType() {
    String[] schemaFieldNames = this.schema.getFieldNames();
    DataType[] schemaTypes = this.schema.getFieldDataTypes();

    return DataTypes.ROW(Arrays.stream(this.requiredPos)
        .mapToObj(i -> DataTypes.FIELD(schemaFieldNames[i], schemaTypes[i]))
        .toArray(DataTypes.Field[]::new))
        .bridgedTo(RowData.class);
  }

  private List<Map<String, String>> getOrFetchPartitions() {
    if (requiredPartitions == null) {
      requiredPartitions = getPartitions();
    }
    return requiredPartitions;
  }

  private List<MergeOnReadInputSplit> buildFileIndex(Path[] paths) {
    FileStatus[] fileStatuses = Arrays.stream(paths)
        .flatMap(path ->
            Arrays.stream(FilePathUtils.getFileStatusRecursively(path, 1, hadoopConf)))
        .toArray(FileStatus[]::new);
    if (fileStatuses.length == 0) {
      throw new HoodieException("No files found for reading in user provided path.");
    }

    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitsTimeline()
            .filterCompletedInstants(), fileStatuses);
    List<HoodieBaseFile> latestFiles = fsView.getLatestBaseFiles().collect(Collectors.toList());
    String latestCommit = fsView.getLastInstant().get().getTimestamp();
    final String mergeType = this.conf.getString(FlinkOptions.MERGE_TYPE);
    final AtomicInteger cnt = new AtomicInteger(0);
    if (latestFiles.size() > 0) {
      Map<HoodieBaseFile, List<String>> fileGroup =
          HoodieRealtimeInputFormatUtils.groupLogsByBaseFile(hadoopConf, latestFiles);
      return fileGroup.entrySet().stream().map(kv -> {
        HoodieBaseFile baseFile = kv.getKey();
        Option<List<String>> logPaths = kv.getValue().size() == 0
            ? Option.empty()
            : Option.of(kv.getValue());
        return new MergeOnReadInputSplit(cnt.getAndAdd(1),
            baseFile.getPath(), logPaths, latestCommit,
            metaClient.getBasePath(), maxCompactionMemoryInBytes, mergeType, null);
      }).collect(Collectors.toList());
    } else {
      // all the files are logs
      return Arrays.stream(paths).map(partitionPath -> {
        String relPartitionPath = FSUtils.getRelativePartitionPath(path, partitionPath);
        return fsView.getLatestMergedFileSlicesBeforeOrOn(relPartitionPath, latestCommit)
            .map(fileSlice -> {
              Option<List<String>> logPaths = Option.ofNullable(fileSlice.getLogFiles()
                  .sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString())
                  .collect(Collectors.toList()));
              return new MergeOnReadInputSplit(cnt.getAndAdd(1),
                  null, logPaths, latestCommit,
                  metaClient.getBasePath(), maxCompactionMemoryInBytes, mergeType, null);
            }).collect(Collectors.toList()); })
          .flatMap(Collection::stream)
          .collect(Collectors.toList());
    }
  }

  public InputFormat<RowData, ?> getInputFormat() {
    return getInputFormat(false);
  }

  @VisibleForTesting
  public InputFormat<RowData, ?> getInputFormat(boolean isStreaming) {
    // When this table has no partition, just return an empty source.
    if (!partitionKeys.isEmpty() && getOrFetchPartitions().isEmpty()) {
      return new CollectionInputFormat<>(Collections.emptyList(), null);
    }

    final Path[] paths = getReadPaths();
    if (paths.length == 0) {
      return new CollectionInputFormat<>(Collections.emptyList(), null);
    }

    TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
    final Schema tableAvroSchema;
    try {
      tableAvroSchema = schemaUtil.getTableAvroSchema();
    } catch (Exception e) {
      throw new HoodieException("Get table avro schema error", e);
    }
    final DataType rowDataType = AvroSchemaConverter.convertToDataType(tableAvroSchema);
    final RowType rowType = (RowType) rowDataType.getLogicalType();
    final RowType requiredRowType = (RowType) getProducedDataType().notNull().getLogicalType();

    final String queryType = this.conf.getString(FlinkOptions.QUERY_TYPE);
    switch (queryType) {
      case FlinkOptions.QUERY_TYPE_SNAPSHOT:
        final HoodieTableType tableType = HoodieTableType.valueOf(this.conf.getString(FlinkOptions.TABLE_TYPE));
        switch (tableType) {
          case MERGE_ON_READ:
            final List<MergeOnReadInputSplit> inputSplits;
            if (!isStreaming) {
              inputSplits = buildFileIndex(paths);
              if (inputSplits.size() == 0) {
                // When there is no input splits, just return an empty source.
                LOG.warn("No input splits generate for MERGE_ON_READ input format, returns empty collection instead");
                return new CollectionInputFormat<>(Collections.emptyList(), null);
              }
            } else {
              // streaming reader would build the splits automatically.
              inputSplits = Collections.emptyList();
            }
            final MergeOnReadTableState hoodieTableState = new MergeOnReadTableState(
                rowType,
                requiredRowType,
                tableAvroSchema.toString(),
                AvroSchemaConverter.convertToSchema(requiredRowType).toString(),
                inputSplits);
            return new MergeOnReadInputFormat(
                this.conf,
                FilePathUtils.toFlinkPaths(paths),
                hoodieTableState,
                rowDataType.getChildren(), // use the explicit fields data type because the AvroSchemaConverter is not very stable.
                "default",
                this.limit);
          case COPY_ON_WRITE:
            FileInputFormat<RowData> format = new CopyOnWriteInputFormat(
                FilePathUtils.toFlinkPaths(paths),
                this.schema.getFieldNames(),
                this.schema.getFieldDataTypes(),
                this.requiredPos,
                "default",
                this.limit == NO_LIMIT_CONSTANT ? Long.MAX_VALUE : this.limit, // ParquetInputFormat always uses the limit value
                getParquetConf(this.conf, this.hadoopConf),
                this.conf.getBoolean(FlinkOptions.UTC_TIMEZONE)
            );
            format.setFilesFilter(new LatestFileFilter(this.hadoopConf));
            return format;
          default:
            throw new HoodieException("Unexpected table type: " + this.conf.getString(FlinkOptions.TABLE_TYPE));
        }
      case FlinkOptions.QUERY_TYPE_READ_OPTIMIZED:
        FileInputFormat<RowData> format = new CopyOnWriteInputFormat(
            FilePathUtils.toFlinkPaths(paths),
            this.schema.getFieldNames(),
            this.schema.getFieldDataTypes(),
            this.requiredPos,
            "default",
            this.limit == NO_LIMIT_CONSTANT ? Long.MAX_VALUE : this.limit, // ParquetInputFormat always uses the limit value
            getParquetConf(this.conf, this.hadoopConf),
            this.conf.getBoolean(FlinkOptions.UTC_TIMEZONE)
        );
        format.setFilesFilter(new LatestFileFilter(this.hadoopConf));
        return format;
      default:
        String errMsg = String.format("Invalid query type : '%s', options ['%s', '%s'] are supported now", queryType,
            FlinkOptions.QUERY_TYPE_SNAPSHOT, FlinkOptions.QUERY_TYPE_READ_OPTIMIZED);
        throw new HoodieException(errMsg);
    }
  }

  @VisibleForTesting
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Reload the active timeline view.
   */
  @VisibleForTesting
  public void reloadActiveTimeline() {
    this.metaClient.reloadActiveTimeline();
  }

  /**
   * Get the reader paths with partition path expanded.
   */
  @VisibleForTesting
  public Path[] getReadPaths() {
    return partitionKeys.isEmpty()
        ? new Path[] {path}
        : FilePathUtils.partitionPath2ReadPath(path, partitionKeys, getOrFetchPartitions(),
        conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITION));
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
