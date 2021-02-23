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

package org.apache.hudi.source;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.HoodieROTablePathFilter;
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils;
import org.apache.hudi.operator.FlinkOptions;
import org.apache.hudi.source.format.FilePathUtils;
import org.apache.hudi.source.format.cow.CopyOnWriteInputFormat;
import org.apache.hudi.source.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.source.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.source.format.mor.MergeOnReadTableState;
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
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes;
import static org.apache.hudi.source.format.FormatUtils.getParquetConf;

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
  }

  @Override
  public DataStream<RowData> getDataStream(StreamExecutionEnvironment execEnv) {
    @SuppressWarnings("unchecked")
    TypeInformation<RowData> typeInfo =
        (TypeInformation<RowData>) TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(getProducedDataType());
    InputFormatSourceFunction<RowData> func = new InputFormatSourceFunction<>(getInputFormat(), typeInfo);
    DataStreamSource<RowData> source = execEnv.addSource(func, explainSource(), typeInfo);
    return source.name(explainSource());
  }

  @Override
  public boolean isBounded() {
    return true;
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
    try {
      return FilePathUtils
          .searchPartKeyValueAndPaths(
              path.getFileSystem(),
              path,
              conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITION),
              partitionKeys.toArray(new String[0]))
          .stream()
          .map(tuple2 -> tuple2.f0)
          .map(spec -> {
            LinkedHashMap<String, String> ret = new LinkedHashMap<>();
            spec.forEach((k, v) -> ret.put(k, defaultPartName.equals(v) ? null : v));
            return ret;
          })
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new TableException("Fetch partitions fail.", e);
    }
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
        .flatMap(path -> Arrays.stream(FilePathUtils.getHadoopFileStatusRecursively(path, 1, hadoopConf)))
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
            metaClient.getBasePath(), maxCompactionMemoryInBytes, mergeType);
      }).collect(Collectors.toList());
    } else {
      // all the files are logs
      return Arrays.stream(paths).map(partitionPath -> {
        String relPartitionPath = FSUtils.getRelativePartitionPath(
            new org.apache.hadoop.fs.Path(path.toUri()),
            new org.apache.hadoop.fs.Path(partitionPath.toUri()));
        return fsView.getLatestMergedFileSlicesBeforeOrOn(relPartitionPath, latestCommit)
            .map(fileSlice -> {
              Option<List<String>> logPaths = Option.ofNullable(fileSlice.getLogFiles()
                  .sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString())
                  .collect(Collectors.toList()));
              return new MergeOnReadInputSplit(cnt.getAndAdd(1),
                  null, logPaths, latestCommit,
                  metaClient.getBasePath(), maxCompactionMemoryInBytes, mergeType);
            }).collect(Collectors.toList()); })
          .flatMap(Collection::stream)
          .collect(Collectors.toList());
    }
  }

  @VisibleForTesting
  public InputFormat<RowData, ?> getInputFormat() {
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
    if (queryType.equals(FlinkOptions.QUERY_TYPE_SNAPSHOT)) {
      switch (this.conf.getString(FlinkOptions.TABLE_TYPE)) {
        case FlinkOptions.TABLE_TYPE_MERGE_ON_READ:
          final List<MergeOnReadInputSplit> inputSplits = buildFileIndex(paths);
          if (inputSplits.size() == 0) {
            // When there is no input splits, just return an empty source.
            LOG.warn("No input inputs generate for MERGE_ON_READ input format, returns empty collection instead");
            return new CollectionInputFormat<>(Collections.emptyList(), null);
          }
          final MergeOnReadTableState hoodieTableState = new MergeOnReadTableState(
              rowType,
              requiredRowType,
              tableAvroSchema.toString(),
              AvroSchemaConverter.convertToSchema(requiredRowType).toString(),
              inputSplits);
          return new MergeOnReadInputFormat(
              this.conf,
              paths,
              hoodieTableState,
              rowDataType.getChildren(), // use the explicit fields data type because the AvroSchemaConvertr is not very stable.
              "default",
              this.limit);
        case FlinkOptions.TABLE_TYPE_COPY_ON_WRITE:
          FileInputFormat<RowData> format = new CopyOnWriteInputFormat(
              paths,
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
    } else {
      throw new HoodieException("Invalid query type : '" + queryType + "'. Only '"
          + FlinkOptions.QUERY_TYPE_SNAPSHOT + "' is supported now");
    }
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
    if (partitionKeys.isEmpty()) {
      return new Path[] {path};
    } else {
      return getOrFetchPartitions().stream()
          .map(HoodieTableSource.this::validateAndReorderPartitions)
          .map(kvs -> FilePathUtils.generatePartitionPath(kvs, conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITION)))
          .map(n -> new Path(path, n))
          .toArray(Path[]::new);
    }
  }

  private LinkedHashMap<String, String> validateAndReorderPartitions(Map<String, String> part) {
    LinkedHashMap<String, String> map = new LinkedHashMap<>();
    for (String k : partitionKeys) {
      if (!part.containsKey(k)) {
        throw new TableException("Partition keys are: " + partitionKeys
            + ", incomplete partition spec: " + part);
      }
      map.put(k, part.get(k));
    }
    return map;
  }

  private static class LatestFileFilter extends FilePathFilter {
    private final HoodieROTablePathFilter hoodieFilter;

    public LatestFileFilter(org.apache.hadoop.conf.Configuration hadoopConf) {
      this.hoodieFilter = new HoodieROTablePathFilter(hadoopConf);
    }

    @Override
    public boolean filterPath(Path filePath) {
      return !this.hoodieFilter.accept(new org.apache.hadoop.fs.Path(filePath.toUri()));
    }
  }
}
