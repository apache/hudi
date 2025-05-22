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

import org.apache.hudi.adapter.DataStreamScanProviderAdapter;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.source.ExpressionEvaluators;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.source.ExpressionPredicates.Predicate;
import org.apache.hudi.source.FileIndex;
import org.apache.hudi.source.IncrementalInputSplits;
import org.apache.hudi.source.StreamReadMonitoringFunction;
import org.apache.hudi.source.StreamReadOperator;
import org.apache.hudi.source.prune.ColumnStatsProbe;
import org.apache.hudi.source.prune.PartitionBucketIdFunc;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.source.prune.PrimaryKeyPruners;
import org.apache.hudi.source.rebalance.partitioner.StreamReadAppendPartitioner;
import org.apache.hudi.source.rebalance.partitioner.StreamReadBucketIndexPartitioner;
import org.apache.hudi.source.rebalance.selector.StreamReadAppendKeySelector;
import org.apache.hudi.source.rebalance.selector.StreamReadBucketIndexKeySelector;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.cdc.CdcInputFormat;
import org.apache.hudi.table.format.cow.CopyOnWriteInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.table.lookup.HoodieLookupFunction;
import org.apache.hudi.table.lookup.HoodieLookupTableReader;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.ChangelogModes;
import org.apache.hudi.util.ExpressionUtils;
import org.apache.hudi.util.InputFormats;
import org.apache.hudi.util.SerializableSchema;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.configuration.FlinkOptions.LOOKUP_JOIN_CACHE_TTL;
import static org.apache.hudi.configuration.HadoopConfigurations.getParquetConf;
import static org.apache.hudi.util.ExpressionUtils.filterSimpleCallExpression;
import static org.apache.hudi.util.ExpressionUtils.splitExprByPartitionCall;

/**
 * Hoodie batch table source that always read the latest snapshot of the underneath table.
 */
public class HoodieTableSource implements
    ScanTableSource,
    SupportsProjectionPushDown,
    SupportsLimitPushDown,
    SupportsFilterPushDown,
    LookupTableSource,
    Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableSource.class);

  private static final long NO_LIMIT_CONSTANT = -1;

  private final StorageConfiguration<org.apache.hadoop.conf.Configuration> hadoopConf;
  private final HoodieTableMetaClient metaClient;
  private final long maxCompactionMemoryInBytes;

  private final SerializableSchema schema;
  private final RowType tableRowType;
  private final StoragePath path;
  private final List<String> partitionKeys;
  private final String defaultPartName;
  private final Configuration conf;
  private final InternalSchemaManager internalSchemaManager;

  private int[] requiredPos;
  private long limit;
  private List<Predicate> predicates;
  private ColumnStatsProbe columnStatsProbe;
  private PartitionPruners.PartitionPruner partitionPruner;
  private Option<Function<Integer, Integer>> dataBucketFunc; // numBuckets -> bucketId
  private transient FileIndex fileIndex;

  public HoodieTableSource(
      SerializableSchema schema,
      StoragePath path,
      List<String> partitionKeys,
      String defaultPartName,
      Configuration conf) {
    this(schema, path, partitionKeys, defaultPartName, conf, null, null, null,
        Option.empty(), null, null, null, null);
  }

  public HoodieTableSource(
      SerializableSchema schema,
      StoragePath path,
      List<String> partitionKeys,
      String defaultPartName,
      Configuration conf,
      @Nullable List<Predicate> predicates,
      @Nullable ColumnStatsProbe columnStatsProbe,
      @Nullable PartitionPruners.PartitionPruner partitionPruner,
      Option<Function<Integer, Integer>> dataBucketFunc,
      @Nullable int[] requiredPos,
      @Nullable Long limit,
      @Nullable HoodieTableMetaClient metaClient,
      @Nullable InternalSchemaManager internalSchemaManager) {
    this.schema = schema;
    this.tableRowType = (RowType) this.schema.toSourceRowDataType().notNull().getLogicalType();
    this.path = path;
    this.partitionKeys = partitionKeys;
    this.defaultPartName = defaultPartName;
    this.conf = conf;
    this.predicates = Optional.ofNullable(predicates).orElse(Collections.emptyList());
    this.columnStatsProbe = columnStatsProbe;
    this.partitionPruner = partitionPruner;
    this.dataBucketFunc = dataBucketFunc;
    this.requiredPos = Optional.ofNullable(requiredPos).orElseGet(() -> IntStream.range(0, this.tableRowType.getFieldCount()).toArray());
    this.limit = Optional.ofNullable(limit).orElse(NO_LIMIT_CONSTANT);
    this.hadoopConf = new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(conf));
    this.metaClient = Optional.ofNullable(metaClient).orElseGet(() -> StreamerUtil.metaClientForReader(conf, this.hadoopConf.unwrap()));
    this.maxCompactionMemoryInBytes = StreamerUtil.getMaxCompactionMemoryInBytes(conf);
    this.internalSchemaManager = Optional.ofNullable(internalSchemaManager).orElseGet(() -> InternalSchemaManager.get(this.conf, this.metaClient));
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    return new DataStreamScanProviderAdapter() {

      @Override
      public boolean isBounded() {
        return !conf.getBoolean(FlinkOptions.READ_AS_STREAMING);
      }

      @Override
      public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
        @SuppressWarnings("unchecked")
        TypeInformation<RowData> typeInfo =
            (TypeInformation<RowData>) TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(getProducedDataType());
        OptionsInference.setupSourceTasks(conf, execEnv.getParallelism());
        if (conf.getBoolean(FlinkOptions.READ_AS_STREAMING)) {
          StreamReadMonitoringFunction monitoringFunction = new StreamReadMonitoringFunction(
              conf, FilePathUtils.toFlinkPath(path), tableRowType, maxCompactionMemoryInBytes, partitionPruner);
          InputFormat<RowData, ?> inputFormat = getInputFormat(true);
          OneInputStreamOperatorFactory<MergeOnReadInputSplit, RowData> factory = StreamReadOperator.factory((MergeOnReadInputFormat) inputFormat);
          SingleOutputStreamOperator<MergeOnReadInputSplit> monitorOperatorStream = execEnv.addSource(monitoringFunction, getSourceOperatorName("split_monitor"))
              .uid(Pipelines.opUID("split_monitor", conf))
              .setParallelism(1)
              .setMaxParallelism(1);

          DataStream<MergeOnReadInputSplit> sourceWithKey = addFileDistributionStrategy(monitorOperatorStream);

          SingleOutputStreamOperator<RowData> streamReadSource = sourceWithKey
              .transform("split_reader", typeInfo, factory)
              .uid(Pipelines.opUID("split_reader", conf))
              .setParallelism(conf.getInteger(FlinkOptions.READ_TASKS));
          return new DataStreamSource<>(streamReadSource);
        } else {
          InputFormatSourceFunction<RowData> func = new InputFormatSourceFunction<>(getInputFormat(), typeInfo);
          DataStreamSource<RowData> source = execEnv.addSource(func, asSummaryString(), typeInfo);
          return source.name(getSourceOperatorName("bounded_source")).setParallelism(conf.getInteger(FlinkOptions.READ_TASKS));
        }
      }
    };
  }

  /**
   * Specify the file distribution strategy based on different upstream writing mechanisms,
   *  to prevent hot spot issues during stream reading.
   */
  private DataStream<MergeOnReadInputSplit> addFileDistributionStrategy(SingleOutputStreamOperator<MergeOnReadInputSplit> source) {
    if (OptionsResolver.isMorWithBucketIndexUpsert(conf)) {
      return source.partitionCustom(new StreamReadBucketIndexPartitioner(conf.getInteger(FlinkOptions.READ_TASKS)), new StreamReadBucketIndexKeySelector());
    } else if (OptionsResolver.isAppendMode(conf)) {
      return source.partitionCustom(new StreamReadAppendPartitioner(conf.getInteger(FlinkOptions.READ_TASKS)), new StreamReadAppendKeySelector());
    } else {
      return source.keyBy(MergeOnReadInputSplit::getFileId);
    }
  }

  @Override
  public ChangelogMode getChangelogMode() {
    // when read as streaming and changelog mode is enabled, emit as FULL mode;
    // when read as incremental and cdc is enabled, emit as FULL mode;
    // when all the changes are compacted or read as batch, emit as INSERT mode.
    return OptionsResolver.emitChangelog(conf) ? ChangelogModes.FULL : ChangelogMode.insertOnly();
  }

  @Override
  public DynamicTableSource copy() {
    return new HoodieTableSource(schema, path, partitionKeys, defaultPartName,
        conf, predicates, columnStatsProbe, partitionPruner, dataBucketFunc, requiredPos, limit, metaClient, internalSchemaManager);
  }

  @Override
  public String asSummaryString() {
    return "HudiTableSource";
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> filters) {
    List<ResolvedExpression> simpleFilters = filterSimpleCallExpression(filters);
    Tuple2<List<ResolvedExpression>, List<ResolvedExpression>> splitFilters = splitExprByPartitionCall(simpleFilters, this.partitionKeys, this.tableRowType);
    this.predicates = ExpressionPredicates.fromExpression(splitFilters.f0);
    this.columnStatsProbe = ColumnStatsProbe.newInstance(splitFilters.f0);
    this.partitionPruner = createPartitionPruner(splitFilters.f1, columnStatsProbe);
    this.dataBucketFunc = getDataBucketFunc(splitFilters.f0);
    // refuse all the filters now
    return SupportsFilterPushDown.Result.of(new ArrayList<>(splitFilters.f1), new ArrayList<>(filters));
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

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    Duration duration = conf.get(LOOKUP_JOIN_CACHE_TTL);
    return TableFunctionProvider.of(
        new HoodieLookupFunction(
            new HoodieLookupTableReader(this::getBatchInputFormat, conf),
            (RowType) getProducedDataType().notNull().getLogicalType(),
            getLookupKeys(context.getKeys()),
            duration,
            conf
        ));
  }

  private DataType getProducedDataType() {
    String[] schemaFieldNames = this.schema.getColumnNames().toArray(new String[0]);
    DataType[] schemaTypes = this.schema.getColumnDataTypes().toArray(new DataType[0]);

    return DataTypes.ROW(Arrays.stream(this.requiredPos)
            .mapToObj(i -> DataTypes.FIELD(schemaFieldNames[i], schemaTypes[i]))
            .toArray(DataTypes.Field[]::new))
        .bridgedTo(RowData.class);
  }

  private String getSourceOperatorName(String operatorName) {
    String[] schemaFieldNames = this.schema.getColumnNames().toArray(new String[0]);
    List<String> fields = Arrays.stream(this.requiredPos)
        .mapToObj(i -> schemaFieldNames[i])
        .collect(Collectors.toList());
    return operatorName + "("
        + "table=" + Collections.singletonList(conf.get(FlinkOptions.TABLE_NAME))
        + ", " + "fields=" + fields
        + ")";
  }

  @Nullable
  private PartitionPruners.PartitionPruner createPartitionPruner(List<ResolvedExpression> partitionFilters, ColumnStatsProbe columnStatsProbe) {
    if (!isPartitioned() || partitionFilters.isEmpty() && columnStatsProbe == null) {
      return null;
    }
    StringJoiner joiner = new StringJoiner(" and ");
    partitionFilters.forEach(f -> joiner.add(f.asSummaryString()));
    LOG.info("Partition pruner for hoodie source, condition is:\n" + joiner);
    List<ExpressionEvaluators.Evaluator> evaluators = ExpressionEvaluators.fromExpression(partitionFilters);
    List<DataType> partitionTypes = this.partitionKeys.stream().map(name ->
            this.schema.getColumn(name).orElseThrow(() -> new HoodieValidationException("Field " + name + " does not exist")))
        .map(SerializableSchema.Column::getDataType)
        .collect(Collectors.toList());
    String defaultParName = conf.get(FlinkOptions.PARTITION_DEFAULT_NAME);
    boolean hivePartition = conf.get(FlinkOptions.HIVE_STYLE_PARTITIONING);

    return PartitionPruners.builder()
        .basePath(path.toString())
        .rowType(tableRowType)
        .conf(conf)
        .columnStatsProbe(columnStatsProbe)
        .partitionEvaluators(evaluators)
        .partitionKeys(partitionKeys)
        .partitionTypes(partitionTypes)
        .defaultParName(defaultParName)
        .hivePartition(hivePartition)
        .build();
  }

  private Option<Function<Integer, Integer>> getDataBucketFunc(List<ResolvedExpression> dataFilters) {
    if (!OptionsResolver.isBucketIndexType(conf) || dataFilters.isEmpty()) {
      return Option.empty();
    }
    Set<String> indexKeyFields = Arrays.stream(OptionsResolver.getIndexKeys(conf)).collect(Collectors.toSet());
    List<ResolvedExpression> indexKeyFilters = dataFilters.stream().filter(expr -> ExpressionUtils.isEqualsLitExpr(expr, indexKeyFields)).collect(Collectors.toList());
    if (!ExpressionUtils.isFilteringByAllFields(indexKeyFilters, indexKeyFields)) {
      return Option.empty();
    }
    return Option.of(PrimaryKeyPruners.getBucketIdFunc(indexKeyFilters, conf));
  }

  private List<MergeOnReadInputSplit> buildInputSplits() {
    FileIndex fileIndex = getOrBuildFileIndex();
    List<String> relPartitionPaths = fileIndex.getOrBuildPartitionPaths();
    if (relPartitionPaths.isEmpty()) {
      return Collections.emptyList();
    }
    List<StoragePathInfo> pathInfoList = fileIndex.getFilesInPartitions();
    if (pathInfoList.isEmpty()) {
      throw new HoodieException("No files found for reading in user provided path.");
    }

    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        // file-slice after pending compaction-requested instant-time is also considered valid
        metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants(),
        pathInfoList);
    if (!fsView.getLastInstant().isPresent()) {
      return Collections.emptyList();
    }
    String latestCommit = fsView.getLastInstant().get().requestedTime();
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
                  metaClient.getBasePath().toString(), maxCompactionMemoryInBytes, mergeType, null, fileSlice.getFileId());
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
            final List<MergeOnReadInputSplit> inputSplits = buildInputSplits();
            if (inputSplits.isEmpty()) {
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
            .conf(conf)
            .path(FilePathUtils.toFlinkPath(path))
            .rowType(this.tableRowType)
            .maxCompactionMemoryInBytes(maxCompactionMemoryInBytes)
            .partitionPruner(partitionPruner)
            .build();
        final boolean cdcEnabled = this.conf.getBoolean(FlinkOptions.CDC_ENABLED);
        final IncrementalInputSplits.Result result = incrementalInputSplits.inputSplits(metaClient, cdcEnabled);
        if (result.isEmpty()) {
          // When there is no input splits, just return an empty source.
          LOG.warn("No input splits generate for incremental read, returns empty collection instead");
          return InputFormats.EMPTY_INPUT_FORMAT;
        } else if (cdcEnabled) {
          return cdcInputFormat(rowType, requiredRowType, tableAvroSchema, rowDataType, result.getInputSplits());
        } else {
          return mergeOnReadInputFormat(rowType, requiredRowType, tableAvroSchema,
              rowDataType, result.getInputSplits(), false);
        }
      default:
        String errMsg = String.format("Invalid query type : '%s', options ['%s', '%s', '%s'] are supported now", queryType,
            FlinkOptions.QUERY_TYPE_SNAPSHOT, FlinkOptions.QUERY_TYPE_READ_OPTIMIZED, FlinkOptions.QUERY_TYPE_INCREMENTAL);
        throw new HoodieException(errMsg);
    }
  }

  private InputFormat<RowData, ?> getStreamInputFormat() {
    // if table does not exist or table data does not exist, use schema from the DDL
    Schema tableAvroSchema = (this.metaClient == null || !tableDataExists()) ? inferSchemaFromDdl() : getTableAvroSchema();
    final DataType rowDataType = AvroSchemaConverter.convertToDataType(tableAvroSchema);
    final RowType rowType = (RowType) rowDataType.getLogicalType();
    final RowType requiredRowType = (RowType) getProducedDataType().notNull().getLogicalType();

    final String queryType = this.conf.getString(FlinkOptions.QUERY_TYPE);
    switch (queryType) {
      case FlinkOptions.QUERY_TYPE_SNAPSHOT:
      case FlinkOptions.QUERY_TYPE_INCREMENTAL:
        final HoodieTableType tableType = HoodieTableType.valueOf(this.conf.getString(FlinkOptions.TABLE_TYPE));
        boolean emitDelete = tableType == HoodieTableType.MERGE_ON_READ;
        if (this.conf.getBoolean(FlinkOptions.CDC_ENABLED)) {
          return cdcInputFormat(rowType, requiredRowType, tableAvroSchema, rowDataType, Collections.emptyList());
        } else {
          return mergeOnReadInputFormat(rowType, requiredRowType, tableAvroSchema,
              rowDataType, Collections.emptyList(), emitDelete);
        }
      default:
        String errMsg = String.format("Invalid query type : '%s', options ['%s', '%s'] are supported now", queryType,
            FlinkOptions.QUERY_TYPE_SNAPSHOT, FlinkOptions.QUERY_TYPE_INCREMENTAL);
        throw new HoodieException(errMsg);
    }
  }

  /**
   * Returns whether the hoodie table data exists .
   */
  private boolean tableDataExists() {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    Option<Pair<HoodieInstant, HoodieCommitMetadata>> instantAndCommitMetadata = activeTimeline.getLastCommitMetadataWithValidData();
    return instantAndCommitMetadata.isPresent();
  }

  private MergeOnReadInputFormat cdcInputFormat(
      RowType rowType,
      RowType requiredRowType,
      Schema tableAvroSchema,
      DataType rowDataType,
      List<MergeOnReadInputSplit> inputSplits) {
    final MergeOnReadTableState hoodieTableState = new MergeOnReadTableState(
        rowType,
        requiredRowType,
        tableAvroSchema.toString(),
        AvroSchemaConverter.convertToSchema(requiredRowType).toString(),
        inputSplits,
        conf.getString(FlinkOptions.RECORD_KEY_FIELD).split(","));
    return CdcInputFormat.builder()
        .config(this.conf)
        .tableState(hoodieTableState)
        // use the explicit fields' data type because the AvroSchemaConverter
        // is not very stable.
        .fieldTypes(rowDataType.getChildren())
        .predicates(this.predicates)
        .limit(this.limit)
        .emitDelete(false) // the change logs iterator can handle the DELETE records
        .build();
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
        .predicates(this.predicates)
        .limit(this.limit)
        .emitDelete(emitDelete)
        .internalSchemaManager(internalSchemaManager)
        .build();
  }

  private InputFormat<RowData, ?> baseFileOnlyInputFormat() {
    final List<StoragePathInfo> pathInfoList = getReadFiles();
    if (pathInfoList.isEmpty()) {
      return InputFormats.EMPTY_INPUT_FORMAT;
    }

    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants(), pathInfoList);
    Path[] paths = fsView.getLatestBaseFiles()
        .map(HoodieBaseFile::getPathInfo)
        .map(e -> new Path(e.getPath().toUri())).toArray(Path[]::new);

    if (paths.length == 0) {
      return InputFormats.EMPTY_INPUT_FORMAT;
    }

    return new CopyOnWriteInputFormat(
        FilePathUtils.toFlinkPaths(paths),
        this.schema.getColumnNames().toArray(new String[0]),
        this.schema.getColumnDataTypes().toArray(new DataType[0]),
        this.requiredPos,
        this.conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME),
        this.conf.getString(FlinkOptions.PARTITION_PATH_FIELD),
        this.conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING),
        this.predicates,
        this.limit == NO_LIMIT_CONSTANT ? Long.MAX_VALUE : this.limit, // ParquetInputFormat always uses the limit value
        getParquetConf(this.conf, this.hadoopConf.unwrap()),
        this.conf.getBoolean(FlinkOptions.READ_UTC_TIMEZONE),
        this.internalSchemaManager
    );
  }

  private Schema inferSchemaFromDdl() {
    Schema schema = AvroSchemaConverter.convertToSchema(this.tableRowType);
    return HoodieAvroUtils.addMetadataFields(schema, conf.getBoolean(FlinkOptions.CHANGELOG_ENABLED));
  }

  private FileIndex getOrBuildFileIndex() {
    if (this.fileIndex == null) {
      this.fileIndex = FileIndex.builder()
          .path(this.path)
          .conf(this.conf)
          .rowType(this.tableRowType)
          .columnStatsProbe(this.columnStatsProbe)
          .partitionPruner(this.partitionPruner)
          .partitionBucketIdFunc(PartitionBucketIdFunc.create(this.dataBucketFunc,
              this.metaClient, conf.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS)))
          .build();
    }
    return this.fileIndex;
  }

  private int[] getLookupKeys(int[][] keys) {
    int[] keyIndices = new int[keys.length];
    int i = 0;
    for (int[] key : keys) {
      if (key.length > 1) {
        throw new UnsupportedOperationException(
            "Hoodie lookup can not support nested key now.");
      }
      keyIndices[i] = key[0];
      i++;
    }
    return keyIndices;
  }

  private boolean isPartitioned() {
    return !this.partitionKeys.isEmpty() && this.partitionKeys.stream().noneMatch(String::isEmpty);
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
    this.fileIndex = null;
  }

  /**
   * Get the reader paths with partition path expanded.
   */
  @VisibleForTesting
  public List<StoragePathInfo> getReadFiles() {
    List<String> relPartitionPaths = getReadPartitions();
    if (relPartitionPaths.isEmpty()) {
      return Collections.emptyList();
    }
    return fileIndex.getFilesInPartitions();
  }

  @VisibleForTesting
  public List<String> getReadPartitions() {
    FileIndex fileIndex = getOrBuildFileIndex();
    return fileIndex.getOrBuildPartitionPaths();
  }

  @VisibleForTesting
  public List<Predicate> getPredicates() {
    return predicates;
  }

  @VisibleForTesting
  public ColumnStatsProbe getColumnStatsProbe() {
    return columnStatsProbe;
  }

  @VisibleForTesting
  public Option<Function<Integer, Integer>> getDataBucketFunc() {
    return dataBucketFunc;
  }
}
