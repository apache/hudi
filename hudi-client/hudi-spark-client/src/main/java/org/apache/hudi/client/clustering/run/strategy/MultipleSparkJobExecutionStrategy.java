/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.HoodieDatasetBulkInsertHelper;
import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.utils.SerDeHelper;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.FutureUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SortUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.LazyConcatenatingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.CloseableIteratorListener;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerFactory;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerWithRowsFactory;
import org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RDDSpatialCurveSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RowCustomColumnsSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RowSpatialCurveSortPartitioner;
import org.apache.hudi.execution.bulkinsert.SpatialCurveSortPartitionerBase;
import org.apache.hudi.io.MergeUtils;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS;
import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * Clustering strategy to submit multiple spark jobs and union the results.
 */
@Slf4j
public abstract class MultipleSparkJobExecutionStrategy<T>
    extends ClusteringExecutionStrategy<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> {

  /*
   * Clustering builds one RDD per group and unions them into a single write stage, so Spark ships the lineage of
   * every group to every task of that stage. Java serialization writes an instance once per stream however many
   * closures reference it, but writes each distinct instance in full: an object built per group is deserialized
   * once per group by every task, and held by every running task. Whatever does not depend on the group (table
   * schema, reader context factory, reader properties, write config, writer schema) is therefore built once per run
   * and shared by all groups: see ClusteringRunInvariants.
   */
  private transient volatile ClusteringRunInvariants runInvariants;

  public MultipleSparkJobExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  /**
   * The write config for a group's bulk insert: this strategy's write config, with the max Parquet file size
   * overridden when one is given. Every group of a run gets the same instance (see above); a
   * {@link HoodieWriteConfig} holds a full copy of the write properties for itself and for each of its sub-configs.
   * The returned config must not be mutated. Called outside {@link #performClustering}, it builds a new config.
   *
   * <p>Bulk-insert parallelism is deliberately not set: the clustering write paths pass the number of output groups
   * to the partitioner and to the bulk-insert helper explicitly, and nothing on them reads
   * {@link HoodieWriteConfig#BULKINSERT_PARALLELISM_VALUE}.
   */
  protected HoodieWriteConfig getGroupWriteConfig(Option<Long> parquetMaxFileSize) {
    ClusteringRunInvariants run = runInvariants;
    return run != null
        ? run.groupWriteConfigs.computeIfAbsent(parquetMaxFileSize, this::newGroupWriteConfig)
        : newGroupWriteConfig(parquetMaxFileSize);
  }

  private HoodieWriteConfig newGroupWriteConfig(Option<Long> parquetMaxFileSize) {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(getWriteConfig().getProps()).build();
    parquetMaxFileSize.ifPresent(bytes -> config.setValue(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE, String.valueOf(bytes)));
    return config;
  }

  /**
   * The schema a group's bulk-insert writers use, as {@link HoodieDatasetBulkInsertHelper#writerSchema} derives it.
   * Every group with an equal dataset schema and the same table schema gets the same instance (see above);
   * {@link StructType} is immutable. Called outside {@link #performClustering}, it derives a new schema.
   */
  protected StructType getGroupWriterSchema(StructType datasetSchema, HoodieWriteConfig config) {
    ClusteringRunInvariants run = runInvariants;
    return run != null
        ? run.groupWriterSchemas.computeIfAbsent(Pair.of(datasetSchema, config.getSchema()),
            key -> HoodieDatasetBulkInsertHelper.writerSchema(datasetSchema, config))
        : HoodieDatasetBulkInsertHelper.writerSchema(datasetSchema, config);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> performClustering(final HoodieClusteringPlan clusteringPlan, final HoodieSchema schema, final String instantTime) {
    JavaSparkContext engineContext = HoodieSparkEngineContext.getSparkContext(getEngineContext());
    boolean shouldPreserveMetadata = Option.ofNullable(clusteringPlan.getPreserveHoodieMetadata()).orElse(true);
    ExecutorService clusteringExecutorService = Executors.newFixedThreadPool(
        Math.min(clusteringPlan.getInputGroups().size(), writeConfig.getClusteringMaxParallelism()),
        new CustomizedThreadFactory("clustering-job-group", true));
    try {
      boolean canUseRowWriter = getWriteConfig().getBooleanOrDefault("hoodie.datasource.write.row.writer.enable", true);
      Map<String, String> strategyParams = clusteringPlan.getStrategy().getStrategyParams();
      // Before any group is submitted, and after a subclass has settled the write config
      // (SparkBinaryCopyClusteringExecutionStrategy replaces it before calling this method).
      runInvariants = canUseRowWriter ? newRowWriterRunInvariants() : newRecordsRunInvariants();
      // execute clustering for each group async and collect WriteStatus
      Stream<HoodieData<WriteStatus>> writeStatusesStream = FutureUtils.allOf(
              clusteringPlan.getInputGroups().stream()
                  .map(inputGroup -> canUseRowWriter
                      ? runClusteringForGroupAsyncAsRow(inputGroup, strategyParams, shouldPreserveMetadata, instantTime,
                          clusteringExecutorService)
                      : runClusteringForGroupAsync(inputGroup, strategyParams, shouldPreserveMetadata, instantTime,
                          clusteringExecutorService))
                  .collect(Collectors.toList()))
          .join()
          .stream();
      JavaRDD<WriteStatus>[] writeStatuses = convertStreamToArray(writeStatusesStream.map(HoodieJavaRDD::getJavaRDD));
      JavaRDD<WriteStatus> writeStatusRDD = engineContext.union(writeStatuses);

      HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = new HoodieWriteMetadata<>();
      writeMetadata.setWriteStatuses(HoodieJavaRDD.of(writeStatusRDD));
      return writeMetadata;
    } finally {
      // Every group has been submitted and joined: the groups' RDDs hold what they need from the run invariants.
      runInvariants = null;
      clusteringExecutorService.shutdown();
    }
  }

  /**
   * What every group of one {@link #performClustering} run shares (see the class comment), fixed for the run: built on
   * the driver before the groups are submitted, dropped when they have all been submitted. The caches only memoize:
   * each derives its value from the key and the run's write config, so a key's value never changes within the run.
   * Never serialized: groups capture its values, not the holder.
   */
  private static final class ClusteringRunInvariants {
    /** The schema the groups' records have, with meta fields, as handed to performClusteringWithRecords*. */
    private final HoodieSchema readerSchema;
    private final ReaderContextFactory<?> readerContextFactory;
    /** Row writer only: {@link #readerSchema} as Spark types. */
    private final StructType sparkReaderSchema;
    /** Row writer only: the file group readers' properties. */
    private final TypedProperties readerProperties;
    /** Groups run concurrently, on the clustering thread pool. */
    private final ConcurrentMap<Option<Long>, HoodieWriteConfig> groupWriteConfigs = new ConcurrentHashMap<>();
    /** Keyed by dataset schema and table schema: groups may read different schemas. */
    private final ConcurrentMap<Pair<StructType, String>, StructType> groupWriterSchemas = new ConcurrentHashMap<>();

    private ClusteringRunInvariants(HoodieSchema readerSchema, ReaderContextFactory<?> readerContextFactory,
                               StructType sparkReaderSchema, TypedProperties readerProperties) {
      this.readerSchema = readerSchema;
      this.readerContextFactory = readerContextFactory;
      this.sparkReaderSchema = sparkReaderSchema;
      this.readerProperties = readerProperties;
    }

    @SuppressWarnings("unchecked")
    private <R> ReaderContextFactory<R> readerContextFactory() {
      return (ReaderContextFactory<R>) readerContextFactory;
    }
  }

  /** The invariants of the {@link #performClustering} run that is submitting groups. */
  private ClusteringRunInvariants currentRunInvariants() {
    ClusteringRunInvariants run = runInvariants;
    ValidationUtils.checkState(run != null, "Clustering groups are submitted by performClustering");
    return run;
  }

  /** The row writer reads with the table schema resolved from the timeline: see getTableSchemaWithMetaFields. */
  private ClusteringRunInvariants newRowWriterRunInvariants() {
    HoodieSchema tableSchemaWithMetaFields = getTableSchemaWithMetaFields();
    return new ClusteringRunInvariants(tableSchemaWithMetaFields,
        getEngineContext().getReaderContextFactory(getHoodieTable().getMetaClient()),
        HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(tableSchemaWithMetaFields),
        getReaderProperties(MergeUtils.getMaxMemoryPerCompaction(getEngineContext().getTaskContextSupplier(), writeConfig)));
  }

  /** The records path reads with the write schema; its tasks size their own merge memory. */
  private ClusteringRunInvariants newRecordsRunInvariants() {
    return new ClusteringRunInvariants(HoodieSchemaUtils.addMetadataFields(HoodieSchema.parse(getWriteConfig().getSchema())),
        getEngineContext().getReaderContextFactory(getHoodieTable().getMetaClient()), null, null);
  }

  /**
   * Execute clustering to write inputRecords into new files based on strategyParams.
   * Different from {@link MultipleSparkJobExecutionStrategy#performClusteringWithRecordsRDD}, this method take {@link Dataset<Row>}
   * as inputs.
   */
  public abstract HoodieData<WriteStatus> performClusteringWithRecordsAsRow(final Dataset<Row> inputRecords,
                                                                            final int numOutputGroups,
                                                                            final String instantTime,
                                                                            final Map<String, String> strategyParams,
                                                                            final HoodieSchema schema,
                                                                            final List<HoodieFileGroupId> fileGroupIdList,
                                                                            final boolean shouldPreserveHoodieMetadata,
                                                                            final Map<String, String> extraMetadata);

  /**
   * Execute clustering to write inputRecords into new files as defined by rules in strategy parameters.
   * The number of new file groups created is bounded by numOutputGroups.
   * Note that commit is not done as part of strategy. commit is callers responsibility.
   *
   * @param inputRecords                 RDD of {@link HoodieRecord}.
   * @param numOutputGroups              Number of output file groups.
   * @param instantTime                  Clustering (replace commit) instant time.
   * @param strategyParams               Strategy parameters containing columns to sort the data by when clustering.
   * @param schema                       Schema of the data including metadata fields.
   * @param fileGroupIdList              File group id corresponding to each out group.
   * @param shouldPreserveHoodieMetadata Whether to preserve commit metadata while clustering.
   * @return RDD of {@link WriteStatus}.
   */
  public abstract HoodieData<WriteStatus> performClusteringWithRecordsRDD(final HoodieData<HoodieRecord<T>> inputRecords,
                                                                          final int numOutputGroups,
                                                                          final String instantTime,
                                                                          final Map<String, String> strategyParams,
                                                                          final HoodieSchema schema,
                                                                          final List<HoodieFileGroupId> fileGroupIdList,
                                                                          final boolean shouldPreserveHoodieMetadata,
                                                                          final Map<String, String> extraMetadata);

  protected BulkInsertPartitioner<Dataset<Row>> getRowPartitioner(Map<String, String> strategyParams,
                                                                  HoodieSchema schema) {
    return getPartitioner(strategyParams, schema, true);
  }

  protected BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> getRDDPartitioner(Map<String, String> strategyParams,
                                                                              HoodieSchema schema) {
    return getPartitioner(strategyParams, schema, false);
  }

  /**
   * Create {@link BulkInsertPartitioner} based on strategy params.
   *
   * @param strategyParams Strategy parameters containing columns to sort the data by when clustering.
   * @param schema         Schema of the data including metadata fields.
   */
  private <I> BulkInsertPartitioner<I> getPartitioner(Map<String, String> strategyParams,
                                                      HoodieSchema schema,
                                                      boolean isRowPartitioner) {
    // Trim: the config and inline paths pass the list through verbatim (`id, ts`), while the
    // partitioners look up the column names as given.
    Option<String[]> orderByColumnsOpt =
        Option.ofNullable(strategyParams.get(PLAN_STRATEGY_SORT_COLUMNS.key()))
            .map(listStr -> Arrays.stream(listStr.split(",")).map(String::trim).toArray(String[]::new));

    return orderByColumnsOpt.map(orderByColumns -> {
      // The custom-columns partitioners re-validate in their constructors; this earlier check
      // additionally covers the spatial-curve (ZORDER/HILBERT) partitioners below, which hold no
      // schema of their own. The spatial arm additionally requires a top-level name, see
      // SpatialCurveSortPartitionerBase.validateOrderByColumns.
      SortUtils.validateSortableColumns(orderByColumns, schema);
      HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy = getWriteConfig().getLayoutOptimizationStrategy();
      switch (layoutOptStrategy) {
        case ZORDER:
        case HILBERT:
          // Both partitioners build the curve over a frame carrying the meta columns, so they are
          // validated against the same schema the RDD one is handed below; addMetadataFields is
          // idempotent, so it does not matter whether the caller resolved it with them or without.
          SpatialCurveSortPartitionerBase.validateOrderByColumns(orderByColumns, HoodieSchemaUtils.addMetadataFields(schema), layoutOptStrategy);
          return isRowPartitioner
              ? new RowSpatialCurveSortPartitioner(getWriteConfig())
              : new RDDSpatialCurveSortPartitioner((HoodieSparkEngineContext) getEngineContext(), orderByColumns, layoutOptStrategy,
              getWriteConfig().getLayoutOptimizationCurveBuildMethod(), HoodieSchemaUtils.addMetadataFields(schema), recordType);
        case LINEAR:
          return isRowPartitioner
              ? new RowCustomColumnsSortPartitioner(orderByColumns, getWriteConfig())
              : new RDDCustomColumnsSortPartitioner(orderByColumns, HoodieSchemaUtils.addMetadataFields(schema), getWriteConfig());
        default:
          throw new UnsupportedOperationException(String.format("Layout optimization strategy '%s' is not supported", layoutOptStrategy));
      }
    }).orElseGet(() -> isRowPartitioner
        ? BulkInsertInternalPartitionerWithRowsFactory.get(
            getHoodieTable().getMetaClient().getTableConfig(), getWriteConfig(),
            getHoodieTable().isPartitioned(), true)
        : BulkInsertInternalPartitionerFactory.get(getHoodieTable(), getWriteConfig(), true));
  }

  /**
   * Submit job to execute clustering for the group using Avro/HoodieRecord representation.
   */
  protected CompletableFuture<HoodieData<WriteStatus>> runClusteringForGroupAsync(HoodieClusteringGroup clusteringGroup, Map<String, String> strategyParams,
                                                                                  boolean preserveHoodieMetadata, String instantTime,
                                                                                  ExecutorService clusteringExecutorService) {
    ClusteringRunInvariants run = currentRunInvariants();
    HoodieSchema readerSchema = run.readerSchema;
    return CompletableFuture.supplyAsync(() -> {
      JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(getEngineContext());
      HoodieData<HoodieRecord<T>> inputRecords = readRecordsForGroup(jsc, clusteringGroup, instantTime, run.readerContextFactory());
      // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
      //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
      //       it since these records will be shuffled later.
      List<HoodieFileGroupId> inputFileIds = clusteringGroup.getSlices().stream()
          .map(info -> new HoodieFileGroupId(info.getPartitionPath(), info.getFileId()))
          .collect(Collectors.toList());
      return performClusteringWithRecordsRDD(inputRecords, clusteringGroup.getNumOutputFileGroups(), instantTime, strategyParams, readerSchema, inputFileIds, preserveHoodieMetadata,
          clusteringGroup.getExtraMetadata());
    }, clusteringExecutorService);
  }

  /**
   * Submit job to execute clustering for the group, directly using the spark native Row representation.
   */
  protected CompletableFuture<HoodieData<WriteStatus>> runClusteringForGroupAsyncAsRow(HoodieClusteringGroup clusteringGroup,
                                                                                       Map<String, String> strategyParams,
                                                                                       boolean shouldPreserveHoodieMetadata,
                                                                                       String instantTime,
                                                                                       ExecutorService clusteringExecutorService) {
    ClusteringRunInvariants run = currentRunInvariants();
    HoodieSchema tableSchemaWithMetaFields = run.readerSchema;
    return CompletableFuture.supplyAsync(() -> {
      JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(getEngineContext());
      Dataset<Row> inputRecords = readRecordsForGroupAsRow(jsc, clusteringGroup, instantTime, tableSchemaWithMetaFields,
          run.sparkReaderSchema, run.readerContextFactory(), run.readerProperties);

      List<HoodieFileGroupId> inputFileIds = clusteringGroup.getSlices().stream()
          .map(info -> new HoodieFileGroupId(info.getPartitionPath(), info.getFileId()))
          .collect(Collectors.toList());
      return performClusteringWithRecordsAsRow(inputRecords, clusteringGroup.getNumOutputFileGroups(), instantTime, strategyParams,
          tableSchemaWithMetaFields, inputFileIds, shouldPreserveHoodieMetadata, clusteringGroup.getExtraMetadata());
    }, clusteringExecutorService);
  }

  /**
   * The table schema with meta fields, resolved from the timeline: in case of MIT, config.getSchema may not contain
   * the full table schema.
   */
  private HoodieSchema getTableSchemaWithMetaFields() {
    try {
      return HoodieSchemaUtils.addMetadataFields(new TableSchemaResolver(getHoodieTable().getMetaClient()).getTableSchema(false),
          getWriteConfig().allowOperationMetadataField());
    } catch (Exception e) {
      throw new HoodieException("Failed to get table schema during clustering", e);
    }
  }

  /**
   * Get RDD of all records for the group. This includes all records from file slice (Apply updates from log files, if any).
   */
  private HoodieData<HoodieRecord<T>> readRecordsForGroup(JavaSparkContext jsc, HoodieClusteringGroup clusteringGroup, String instantTime,
                                                          ReaderContextFactory<T> readerContextFactory) {
    List<ClusteringOperation> clusteringOps = clusteringGroup.getSlices().stream().map(ClusteringOperation::create).collect(Collectors.toList());
    int readParallelism = Math.min(writeConfig.getClusteringGroupReadParallelism(), clusteringOps.size());
    return HoodieJavaRDD.of(jsc.parallelize(clusteringOps, readParallelism).mapPartitions(clusteringOpsPartition -> {
      List<Supplier<ClosableIterator<HoodieRecord<T>>>> suppliers = new ArrayList<>();
      long maxMemoryPerCompaction = MergeUtils.getMaxMemoryPerCompaction(new SparkTaskContextSupplier(), getWriteConfig());
      log.info("MaxMemoryPerCompaction run as part of clustering => {}", maxMemoryPerCompaction);
      clusteringOpsPartition.forEachRemaining(clusteringOp -> {
        Supplier<ClosableIterator<HoodieRecord<T>>> iteratorSupplier = () -> getRecordIterator(readerContextFactory, clusteringOp, instantTime, maxMemoryPerCompaction);
        suppliers.add(iteratorSupplier);
      });
      return CloseableIteratorListener.addListener(new LazyConcatenatingIterator<>(suppliers));
    }));
  }

  /**
   * Get dataset of all records for the group. This includes all records from file slice (Apply updates from log files, if any).
   */
  private Dataset<Row> readRecordsForGroupAsRow(JavaSparkContext jsc,
                                                HoodieClusteringGroup clusteringGroup,
                                                String instantTime,
                                                HoodieSchema tableSchemaWithMetaFields,
                                                StructType sparkSchemaWithMetaFields,
                                                ReaderContextFactory<InternalRow> readerContextFactory,
                                                TypedProperties readerProperties) {
    List<ClusteringOperation> clusteringOps = clusteringGroup.getSlices().stream()
        .map(ClusteringOperation::create).collect(Collectors.toList());
    String basePath = getWriteConfig().getBasePath();
    final boolean usePosition = getWriteConfig().getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    String internalSchemaStr = getWriteConfig().getInternalSchema();
    HoodieTableMetaClient metaClient = getHoodieTable().getMetaClient();

    RDD<InternalRow> internalRowRDD = jsc.parallelize(clusteringOps, clusteringOps.size()).flatMap(new FlatMapFunction<ClusteringOperation, InternalRow>() {
      @Override
      public Iterator<InternalRow> call(ClusteringOperation clusteringOperation) throws Exception {
        FileSlice fileSlice = clusteringOperationToFileSlice(basePath, clusteringOperation);
        // instantiate other supporting cast
        Option<InternalSchema> internalSchemaOption = SerDeHelper.fromJson(internalSchemaStr);

        // instantiate FG reader
        HoodieFileGroupReader<InternalRow> fileGroupReader = getFileGroupReader(metaClient, fileSlice, tableSchemaWithMetaFields, internalSchemaOption,
            readerContextFactory, instantTime, readerProperties, usePosition);
        // read records from the FG reader
        return CloseableIteratorListener.addListener(fileGroupReader.getClosableIterator());
      }
    }).rdd();

    return SparkAdapterSupport$.MODULE$.sparkAdapter().getUnsafeUtils()
        .createDataFrameFromRDD(((HoodieSparkEngineContext) getEngineContext()).getSqlContext().sparkSession(),
            internalRowRDD, sparkSchemaWithMetaFields);
  }

  /**
   * Stream to array conversion with generic type is not straightforward.
   * Implement a utility method to abstract high level logic. This needs to be improved in future
   */
  private JavaRDD<WriteStatus>[] convertStreamToArray(Stream<JavaRDD<WriteStatus>> writeStatusRDDStream) {
    Object[] writeStatusObjects = writeStatusRDDStream.toArray(Object[]::new);
    JavaRDD<WriteStatus>[] writeStatusRDDArray = new JavaRDD[writeStatusObjects.length];
    for (int i = 0; i < writeStatusObjects.length; i++) {
      writeStatusRDDArray[i] = (JavaRDD<WriteStatus>) writeStatusObjects[i];
    }
    return writeStatusRDDArray;
  }
}
