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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.LazyConcatenatingIterator;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.FutureUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.CloseableIteratorListener;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerFactory;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerWithRowsFactory;
import org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RDDSpatialCurveSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RowCustomColumnsSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RowSpatialCurveSortPartitioner;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.HoodieDataTypeUtils;
import org.apache.spark.sql.HoodieUnsafeUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.client.utils.SparkPartitionUtils.getPartitionFieldVals;
import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS;
import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;
import static org.apache.hudi.io.storage.HoodieSparkIOFactory.getHoodieSparkIOFactory;

/**
 * Clustering strategy to submit multiple spark jobs and union the results.
 */
public abstract class MultipleSparkJobExecutionStrategy<T>
    extends ClusteringExecutionStrategy<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> {
  private static final Logger LOG = LoggerFactory.getLogger(MultipleSparkJobExecutionStrategy.class);

  public MultipleSparkJobExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> performClustering(final HoodieClusteringPlan clusteringPlan, final Schema schema, final String instantTime) {
    JavaSparkContext engineContext = HoodieSparkEngineContext.getSparkContext(getEngineContext());
    boolean shouldPreserveMetadata = Option.ofNullable(clusteringPlan.getPreserveHoodieMetadata()).orElse(true);
    ExecutorService clusteringExecutorService = Executors.newFixedThreadPool(
        Math.min(clusteringPlan.getInputGroups().size(), writeConfig.getClusteringMaxParallelism()),
        new CustomizedThreadFactory("clustering-job-group", true));
    try {
      boolean canUseRowWriter = getWriteConfig().getBooleanOrDefault("hoodie.datasource.write.row.writer.enable", true)
          && HoodieDataTypeUtils.canUseRowWriter(schema, engineContext.hadoopConfiguration());
      if (canUseRowWriter) {
        HoodieDataTypeUtils.tryOverrideParquetWriteLegacyFormatProperty(writeConfig.getProps(), schema);
      }
      // execute clustering for each group async and collect WriteStatus
      Stream<HoodieData<WriteStatus>> writeStatusesStream = FutureUtils.allOf(
              clusteringPlan.getInputGroups().stream()
                  .map(inputGroup -> {
                    if (canUseRowWriter) {
                      return runClusteringForGroupAsyncAsRow(inputGroup,
                          clusteringPlan.getStrategy().getStrategyParams(),
                          shouldPreserveMetadata,
                          instantTime,
                          clusteringExecutorService);
                    }
                    return runClusteringForGroupAsync(inputGroup,
                        clusteringPlan.getStrategy().getStrategyParams(),
                        shouldPreserveMetadata,
                        instantTime,
                        clusteringExecutorService);
                  })
                  .collect(Collectors.toList()))
          .join()
          .stream();
      JavaRDD<WriteStatus>[] writeStatuses = convertStreamToArray(writeStatusesStream.map(HoodieJavaRDD::getJavaRDD));
      JavaRDD<WriteStatus> writeStatusRDD = engineContext.union(writeStatuses);

      HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = new HoodieWriteMetadata<>();
      writeMetadata.setWriteStatuses(HoodieJavaRDD.of(writeStatusRDD));
      return writeMetadata;
    } finally {
      clusteringExecutorService.shutdown();
    }
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
                                                                            final Schema schema,
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
                                                                          final Schema schema,
                                                                          final List<HoodieFileGroupId> fileGroupIdList,
                                                                          final boolean shouldPreserveHoodieMetadata,
                                                                          final Map<String, String> extraMetadata);

  protected BulkInsertPartitioner<Dataset<Row>> getRowPartitioner(Map<String, String> strategyParams,
                                                                  Schema schema) {
    return getPartitioner(strategyParams, schema, true);
  }

  protected BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> getRDDPartitioner(Map<String, String> strategyParams,
                                                                              Schema schema) {
    return getPartitioner(strategyParams, schema, false);
  }

  /**
   * Create {@link BulkInsertPartitioner} based on strategy params.
   *
   * @param strategyParams Strategy parameters containing columns to sort the data by when clustering.
   * @param schema         Schema of the data including metadata fields.
   */
  private <I> BulkInsertPartitioner<I> getPartitioner(Map<String, String> strategyParams,
                                                      Schema schema,
                                                      boolean isRowPartitioner) {
    Option<String[]> orderByColumnsOpt =
        Option.ofNullable(strategyParams.get(PLAN_STRATEGY_SORT_COLUMNS.key()))
            .map(listStr -> listStr.split(","));

    return orderByColumnsOpt.map(orderByColumns -> {
      HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy = getWriteConfig().getLayoutOptimizationStrategy();
      switch (layoutOptStrategy) {
        case ZORDER:
        case HILBERT:
          return isRowPartitioner
              ? new RowSpatialCurveSortPartitioner(getWriteConfig())
              : new RDDSpatialCurveSortPartitioner((HoodieSparkEngineContext) getEngineContext(), orderByColumns, layoutOptStrategy,
              getWriteConfig().getLayoutOptimizationCurveBuildMethod(), HoodieAvroUtils.addMetadataFields(schema), recordType);
        case LINEAR:
          return isRowPartitioner
              ? new RowCustomColumnsSortPartitioner(orderByColumns, getWriteConfig())
              : new RDDCustomColumnsSortPartitioner(orderByColumns, HoodieAvroUtils.addMetadataFields(schema), getWriteConfig());
        default:
          throw new UnsupportedOperationException(String.format("Layout optimization strategy '%s' is not supported", layoutOptStrategy));
      }
    }).orElseGet(() -> isRowPartitioner
        ? BulkInsertInternalPartitionerWithRowsFactory.get(getWriteConfig(), getHoodieTable().isPartitioned(), true)
        : BulkInsertInternalPartitionerFactory.get(getHoodieTable(), getWriteConfig(), true));
  }

  /**
   * Submit job to execute clustering for the group using Avro/HoodieRecord representation.
   */
  private CompletableFuture<HoodieData<WriteStatus>> runClusteringForGroupAsync(HoodieClusteringGroup clusteringGroup, Map<String, String> strategyParams,
                                                                                boolean preserveHoodieMetadata, String instantTime,
                                                                                ExecutorService clusteringExecutorService) {
    return CompletableFuture.supplyAsync(() -> {
      JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(getEngineContext());
      HoodieData<HoodieRecord<T>> inputRecords = readRecordsForGroup(jsc, clusteringGroup, instantTime);
      Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(getWriteConfig().getSchema()));
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
  private CompletableFuture<HoodieData<WriteStatus>> runClusteringForGroupAsyncAsRow(HoodieClusteringGroup clusteringGroup,
                                                                                     Map<String, String> strategyParams,
                                                                                     boolean shouldPreserveHoodieMetadata,
                                                                                     String instantTime,
                                                                                     ExecutorService clusteringExecutorService) {
    return CompletableFuture.supplyAsync(() -> {
      JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(getEngineContext());      // incase of MIT, config.getSchema may not contain the full table schema
      Schema tableSchemaWithMetaFields = null;
      try {
        tableSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(new TableSchemaResolver(getHoodieTable().getMetaClient()).getTableAvroSchema(false),
            getWriteConfig().allowOperationMetadataField());
      } catch (Exception e) {
        throw new HoodieException("Failed to get table schema during clustering", e);
      }
      Dataset<Row> inputRecords = readRecordsForGroupAsRow(jsc, clusteringGroup, instantTime, tableSchemaWithMetaFields);

      List<HoodieFileGroupId> inputFileIds = clusteringGroup.getSlices().stream()
          .map(info -> new HoodieFileGroupId(info.getPartitionPath(), info.getFileId()))
          .collect(Collectors.toList());
      return performClusteringWithRecordsAsRow(inputRecords, clusteringGroup.getNumOutputFileGroups(), instantTime, strategyParams,
          tableSchemaWithMetaFields, inputFileIds, shouldPreserveHoodieMetadata, clusteringGroup.getExtraMetadata());
    }, clusteringExecutorService);
  }

  /**
   * Get RDD of all records for the group. This includes all records from file slice (Apply updates from log files, if any).
   */
  private HoodieData<HoodieRecord<T>> readRecordsForGroup(JavaSparkContext jsc, HoodieClusteringGroup clusteringGroup, String instantTime) {
    List<ClusteringOperation> clusteringOps = clusteringGroup.getSlices().stream().map(ClusteringOperation::create).collect(Collectors.toList());
    boolean hasLogFiles = clusteringOps.stream().anyMatch(op -> op.getDeltaFilePaths().size() > 0);
    if (hasLogFiles) {
      // if there are log files, we read all records into memory for a file group and apply updates.
      return readRecordsForGroupWithLogs(jsc, clusteringOps, instantTime);
    } else {
      // We want to optimize reading records for case there are no log files.
      return readRecordsForGroupBaseFiles(jsc, clusteringOps);
    }
  }

  /**
   * Read records from baseFiles, apply updates and convert to RDD.
   */
  private HoodieData<HoodieRecord<T>> readRecordsForGroupWithLogs(JavaSparkContext jsc,
                                                                  List<ClusteringOperation> clusteringOps,
                                                                  String instantTime) {
    int readParallelism = Math.min(writeConfig.getClusteringGroupReadParallelism(), clusteringOps.size());

    return HoodieJavaRDD.of(jsc.parallelize(clusteringOps, readParallelism).mapPartitions(clusteringOpsPartition -> {
      List<Supplier<ClosableIterator<HoodieRecord<T>>>> suppliers = new ArrayList<>();
      long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(new SparkTaskContextSupplier(), getWriteConfig());
      LOG.info("MaxMemoryPerCompaction run as part of clustering => {}", maxMemoryPerCompaction);
      Option<BaseKeyGenerator> keyGeneratorOpt = HoodieSparkKeyGeneratorFactory.createBaseKeyGenerator(writeConfig);
      clusteringOpsPartition.forEachRemaining(clusteringOp -> {
        Supplier<ClosableIterator<HoodieRecord<T>>> iteratorSupplier = () -> {
          Option<HoodieFileReader> baseOrBootstrapFileReader = getBaseOrBootstrapFileReader(clusteringOp);

          return getRecordIteratorWithLogFiles(clusteringOp, instantTime, maxMemoryPerCompaction, keyGeneratorOpt, baseOrBootstrapFileReader);
        };
        suppliers.add(iteratorSupplier);
      });
      return CloseableIteratorListener.addListener(new LazyConcatenatingIterator<>(suppliers));
    }));
  }

  /**
   * Read records from baseFiles and convert to RDD.
   */
  private HoodieData<HoodieRecord<T>> readRecordsForGroupBaseFiles(JavaSparkContext jsc,
                                                                   List<ClusteringOperation> clusteringOps) {
    int readParallelism = Math.min(writeConfig.getClusteringGroupReadParallelism(), clusteringOps.size());

    Option<BaseKeyGenerator> keyGeneratorOpt = HoodieSparkKeyGeneratorFactory.createBaseKeyGenerator(writeConfig);
    return HoodieJavaRDD.of(jsc.parallelize(clusteringOps, readParallelism)
        .mapPartitions(clusteringOpsPartition -> {
          List<Supplier<ClosableIterator<HoodieRecord<T>>>> iteratorGettersForPartition = new ArrayList<>();
          clusteringOpsPartition.forEachRemaining(clusteringOp -> {
            Option<HoodieFileReader> baseOrBootstrapFileReader = getBaseOrBootstrapFileReader(clusteringOp);
            ValidationUtils.checkArgument(baseOrBootstrapFileReader.isPresent(), "Base file reader must be present for clustering operation");
            Supplier<ClosableIterator<HoodieRecord<T>>> recordIteratorGetter = () -> getRecordIteratorWithBaseFileOnly(keyGeneratorOpt, baseOrBootstrapFileReader.get());
            iteratorGettersForPartition.add(recordIteratorGetter);
          });

          return CloseableIteratorListener.addListener(new LazyConcatenatingIterator<>(iteratorGettersForPartition));
        }));
  }

  /**
   * Get dataset of all records for the group. This includes all records from file slice (Apply updates from log files, if any).
   */
  private Dataset<Row> readRecordsForGroupAsRow(JavaSparkContext jsc,
                                                HoodieClusteringGroup clusteringGroup,
                                                String instantTime,
                                                Schema tableSchemaWithMetaFields) {
    List<ClusteringOperation> clusteringOps = clusteringGroup.getSlices().stream()
        .map(ClusteringOperation::create).collect(Collectors.toList());
    boolean canUseFileGroupReaderBasedClustering = getWriteConfig().getBooleanOrDefault(HoodieReaderConfig.FILE_GROUP_READER_ENABLED)
        && getWriteConfig().getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS)
        && clusteringOps.stream().allMatch(slice -> StringUtils.isNullOrEmpty(slice.getBootstrapFilePath()));

    if (canUseFileGroupReaderBasedClustering) {
      return readRecordsForGroupAsRowWithFileGroupReader(jsc, instantTime, tableSchemaWithMetaFields, clusteringOps);
    } else {
      return readRecordsForGroupAsRow(jsc, clusteringOps);
    }
  }

  /**
   * Get dataset of all records for the group. This includes all records from file slice (Apply updates from log files, if any).
   */
  private Dataset<Row> readRecordsForGroupAsRow(JavaSparkContext jsc, List<ClusteringOperation> clusteringOps) {
    boolean hasLogFiles = clusteringOps.stream().anyMatch(op -> !op.getDeltaFilePaths().isEmpty());
    SQLContext sqlContext = new SQLContext(jsc.sc());

    StoragePath[] baseFilePaths = clusteringOps
        .stream()
        .map(op -> {
          ArrayList<String> readPaths = new ArrayList<>();
          // NOTE: for bootstrap tables, only need to handle data file path (which is the skeleton file) because
          // HoodieBootstrapRelation takes care of stitching if there is bootstrap path for the skeleton file.
          if (op.getDataFilePath() != null) {
            readPaths.add(op.getDataFilePath());
          }
          return readPaths;
        })
        .flatMap(Collection::stream)
        .filter(path -> !path.isEmpty())
        .map(StoragePath::new)
        .toArray(StoragePath[]::new);

    HashMap<String, String> params = new HashMap<>();
    if (hasLogFiles) {
      params.put("hoodie.datasource.query.type", "snapshot");
    } else {
      params.put("hoodie.datasource.query.type", "read_optimized");
    }

    StoragePath[] paths;
    if (hasLogFiles) {
      String rawFractionConfig = getWriteConfig().getString(HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_COMPACTION);
      String compactionFractor = rawFractionConfig != null
          ? rawFractionConfig : HoodieMemoryConfig.DEFAULT_MR_COMPACTION_MEMORY_FRACTION;
      params.put(HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_COMPACTION.key(), compactionFractor);

      StoragePath[] deltaPaths = clusteringOps
          .stream()
          .filter(op -> !op.getDeltaFilePaths().isEmpty())
          .flatMap(op -> op.getDeltaFilePaths().stream())
          .map(StoragePath::new)
          .toArray(StoragePath[]::new);
      paths = CollectionUtils.combine(baseFilePaths, deltaPaths);
    } else {
      paths = baseFilePaths;
    }

    String readPathString =
        String.join(",", Arrays.stream(paths).map(StoragePath::toString).toArray(String[]::new));
    String globPathString = String.join(",", Arrays.stream(paths).map(StoragePath::getParent).map(StoragePath::toString).distinct().toArray(String[]::new));
    params.put("hoodie.datasource.read.paths", readPathString);
    // Building HoodieFileIndex needs this param to decide query path
    params.put("glob.paths", globPathString);

    // Let Hudi relations to fetch the schema from the table itself
    BaseRelation relation = SparkAdapterSupport$.MODULE$.sparkAdapter()
        .createRelation(sqlContext, getHoodieTable().getMetaClient(), null, paths, params);
    return sqlContext.baseRelationToDataFrame(relation);
  }

  private Dataset<Row> readRecordsForGroupAsRowWithFileGroupReader(JavaSparkContext jsc,
                                                                   String instantTime,
                                                                   Schema tableSchemaWithMetaFields,
                                                                   List<ClusteringOperation> clusteringOps) {
    String basePath = getWriteConfig().getBasePath();
    // construct supporting cast that executors might need
    final boolean usePosition = getWriteConfig().getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    String internalSchemaStr = getWriteConfig().getInternalSchema();
    boolean isInternalSchemaPresent = !StringUtils.isNullOrEmpty(internalSchemaStr);
    SerializableSchema serializableTableSchemaWithMetaFields = new SerializableSchema(tableSchemaWithMetaFields);

    // broadcast reader context.
    ReaderContextFactory<InternalRow> readerContextFactory = getEngineContext().getReaderContextFactory(getHoodieTable().getMetaClient());
    StructType sparkSchemaWithMetaFields = AvroConversionUtils.convertAvroSchemaToStructType(tableSchemaWithMetaFields);

    RDD<InternalRow> internalRowRDD = jsc.parallelize(clusteringOps, clusteringOps.size()).flatMap(new FlatMapFunction<ClusteringOperation, InternalRow>() {
      @Override
      public Iterator<InternalRow> call(ClusteringOperation clusteringOperation) throws Exception {
        FileSlice fileSlice = clusteringOperation2FileSlice(basePath, clusteringOperation);
        // instantiate other supporting cast
        Schema readerSchema = serializableTableSchemaWithMetaFields.get();
        Option<InternalSchema> internalSchemaOption = Option.empty();
        if (isInternalSchemaPresent) {
          internalSchemaOption = SerDeHelper.fromJson(internalSchemaStr);
        }

        // instantiate FG reader
        HoodieTableMetaClient metaClient = getHoodieTable().getMetaClient();
        HoodieReaderContext<InternalRow> readerContext = readerContextFactory.getContext();
        HoodieFileGroupReader<InternalRow> fileGroupReader = HoodieFileGroupReader.<InternalRow>newBuilder()
            .withReaderContext(readerContext).withHoodieTableMetaClient(metaClient).withLatestCommitTime(instantTime)
            .withFileSlice(fileSlice).withDataSchema(readerSchema).withRequestedSchema(readerSchema).withInternalSchema(internalSchemaOption)
            .withShouldUseRecordPosition(usePosition).withProps(metaClient.getTableConfig().getProps()).build();
        // read records from the FG reader
        return CloseableIteratorListener.addListener(fileGroupReader.getClosableIterator());
      }
    }).rdd();

    return HoodieUnsafeUtils.createDataFrameFromRDD(((HoodieSparkEngineContext) getEngineContext()).getSqlContext().sparkSession(),
        internalRowRDD, sparkSchemaWithMetaFields);
  }

  /**
   * Construct FileSlice from a given clustering operation {@code clusteringOperation}.
   */
  private FileSlice clusteringOperation2FileSlice(String basePath, ClusteringOperation clusteringOperation) {
    String partitionPath = clusteringOperation.getPartitionPath();
    boolean baseFileExists = !StringUtils.isNullOrEmpty(clusteringOperation.getDataFilePath());
    HoodieBaseFile baseFile = baseFileExists ? new HoodieBaseFile(new StoragePath(basePath, clusteringOperation.getDataFilePath()).toString()) : null;
    List<HoodieLogFile> logFiles = clusteringOperation.getDeltaFilePaths().stream().map(p ->
            new HoodieLogFile(new StoragePath(FSUtils.constructAbsolutePath(
                basePath, partitionPath), p)))
        .sorted(new HoodieLogFile.LogFileComparator())
        .collect(Collectors.toList());

    ValidationUtils.checkState(baseFileExists || !logFiles.isEmpty(), "Both base file and log files are missing from this clustering operation " + clusteringOperation);
    String baseInstantTime = baseFileExists ? baseFile.getCommitTime() : logFiles.get(0).getDeltaCommitTime();
    FileSlice fileSlice = new FileSlice(partitionPath, baseInstantTime, clusteringOperation.getFileId());
    fileSlice.setBaseFile(baseFile);
    logFiles.forEach(fileSlice::addLogFile);
    return fileSlice;
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

  protected Option<HoodieFileReader> getBaseOrBootstrapFileReader(ClusteringOperation clusteringOp) {
    HoodieStorage storage = getHoodieTable().getStorage();
    StorageConfiguration<?> storageConf = getHoodieTable().getStorageConf();
    HoodieTableConfig tableConfig = getHoodieTable().getMetaClient().getTableConfig();
    String bootstrapBasePath = tableConfig.getBootstrapBasePath().orElse(null);
    Option<String[]> partitionFields = tableConfig.getPartitionFields();
    Option<HoodieFileReader> baseFileReaderOpt = ClusteringUtils.getBaseFileReader(storage, recordType, writeConfig, clusteringOp.getDataFilePath());
    if (baseFileReaderOpt.isEmpty()) {
      return Option.empty();
    }
    try {
      HoodieFileReader baseFileReader = baseFileReaderOpt.get();
      // handle bootstrap path
      if (StringUtils.nonEmpty(clusteringOp.getBootstrapFilePath()) && StringUtils.nonEmpty(bootstrapBasePath)) {
        String bootstrapFilePath = clusteringOp.getBootstrapFilePath();
        Object[] partitionValues = new Object[0];
        if (partitionFields.isPresent()) {
          int startOfPartitionPath = bootstrapFilePath.indexOf(bootstrapBasePath) + bootstrapBasePath.length() + 1;
          String partitionFilePath = bootstrapFilePath.substring(startOfPartitionPath, bootstrapFilePath.lastIndexOf("/"));
          partitionValues = getPartitionFieldVals(partitionFields, partitionFilePath, bootstrapBasePath, baseFileReader.getSchema(),
              storageConf.unwrapAs(Configuration.class));
        }
        return Option.of(getHoodieSparkIOFactory(storage).getReaderFactory(recordType).newBootstrapFileReader(
            baseFileReader,
            getHoodieSparkIOFactory(storage).getReaderFactory(recordType).getFileReader(
                writeConfig, new StoragePath(bootstrapFilePath)), partitionFields,
            partitionValues));
      }
      return baseFileReaderOpt;
    } catch (IOException e) {
      throw new HoodieClusteringException("Error reading base file", e);
    }
  }

}
