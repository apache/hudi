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

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.ConcatenatingIterator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieFileSliceReader;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.FutureUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerFactory;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerWithRowsFactory;
import org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RDDSpatialCurveSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RowCustomColumnsSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RowSpatialCurveSortPartitioner;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.client.utils.SparkPartitionUtils.getPartitionFieldVals;
import static org.apache.hudi.common.config.HoodieCommonConfig.TIMESTAMP_AS_OF;
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
      // execute clustering for each group async and collect WriteStatus
      Stream<HoodieData<WriteStatus>> writeStatusesStream = FutureUtils.allOf(
              clusteringPlan.getInputGroups().stream()
                  .map(inputGroup -> {
                    if (getWriteConfig().getBooleanOrDefault("hoodie.datasource.write.row.writer.enable", false)) {
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
      JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(getEngineContext());
      Dataset<Row> inputRecords = readRecordsForGroupAsRow(jsc, clusteringGroup, instantTime);
      Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(getWriteConfig().getSchema()));
      List<HoodieFileGroupId> inputFileIds = clusteringGroup.getSlices().stream()
          .map(info -> new HoodieFileGroupId(info.getPartitionPath(), info.getFileId()))
          .collect(Collectors.toList());
      return performClusteringWithRecordsAsRow(inputRecords, clusteringGroup.getNumOutputFileGroups(), instantTime, strategyParams, readerSchema, inputFileIds, shouldPreserveHoodieMetadata,
          clusteringGroup.getExtraMetadata());
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
    HoodieWriteConfig config = getWriteConfig();
    HoodieTable table = getHoodieTable();
    // NOTE: It's crucial to make sure that we don't capture whole "this" object into the
    //       closure, as this might lead to issues attempting to serialize its nested fields
    StorageConfiguration<?> storageConf = table.getStorageConf();
    HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
    String bootstrapBasePath = tableConfig.getBootstrapBasePath().orElse(null);
    Option<String[]> partitionFields = tableConfig.getPartitionFields();

    return HoodieJavaRDD.of(jsc.parallelize(clusteringOps, clusteringOps.size()).mapPartitions(clusteringOpsPartition -> {
      List<Iterator<HoodieRecord<T>>> recordIterators = new ArrayList<>();
      clusteringOpsPartition.forEachRemaining(clusteringOp -> {
        long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(new SparkTaskContextSupplier(), config);
        LOG.info("MaxMemoryPerCompaction run as part of clustering => " + maxMemoryPerCompaction);
        try {
          Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
          HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
              .withStorage(table.getStorage())
              .withBasePath(table.getMetaClient().getBasePath())
              .withLogFilePaths(clusteringOp.getDeltaFilePaths())
              .withReaderSchema(readerSchema)
              .withLatestInstantTime(instantTime)
              .withMaxMemorySizeInBytes(maxMemoryPerCompaction)
              .withReverseReader(config.getCompactionReverseLogReadEnabled())
              .withBufferSize(config.getMaxDFSStreamBufferSize())
              .withSpillableMapBasePath(config.getSpillableMapBasePath())
              .withPartition(clusteringOp.getPartitionPath())
              .withOptimizedLogBlocksScan(config.enableOptimizedLogBlocksScan())
              .withDiskMapType(config.getCommonConfig().getSpillableDiskMapType())
              .withBitCaskDiskMapCompressionEnabled(config.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
              .withRecordMerger(config.getRecordMerger())
              .withTableMetaClient(table.getMetaClient())
              .build();

          Option<HoodieFileReader> baseFileReader = StringUtils.isNullOrEmpty(clusteringOp.getDataFilePath())
              ? Option.empty()
              : Option.of(getBaseOrBootstrapFileReader(storageConf, bootstrapBasePath, partitionFields, clusteringOp));
          recordIterators.add(new HoodieFileSliceReader(baseFileReader, scanner, readerSchema, tableConfig.getPreCombineField(), config.getRecordMerger(),
              tableConfig.getProps(),
              tableConfig.populateMetaFields() ? Option.empty() : Option.of(Pair.of(tableConfig.getRecordKeyFieldProp(),
                  tableConfig.getPartitionFieldProp()))));
        } catch (IOException e) {
          throw new HoodieClusteringException("Error reading input data for " + clusteringOp.getDataFilePath()
              + " and " + clusteringOp.getDeltaFilePaths(), e);
        }
      });

      return new ConcatenatingIterator<>(recordIterators);
    }));
  }

  /**
   * Read records from baseFiles and convert to RDD.
   */
  private HoodieData<HoodieRecord<T>> readRecordsForGroupBaseFiles(JavaSparkContext jsc,
                                                                   List<ClusteringOperation> clusteringOps) {
    StorageConfiguration<?> storageConf = getHoodieTable().getStorageConf();
    HoodieWriteConfig writeConfig = getWriteConfig();

    // NOTE: It's crucial to make sure that we don't capture whole "this" object into the
    //       closure, as this might lead to issues attempting to serialize its nested fields
    HoodieTableConfig  tableConfig = getHoodieTable().getMetaClient().getTableConfig();
    String bootstrapBasePath = tableConfig.getBootstrapBasePath().orElse(null);
    Option<String[]> partitionFields = tableConfig.getPartitionFields();

    return HoodieJavaRDD.of(jsc.parallelize(clusteringOps, clusteringOps.size())
        .mapPartitions(clusteringOpsPartition -> {
          List<Iterator<HoodieRecord<T>>> iteratorsForPartition = new ArrayList<>();
          clusteringOpsPartition.forEachRemaining(clusteringOp -> {
            try {
              Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(writeConfig.getSchema()));
              HoodieFileReader baseFileReader = getBaseOrBootstrapFileReader(storageConf, bootstrapBasePath, partitionFields, clusteringOp);

              Option<BaseKeyGenerator> keyGeneratorOp = HoodieSparkKeyGeneratorFactory.createBaseKeyGenerator(writeConfig);
              // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
              //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
              //       it since these records will be shuffled later.
              CloseableMappingIterator mappingIterator = new CloseableMappingIterator(
                  (ClosableIterator<HoodieRecord>) baseFileReader.getRecordIterator(readerSchema),
                  rec -> ((HoodieRecord) rec).copy().wrapIntoHoodieRecordPayloadWithKeyGen(readerSchema, writeConfig.getProps(), keyGeneratorOp));
              iteratorsForPartition.add(mappingIterator);
            } catch (IOException e) {
              throw new HoodieClusteringException("Error reading input data for " + clusteringOp.getDataFilePath()
                  + " and " + clusteringOp.getDeltaFilePaths(), e);
            }
          });

          return new ConcatenatingIterator<>(iteratorsForPartition);
        }));
  }

  private HoodieFileReader getBaseOrBootstrapFileReader(StorageConfiguration<?> storageConf, String bootstrapBasePath, Option<String[]> partitionFields, ClusteringOperation clusteringOp)
      throws IOException {
    StoragePath dataFilePath = new StoragePath(clusteringOp.getDataFilePath());
    HoodieStorage storage = new HoodieHadoopStorage(dataFilePath, storageConf);
    HoodieFileReader baseFileReader = getHoodieSparkIOFactory(storage).getReaderFactory(recordType)
        .getFileReader(writeConfig, dataFilePath);
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
      baseFileReader = getHoodieSparkIOFactory(storage).getReaderFactory(recordType).newBootstrapFileReader(
          baseFileReader,
          getHoodieSparkIOFactory(storage).getReaderFactory(recordType).getFileReader(
              writeConfig, new StoragePath(bootstrapFilePath)), partitionFields,
          partitionValues);
    }
    return baseFileReader;
  }

  /**
   * Get dataset of all records for the group. This includes all records from file slice (Apply updates from log files, if any).
   */
  private Dataset<Row> readRecordsForGroupAsRow(JavaSparkContext jsc,
                                                HoodieClusteringGroup clusteringGroup,
                                                String instantTime) {
    List<ClusteringOperation> clusteringOps = clusteringGroup.getSlices().stream()
        .map(ClusteringOperation::create).collect(Collectors.toList());
    boolean hasLogFiles = clusteringOps.stream().anyMatch(op -> op.getDeltaFilePaths().size() > 0);
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
    params.put("hoodie.datasource.query.type", "snapshot");
    params.put(TIMESTAMP_AS_OF.key(), instantTime);

    StoragePath[] paths;
    if (hasLogFiles) {
      String compactionFractor = Option.ofNullable(getWriteConfig().getString("compaction.memory.fraction"))
          .orElse("0.75");
      params.put("compaction.memory.fraction", compactionFractor);

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
