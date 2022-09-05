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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.HoodieInternalWriteStatusCoordinator;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.ConcatenatingIterator;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.RewriteAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FutureUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerFactory;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerWithRowsFactory;
import org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RDDSpatialCurveSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RowCustomColumnsSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RowSpatialCurveSortPartitioner;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.log.HoodieFileSliceReader.getFileSliceReader;
import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * Clustering strategy to submit multiple spark jobs and union the results.
 */
public abstract class MultipleSparkJobExecutionStrategy<T extends HoodieRecordPayload<T>>
    extends ClusteringExecutionStrategy<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> {
  private static final Logger LOG = LogManager.getLogger(MultipleSparkJobExecutionStrategy.class);

  public MultipleSparkJobExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> performClustering(final HoodieClusteringPlan clusteringPlan, final Schema schema, final String instantTime) {
    JavaSparkContext engineContext = HoodieSparkEngineContext.getSparkContext(getEngineContext());
    // execute clustering for each group async and collect WriteStatus
    Stream<HoodieData<WriteStatus>> writeStatusesStream = FutureUtils.allOf(
        clusteringPlan.getInputGroups().stream()
            .map(inputGroup -> {
              if (getWriteConfig().getBooleanOrDefault("hoodie.datasource.write.row.writer.enable", false)) {
                return runClusteringForGroupAsyncWithRow(inputGroup,
                    clusteringPlan.getStrategy().getStrategyParams(),
                    Option.ofNullable(clusteringPlan.getPreserveHoodieMetadata()).orElse(false),
                    instantTime);
              }
              return runClusteringForGroupAsyncWithRDD(inputGroup,
                  clusteringPlan.getStrategy().getStrategyParams(),
                  Option.ofNullable(clusteringPlan.getPreserveHoodieMetadata()).orElse(false),
                  instantTime);
            })
            .collect(Collectors.toList()))
        .join()
        .stream();
    JavaRDD<WriteStatus>[] writeStatuses = convertStreamToArray(writeStatusesStream.map(HoodieJavaRDD::getJavaRDD));
    JavaRDD<WriteStatus> writeStatusRDD = engineContext.union(writeStatuses);

    HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = new HoodieWriteMetadata<>();
    writeMetadata.setWriteStatuses(HoodieJavaRDD.of(writeStatusRDD));
    return writeMetadata;
  }

  /**
   * Execute clustering to write inputRecords into new files based on strategyParams.
   * Different from {@link performClusteringWithRecordsRDD}, this method take {@link Dataset<Row>}
   * as inputs.
   */
  public abstract HoodieData<WriteStatus> performClusteringWithRecordsRow(final Dataset<Row> inputRecords, final int numOutputGroups, final String instantTime,
                                                                          final Map<String, String> strategyParams, final Schema schema,
                                                                          final List<HoodieFileGroupId> fileGroupIdList, final boolean preserveHoodieMetadata);


  /**
   * Execute clustering to write inputRecords into new files as defined by rules in strategy parameters.
   * The number of new file groups created is bounded by numOutputGroups.
   * Note that commit is not done as part of strategy. commit is callers responsibility.
   *
   * @param inputRecords           RDD of {@link HoodieRecord}.
   * @param numOutputGroups        Number of output file groups.
   * @param instantTime            Clustering (replace commit) instant time.
   * @param strategyParams         Strategy parameters containing columns to sort the data by when clustering.
   * @param schema                 Schema of the data including metadata fields.
   * @param fileGroupIdList        File group id corresponding to each out group.
   * @param preserveHoodieMetadata Whether to preserve commit metadata while clustering.
   * @return RDD of {@link WriteStatus}.
   */
  public abstract HoodieData<WriteStatus> performClusteringWithRecordsRDD(final HoodieData<HoodieRecord<T>> inputRecords, final int numOutputGroups, final String instantTime,
                                                                       final Map<String, String> strategyParams, final Schema schema,
                                                                       final List<HoodieFileGroupId> fileGroupIdList, final boolean preserveHoodieMetadata);

  protected HoodieData<WriteStatus> performRowWrite(Dataset<Row> inputRecords, Map<String, String> parameters) {
    String uuid = UUID.randomUUID().toString();
    parameters.put(HoodieWriteConfig.BULKINSERT_ROW_IDENTIFY_ID.key(), uuid);
    try {
      inputRecords.write()
          .format("hudi")
          .options(JavaConverters.mapAsScalaMapConverter(parameters).asScala())
          .mode(SaveMode.Append)
          .save(getWriteConfig().getBasePath());
      List<WriteStatus> writeStatusList = HoodieInternalWriteStatusCoordinator.get().getWriteStatuses(uuid)
          .stream()
          .map(internalWriteStatus -> {
            WriteStatus status = new WriteStatus(
                internalWriteStatus.isTrackSuccessRecords(), internalWriteStatus.getFailureFraction());
            status.setFileId(internalWriteStatus.getFileId());
            status.setTotalRecords(internalWriteStatus.getTotalRecords());
            status.setPartitionPath(internalWriteStatus.getPartitionPath());
            status.setStat(internalWriteStatus.getStat());
            return status;
          }).collect(Collectors.toList());
      return getEngineContext().parallelize(writeStatusList);
    } finally {
      HoodieInternalWriteStatusCoordinator.get().removeStatuses(uuid);
    }
  }

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
  protected <I> BulkInsertPartitioner<I> getPartitioner(Map<String, String> strategyParams,
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
              : new RDDSpatialCurveSortPartitioner(
              (HoodieSparkEngineContext) getEngineContext(),
              orderByColumns,
              layoutOptStrategy,
              getWriteConfig().getLayoutOptimizationCurveBuildMethod(),
              HoodieAvroUtils.addMetadataFields(schema));
        case LINEAR:
          return isRowPartitioner
              ? new RowCustomColumnsSortPartitioner(orderByColumns)
              : new RDDCustomColumnsSortPartitioner(orderByColumns, HoodieAvroUtils.addMetadataFields(schema),
              getWriteConfig().isConsistentLogicalTimestampEnabled());
        default:
          throw new UnsupportedOperationException(String.format("Layout optimization strategy '%s' is not supported", layoutOptStrategy));
      }
    }).orElse(isRowPartitioner ? BulkInsertInternalPartitionerWithRowsFactory.get(getWriteConfig().getBulkInsertSortMode()) :
        BulkInsertInternalPartitionerFactory.get(getWriteConfig().getBulkInsertSortMode()));
  }

  /**
   * Configure {@link BulkInsertPartitioner<Dataset<Row>>} based on strategy params.
   *
   * @param strategyParams Strategy parameters containing columns to sort the data by when clustering.
   * @param parameters Map used to write the partitioner configurations.
   * @return {@link RDDCustomColumnsSortPartitioner} if sort columns are provided, otherwise empty.
   */
  protected void configRowPartitioner(Map<String, String> strategyParams, Map<String, String> parameters) {
    Option.ofNullable(strategyParams.get(PLAN_STRATEGY_SORT_COLUMNS.key()))
        .map(orderColumnStr -> {
          parameters.put(HoodieWriteConfig.BULKINSERT_USER_DEFINED_PARTITIONER_SORT_COLUMNS.key(), orderColumnStr);
          HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy = getWriteConfig().getLayoutOptimizationStrategy();
          switch (layoutOptStrategy) {
            case ZORDER:
            case HILBERT:
              parameters.put(HoodieWriteConfig.BULKINSERT_USER_DEFINED_PARTITIONER_CLASS_NAME.key(), RowSpatialCurveSortPartitioner.class.getCanonicalName());
              break;
            case LINEAR:
              parameters.put(HoodieWriteConfig.BULKINSERT_USER_DEFINED_PARTITIONER_CLASS_NAME.key(), RowCustomColumnsSortPartitioner.class.getCanonicalName());
              break;
            default:
              throw new UnsupportedOperationException(String.format("Layout optimization strategy '%s' is not supported", layoutOptStrategy));
          }
          parameters.compute(HoodieClusteringConfig.LAYOUT_OPTIMIZE_STRATEGY.key(),
              (k, v) -> getWriteConfig().getString(HoodieClusteringConfig.LAYOUT_OPTIMIZE_STRATEGY));
          parameters.compute(HoodieClusteringConfig.LAYOUT_OPTIMIZE_SPATIAL_CURVE_BUILD_METHOD.key(),
              (k, v) -> getWriteConfig().getString(HoodieClusteringConfig.LAYOUT_OPTIMIZE_SPATIAL_CURVE_BUILD_METHOD));
          return orderColumnStr;
        });
  }


  /**
   * Submit job to execute clustering for the group with RDD APIs.
   */
  private CompletableFuture<HoodieData<WriteStatus>> runClusteringForGroupAsyncWithRDD(HoodieClusteringGroup clusteringGroup, Map<String, String> strategyParams,
                                                                                       boolean preserveHoodieMetadata, String instantTime) {
    return CompletableFuture.supplyAsync(() -> {
      JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(getEngineContext());
      HoodieData<HoodieRecord<T>> inputRecords = readRecordsForGroupAsRDD(jsc, clusteringGroup, instantTime);
      Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(getWriteConfig().getSchema()));
      List<HoodieFileGroupId> inputFileIds = clusteringGroup.getSlices().stream()
          .map(info -> new HoodieFileGroupId(info.getPartitionPath(), info.getFileId()))
          .collect(Collectors.toList());
      return performClusteringWithRecordsRDD(inputRecords, clusteringGroup.getNumOutputFileGroups(), instantTime, strategyParams, readerSchema, inputFileIds, preserveHoodieMetadata);
    });
  }

  /**
   * Submit job to execute clustering for the group with dataset APIs.
   */
  private CompletableFuture<HoodieData<WriteStatus>> runClusteringForGroupAsyncWithRow(HoodieClusteringGroup clusteringGroup, Map<String, String> strategyParams,
                                                                                       boolean preserveHoodieMetadata, String instantTime) {
    return CompletableFuture.supplyAsync(() -> {
      JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(getEngineContext());
      Dataset<Row> inputRecords = readRecordsForGroupAsRow(jsc, clusteringGroup, instantTime);
      Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(getWriteConfig().getSchema()));
      List<HoodieFileGroupId> inputFileIds = clusteringGroup.getSlices().stream()
          .map(info -> new HoodieFileGroupId(info.getPartitionPath(), info.getFileId()))
          .collect(Collectors.toList());
      return performClusteringWithRecordsRow(inputRecords, clusteringGroup.getNumOutputFileGroups(), instantTime, strategyParams, readerSchema, inputFileIds, preserveHoodieMetadata);
    });
  }

  /**
   * Get RDD of all records for the group. This includes all records from file slice (Apply updates from log files, if any).
   */
  private HoodieData<HoodieRecord<T>> readRecordsForGroupAsRDD(JavaSparkContext jsc, HoodieClusteringGroup clusteringGroup, String instantTime) {
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
    return HoodieJavaRDD.of(jsc.parallelize(clusteringOps, clusteringOps.size()).mapPartitions(clusteringOpsPartition -> {
      List<Iterator<HoodieRecord<T>>> recordIterators = new ArrayList<>();
      clusteringOpsPartition.forEachRemaining(clusteringOp -> {
        long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(new SparkTaskContextSupplier(), config);
        LOG.info("MaxMemoryPerCompaction run as part of clustering => " + maxMemoryPerCompaction);
        try {
          Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
          HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
              .withFileSystem(table.getMetaClient().getFs())
              .withBasePath(table.getMetaClient().getBasePath())
              .withLogFilePaths(clusteringOp.getDeltaFilePaths())
              .withReaderSchema(readerSchema)
              .withLatestInstantTime(instantTime)
              .withMaxMemorySizeInBytes(maxMemoryPerCompaction)
              .withReadBlocksLazily(config.getCompactionLazyBlockReadEnabled())
              .withReverseReader(config.getCompactionReverseLogReadEnabled())
              .withBufferSize(config.getMaxDFSStreamBufferSize())
              .withSpillableMapBasePath(config.getSpillableMapBasePath())
              .withPartition(clusteringOp.getPartitionPath())
              .withDiskMapType(config.getCommonConfig().getSpillableDiskMapType())
              .withBitCaskDiskMapCompressionEnabled(config.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
              .build();

          Option<HoodieFileReader> baseFileReader = StringUtils.isNullOrEmpty(clusteringOp.getDataFilePath())
              ? Option.empty()
              : Option.of(HoodieFileReaderFactory.getFileReader(table.getHadoopConf(), new Path(clusteringOp.getDataFilePath())));
          HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
          recordIterators.add(getFileSliceReader(baseFileReader, scanner, readerSchema,
              tableConfig.getPayloadClass(),
              tableConfig.getPreCombineField(),
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
    SerializableConfiguration hadoopConf = new SerializableConfiguration(getHoodieTable().getHadoopConf());
    HoodieWriteConfig writeConfig = getWriteConfig();

    // NOTE: It's crucial to make sure that we don't capture whole "this" object into the
    //       closure, as this might lead to issues attempting to serialize its nested fields
    return HoodieJavaRDD.of(jsc.parallelize(clusteringOps, clusteringOps.size())
        .mapPartitions(clusteringOpsPartition -> {
          List<Iterator<IndexedRecord>> iteratorsForPartition = new ArrayList<>();
          clusteringOpsPartition.forEachRemaining(clusteringOp -> {
            try {
              Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(writeConfig.getSchema()));
              HoodieFileReader<IndexedRecord> baseFileReader = HoodieFileReaderFactory.getFileReader(hadoopConf.get(), new Path(clusteringOp.getDataFilePath()));
              iteratorsForPartition.add(baseFileReader.getRecordIterator(readerSchema));
            } catch (IOException e) {
              throw new HoodieClusteringException("Error reading input data for " + clusteringOp.getDataFilePath()
                  + " and " + clusteringOp.getDeltaFilePaths(), e);
            }
          });

          return new ConcatenatingIterator<>(iteratorsForPartition);
        })
        .map(record -> transform(record, writeConfig)));
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

    String[] baseFilePaths = clusteringOps
        .stream()
        .map(op -> {
          ArrayList<String> pairs = new ArrayList<>();
          if (op.getBootstrapFilePath() != null) {
            pairs.add(op.getBootstrapFilePath());
          }
          if (op.getDataFilePath() != null) {
            pairs.add(op.getDataFilePath());
          }
          return pairs;
        })
        .flatMap(Collection::stream)
        .filter(path -> !path.isEmpty())
        .toArray(String[]::new);
    String[] deltaPaths = clusteringOps
        .stream()
        .filter(op -> !op.getDeltaFilePaths().isEmpty())
        .flatMap(op -> op.getDeltaFilePaths().stream())
        .toArray(String[]::new);

    Dataset<Row> inputRecords;
    if (hasLogFiles) {
      String compactionFractor = Option.ofNullable(getWriteConfig().getString("compaction.memory.fraction"))
          .orElse("0.75");
      String[] paths = CollectionUtils.combine(baseFilePaths, deltaPaths);
      inputRecords = sqlContext.read()
          .format("org.apache.hudi")
          .option("hoodie.datasource.query.type", "snapshot")
          .option("compaction.memory.fraction", compactionFractor)
          .option("as.of.instant", instantTime)
          .option("hoodie.datasource.read.paths", String.join(",", paths))
          .load();
    } else {
      inputRecords = sqlContext.read()
          .format("org.apache.hudi")
          .option("as.of.instant", instantTime)
          .option("hoodie.datasource.read.paths", String.join(",", baseFilePaths))
          .load();
    }
    return inputRecords;
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

  /**
   * Transform IndexedRecord into HoodieRecord.
   */
  private static <T> HoodieRecord<T> transform(IndexedRecord indexedRecord, HoodieWriteConfig writeConfig) {
    GenericRecord record = (GenericRecord) indexedRecord;
    Option<BaseKeyGenerator> keyGeneratorOpt = Option.empty();
    if (!writeConfig.populateMetaFields()) {
      try {
        keyGeneratorOpt = Option.of((BaseKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(writeConfig.getProps()));
      } catch (IOException e) {
        throw new HoodieIOException("Only BaseKeyGenerators are supported when meta columns are disabled ", e);
      }
    }
    String key = KeyGenUtils.getRecordKeyFromGenericRecord(record, keyGeneratorOpt);
    String partition = KeyGenUtils.getPartitionPathFromGenericRecord(record, keyGeneratorOpt);
    HoodieKey hoodieKey = new HoodieKey(key, partition);

    HoodieRecordPayload avroPayload = new RewriteAvroPayload(record);
    HoodieRecord hoodieRecord = new HoodieAvroRecord(hoodieKey, avroPayload);
    return hoodieRecord;
  }
}
