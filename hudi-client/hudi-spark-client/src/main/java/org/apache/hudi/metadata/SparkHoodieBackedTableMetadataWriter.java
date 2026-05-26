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

package org.apache.hudi.metadata;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDMetadataWriteClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.SparkMetadataWriterUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.index.vector.VectorIndexBootstrapUtils;
import org.apache.hudi.common.index.vector.VectorIndexOptions;
import org.apache.hudi.common.index.vector.RaBitQEncoder;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.HoodieSparkIndexClient;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.BulkInsertPartitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;
import static org.apache.hudi.common.model.HoodieRecord.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getProjectedSchemaForExpressionIndex;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;

@Slf4j
public class SparkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter<JavaRDD<HoodieRecord>, JavaRDD<WriteStatus>> {

  /**
   * Return a Spark based implementation of {@code HoodieTableMetadataWriter} which can be used to
   * write to the metadata table.
   * <p>
   * If the metadata table does not exist, an attempt is made to bootstrap it but there is no guaranteed that
   * table will end up bootstrapping at this time.
   *
   * @param conf
   * @param writeConfig
   * @param context
   * @param inflightInstantTimestamp Timestamp of an instant which is in-progress. This instant is ignored while
   *                                 attempting to bootstrap the table.
   * @return An instance of the {@code HoodieTableMetadataWriter}
   */
  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new SparkHoodieBackedTableMetadataWriter(
        conf, writeConfig, EAGER, context, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new SparkHoodieBackedTableMetadataWriter(
        conf, writeConfig, failedWritesCleaningPolicy, context, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf, HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context) {
    return create(conf, writeConfig, context, Option.empty());
  }

  SparkHoodieBackedTableMetadataWriter(StorageConfiguration<?> hadoopConf,
                                       HoodieWriteConfig writeConfig,
                                       HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                       HoodieEngineContext engineContext,
                                       Option<String> inflightInstantTimestamp) {
    this(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp, false);
  }

  SparkHoodieBackedTableMetadataWriter(StorageConfiguration<?> hadoopConf,
                                       HoodieWriteConfig writeConfig,
                                       HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                       HoodieEngineContext engineContext,
                                       Option<String> inflightInstantTimestamp,
                                       boolean streamingWrites) {
    super(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp, streamingWrites);
  }

  @Override
  protected void initRegistry() {
    if (metadataWriteConfig.isMetricsOn()) {
      Registry registry;
      if (metadataWriteConfig.isExecutorMetricsEnabled() && metadataWriteConfig.getMetricsReporterType() != MetricsReporterType.INMEMORY) {
        registry = Registry.getRegistry("HoodieMetadata", DistributedRegistry.class.getName());
        HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext) engineContext;
        ((DistributedRegistry) registry).register(sparkEngineContext.getJavaSparkContext());
      } else {
        registry = Registry.getRegistry("HoodieMetadata");
      }
      this.metrics = Option.of(new HoodieMetadataMetrics(metadataWriteConfig.getMetricsConfig(), dataMetaClient.getStorage()));
    } else {
      this.metrics = Option.empty();
    }
  }

  @Override
  protected void commit(String instantTime, Map<String, HoodieData<HoodieRecord>> partitionRecordsMap) {
    commitInternal(instantTime, partitionRecordsMap, false, Option.empty());
  }

  @Override
  protected JavaRDD<HoodieRecord> convertHoodieDataToEngineSpecificData(HoodieData<HoodieRecord> records) {
    return HoodieJavaRDD.getJavaRDD(records);
  }

  @Override
  protected HoodieData<WriteStatus> convertEngineSpecificDataToHoodieData(JavaRDD<WriteStatus> records) {
    return HoodieJavaRDD.of(records);
  }

  @Override
  public JavaRDD<WriteStatus> streamWriteToMetadataTable(Pair<List<HoodieFileGroupId>, HoodieData<HoodieRecord>> fileGroupIdToTaggedRecords, String instantTime) {
    JavaRDD<HoodieRecord> mdtRecords = HoodieJavaRDD.getJavaRDD(fileGroupIdToTaggedRecords.getValue());
    engineContext.setJobStatus(this.getClass().getSimpleName(), String.format("Upserting with instant %s into metadata table %s", instantTime, metadataWriteConfig.getTableName()));
    JavaRDD<WriteStatus> partialMetadataWriteStatuses = getSparkWriteClient(Option.empty()).firstUpsertPreppedRecords(mdtRecords, instantTime, fileGroupIdToTaggedRecords.getKey());
    return partialMetadataWriteStatuses;
  }

  @Override
  public JavaRDD<WriteStatus> secondaryWriteToMetadataTablePartitions(JavaRDD<HoodieRecord> preppedRecords, String instantTime) {
    engineContext.setJobStatus(this.getClass().getSimpleName(), String.format("Upserting at %s into metadata table %s", instantTime, metadataWriteConfig.getTableName()));
    JavaRDD<WriteStatus> partialMetadataWriteStatuses = getSparkWriteClient(Option.empty()).secondaryUpsertPreppedRecords(preppedRecords, instantTime);
    return partialMetadataWriteStatuses;
  }

  @Override
  protected void bulkInsertAndCommit(BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> writeClient,
                                     String instantTime,
                                     JavaRDD<HoodieRecord> preppedRecordInputs,
                                     Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    JavaRDD<WriteStatus> writeStatusJavaRDD = writeClient.bulkInsertPreppedRecords(preppedRecordInputs, instantTime, bulkInsertPartitioner);
    writeClient.commit(instantTime, writeStatusJavaRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
  }

  @Override
  protected void upsertAndCommit(BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> writeClient, String instantTime, JavaRDD<HoodieRecord> preppedRecordInputs) {
    // When specified, reduce the parallelism of input record RDD to improve write performance.
    int parallelism = dataWriteConfig.getMetadataConfig().getRecordPreparationParallelism();
    if (parallelism > 0 && preppedRecordInputs.getNumPartitions() > parallelism) {
      preppedRecordInputs = preppedRecordInputs.coalesce(parallelism);
    }

    JavaRDD<WriteStatus> writeStatusJavaRDD = writeClient.upsertPreppedRecords(preppedRecordInputs, instantTime);
    writeClient.commit(instantTime, writeStatusJavaRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
  }

  @Override
  protected void upsertAndCommit(BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> writeClient,
                                 String instantTime,
                                 JavaRDD<HoodieRecord> preppedRecordInputs,
                                 List<HoodieFileGroupId> fileGroupsIdsToUpdate) {
    JavaRDD<WriteStatus> writeStatusJavaRDD = getSparkWriteClient(Option.of(writeClient)).firstUpsertPreppedRecords(preppedRecordInputs, instantTime, fileGroupsIdsToUpdate);
    writeClient.commit(instantTime, writeStatusJavaRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
  }

  @Override
  protected void bulkCommit(String instantTime, String partitionPath, HoodieData<HoodieRecord> records,
      MetadataTableFileGroupIndexParser indexParser) {
    SparkHoodieMetadataBulkInsertPartitioner partitioner = new SparkHoodieMetadataBulkInsertPartitioner(indexParser);
    commitInternal(instantTime, Collections.singletonMap(partitionPath, records), true, Option.of(partitioner));
  }

  @Override
  public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
    List<String> partitionsToDrop = partitions.stream().map(MetadataPartitionType::getPartitionPath).collect(Collectors.toList());
    log.info("Deleting Metadata Table partitions: {}", partitionsToDrop);

    SparkRDDWriteClient writeClient = (SparkRDDWriteClient) getWriteClient();
    String actionType = CommitUtils.getCommitActionType(WriteOperationType.DELETE_PARTITION, HoodieTableType.MERGE_ON_READ);
    writeClient.startCommitForMetadataTable(metadataMetaClient, instantTime, actionType);
    HoodieWriteResult result = writeClient.deletePartitions(partitionsToDrop, instantTime);
    writeClient.commit(instantTime, result.getWriteStatuses(), Option.empty(), REPLACE_COMMIT_ACTION, result.getPartitionToReplaceFileIds());
  }

  /**
   * Loads the file slices touched by the commit due to given instant time and returns the records for the expression index.
   * This generates partition stat record updates along with EI column stat update records. Partition stat record updates are generated
   * by reloading the affected partitions column range metadata from EI and then merging it with partition stat record from the updated data.
   *
   * @param commitMetadata {@code HoodieCommitMetadata}
   * @param indexPartition partition name of the expression index
   * @param instantTime    timestamp at of the current update commit
   */
  @Override
  protected HoodieData<HoodieRecord> getExpressionIndexUpdates(HoodieCommitMetadata commitMetadata, String indexPartition, String instantTime) throws Exception {
    HoodieIndexDefinition indexDefinition = getIndexDefinition(indexPartition);
    boolean isExprIndexUsingColumnStats = indexDefinition.getIndexType().equals(PARTITION_NAME_COLUMN_STATS);
    Option<Function<HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>>, HoodieData<HoodieRecord>>> partitionRecordsFunctionOpt = Option.empty();
    if (isExprIndexUsingColumnStats) {
      // Fetch column range metadata for affected partitions in the commit
      HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>> exprIndexPartitionStatUpdates =
          SparkMetadataWriterUtils.getExpressionIndexPartitionStatsForExistingFiles(
                  commitMetadata, indexPartition, engineContext, getTableMetadata(), dataMetaClient, dataWriteConfig.getMetadataConfig(),
                  Option.of(dataWriteConfig.getRecordMerger().getRecordType()), instantTime, dataWriteConfig)
              .flatMapValues(List::iterator);
      // The function below merges the column range metadata from the updated data with latest column range metadata of affected partition computed above
      partitionRecordsFunctionOpt = Option.of(rangeMetadata ->
          HoodieTableMetadataUtil.collectAndProcessExprIndexPartitionStatRecords(exprIndexPartitionStatUpdates.union(rangeMetadata), true, Option.of(indexDefinition.getIndexName())));
    }

    // Step 1: Generate partition name, file path and size triplets from the newly created files in the commit metadata
    List<Pair<String, Pair<String, Long>>> partitionFilePathPairs = new ArrayList<>();
    commitMetadata.getPartitionToWriteStats().forEach((dataPartition, writeStats) -> writeStats.forEach(writeStat -> partitionFilePathPairs.add(
        Pair.of(writeStat.getPartitionPath(), Pair.of(new StoragePath(dataMetaClient.getBasePath(), writeStat.getPath()).toString(), writeStat.getFileSizeInBytes())))));
    int parallelism = Math.min(partitionFilePathPairs.size(), dataWriteConfig.getMetadataConfig().getExpressionIndexParallelism());
    HoodieSchema tableSchema = new TableSchemaResolver(dataMetaClient).getTableSchema();
    HoodieSchema readerSchema = getProjectedSchemaForExpressionIndex(indexDefinition, dataMetaClient, tableSchema);
    // Step 2: Compute the expression index column stat and partition stat records for these newly created files
    // partitionRecordsFunctionOpt - Function used to generate partition stats. These stats are generated only for expression index created using column stats
    //
    // In the partitionRecordsFunctionOpt function we merge the expression index records from the new files created in the commit metadata
    // with the expression index records from the unmodified files to get the new partition stat records
    HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata expressionIndexComputationMetadata =
        SparkMetadataWriterUtils.getExprIndexRecords(partitionFilePathPairs, indexDefinition, dataMetaClient, parallelism, tableSchema, readerSchema, instantTime, engineContext, dataWriteConfig,
            partitionRecordsFunctionOpt);
    return expressionIndexComputationMetadata.getPartitionStatRecordsOpt().isPresent()
        ? expressionIndexComputationMetadata.getExpressionIndexRecords().union(expressionIndexComputationMetadata.getPartitionStatRecordsOpt().get())
        : expressionIndexComputationMetadata.getExpressionIndexRecords();
  }

  @Override
  protected HoodieData<HoodieRecord> getExpressionIndexRecords(List<Pair<String, Pair<String, Long>>> partitionFilePathAndSizeTriplet,
                                                               HoodieIndexDefinition indexDefinition,
                                                               HoodieTableMetaClient metaClient, int parallelism,
                                                               HoodieSchema tableSchema, HoodieSchema readerSchema, StorageConfiguration<?> storageConf,
                                                               String instantTime) {
    ExpressionIndexComputationMetadata expressionIndexComputationMetadata = SparkMetadataWriterUtils.getExprIndexRecords(partitionFilePathAndSizeTriplet, indexDefinition,
        metaClient, parallelism, tableSchema, readerSchema, instantTime, engineContext, dataWriteConfig,
        Option.of(rangeMetadata ->
            HoodieTableMetadataUtil.collectAndProcessExprIndexPartitionStatRecords(rangeMetadata, true, Option.of(indexDefinition.getIndexName()))));
    HoodieData<HoodieRecord> exprIndexRecords = expressionIndexComputationMetadata.getExpressionIndexRecords();
    if (indexDefinition.getIndexType().equals(PARTITION_NAME_COLUMN_STATS)) {
      exprIndexRecords = exprIndexRecords.union(expressionIndexComputationMetadata.getPartitionStatRecordsOpt().get());
    }
    return exprIndexRecords;
  }

  @Override
  protected HoodieData<HoodieRecord> getVectorIndexRecords(HoodieIndexDefinition indexDefinition) {
    try {
      String vectorColumn = indexDefinition.getSourceFields().get(0);
      HoodieSchema tableSchema = new TableSchemaResolver(dataMetaClient).getTableSchema();
      Pair<String, HoodieSchemaField> fieldSchema = HoodieSchemaUtils.getNestedField(tableSchema, vectorColumn)
          .orElseThrow(() -> new HoodieMetadataException("Vector column not found in table schema: " + vectorColumn));
      HoodieSchema vectorSchema = fieldSchema.getRight().schema().getNonNullType();
      ValidationUtils.checkState(vectorSchema.getType() == HoodieSchemaType.VECTOR,
          "Vector index can only be bootstrapped from VECTOR columns: " + vectorColumn);

      HoodieSchema.Vector resolvedVectorType = (HoodieSchema.Vector) vectorSchema;
      int configuredDimension = VectorIndexOptions.getDimension(indexDefinition.getIndexOptions());
      ValidationUtils.checkState(resolvedVectorType.getDimension() == configuredDimension,
          String.format("Vector dimension mismatch for %s: schema=%s, configured=%s",
              vectorColumn, resolvedVectorType.getDimension(), configuredDimension));

      SparkSession sparkSession = ((HoodieSparkEngineContext) engineContext).getSqlContext().sparkSession();
      List<String> latestBaseFilePaths;
      try (HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
          engineContext, dataMetaClient, dataMetaClient.getCommitsTimeline().filterCompletedInstants())) {
        List<String> partitionPaths = FSUtils.getAllPartitionPaths(engineContext, dataMetaClient, false);
        fsView.loadAllPartitions();
        latestBaseFilePaths = partitionPaths.stream()
            .flatMap(partition -> fsView.getLatestBaseFiles(partition))
            .map(HoodieBaseFile::getPath)
            .collect(Collectors.toList());
      }

      if (latestBaseFilePaths.isEmpty()) {
        log.warn("Vector index bootstrap found no latest base files for {}", dataMetaClient.getBasePath());
        return engineContext.emptyHoodieData();
      }

      log.info("Vector index bootstrap discovered {} latest base files for {}", latestBaseFilePaths.size(), indexDefinition.getIndexName());

      Dataset<Row> tableRows = sparkSession.read()
          .parquet(latestBaseFilePaths.toArray(new String[0]))
          .select(
              col(RECORD_KEY_METADATA_FIELD),
              col(PARTITION_PATH_METADATA_FIELD).alias("__partition_path"),
              col(FILENAME_METADATA_FIELD).alias("__file_name"),
              col(vectorColumn).alias("__vector_value"));

      String vectorToFeaturesUdf = "__hudi_vector_index_to_features";
      UDF1<Object, org.apache.spark.ml.linalg.Vector> vectorFeatureUdf =
          rawValue -> rawValue == null
              ? null
              : Vectors.dense(toNumericArray(rawValue, configuredDimension, resolvedVectorType.getVectorElementType()));
      sparkSession.udf().register(vectorToFeaturesUdf, vectorFeatureUdf, new VectorUDT());

      Dataset<Row> featureDataset = tableRows
          .withColumn("features", callUDF(vectorToFeaturesUdf, col("__vector_value")))
          .filter(col("features").isNotNull())
          .drop("__vector_value")
          .persist(StorageLevel.MEMORY_AND_DISK());

      long documentCount = featureDataset.count();
      if (documentCount == 0) {
        long tableRowCount = tableRows.count();
        log.warn("Vector index bootstrap produced zero feature rows for {} despite reading {} table rows",
            indexDefinition.getIndexName(), tableRowCount);
        return engineContext.emptyHoodieData();
      }

      log.info("Vector index bootstrap produced {} feature rows for {}", documentCount, indexDefinition.getIndexName());

      int requestedClusters = Math.max(1, VectorIndexOptions.getNumClusters(indexDefinition.getIndexOptions()));
      int numClusters = (int) Math.min(documentCount, requestedClusters);
      int maxIterations = Math.max(1, VectorIndexOptions.getMaxIter(indexDefinition.getIndexOptions()));
      String quantizerType = getQuantizerType(indexDefinition.getIndexOptions());
      long quantizerSeed = getRaBitQSeed(indexDefinition.getIndexOptions());
      boolean assumeNormalized = isRaBitQAssumeNormalized(indexDefinition.getIndexOptions());
      boolean storeRaBitQCodesInMdt = "IVF_RABITQ".equals(quantizerType)
          && VectorIndexOptions.shouldStoreRaBitQCodesInMdt(indexDefinition.getIndexOptions());
      int targetRowsPerShard = Math.max(1, VectorIndexOptions.getRaBitQPostingTargetRowsPerShard(indexDefinition.getIndexOptions()));
      int maxShardsPerCluster = Math.max(1, VectorIndexOptions.getRaBitQPostingMaxShardsPerCluster(indexDefinition.getIndexOptions()));
      int quantizedCodeBytes = "IVF_RABITQ".equals(quantizerType)
          ? new RaBitQEncoder(configuredDimension, quantizerSeed, assumeNormalized).codeBytes()
          : 0;

      KMeans kMeans = new KMeans()
          .setK(numClusters)
          .setSeed(VectorIndexOptions.getRaBitQSeed(indexDefinition.getIndexOptions()))
          .setMaxIter(maxIterations)
          .setFeaturesCol("features")
          .setPredictionCol("__cluster_id");
      KMeansModel model = kMeans.fit(featureDataset);

      Dataset<Row> predictions = model.transform(featureDataset)
          .select(
              col(RECORD_KEY_METADATA_FIELD),
              col("__cluster_id"),
              col("__file_name"),
              col("__partition_path"),
              col("features"))
          .persist(StorageLevel.MEMORY_AND_DISK());
      long predictionCount = predictions.count();
      log.info("Vector index bootstrap materialized {} prediction rows for {}", predictionCount, indexDefinition.getIndexName());
      long lastUpdatedTs = System.currentTimeMillis();
      String generationId = Integer.toUnsignedString((int) (lastUpdatedTs / 1000L));
      Map<Integer, Long> clusterVectorCounts = predictions.groupBy(col("__cluster_id"))
          .count()
          .collectAsList()
          .stream()
          .collect(Collectors.toMap(
              row -> ((Number) row.get(0)).intValue(),
              row -> ((Number) row.get(1)).longValue()));
      Map<Integer, Integer> clusterShardCounts = clusterVectorCounts.entrySet().stream()
          .collect(Collectors.toMap(
              Map.Entry::getKey,
              entry -> computeShardCount(entry.getValue(), targetRowsPerShard, maxShardsPerCluster)));
      JavaRDD<HoodieRecord> assignmentRecords = predictions.javaRDD().map(row -> {
        int clusterId = ((Number) row.get(1)).intValue();
        int shardId = computeShardId(row.getString(0), clusterShardCounts.getOrDefault(clusterId, 1));
        return HoodieMetadataPayload.createVectorIndexAssignmentRecord(
            row.getString(0),
            generationId,
            clusterId,
            shardId,
            row.isNullAt(2) ? null : FSUtils.getFileIdFromFileName(row.getString(2)),
            row.isNullAt(3) ? null : row.getString(3),
            indexDefinition.getIndexName());
      });
      StructType fgMappingSchema = new StructType(new StructField[] {
          new StructField("__cluster_id", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("__partition_path", DataTypes.StringType, true, Metadata.empty()),
          new StructField("__file_group_id", DataTypes.StringType, true, Metadata.empty())
      });
      Dataset<Row> fgMappingDataset = sparkSession.createDataFrame(
              predictions.javaRDD()
          .map(row -> RowFactory.create(
              ((Number) row.get(1)).intValue(),
              row.isNullAt(3) ? null : row.getString(3),
              row.isNullAt(2) ? null : FSUtils.getFileIdFromFileName(row.getString(2)))),
          fgMappingSchema);
      JavaRDD<HoodieRecord> fgMappingRecords = buildFgMappingRecords(fgMappingDataset, lastUpdatedTs, indexDefinition.getIndexName());
      double[][] centroids = Arrays.stream(model.clusterCenters())
          .map(center -> center.toArray())
          .toArray(double[][]::new);
      JavaRDD<HoodieRecord> centroidRecord = ((HoodieSparkEngineContext) engineContext).getJavaSparkContext()
          .parallelize(Collections.singletonList(
              HoodieMetadataPayload.createVectorIndexCentroidsRecord(
                  VectorIndexBootstrapUtils.serializeCentroids(centroids, resolvedVectorType.getVectorElementType()),
                  indexDefinition.getIndexName())), 1);
      JavaRDD<HoodieRecord> quantizerRecord = buildQuantizerRecord(
          quantizerType, quantizedCodeBytes, quantizerSeed, assumeNormalized, indexDefinition.getIndexName());
      JavaRDD<HoodieRecord> manifestRecord = storeRaBitQCodesInMdt
          ? buildManifestRecord(generationId, quantizerType, quantizedCodeBytes, quantizerSeed, assumeNormalized, lastUpdatedTs, indexDefinition.getIndexName())
          : ((HoodieSparkEngineContext) engineContext).getJavaSparkContext().parallelize(Collections.emptyList(), 1);
      JavaRDD<HoodieRecord> generationManifestRecord = storeRaBitQCodesInMdt
          ? buildGenerationManifestRecord(generationId, quantizerType, quantizedCodeBytes, quantizerSeed, assumeNormalized, lastUpdatedTs, indexDefinition.getIndexName())
          : ((HoodieSparkEngineContext) engineContext).getJavaSparkContext().parallelize(Collections.emptyList(), 1);
      JavaRDD<HoodieRecord> clusterManifestRecords = storeRaBitQCodesInMdt
          ? buildClusterManifestRecords(predictions, generationId, clusterVectorCounts, clusterShardCounts, lastUpdatedTs, indexDefinition.getIndexName())
          : ((HoodieSparkEngineContext) engineContext).getJavaSparkContext().parallelize(Collections.emptyList(), 1);
      JavaRDD<HoodieRecord> postingRecords = storeRaBitQCodesInMdt
          ? buildPostingRecords(predictions, configuredDimension, quantizerSeed, assumeNormalized, generationId,
              clusterShardCounts, lastUpdatedTs, indexDefinition.getIndexName())
          : ((HoodieSparkEngineContext) engineContext).getJavaSparkContext().parallelize(Collections.emptyList(), 1);
      HoodieData<HoodieRecord> metadataRecords = HoodieJavaRDD.of(
          centroidRecord
              .union(quantizerRecord)
              .union(manifestRecord)
              .union(generationManifestRecord)
              .union(clusterManifestRecords)
              .union(assignmentRecords)
              .union(fgMappingRecords)
              .union(postingRecords));
      predictions.unpersist(false);
      featureDataset.unpersist(false);
      return metadataRecords;
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to bootstrap vector index records", e);
    }
  }

  private static String getQuantizerType(Map<String, String> options) {
    return options.getOrDefault(VectorIndexOptions.QUANTIZER, "IVF_RABITQ").toUpperCase();
  }

  private static long getRaBitQSeed(Map<String, String> options) {
    return Long.parseLong(options.getOrDefault(VectorIndexOptions.RABITQ_RANDOM_SEED, "42"));
  }

  private static boolean isRaBitQAssumeNormalized(Map<String, String> options) {
    return Boolean.parseBoolean(options.getOrDefault(VectorIndexOptions.RABITQ_ASSUME_NORMALIZED, "false"));
  }

  @SuppressWarnings("unchecked")
  private JavaRDD<HoodieRecord> buildQuantizerRecord(String quantizerType,
                                                     int quantizedCodeBytes,
                                                     long quantizerSeed,
                                                     boolean assumeNormalized,
                                                     String indexName) {
    try {
      Method method = HoodieMetadataPayload.class.getMethod(
          "createVectorIndexQuantizerMetadataRecord",
          String.class, int.class, long.class, boolean.class, String.class);
      HoodieRecord record = (HoodieRecord) method.invoke(null,
          quantizerType, quantizedCodeBytes, quantizerSeed, assumeNormalized, indexName);
      return ((HoodieSparkEngineContext) engineContext).getJavaSparkContext()
          .parallelize(Collections.singletonList(record), 1);
    } catch (ReflectiveOperationException | LinkageError e) {
      log.warn("Skipping vector quantizer metadata record because runtime hudi-common does not provide the required API");
      return ((HoodieSparkEngineContext) engineContext).getJavaSparkContext()
          .parallelize(Collections.emptyList(), 1);
    }
  }

  @SuppressWarnings("unchecked")
  private JavaRDD<HoodieRecord> buildManifestRecord(String generationId,
                                                    String quantizerType,
                                                    int quantizedCodeBytes,
                                                    long quantizerSeed,
                                                    boolean assumeNormalized,
                                                    long lastUpdatedTs,
                                                    String indexName) {
    try {
      Method method = HoodieMetadataPayload.class.getMethod(
          "createVectorIndexManifestRecord",
          String.class, String.class, int.class, long.class, boolean.class, long.class, String.class);
      HoodieRecord record = (HoodieRecord) method.invoke(null,
          generationId, quantizerType, quantizedCodeBytes, quantizerSeed, assumeNormalized, lastUpdatedTs, indexName);
      return ((HoodieSparkEngineContext) engineContext).getJavaSparkContext()
          .parallelize(Collections.singletonList(record), 1);
    } catch (ReflectiveOperationException | LinkageError e) {
      log.warn("Skipping vector manifest record because runtime hudi-common does not provide the required API");
      return ((HoodieSparkEngineContext) engineContext).getJavaSparkContext()
          .parallelize(Collections.emptyList(), 1);
    }
  }

  @SuppressWarnings("unchecked")
  private JavaRDD<HoodieRecord> buildGenerationManifestRecord(String generationId,
                                                              String quantizerType,
                                                              int quantizedCodeBytes,
                                                              long quantizerSeed,
                                                              boolean assumeNormalized,
                                                              long lastUpdatedTs,
                                                              String indexName) {
    try {
      Method method = HoodieMetadataPayload.class.getMethod(
          "createVectorIndexGenerationManifestRecord",
          String.class, String.class, int.class, long.class, boolean.class, long.class, String.class);
      HoodieRecord record = (HoodieRecord) method.invoke(null,
          generationId, quantizerType, quantizedCodeBytes, quantizerSeed, assumeNormalized, lastUpdatedTs, indexName);
      return ((HoodieSparkEngineContext) engineContext).getJavaSparkContext()
          .parallelize(Collections.singletonList(record), 1);
    } catch (ReflectiveOperationException | LinkageError e) {
      log.warn("Skipping vector generation manifest record because runtime hudi-common does not provide the required API");
      return ((HoodieSparkEngineContext) engineContext).getJavaSparkContext()
          .parallelize(Collections.emptyList(), 1);
    }
  }

  @SuppressWarnings("unchecked")
  private JavaRDD<HoodieRecord> buildClusterManifestRecords(Dataset<Row> predictions,
                                                            String generationId,
                                                            Map<Integer, Long> clusterVectorCounts,
                                                            Map<Integer, Integer> clusterShardCounts,
                                                            long lastUpdatedTs,
                                                            String indexName) {
    try {
      Map<Integer, List<String>> clusterFileGroups = predictions
          .where(col("__file_name").isNotNull())
          .groupBy(col("__cluster_id"))
          .agg(collect_set(col("__file_name")).alias("__file_names"))
          .collectAsList()
          .stream()
          .collect(Collectors.toMap(
              row -> ((Number) row.get(0)).intValue(),
              row -> row.<String>getList(1).stream()
                  .map(FSUtils::getFileIdFromFileName)
                  .distinct()
                  .collect(Collectors.toCollection(ArrayList::new))));
      Method method = HoodieMetadataPayload.class.getMethod(
          "createVectorIndexClusterManifestRecord",
          String.class, int.class, int.class, java.util.Collection.class, long.class, long.class, String.class);
      List<HoodieRecord> records = clusterVectorCounts.entrySet().stream()
          .map(entry -> {
            try {
              return (HoodieRecord) method.invoke(
                  null,
                  generationId,
                  entry.getKey(),
                  clusterShardCounts.getOrDefault(entry.getKey(), 1),
                  clusterFileGroups.getOrDefault(entry.getKey(), Collections.emptyList()),
                  entry.getValue(),
                  lastUpdatedTs,
                  indexName);
            } catch (ReflectiveOperationException ex) {
              throw new HoodieMetadataException("Failed to create vector cluster manifest record reflectively", ex);
            }
          })
          .collect(Collectors.toList());
      return ((HoodieSparkEngineContext) engineContext).getJavaSparkContext()
          .parallelize(records, Math.max(1, Math.min(records.size(), 64)));
    } catch (ReflectiveOperationException | LinkageError e) {
      log.warn("Skipping vector cluster manifest records because runtime hudi-common does not provide the required API");
      return ((HoodieSparkEngineContext) engineContext).getJavaSparkContext()
          .parallelize(Collections.emptyList(), 1);
    }
  }

  @SuppressWarnings("unchecked")
  private JavaRDD<HoodieRecord> buildPostingRecords(Dataset<Row> predictions,
                                                    int dimension,
                                                    long quantizerSeed,
                                                    boolean assumeNormalized,
                                                    String generationId,
                                                    Map<Integer, Integer> clusterShardCounts,
                                                    long lastUpdatedTs,
                                                    String indexName) {
    try {
      HoodieMetadataPayload.class.getMethod(
          "createVectorIndexPostingRecord",
          String.class, String.class, int.class, int.class, String.class, String.class, byte[].class, Float.class, long.class, String.class);
      final RaBitQEncoder encoder = new RaBitQEncoder(dimension, quantizerSeed, assumeNormalized);
      return predictions.javaRDD().map(row -> createPostingRecordCompat(
          row, encoder, assumeNormalized, generationId, clusterShardCounts, lastUpdatedTs, indexName));
    } catch (ReflectiveOperationException | LinkageError e) {
      log.warn("Skipping vector posting records because runtime hudi-common does not provide the required API");
      return ((HoodieSparkEngineContext) engineContext).getJavaSparkContext()
          .parallelize(Collections.emptyList(), 1);
    }
  }

  @SuppressWarnings("unchecked")
  private JavaRDD<HoodieRecord> buildFgMappingRecords(Dataset<Row> fgMappingDataset,
                                                      long lastUpdatedTs,
                                                      String indexName) {
    try {
      HoodieMetadataPayload.class.getMethod(
          "createVectorIndexFgMappingRecord",
          int.class, String.class, java.util.Collection.class, long.class, long.class, String.class);
      return fgMappingDataset
          .groupBy(col("__cluster_id"), col("__partition_path"))
          .agg(
              collect_set(col("__file_group_id")).alias("__file_group_ids"),
              count(lit(1)).alias("__vector_count"))
          .javaRDD()
          .map(row -> createFgMappingRecordCompat(row, lastUpdatedTs, indexName));
    } catch (ReflectiveOperationException | LinkageError e) {
      log.warn("Skipping vector fg_mapping records because runtime hudi-common does not provide the required API");
      return ((HoodieSparkEngineContext) engineContext).getJavaSparkContext()
          .parallelize(Collections.emptyList(), 1);
    }
  }

  @SuppressWarnings("unchecked")
  private static HoodieRecord createPostingRecordCompat(Row row,
                                                        RaBitQEncoder encoder,
                                                        boolean assumeNormalized,
                                                        String generationId,
                                                        Map<Integer, Integer> clusterShardCounts,
                                                        long lastUpdatedTs,
                                                        String indexName) {
    try {
      Method method = HoodieMetadataPayload.class.getMethod(
          "createVectorIndexPostingRecord",
          String.class, String.class, int.class, int.class, String.class, String.class, byte[].class, Float.class, long.class, String.class);
      org.apache.spark.ml.linalg.Vector features = row.getAs(4);
      org.apache.hudi.common.index.vector.VectorQuantizer.QuantizedVector quantizedVector =
          encoder.encode(toFloatArray(features));
      Float scalar = assumeNormalized ? null : quantizedVector.scalar;
      int clusterId = ((Number) row.get(1)).intValue();
      int shardCount = clusterShardCounts.getOrDefault(clusterId, 1);
      int shardId = computeShardId(row.getString(0), shardCount);
      return (HoodieRecord) method.invoke(null,
          generationId,
          row.getString(0),
          clusterId,
          shardId,
          row.isNullAt(2) ? null : FSUtils.getFileIdFromFileName(row.getString(2)),
          row.isNullAt(3) ? null : row.getString(3),
          quantizedVector.code,
          scalar,
          lastUpdatedTs,
          indexName);
    } catch (ReflectiveOperationException ex) {
      throw new HoodieMetadataException("Failed to create vector posting record reflectively", ex);
    }
  }

  @SuppressWarnings("unchecked")
  private static HoodieRecord createFgMappingRecordCompat(Row row, long lastUpdatedTs, String indexName) {
    try {
      Method method = HoodieMetadataPayload.class.getMethod(
          "createVectorIndexFgMappingRecord",
          int.class, String.class, java.util.Collection.class, long.class, long.class, String.class);
      return (HoodieRecord) method.invoke(null,
          ((Number) row.get(0)).intValue(),
          row.isNullAt(1) ? "" : row.getString(1),
          row.getList(2),
          ((Number) row.get(3)).longValue(),
          lastUpdatedTs,
          indexName);
    } catch (ReflectiveOperationException ex) {
      throw new HoodieMetadataException("Failed to create vector fg_mapping record reflectively", ex);
    }
  }

  private static float[] toFloatArray(org.apache.spark.ml.linalg.Vector vector) {
    double[] source = vector.toArray();
    float[] converted = new float[source.length];
    for (int i = 0; i < source.length; i++) {
      converted[i] = (float) source[i];
    }
    return converted;
  }

  private static int computeShardCount(long clusterPopulation, int targetRowsPerShard, int maxShardsPerCluster) {
    if (clusterPopulation <= 0) {
      return 1;
    }
    long computed = (clusterPopulation + targetRowsPerShard - 1L) / targetRowsPerShard;
    computed = Math.max(1L, computed);
    computed = Math.min(computed, (long) maxShardsPerCluster);
    return (int) computed;
  }

  private static int computeShardId(String recordKey, int shardCount) {
    return Math.floorMod(recordKey.hashCode(), Math.max(1, shardCount));
  }

  @SuppressWarnings("unchecked")
  private static double[] toNumericArray(Object rawValue,
                                         int expectedDimension,
                                         HoodieSchema.Vector.VectorElementType elementType) {
    List<?> values;
    if (rawValue instanceof List) {
      values = (List<?>) rawValue;
    } else if (rawValue instanceof scala.collection.Seq) {
      values = scala.collection.JavaConverters.seqAsJavaList((scala.collection.Seq<Object>) rawValue);
    } else if (rawValue instanceof float[] && elementType == HoodieSchema.Vector.VectorElementType.FLOAT) {
      float[] source = (float[]) rawValue;
      ValidationUtils.checkState(source.length == expectedDimension,
          String.format("Expected VECTOR(%s) but found %s elements", expectedDimension, source.length));
      double[] converted = new double[source.length];
      for (int i = 0; i < source.length; i++) {
        converted[i] = source[i];
      }
      return converted;
    } else if (rawValue instanceof double[]) {
      double[] source = (double[]) rawValue;
      ValidationUtils.checkState(source.length == expectedDimension,
          String.format("Expected VECTOR(%s) but found %s elements", expectedDimension, source.length));
      if (elementType == HoodieSchema.Vector.VectorElementType.DOUBLE || elementType == HoodieSchema.Vector.VectorElementType.FLOAT) {
        return source;
      }
      throw new HoodieMetadataException("Expected INT8 vector values but received double[]");
    } else if (rawValue instanceof byte[]) {
      byte[] source = (byte[]) rawValue;
      int expectedSize = expectedDimension * elementType.getElementSize();
      ValidationUtils.checkState(source.length == expectedSize,
          String.format("Expected VECTOR(%s) backing size %s but found %s bytes",
              expectedDimension, expectedSize, source.length));
      ByteBuffer buffer = ByteBuffer.wrap(source).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
      double[] converted = new double[expectedDimension];
      switch (elementType) {
        case FLOAT:
          for (int i = 0; i < expectedDimension; i++) {
            converted[i] = buffer.getFloat();
          }
          return converted;
        case DOUBLE:
          for (int i = 0; i < expectedDimension; i++) {
            converted[i] = buffer.getDouble();
          }
          return converted;
        case INT8:
          for (int i = 0; i < expectedDimension; i++) {
            converted[i] = buffer.get();
          }
          return converted;
        default:
          throw new HoodieMetadataException("Unsupported vector element type: " + elementType);
      }
    } else if (rawValue instanceof Object[]) {
      values = Arrays.asList((Object[]) rawValue);
    } else {
      throw new HoodieMetadataException("Unsupported Spark vector value type: " + rawValue.getClass().getName());
    }

    ValidationUtils.checkState(values.size() == expectedDimension,
        String.format("Expected VECTOR(%s) but found %s elements", expectedDimension, values.size()));
    double[] converted = new double[values.size()];
    for (int i = 0; i < values.size(); i++) {
      Object value = values.get(i);
      ValidationUtils.checkState(value instanceof Number, "Vector element must be numeric: " + value);
      converted[i] = ((Number) value).doubleValue();
    }
    return converted;
  }

  protected SparkRDDMetadataWriteClient getSparkWriteClient(Option<BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>>> writeClientOpt) {
    return ((SparkRDDMetadataWriteClient) writeClientOpt.orElse(getWriteClient()));
  }

  @Override
  public BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> initializeWriteClient() {
    return new SparkRDDMetadataWriteClient(engineContext, metadataWriteConfig, Option.empty());
  }

  @Override
  protected EngineType getEngineType() {
    return EngineType.SPARK;
  }

  @Override
  protected void updateColumnsToIndexWithColStats(List<String> columnsToIndex) {
    new HoodieSparkIndexClient(dataWriteConfig, engineContext).createOrUpdateColumnStatsIndexDefinition(dataMetaClient, columnsToIndex);
  }
}
