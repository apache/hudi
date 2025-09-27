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
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.index.HoodieSparkIndexClient;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getProjectedSchemaForExpressionIndex;

public class SparkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter<JavaRDD<HoodieRecord>, JavaRDD<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkHoodieBackedTableMetadataWriter.class);

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
    LOG.info("Deleting Metadata Table partitions: {}", partitionsToDrop);

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
          SparkMetadataWriterUtils.getExpressionIndexPartitionStatUpdates(commitMetadata, indexPartition, engineContext, getTableMetadata(), dataMetaClient, dataWriteConfig.getMetadataConfig(),
                  Option.of(dataWriteConfig.getRecordMerger().getRecordType()))
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
    Schema tableSchema = new TableSchemaResolver(dataMetaClient).getTableAvroSchema();
    Schema readerSchema = getProjectedSchemaForExpressionIndex(indexDefinition, dataMetaClient, tableSchema);
    // Step 2: Compute the expression index column stat and partition stat records for these newly created files
    // partitionRecordsFunctionOpt - Function used to generate partition stats. These stats are generated only for expression index created using column stats
    //
    // In the partitionRecordsFunctionOpt function we merge the expression index records from the new files created in the commit metadata
    // with the expression index records from the unmodified files to get the new partition stat records
    HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata expressionIndexComputationMetadata =
        SparkMetadataWriterUtils.getExprIndexRecords(partitionFilePathPairs, indexDefinition, dataMetaClient, parallelism, tableSchema, readerSchema, instantTime, engineContext, dataWriteConfig,
            partitionRecordsFunctionOpt);
    return expressionIndexComputationMetadata.getPartitionStatRecordsOption().isPresent()
        ? expressionIndexComputationMetadata.getExpressionIndexRecords().union(expressionIndexComputationMetadata.getPartitionStatRecordsOption().get())
        : expressionIndexComputationMetadata.getExpressionIndexRecords();
  }

  @Override
  protected HoodieData<HoodieRecord> getExpressionIndexRecords(List<Pair<String, Pair<String, Long>>> partitionFilePathAndSizeTriplet,
                                                               HoodieIndexDefinition indexDefinition,
                                                               HoodieTableMetaClient metaClient, int parallelism,
                                                               Schema tableSchema, Schema readerSchema, StorageConfiguration<?> storageConf,
                                                               String instantTime) {
    ExpressionIndexComputationMetadata expressionIndexComputationMetadata = SparkMetadataWriterUtils.getExprIndexRecords(partitionFilePathAndSizeTriplet, indexDefinition,
        metaClient, parallelism, tableSchema, readerSchema, instantTime, engineContext, dataWriteConfig,
        Option.of(rangeMetadata ->
            HoodieTableMetadataUtil.collectAndProcessExprIndexPartitionStatRecords(rangeMetadata, true, Option.of(indexDefinition.getIndexName()))));
    HoodieData<HoodieRecord> exprIndexRecords = expressionIndexComputationMetadata.getExpressionIndexRecords();
    if (indexDefinition.getIndexType().equals(PARTITION_NAME_COLUMN_STATS)) {
      exprIndexRecords = exprIndexRecords.union(expressionIndexComputationMetadata.getPartitionStatRecordsOption().get());
    }
    return exprIndexRecords;
  }

  protected MetadataIndexGenerator initializeMetadataIndexGenerator() {
    return new MetadataIndexGenerator();
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
