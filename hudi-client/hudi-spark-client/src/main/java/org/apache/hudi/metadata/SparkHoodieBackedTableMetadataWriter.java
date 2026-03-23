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
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.index.HoodieSparkIndexClient;
import org.apache.hudi.metadata.index.SparkExpressionIndexRecordGenerator;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.BulkInsertPartitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

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
    super(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext,
        new SparkExpressionIndexRecordGenerator(engineContext, writeConfig), inflightInstantTimestamp, streamingWrites);
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
  protected void commit(String instantTime, List<IndexPartitionAndRecords> partitionRecords) {
    commitInternal(instantTime, partitionRecords, false, Option.empty());
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
    commitInternal(instantTime, Collections.singletonList(IndexPartitionAndRecords.of(partitionPath, records)), true, Option.of(partitioner));
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
