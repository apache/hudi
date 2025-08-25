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
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieSparkIndexClient;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

public class SparkHoodieBackedTableMetadataWriterTableVersionSix extends HoodieBackedTableMetadataWriterTableVersionSix<JavaRDD<HoodieRecord>, JavaRDD<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkHoodieBackedTableMetadataWriter.class);

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new SparkHoodieBackedTableMetadataWriterTableVersionSix(
        conf, writeConfig, EAGER, context, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new SparkHoodieBackedTableMetadataWriterTableVersionSix(
        conf, writeConfig, failedWritesCleaningPolicy, context, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf, HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context) {
    return create(conf, writeConfig, context, Option.empty());
  }

  SparkHoodieBackedTableMetadataWriterTableVersionSix(StorageConfiguration<?> hadoopConf,
                                       HoodieWriteConfig writeConfig,
                                       HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                       HoodieEngineContext engineContext,
                                       Option<String> inflightInstantTimestamp) {
    super(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp);
  }

  @Override
  MetadataIndexGenerator initializeMetadataIndexGenerator() {
    throw new UnsupportedOperationException("Streaming writes are not supported for Spark table version six");
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
    throw new HoodieNotSupportedException("Unsupported flow for table version 6");
  }

  @Override
  protected void bulkInsertAndCommit(BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> writeClient, String instantTime, JavaRDD<HoodieRecord> preppedRecordInputs,
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
  protected void upsertAndCommit(BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> writeClient, String instantTime, JavaRDD<HoodieRecord> preppedRecordInputs,
                                 List<HoodieFileGroupId> fileGroupsIdsToUpdate) {
    JavaRDD<WriteStatus> writeStatusJavaRDD = writeClient.upsertPreppedRecords(preppedRecordInputs, instantTime);
    writeClient.commit(instantTime, writeStatusJavaRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
  }

  @Override
  protected void bulkCommit(
      String instantTime, String partitionPath, HoodieData<HoodieRecord> records,
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

  @Override
  protected HoodieData<HoodieRecord> getExpressionIndexRecords(List<Pair<String, Pair<String, Long>>> partitionFilePathAndSizeTriplet, HoodieIndexDefinition indexDefinition,
                                                               HoodieTableMetaClient metaClient, int parallelism, Schema tableSchema, Schema readerSchema, StorageConfiguration<?> storageConf,
                                                               String instantTime) {
    throw new HoodieNotSupportedException("Expression index not supported for Java metadata table writer yet.");
  }
}
