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

import org.apache.hudi.HoodieSparkFunctionalIndex;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFunctionalIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.index.functional.HoodieFunctionalIndex;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.client.utils.SparkMetadataWriterUtils.getFunctionalIndexRecordsUsingBloomFilter;
import static org.apache.hudi.client.utils.SparkMetadataWriterUtils.getFunctionalIndexRecordsUsingColumnStats;
import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;

public class SparkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter<JavaRDD<HoodieRecord>> {

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
    super(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp);
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
      this.metrics = Option.of(new HoodieMetadataMetrics(metadataWriteConfig.getMetricsConfig(), storageConf));
    } else {
      this.metrics = Option.empty();
    }
  }

  @Override
  protected void commit(String instantTime, Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap) {
    commitInternal(instantTime, partitionRecordsMap, false, Option.empty());
  }

  @Override
  protected JavaRDD<HoodieRecord> convertHoodieDataToEngineSpecificData(HoodieData<HoodieRecord> records) {
    return HoodieJavaRDD.getJavaRDD(records);
  }

  @Override
  protected void bulkCommit(
      String instantTime, MetadataPartitionType partitionType, HoodieData<HoodieRecord> records,
      int fileGroupCount) {
    SparkHoodieMetadataBulkInsertPartitioner partitioner = new SparkHoodieMetadataBulkInsertPartitioner(fileGroupCount);
    commitInternal(instantTime, Collections.singletonMap(partitionType, records), true, Option.of(partitioner));
  }

  @Override
  public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
    List<String> partitionsToDrop = partitions.stream().map(MetadataPartitionType::getPartitionPath).collect(Collectors.toList());
    LOG.info("Deleting Metadata Table partitions: " + partitionsToDrop);

    SparkRDDWriteClient writeClient = (SparkRDDWriteClient) getWriteClient();
    String actionType = CommitUtils.getCommitActionType(WriteOperationType.DELETE_PARTITION, HoodieTableType.MERGE_ON_READ);
    writeClient.startCommitWithTime(instantTime, actionType);
    writeClient.deletePartitions(partitionsToDrop, instantTime);
  }

  @Override
  protected HoodieData<HoodieRecord> getFunctionalIndexRecords(List<Pair<String, FileSlice>> partitionFileSlicePairs,
                                                               HoodieFunctionalIndexDefinition indexDefinition,
                                                               HoodieTableMetaClient metaClient, int parallelism,
                                                               Schema readerSchema, StorageConfiguration<?> storageConf) {
    HoodieFunctionalIndex<Column, Column> functionalIndex = new HoodieSparkFunctionalIndex(
        indexDefinition.getIndexName(),
        indexDefinition.getIndexFunction(),
        indexDefinition.getSourceFields(),
        indexDefinition.getIndexOptions());
    HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext) engineContext;
    if (indexDefinition.getSourceFields().isEmpty()) {
      // In case there are no columns to index, bail
      return sparkEngineContext.emptyHoodieData();
    }

    // NOTE: We are assuming that the index expression is operating on a single column
    //       HUDI-6994 will address this.
    String columnToIndex = indexDefinition.getSourceFields().get(0);
    SQLContext sqlContext = sparkEngineContext.getSqlContext();
    String basePath = metaClient.getBasePathV2().toString();
    for (Pair<String, FileSlice> pair : partitionFileSlicePairs) {
      String partition = pair.getKey();
      FileSlice fileSlice = pair.getValue();
      // For functional index using column_stats
      if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_COLUMN_STATS)) {
        return getFunctionalIndexRecordsUsingColumnStats(
            metaClient,
            parallelism,
            readerSchema,
            fileSlice,
            basePath,
            partition,
            functionalIndex,
            columnToIndex,
            sqlContext,
            sparkEngineContext);
      }
      // For functional index using bloom_filters
      if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_BLOOM_FILTERS)) {
        return getFunctionalIndexRecordsUsingBloomFilter(
            metaClient,
            parallelism,
            readerSchema,
            fileSlice,
            basePath,
            partition,
            functionalIndex,
            columnToIndex,
            sqlContext,
            sparkEngineContext,
            metadataWriteConfig);
      }
    }
    return HoodieJavaRDD.of(Collections.emptyList(), sparkEngineContext, parallelism);
  }

  @Override
  protected HoodieTable getHoodieTable(HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient) {
    return HoodieSparkTable.create(writeConfig, engineContext, metaClient);
  }

  @Override
  public BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, ?> initializeWriteClient() {
    return new SparkRDDWriteClient(engineContext, metadataWriteConfig, true);
  }
}
