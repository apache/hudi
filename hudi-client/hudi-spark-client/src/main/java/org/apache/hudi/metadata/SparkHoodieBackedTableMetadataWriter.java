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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkFunctionalIndex;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
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
import org.apache.hudi.index.functional.HoodieFunctionalIndex;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.client.utils.SparkMetadataWriterUtils.getFunctionalIndexRecordsUsingBloomFilter;
import static org.apache.hudi.client.utils.SparkMetadataWriterUtils.getFunctionalIndexRecordsUsingColumnStats;
import static org.apache.hudi.client.utils.SparkMetadataWriterUtils.readRecordsAsRows;
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
  protected void bulkCommit(
      String instantTime, String partitionName, HoodieData<HoodieRecord> records,
      int fileGroupCount) {
    SparkHoodieMetadataBulkInsertPartitioner partitioner = new SparkHoodieMetadataBulkInsertPartitioner(fileGroupCount);
    commitInternal(instantTime, Collections.singletonMap(partitionName, records), true, Option.of(partitioner));
  }

  @Override
  public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
    List<String> partitionsToDrop = partitions.stream().map(MetadataPartitionType::getPartitionPath).collect(Collectors.toList());
    LOG.info("Deleting Metadata Table partitions: {}", partitionsToDrop);

    SparkRDDWriteClient writeClient = (SparkRDDWriteClient) getWriteClient();
    String actionType = CommitUtils.getCommitActionType(WriteOperationType.DELETE_PARTITION, HoodieTableType.MERGE_ON_READ);
    writeClient.startCommitWithTime(instantTime, actionType);
    writeClient.deletePartitions(partitionsToDrop, instantTime);
  }

  @Override
  protected HoodieData<HoodieRecord> getFunctionalIndexRecords(List<Pair<String, Pair<String, Long>>> partitionFilePathPairs,
                                                               HoodieIndexDefinition indexDefinition,
                                                               HoodieTableMetaClient metaClient, int parallelism,
                                                               Schema readerSchema, StorageConfiguration<?> storageConf,
                                                               String instantTime) {
    HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext) engineContext;
    if (indexDefinition.getSourceFields().isEmpty()) {
      // In case there are no columns to index, bail
      return sparkEngineContext.emptyHoodieData();
    }

    // NOTE: We are assuming that the index expression is operating on a single column
    //       HUDI-6994 will address this.
    String columnToIndex = indexDefinition.getSourceFields().get(0);
    SQLContext sqlContext = sparkEngineContext.getSqlContext();
    /*ForkJoinPool customThreadPool = new ForkJoinPool(parallelism);
    List<HoodieRecord> allRecords = customThreadPool.submit(() ->
        partitionFilePathPairs.parallelStream()
            .flatMap(entry -> {
              HoodieFunctionalIndex<Column, Column> functionalIndex =
                  new HoodieSparkFunctionalIndex(indexDefinition.getIndexName(), indexDefinition.getIndexFunction(),
                      indexDefinition.getSourceFields(), indexDefinition.getIndexOptions());
              String partition = entry.getKey();
              Pair<String, Long> filePathSizePair = entry.getValue();
              List<HoodieRecord> recordsForPartition = Collections.emptyList();

              if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_COLUMN_STATS)) {
                recordsForPartition = getFunctionalIndexRecordsUsingColumnStats(metaClient, readerSchema, filePathSizePair.getKey(),
                    filePathSizePair.getValue(), partition,
                    functionalIndex, columnToIndex, sqlContext);
              } else if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_BLOOM_FILTERS)) {
                recordsForPartition = getFunctionalIndexRecordsUsingBloomFilter(metaClient, readerSchema, filePathSizePair.getKey(),
                    partition, functionalIndex, columnToIndex,
                    sqlContext, metadataWriteConfig, instantTime);
              }
              return recordsForPartition.stream();
            })
            .collect(Collectors.toList())).join();
    customThreadPool.shutdown();
    return HoodieJavaRDD.of(allRecords, sparkEngineContext, parallelism);*/

    HoodieData<Row> rowData = sparkEngineContext.parallelize(partitionFilePathPairs, parallelism)
        .flatMap((SerializableFunction<Pair<String, Pair<String, Long>>, Iterator<Row>>) entry -> {
          String partition = entry.getKey();
          Pair<String, Long> filePathSizePair = entry.getValue();
          String filePath = filePathSizePair.getKey();
          List<Row> rowsForFilePath = readRecordsAsRows(new StoragePath[] {new StoragePath(filePath)}, sqlContext, metaClient, readerSchema,
              FSUtils.isBaseFile(new StoragePath(filePath.substring(filePath.lastIndexOf("/") + 1))));
          return rowsForFilePath.iterator();
        });

    Dataset<Row> rowDataset = sparkEngineContext.getSqlContext().createDataFrame(HoodieJavaRDD.getJavaRDD(rowData).rdd(), AvroConversionUtils.convertAvroSchemaToStructType(readerSchema));
    // rowDataset.schema()
    // StructType(
    //   StructField(_hoodie_commit_time,StringType,true),
    //   StructField(_hoodie_commit_seqno,StringType,true),
    //   StructField(_hoodie_record_key,StringType,true),
    //   StructField(_hoodie_partition_path,StringType,true),
    //   StructField(_hoodie_file_name,StringType,true),
    //   StructField(ts,LongType,true))


    /*
    rowDataset.show()
    +-------------------+--------------------+------------------+----------------------+--------------------+----+
    |_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name|  ts|
    +-------------------+--------------------+------------------+----------------------+--------------------+----+
    |  20241014190257965|20241014190257965...|                 1|               ts=1000|8f4cdb11-a3b4-4ec...|1000|
    |  20241014190322300|20241014190322300...|                 2|               ts=1001|a6025154-c6d3-49f...|1001|
    |  20241014190326221|20241014190326221...|                 3|               ts=1002|cf434900-9108-460...|1002|
    +-------------------+--------------------+------------------+----------------------+--------------------+----+

     */
    HoodieFunctionalIndex<Column, Column> functionalIndex2 =
        new HoodieSparkFunctionalIndex(indexDefinition.getIndexName(), indexDefinition.getIndexFunction(), indexDefinition.getSourceFields(), indexDefinition.getIndexOptions());
    Column indexedColumn = functionalIndex2.apply(Collections.singletonList(rowDataset.col(columnToIndex)));
    rowDataset = rowDataset.withColumn(columnToIndex, indexedColumn);
    /*
    rowDataset.show()
    +-------------------+---------------------+------------------+----------------------+-------------------------------------------------------------------------+----------+
    |_hoodie_commit_time|_hoodie_commit_seqno |_hoodie_record_key|_hoodie_partition_path|_hoodie_file_name                                                        |ts        |
    +-------------------+---------------------+------------------+----------------------+-------------------------------------------------------------------------+----------+
    |20241014190257965  |20241014190257965_0_0|1                 |ts=1000               |8f4cdb11-a3b4-4ec4-bb05-f77668f486e4-0_0-27-65_20241014190257965.parquet |1970-01-01|
    |20241014190322300  |20241014190322300_0_0|2                 |ts=1001               |a6025154-c6d3-49f6-ae88-b3c7f0617ef2-0_0-62-111_20241014190322300.parquet|1970-01-01|
    |20241014190326221  |20241014190326221_0_0|3                 |ts=1002               |cf434900-9108-460e-b120-aad545533358-0_0-97-157_20241014190326221.parquet|1970-01-01|
    +-------------------+---------------------+------------------+----------------------+-------------------------------------------------------------------------+----------+
     */
    return sparkEngineContext.parallelize(partitionFilePathPairs, parallelism)
        .flatMap((SerializableFunction<Pair<String, Pair<String, Long>>, Iterator<HoodieRecord>>) entry -> {
          HoodieFunctionalIndex<Column, Column> functionalIndex =
              new HoodieSparkFunctionalIndex(indexDefinition.getIndexName(), indexDefinition.getIndexFunction(), indexDefinition.getSourceFields(), indexDefinition.getIndexOptions());
          String partition = entry.getKey();
          Pair<String, Long> filePathSizePair = entry.getValue();
          List<HoodieRecord> recordsForPartition = Collections.emptyList();
          if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_COLUMN_STATS)) {
            recordsForPartition = getFunctionalIndexRecordsUsingColumnStats(metaClient, readerSchema, filePathSizePair.getKey(), filePathSizePair.getValue(), partition,
                functionalIndex, columnToIndex, sqlContext);
          } else if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_BLOOM_FILTERS)) {
            recordsForPartition = getFunctionalIndexRecordsUsingBloomFilter(metaClient, readerSchema, filePathSizePair.getKey(), partition,
                functionalIndex, columnToIndex, sqlContext, metadataWriteConfig, instantTime);
          }
          return recordsForPartition.iterator();
        });
  }

  @Override
  protected HoodieTable getTable(HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient) {
    return HoodieSparkTable.create(writeConfig, engineContext, metaClient);
  }

  @Override
  public BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, ?> initializeWriteClient() {
    return new SparkRDDWriteClient(engineContext, metadataWriteConfig, Option.empty());
  }

  @Override
  protected EngineType getEngineType() {
    return EngineType.SPARK;
  }

  @Override
  public HoodieData<HoodieRecord> getDeletedSecondaryRecordMapping(HoodieEngineContext engineContext, Map<String, String> recordKeySecondaryKeyMap, HoodieIndexDefinition indexDefinition) {
    HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext) engineContext;
    if (recordKeySecondaryKeyMap.isEmpty()) {
      return sparkEngineContext.emptyHoodieData();
    }

    List<HoodieRecord> deletedRecords = new ArrayList<>();
    recordKeySecondaryKeyMap.forEach((key, value) -> {
      HoodieRecord<HoodieMetadataPayload> siRecord = HoodieMetadataPayload.createSecondaryIndex(key, value, indexDefinition.getIndexName(), true);
      deletedRecords.add(siRecord);
    });

    return HoodieJavaRDD.of(deletedRecords, sparkEngineContext, 1);
  }
}
