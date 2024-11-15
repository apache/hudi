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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.SparkMetadataWriterUtils;
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
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.client.utils.SparkMetadataWriterUtils.readRecordsAsRows;
import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;

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
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp,
                                                 boolean shortLivedWriteClient) {
    return new SparkHoodieBackedTableMetadataWriter(
        conf, writeConfig, EAGER, context, inflightInstantTimestamp, shortLivedWriteClient);
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new SparkHoodieBackedTableMetadataWriter(
        conf, writeConfig, failedWritesCleaningPolicy, context, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp,
                                                 boolean shortLivedWriteClient) {
    return new SparkHoodieBackedTableMetadataWriter(
        conf, writeConfig, failedWritesCleaningPolicy, context, inflightInstantTimestamp, shortLivedWriteClient);
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
    this(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp, true);
  }

  SparkHoodieBackedTableMetadataWriter(StorageConfiguration<?> hadoopConf,
                                       HoodieWriteConfig writeConfig,
                                       HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                       HoodieEngineContext engineContext,
                                       Option<String> inflightInstantTimestamp,
                                       boolean shortLivedWriteClient) {
    super(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp, shortLivedWriteClient);
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
  protected HoodieData<HoodieRecord> repartitionByMDTFileSlice(HoodieData<HoodieRecord> records, int numPartitions) {
    return HoodieJavaRDD.of(HoodieJavaRDD.getJavaRDD(records).mapToPair(new PairFunction<HoodieRecord, Pair<String, String>, HoodieRecord>() {

      @Override
      public Tuple2<Pair<String, String>, HoodieRecord> call(HoodieRecord record) throws Exception {
        return new Tuple2<>(Pair.of(record.getPartitionPath(), record.getCurrentLocation().getFileId()), record);
      }
    }).partitionBy(new Partitioner() {
      @Override
      public int numPartitions() {
        return numPartitions;
      }

      @Override
      public int getPartition(Object key) {
        Pair<String, String> entry = (Pair<String, String>) key;
        return mapPartitionKeyToSparkPartition(entry.getKey().concat(entry.getValue()), numPartitions);
      }
    }).values());
  }

  @Override
  public JavaRDD<WriteStatus> writeToMDT(Pair<List<Pair<String, String>>, HoodieData<HoodieRecord>> mdtRecordsHoodieData, String instantTime, boolean initialCall) {
    JavaRDD<HoodieRecord> mdtRecords = HoodieJavaRDD.getJavaRDD(mdtRecordsHoodieData.getValue());

    if (initialCall) {
      preWrite(instantTime);
    }
    engineContext.setJobStatus(this.getClass().getSimpleName(), String.format("Upserting at %s into metadata table %s", instantTime, metadataWriteConfig.getTableName()));
    JavaRDD<WriteStatus> mdtWriteStatuses = getWriteClient().upsertPreppedPartialRecords(mdtRecords, instantTime, initialCall, true,
        mdtRecordsHoodieData.getKey());

    // todo update metrics.
    return mdtWriteStatuses;
  }

  /**
   * Map a record key to a file group in partition of interest.
   * <p>
   * Note: For hashing, the algorithm is same as String.hashCode() but is being defined here as hashCode()
   * implementation is not guaranteed by the JVM to be consistent across JVM versions and implementations.
   *
   * @return An integer hash of the given string
   */
  public static int mapPartitionKeyToSparkPartition(String partitionKey, int numPartitions) {
    int h = 0;
    for (int i = 0; i < partitionKey.length(); ++i) {
      h = 31 * h + partitionKey.charAt(i);
    }

    return Math.abs(Math.abs(h) % numPartitions);
  }

  @Override
  protected void writeAndCommitBulkInsert(BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> writeClient, String instantTime, JavaRDD<HoodieRecord> preppedRecordInputs,
                                          Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    JavaRDD<WriteStatus> writeStatusJavaRDD = writeClient.bulkInsertPreppedRecords(preppedRecordInputs, instantTime, bulkInsertPartitioner);
    writeClient.commit(instantTime, writeStatusJavaRDD);
  }

  @Override
  protected void writeAndCommitUpsert(BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> writeClient, String instantTime, JavaRDD<HoodieRecord> preppedRecordInputs) {
    JavaRDD<WriteStatus> writeStatusJavaRDD = writeClient.upsertPreppedRecords(preppedRecordInputs, instantTime);
    writeClient.commit(instantTime, writeStatusJavaRDD);
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
  protected HoodieData<HoodieRecord> getFunctionalIndexRecords(List<Pair<String, Pair<String, Long>>> partitionFilePathAndSizeTriplet,
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

    // Read records and append functional index metadata to every row
    HoodieData<Row> rowData = sparkEngineContext.parallelize(partitionFilePathAndSizeTriplet, parallelism)
        .flatMap((SerializableFunction<Pair<String, Pair<String, Long>>, Iterator<Row>>) entry -> {
          String partition = entry.getKey();
          Pair<String, Long> filePathSizePair = entry.getValue();
          String filePath = filePathSizePair.getKey();
          long fileSize = filePathSizePair.getValue();
          List<Row> rowsForFilePath = readRecordsAsRows(new StoragePath[] {new StoragePath(filePath)}, sqlContext, metaClient, readerSchema, dataWriteConfig,
              FSUtils.isBaseFile(new StoragePath(filePath.substring(filePath.lastIndexOf("/") + 1))));
          List<Row> rowsWithIndexMetadata = SparkMetadataWriterUtils.getRowsWithFunctionalIndexMetadata(rowsForFilePath, partition, filePath, fileSize);
          return rowsWithIndexMetadata.iterator();
        });

    // Generate dataset with functional index metadata
    StructType structType = AvroConversionUtils.convertAvroSchemaToStructType(readerSchema)
        .add(StructField.apply(HoodieFunctionalIndex.HOODIE_FUNCTIONAL_INDEX_PARTITION, DataTypes.StringType, false, Metadata.empty()))
        .add(StructField.apply(HoodieFunctionalIndex.HOODIE_FUNCTIONAL_INDEX_FILE_PATH, DataTypes.StringType, false, Metadata.empty()))
        .add(StructField.apply(HoodieFunctionalIndex.HOODIE_FUNCTIONAL_INDEX_FILE_SIZE, DataTypes.LongType, false, Metadata.empty()));
    Dataset<Row> rowDataset = sparkEngineContext.getSqlContext().createDataFrame(HoodieJavaRDD.getJavaRDD(rowData).rdd(), structType);

    // Apply functional index and generate the column to index
    HoodieFunctionalIndex<Column, Column> functionalIndex =
        new HoodieSparkFunctionalIndex(indexDefinition.getIndexName(), indexDefinition.getIndexFunction(), indexDefinition.getSourceFields(), indexDefinition.getIndexOptions());
    Column indexedColumn = functionalIndex.apply(Collections.singletonList(rowDataset.col(columnToIndex)));
    rowDataset = rowDataset.withColumn(columnToIndex, indexedColumn);

    // Generate functional index records
    if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_COLUMN_STATS)) {
      return SparkMetadataWriterUtils.getFunctionalIndexRecordsUsingColumnStats(rowDataset, functionalIndex, columnToIndex);
    } else if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_BLOOM_FILTERS)) {
      return SparkMetadataWriterUtils.getFunctionalIndexRecordsUsingBloomFilter(rowDataset, columnToIndex, metadataWriteConfig, instantTime);
    } else {
      throw new UnsupportedOperationException(indexDefinition.getIndexType() + " is not yet supported");
    }
  }

  protected MetadataIndexGenerator getMetadataIndexGenerator() {
    return new SparkMetadataIndexGenerator();
  }

  @Override
  protected HoodieTable getTable(HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient) {
    return HoodieSparkTable.create(writeConfig, engineContext, metaClient);
  }

  @Override
  public BaseHoodieWriteClient<?, JavaRDD<HoodieRecord>, ?, JavaRDD<WriteStatus>> initializeWriteClient() {
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
      HoodieRecord<HoodieMetadataPayload> siRecord = HoodieMetadataPayload.createSecondaryIndexRecord(key, value, indexDefinition.getIndexName(), true);
      deletedRecords.add(siRecord);
    });

    return HoodieJavaRDD.of(deletedRecords, sparkEngineContext, 1);
  }
}
