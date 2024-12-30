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
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex;
import org.apache.hudi.HoodieSparkIndexClient;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.SparkRDDWriteClient;
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
import org.apache.hudi.index.expression.HoodieExpressionIndex;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.client.utils.SparkMetadataWriterUtils.readRecordsAsRows;
import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getProjectedSchemaForExpressionIndex;

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
  protected HoodieData<HoodieRecord> getExpressionIndexUpdates(Option<HoodiePairData<String, List<List<HoodieColumnRangeMetadata<Comparable>>>>> partitionRangeMetadataPairOpt,
                                                               HoodieCommitMetadata commitMetadata, String indexPartition, String instantTime) throws Exception {
    HoodieIndexDefinition indexDefinition = getIndexDefinition(indexPartition);
//    RDD<HoodieColumnRangeMetadata<Comparable>> rdd = HoodieJavaPairRDD.getJavaPairRDD(partitionRangeMetadataPair).values().flatMap(list -> list.stream().flatMap(
//        Collection::stream).iterator()).rdd();
//
//    Encoder<HoodieColumnRangeMetadata<Comparable>> encoder = (Encoder<HoodieColumnRangeMetadata<Comparable>>) Encoders.kryo(rdd.first().getClass());
//    Dataset<HoodieColumnRangeMetadata<Comparable>> dataset = ((HoodieSparkEngineContext) engineContext).getSqlContext().createDataset(rdd, encoder);

    Option<HoodieData<HoodieRecord>> partitionStatsRDDOpt = Option.empty();
    if (partitionRangeMetadataPairOpt.isPresent()) {
      HoodieExpressionIndex<Column, Column> expressionIndex =
          new HoodieSparkExpressionIndex(indexDefinition.getIndexName(), indexDefinition.getIndexFunction(), indexDefinition.getSourceFields(), indexDefinition.getIndexOptions());
      ObjectMapper objectMapper = new ObjectMapper();
      JavaRDD<String> rangeMetadataJson = HoodieJavaPairRDD.getJavaPairRDD(partitionRangeMetadataPairOpt.get())
          .flatMapValues(list -> list.stream()
              .flatMap(Collection::stream)
              .iterator()
          ).map(rangeMetadata -> objectMapper.writeValueAsString(rangeMetadata));
//    JavaRDD<String> rangeMetadataJson = HoodieJavaPairRDD.getJavaPairRDD(partitionRangeMetadataPartitionPair).values().flatMap(list -> list.stream().flatMap(
//        Collection::stream).iterator()).map(rangeMetadata -> objectMapper.writeValueAsString(rangeMetadata));
      Dataset<Row> rowDataset = ((HoodieSparkEngineContext) engineContext).getSqlContext().read().json(rangeMetadataJson);
      rowDataset = rowDataset.filter(String.format("_2.columnName == '%s'", indexDefinition.getSourceFields().get(0)));
      Column minValueCol = expressionIndex.apply(Collections.singletonList(rowDataset.col("_2.minValue")));
      Column maxValueCol = expressionIndex.apply(Collections.singletonList(rowDataset.col("_2.maxValue")));
      Dataset<Row> transformedDataset = rowDataset.withColumn("transformedMinValue", minValueCol);
      transformedDataset = transformedDataset.withColumn("transformedMaxValue", maxValueCol);
      HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>> finalDataset = HoodieJavaRDD.of(transformedDataset.javaRDD()).mapToPair(row -> {
        Row rangeMetadataRow = row.getAs("_2");
        HoodieColumnRangeMetadata rangeMetadata = HoodieColumnRangeMetadata.create(rangeMetadataRow.getAs("filePath"),
            rangeMetadataRow.getAs("columnName"), row.getAs("transformedMinValue"), row.getAs("transformedMaxValue"),
            rangeMetadataRow.getAs("nullCount"), rangeMetadataRow.getAs("valueCount"), rangeMetadataRow.getAs("totalSize"),
            rangeMetadataRow.getAs("totalUncompressedSize"));
        String partitionName = row.getAs("_1");
        return Pair.of(partitionName, rangeMetadata);
      });
      partitionStatsRDDOpt = Option.of(HoodieTableMetadataUtil.convertMetadataToPartitionStatsRecords(finalDataset,
          dataMetaClient, Option.of(indexPartition)));
    }

//    HoodieData<List<HoodieColumnRangeMetadata<Comparable>>> transformedColumnRangeMetadatas =
//        partitionRangeMetadataPartitionPair.mapValues(rangeMetadatas ->
//            rangeMetadatas.stream().map(rangeMetadata -> {
//              HoodieExpressionIndex<Column, Column> expressionIndex =
//                  new HoodieSparkExpressionIndex(indexDefinition.getIndexName(), indexDefinition.getIndexFunction(), indexDefinition.getSourceFields(), indexDefinition.getIndexOptions());
//              Dataset<Row> dataset = ((HoodieSparkEngineContext) engineContext).getSqlContext().emptyDataFrame();
//              Column minValueCol = expressionIndex.apply(Collections.singletonList(functions.lit(rangeMetadata.getMinValue())));
//              Column maxValueCol = expressionIndex.apply(Collections.singletonList(functions.lit(rangeMetadata.getMaxValue())));
//              dataset.withColumn("minValue", minValueCol);
//              dataset.withColumn("maxValue", maxValueCol);
//              Row transformedRangeRow = dataset.collect()[0];
//              Comparable minValue = (Comparable) transformedRangeRow.get(0);
//              Comparable maxValue = (Comparable) transformedRangeRow.get(1);
//              return HoodieColumnRangeMetadata.create(rangeMetadata.getFilePath(), rangeMetadata.getColumnName(), minValue, maxValue, rangeMetadata.getNullCount(),
//                  rangeMetadata.getValueCount(), rangeMetadata.getTotalSize(), rangeMetadata.getTotalUncompressedSize());
//            }).collect(Collectors.toList()));
    List<Pair<String, Pair<String, Long>>> partitionFilePathPairs = new ArrayList<>();
    commitMetadata.getPartitionToWriteStats().forEach((dataPartition, writeStats) -> writeStats.forEach(writeStat -> partitionFilePathPairs.add(
        Pair.of(writeStat.getPartitionPath(), Pair.of(new StoragePath(dataMetaClient.getBasePath(), writeStat.getPath()).toString(), writeStat.getFileSizeInBytes())))));
    int parallelism = Math.min(partitionFilePathPairs.size(), dataWriteConfig.getMetadataConfig().getExpressionIndexParallelism());
    Schema readerSchema = getProjectedSchemaForExpressionIndex(indexDefinition, dataMetaClient);
    HoodieData<HoodieRecord> expressionIndexRecords = getExpressionIndexRecords(partitionFilePathPairs, indexDefinition, dataMetaClient, parallelism, readerSchema, storageConf, instantTime, false);
    return partitionStatsRDDOpt.isPresent() ? expressionIndexRecords.union(partitionStatsRDDOpt.get()) : expressionIndexRecords;
  }

  @Override
  protected HoodieData<HoodieRecord> getExpressionIndexRecords(List<Pair<String, Pair<String, Long>>> partitionFilePathAndSizeTriplet,
                                                               HoodieIndexDefinition indexDefinition,
                                                               HoodieTableMetaClient metaClient, int parallelism,
                                                               Schema readerSchema, StorageConfiguration<?> storageConf,
                                                               String instantTime, boolean shouldGeneratePartitionStatRecords) {
    HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext) engineContext;
    if (indexDefinition.getSourceFields().isEmpty()) {
      // In case there are no columns to index, bail
      return sparkEngineContext.emptyHoodieData();
    }

    // NOTE: We are assuming that the index expression is operating on a single column
    //       HUDI-6994 will address this.
    String columnToIndex = indexDefinition.getSourceFields().get(0);
    SQLContext sqlContext = sparkEngineContext.getSqlContext();

    // Read records and append expression index metadata to every row
    HoodieData<Row> rowData = sparkEngineContext.parallelize(partitionFilePathAndSizeTriplet, parallelism)
        .flatMap((SerializableFunction<Pair<String, Pair<String, Long>>, Iterator<Row>>) entry -> {
          String partition = entry.getKey();
          Pair<String, Long> filePathSizePair = entry.getValue();
          String filePath = filePathSizePair.getKey();
          String relativeFilePath = FSUtils.getRelativePartitionPath(metaClient.getBasePath(), new StoragePath(filePath));
          long fileSize = filePathSizePair.getValue();
          List<Row> rowsForFilePath = readRecordsAsRows(new StoragePath[] {new StoragePath(filePath)}, sqlContext, metaClient, readerSchema, dataWriteConfig,
              FSUtils.isBaseFile(new StoragePath(filePath.substring(filePath.lastIndexOf("/") + 1))));
          List<Row> rowsWithIndexMetadata = SparkMetadataWriterUtils.getRowsWithExpressionIndexMetadata(rowsForFilePath, partition, relativeFilePath, fileSize);
          return rowsWithIndexMetadata.iterator();
        });

    // Generate dataset with expression index metadata
    StructType structType = AvroConversionUtils.convertAvroSchemaToStructType(readerSchema)
        .add(StructField.apply(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION, DataTypes.StringType, false, Metadata.empty()))
        .add(StructField.apply(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_RELATIVE_FILE_PATH, DataTypes.StringType, false, Metadata.empty()))
        .add(StructField.apply(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_FILE_SIZE, DataTypes.LongType, false, Metadata.empty()));
    Dataset<Row> rowDataset = sparkEngineContext.getSqlContext().createDataFrame(HoodieJavaRDD.getJavaRDD(rowData).rdd(), structType);

    // Apply expression index and generate the column to index
    HoodieExpressionIndex<Column, Column> expressionIndex =
        new HoodieSparkExpressionIndex(indexDefinition.getIndexName(), indexDefinition.getIndexFunction(), indexDefinition.getSourceFields(), indexDefinition.getIndexOptions());
    Column indexedColumn = expressionIndex.apply(Collections.singletonList(rowDataset.col(columnToIndex)));
    rowDataset = rowDataset.withColumn(columnToIndex, indexedColumn);

    // Generate expression index records
    if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_COLUMN_STATS)) {
      return SparkMetadataWriterUtils.getExpressionIndexRecordsUsingColumnStats(rowDataset, expressionIndex, indexDefinition, columnToIndex, shouldGeneratePartitionStatRecords);
    } else if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_BLOOM_FILTERS)) {
      return SparkMetadataWriterUtils.getExpressionIndexRecordsUsingBloomFilter(rowDataset, columnToIndex, metadataWriteConfig, instantTime, indexDefinition.getIndexName());
    } else {
      throw new UnsupportedOperationException(indexDefinition.getIndexType() + " is not yet supported");
    }
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
  protected void updateColumnsToIndexWithColStats(List<String> columnsToIndex) {
    new HoodieSparkIndexClient(dataWriteConfig, engineContext).createOrUpdateColumnStatsIndexDefinition(dataMetaClient, columnsToIndex);
  }
}
