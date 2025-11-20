/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client.utils;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.SparkRowSerDe;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.expression.HoodieExpressionIndex;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.stats.SparkValueMetadataUtils;
import org.apache.hudi.stats.ValueMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.JavaScalaConverters;

import org.apache.avro.Schema;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.collection.immutable.Seq;

import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.BLOOM_FILTER_CONFIG_MAPPING;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createBloomFilterMetadataRecord;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createColumnStatsRecords;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;

/**
 * Utility methods for writing metadata for expression index.
 */
public class SparkMetadataWriterUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SparkMetadataWriterUtils.class);

  public static Column[] getExpressionIndexColumns() {
    return new Column[] {
        functions.col(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION),
        functions.col(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_RELATIVE_FILE_PATH),
        functions.col(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_FILE_SIZE)
    };
  }

  public static String[] getExpressionIndexColumnNames() {
    return new String[] {
        HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION,
        HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_RELATIVE_FILE_PATH,
        HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_FILE_SIZE
    };
  }

  public static ClosableIterator<Row> getRowsWithExpressionIndexMetadata(ClosableIterator<InternalRow> rowsForFilePath, SparkRowSerDe sparkRowSerDe, String partition, String filePath, long fileSize) {
    return new CloseableMappingIterator<>(rowsForFilePath, row -> {
      Seq<Object> indexMetadata = JavaScalaConverters.convertJavaListToScalaList(Arrays.asList(partition, filePath, fileSize));
      Row expressionIndexRow = Row.fromSeq(indexMetadata);
      List<Row> rows = new ArrayList<>(2);
      rows.add(sparkRowSerDe.deserializeRow(row));
      rows.add(expressionIndexRow);
      Seq<Row> rowSeq = JavaScalaConverters.convertJavaListToScalaList(rows);
      return Row.merge(rowSeq);
    });
  }

  @SuppressWarnings("checkstyle:LineLength")
  public static ExpressionIndexComputationMetadata getExpressionIndexRecordsUsingColumnStats(Dataset<Row> dataset, HoodieExpressionIndex<Column, Column> expressionIndex, String columnToIndex,
                                                                                             Option<Function<HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>>, HoodieData<HoodieRecord>>> partitionRecordsFunctionOpt,
                                                                                             HoodieIndexVersion indexVersion) {
    // Aggregate col stats related data for the column to index
    Dataset<Row> columnRangeMetadataDataset = dataset
        .select(columnToIndex, SparkMetadataWriterUtils.getExpressionIndexColumnNames())
        .groupBy(SparkMetadataWriterUtils.getExpressionIndexColumns())
        .agg(functions.count(functions.when(functions.col(columnToIndex).isNull(), 1)).alias(COLUMN_STATS_FIELD_NULL_COUNT),
            functions.min(columnToIndex).alias(COLUMN_STATS_FIELD_MIN_VALUE),
            functions.max(columnToIndex).alias(COLUMN_STATS_FIELD_MAX_VALUE),
            functions.count(columnToIndex).alias(COLUMN_STATS_FIELD_VALUE_COUNT));

    // Generate column stat records using the aggregated data
    ValueMetadata valueMetadata = getValueMetadataFromColumnRangeDatasetSchema(columnRangeMetadataDataset.schema(), indexVersion);
    HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>> rangeMetadataHoodieJavaRDD = HoodieJavaRDD.of(columnRangeMetadataDataset.javaRDD())
        .flatMapToPair((SerializableFunction<Row, Iterator<? extends Pair<String, HoodieColumnRangeMetadata<Comparable>>>>)
            row -> {
              int baseAggregatePosition = SparkMetadataWriterUtils.getExpressionIndexColumnNames().length;
              long nullCount = row.getLong(baseAggregatePosition);
              Comparable minValue = SparkValueMetadataUtils.convertSparkToJava(valueMetadata, row.get(baseAggregatePosition + 1));
              Comparable maxValue = SparkValueMetadataUtils.convertSparkToJava(valueMetadata, row.get(baseAggregatePosition + 2));
              long valueCount = row.getLong(baseAggregatePosition + 3);

              String partitionName = row.getString(0);
              String relativeFilePath = row.getString(1);
              long totalFileSize = row.getLong(2);
              // Total uncompressed size is harder to get directly. This is just an approximation to maintain the order.
              long totalUncompressedSize = totalFileSize * 2;

              HoodieColumnRangeMetadata<Comparable> rangeMetadata = HoodieColumnRangeMetadata.create(
                  relativeFilePath,
                  columnToIndex,
                  minValue,
                  maxValue,
                  nullCount,
                  valueCount,
                  totalFileSize,
                  totalUncompressedSize,
                  valueMetadata
              );
              return Collections.singletonList(Pair.of(partitionName, rangeMetadata)).iterator();
            });

    if (partitionRecordsFunctionOpt.isPresent()) {
      // TODO: HUDI-8848: Allow configurable storage level while computing expression index update
      rangeMetadataHoodieJavaRDD.persist("MEMORY_AND_DISK_SER");
    }
    HoodieData<HoodieRecord> colStatRecords = rangeMetadataHoodieJavaRDD.map(pair ->
            createColumnStatsRecords(pair.getKey(), Collections.singletonList(pair.getValue()), false, expressionIndex.getIndexName(),
                COLUMN_STATS.getRecordType()).collect(Collectors.toList()))
        .flatMap(records -> records.iterator());
    Option<HoodieData<HoodieRecord>> partitionStatRecordsOpt = Option.empty();
    if (partitionRecordsFunctionOpt.isPresent()) {
      partitionStatRecordsOpt = Option.of(partitionRecordsFunctionOpt.get().apply(rangeMetadataHoodieJavaRDD));
      rangeMetadataHoodieJavaRDD.unpersist();
    }
    return partitionRecordsFunctionOpt.isPresent()
        ? new ExpressionIndexComputationMetadata(colStatRecords, partitionStatRecordsOpt)
        : new ExpressionIndexComputationMetadata(colStatRecords);
  }

  private static ValueMetadata getValueMetadataFromColumnRangeDatasetSchema(StructType datasetSchema, HoodieIndexVersion indexVersion) {
    int baseAggregatePosition = SparkMetadataWriterUtils.getExpressionIndexColumnNames().length;
    DataType minDataType = datasetSchema.apply(baseAggregatePosition + 1).dataType();
    DataType maxDataType = datasetSchema.apply(baseAggregatePosition + 2).dataType();
    if (minDataType != maxDataType) {
      throw new HoodieException(String.format("Column stats data types do not match for min (%s) and max (%s)", minDataType, maxDataType));
    }
    return SparkValueMetadataUtils.getValueMetadata(minDataType, indexVersion);
  }

  public static ExpressionIndexComputationMetadata getExpressionIndexRecordsUsingBloomFilter(
      Dataset<Row> dataset, String columnToIndex, HoodieStorageConfig storageConfig, String instantTime,
      HoodieIndexDefinition indexDefinition) {
    String indexName = indexDefinition.getIndexName();
    setBloomFilterProps(storageConfig, indexDefinition.getIndexOptions());

    // Group data using expression index metadata and then create bloom filter on the group
    Dataset<HoodieRecord> bloomFilterRecords = dataset.select(columnToIndex, SparkMetadataWriterUtils.getExpressionIndexColumnNames())
        // row.get(1) refers to partition path value and row.get(2) refers to file name.
        .groupByKey((MapFunction<Row, Pair>) row -> Pair.of(row.getString(1), row.getString(2)), Encoders.kryo(Pair.class))
        .flatMapGroups((FlatMapGroupsFunction<Pair, Row, HoodieRecord>) ((pair, iterator) -> {
          String partition = pair.getLeft().toString();
          String relativeFilePath = pair.getRight().toString();
          String fileName = FSUtils.getFileName(relativeFilePath, partition);
          BloomFilter bloomFilter = HoodieFileWriterFactory.createBloomFilter(storageConfig);
          iterator.forEachRemaining(row -> {
            byte[] key = row.getAs(columnToIndex).toString().getBytes();
            bloomFilter.add(key);
          });
          ByteBuffer bloomByteBuffer = ByteBuffer.wrap(getUTF8Bytes(bloomFilter.serializeToString()));
          HoodieRecord bloomFilterRecord = createBloomFilterMetadataRecord(partition, fileName, instantTime, storageConfig.getBloomFilterType(), bloomByteBuffer, false, indexName);
          return Collections.singletonList(bloomFilterRecord).iterator();
        }), Encoders.kryo(HoodieRecord.class));
    return new ExpressionIndexComputationMetadata(HoodieJavaRDD.of(bloomFilterRecords.javaRDD()));
  }

  private static void setBloomFilterProps(HoodieStorageConfig storageConfig, Map<String, String> indexOptions) {
    BLOOM_FILTER_CONFIG_MAPPING.forEach((sourceKey, targetKey) -> {
      if (indexOptions.containsKey(sourceKey)) {
        storageConfig.getProps().setProperty(targetKey, indexOptions.get(sourceKey));
      }
    });
  }

  /**
   * Generates expression index records
   *
   * @param partitionFilePathAndSizeTriplet Triplet of file path, file size and partition name to which file belongs
   * @param indexDefinition                 Hoodie Index Definition for the expression index for which records need to be generated
   * @param metaClient                      Hoodie Table Meta Client
   * @param parallelism                     Parallelism to use for engine operations
   * @param readerSchema                    Schema of reader
   * @param instantTime                     Instant time
   * @param engineContext                   HoodieEngineContext
   * @param dataWriteConfig                 Write Config for the data table
   * @param partitionRecordsFunctionOpt     Function used to generate partition stat records for the EI. It takes the column range metadata generated for the provided partition files as input
   *                                        and uses those to generate the final partition stats
   * @return ExpressionIndexComputationMetadata containing both EI column stat records and partition stat records if partitionRecordsFunctionOpt is provided
   */
  public static ExpressionIndexComputationMetadata getExprIndexRecords(
      List<Pair<String, Pair<String, Long>>> partitionFilePathAndSizeTriplet, HoodieIndexDefinition indexDefinition,
      HoodieTableMetaClient metaClient, int parallelism, Schema tableSchema, Schema readerSchema, String instantTime,
      HoodieEngineContext engineContext, HoodieWriteConfig dataWriteConfig,
      Option<Function<HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>>, HoodieData<HoodieRecord>>> partitionRecordsFunctionOpt) {
    HoodieSparkEngineContext sparkEngineContext = (HoodieSparkEngineContext) engineContext;
    if (indexDefinition.getSourceFields().isEmpty()) {
      // In case there are no columns to index, bail
      return new ExpressionIndexComputationMetadata(sparkEngineContext.emptyHoodieData());
    }

    // NOTE: We are assuming that the index expression is operating on a single column
    //       HUDI-6994 will address this.
    ValidationUtils.checkArgument(indexDefinition.getSourceFields().size() == 1, "Only one source field is supported for expression index");
    String columnToIndex = indexDefinition.getSourceFields().get(0);

    ReaderContextFactory<InternalRow> readerContextFactory = engineContext.getReaderContextFactory(metaClient);
    // Read records and append expression index metadata to every row
    HoodieData<Row> rowData = sparkEngineContext.parallelize(partitionFilePathAndSizeTriplet, parallelism)
        .flatMap((SerializableFunction<Pair<String, Pair<String, Long>>, Iterator<Row>>) entry ->
            getExpressionIndexRecordsIterator(readerContextFactory.getContext(), metaClient, tableSchema, readerSchema, dataWriteConfig, entry));

    // Generate dataset with expression index metadata
    StructType structType = AvroConversionUtils.convertAvroSchemaToStructType(readerSchema)
        .add(StructField.apply(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION, DataTypes.StringType, false, Metadata.empty()))
        .add(StructField.apply(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_RELATIVE_FILE_PATH, DataTypes.StringType, false, Metadata.empty()))
        .add(StructField.apply(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_FILE_SIZE, DataTypes.LongType, false, Metadata.empty()));
    Dataset<Row> rowDataset = sparkEngineContext.getSqlContext().createDataFrame(HoodieJavaRDD.getJavaRDD(rowData).rdd(), structType);

    // Apply expression index and generate the column to index
    HoodieExpressionIndex<Column, Column> expressionIndex = new HoodieSparkExpressionIndex(indexDefinition);
    Column indexedColumn = expressionIndex.apply(Collections.singletonList(rowDataset.col(columnToIndex)));
    rowDataset = rowDataset.withColumn(columnToIndex, indexedColumn);

    // Generate expression index records
    if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_COLUMN_STATS)) {
      return getExpressionIndexRecordsUsingColumnStats(rowDataset, expressionIndex, columnToIndex, partitionRecordsFunctionOpt, indexDefinition.getVersion());
    } else if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_BLOOM_FILTERS)) {
      return getExpressionIndexRecordsUsingBloomFilter(
          rowDataset, columnToIndex, dataWriteConfig.getStorageConfig(), instantTime, indexDefinition);
    } else {
      throw new UnsupportedOperationException(indexDefinition.getIndexType() + " is not yet supported");
    }
  }

  private static Iterator<Row> getExpressionIndexRecordsIterator(HoodieReaderContext<InternalRow> readerContext, HoodieTableMetaClient metaClient,
                                                                 Schema tableSchema, Schema readerSchema, HoodieWriteConfig dataWriteConfig, Pair<String, Pair<String, Long>> entry) {
    String partition = entry.getKey();
    Pair<String, Long> filePathSizePair = entry.getValue();
    String filePath = filePathSizePair.getKey();
    String relativeFilePath = FSUtils.getRelativePartitionPath(metaClient.getBasePath(), new StoragePath(filePath));
    long fileSize = filePathSizePair.getValue();
    boolean isBaseFile = FSUtils.isBaseFile(new StoragePath(filePath.substring(filePath.lastIndexOf("/") + 1)));
    Stream<HoodieLogFile> logFileStream;
    Option<HoodieBaseFile> baseFileOption;
    if (isBaseFile) {
      baseFileOption = Option.of(new HoodieBaseFile(filePath));
      logFileStream = Stream.empty();
    } else {
      baseFileOption = Option.empty();
      logFileStream = Stream.of(new HoodieLogFile(filePath));
    }
    HoodieFileGroupReader<InternalRow> fileGroupReader = HoodieFileGroupReader.<InternalRow>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metaClient)
        .withDataSchema(tableSchema)
        .withRequestedSchema(readerSchema)
        .withProps(dataWriteConfig.getProps())
        .withLatestCommitTime(metaClient.getActiveTimeline().lastInstant().map(HoodieInstant::requestedTime).orElse(""))
        .withAllowInflightInstants(true)
        .withBaseFileOption(baseFileOption)
        .withLogFiles(logFileStream)
        .withPartitionPath(partition)
        .withEnableOptimizedLogBlockScan(dataWriteConfig.enableOptimizedLogBlocksScan())
        .build();
    try {
      ClosableIterator<InternalRow> rowsForFilePath = fileGroupReader.getClosableIterator();
      SparkRowSerDe sparkRowSerDe = HoodieSparkUtils.getCatalystRowSerDe(HoodieInternalRowUtils.getCachedSchema(readerSchema));
      return getRowsWithExpressionIndexMetadata(rowsForFilePath, sparkRowSerDe, partition, relativeFilePath, fileSize);
    } catch (IOException ex) {
      throw new HoodieIOException("Error reading file " + filePath, ex);
    }
  }

  /**
   * Fetches column range metadata from the EI partition for existing files excluding the files
   * impacted by the commit. This would only take into account completed commits for the partitions
   * since EI updates have not yet been committed.
   *
   * @param commitMetadata Hoodie commit metadata
   * @param indexPartition Partition name for the expression index
   * @param engineContext  Engine context
   * @param tableMetadata  Metadata table reader
   * @param dataMetaClient Data table meta client
   * @param metadataConfig Hoodie metadata config
   * @return HoodiePairData of partition name and list of column range metadata for the partitions
   */
  public static HoodiePairData<String, List<HoodieColumnRangeMetadata<Comparable>>> getExpressionIndexPartitionStatsForExistingFiles(
      HoodieCommitMetadata commitMetadata, String indexPartition,
      HoodieEngineContext engineContext, HoodieTableMetadata tableMetadata,
      HoodieTableMetaClient dataMetaClient, HoodieMetadataConfig metadataConfig,
      Option<HoodieRecord.HoodieRecordType> recordTypeOpt, String instantTime,
      HoodieWriteConfig dataWriteConfig) {
    // In this function we iterate over all the partitions modified by the commit and fetch the latest files in those partitions
    // We fetch stored Expression index records for these latest files and return HoodiePairData of partition name and list of column range metadata of these files

    // Step 1: Validate that partition stats is supported for the column data type
    HoodieIndexVersion indexVersion = HoodieTableMetadataUtil.existingIndexVersionOrDefault(indexPartition, dataMetaClient);
    HoodieIndexDefinition indexDefinition = HoodieTableMetadataUtil.getHoodieIndexDefinition(indexPartition, dataMetaClient);
    List<String> columnsToIndex = Collections.singletonList(indexDefinition.getSourceFields().get(0));
    try {
      Option<Schema> writerSchema =
          Option.ofNullable(commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY))
              .flatMap(writerSchemaStr ->
                  isNullOrEmpty(writerSchemaStr)
                      ? Option.empty()
                      : Option.of(new Schema.Parser().parse(writerSchemaStr)));
      HoodieTableConfig tableConfig = dataMetaClient.getTableConfig();
      Schema tableSchema = writerSchema.map(schema -> tableConfig.populateMetaFields() ? addMetadataFields(schema) : schema)
          .orElseThrow(() -> new IllegalStateException(String.format("Expected writer schema in commit metadata %s", commitMetadata)));
      List<Pair<String, Schema>> columnsToIndexSchemaMap = columnsToIndex.stream()
          .map(columnToIndex -> Pair.of(columnToIndex, HoodieAvroUtils.getSchemaForField(tableSchema, columnToIndex).getValue().schema()))
          .collect(Collectors.toList());
      // filter for supported types
      final List<String> validColumnsToIndex = columnsToIndexSchemaMap.stream()
          .filter(colSchemaPair -> HoodieTableMetadataUtil.SUPPORTED_META_FIELDS_PARTITION_STATS.contains(colSchemaPair.getKey())
              || HoodieTableMetadataUtil.isColumnTypeSupported(colSchemaPair.getValue(), recordTypeOpt, indexVersion))
          .map(entry -> entry.getKey())
          .collect(Collectors.toList());
      if (validColumnsToIndex.isEmpty()) {
        return engineContext.emptyHoodieData().mapToPair(o -> Pair.of("", new ArrayList<>()));
      }

      // Step 2: Compute expression index records for the modified partitions
      LOG.debug("Indexing following columns for partition stats index: {}", validColumnsToIndex);
      List<List<HoodieWriteStat>> partitionedWriteStats = new ArrayList<>(commitMetadata.getWriteStats().stream()
          .collect(Collectors.groupingBy(HoodieWriteStat::getPartitionPath))
          .values());

      Map<String, Set<String>> fileGroupIdsToReplaceMap = (commitMetadata instanceof HoodieReplaceCommitMetadata)
          ? ((HoodieReplaceCommitMetadata) commitMetadata).getPartitionToReplaceFileIds()
          .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new HashSet<>(e.getValue())))
          : Collections.emptyMap();

      String maxInstantTime = HoodieMetadataWriteUtils.getMaxInstantTime(dataMetaClient, instantTime);
      int parallelism = Math.max(Math.min(partitionedWriteStats.size(), metadataConfig.getPartitionStatsIndexParallelism()), 1);
      return engineContext.parallelize(partitionedWriteStats, parallelism).mapToPair(partitionedWriteStat -> {
        final String partitionName = partitionedWriteStat.get(0).getPartitionPath();
        checkState(tableMetadata != null, "tableMetadata should not be null when scanning metadata table");
        // Collect Column Metadata for Each File part of active file system view of latest snapshot
        // Get all file names, including log files, in a set from the file slices
        Set<String> fileNames = HoodieMetadataWriteUtils.getFilesToFetchColumnStats(partitionedWriteStat, dataMetaClient, tableMetadata, dataWriteConfig, partitionName, maxInstantTime, instantTime,
            fileGroupIdsToReplaceMap, validColumnsToIndex, indexVersion);
        // Fetch EI column stat records for above files
        List<HoodieColumnRangeMetadata<Comparable>> partitionColumnMetadata =
            tableMetadata.getRecordsByKeyPrefixes(
                    HoodieListData.lazy(HoodieTableMetadataUtil.generateColumnStatsKeys(validColumnsToIndex, partitionName)),
                    indexPartition, false)
                // schema and properties are ignored in getInsertValue, so simply pass as null
                .map(record -> record.getData().getInsertValue(null, null))
                .filter(Option::isPresent)
                .map(data -> ((HoodieMetadataRecord) data.get()).getColumnStatsMetadata())
                .filter(stats -> fileNames.contains(stats.getFileName()))
                .map(HoodieColumnRangeMetadata::fromColumnStats)
                .collectAsList();
        return Pair.of(partitionName, partitionColumnMetadata);
      });
    } catch (Exception e) {
      throw new HoodieException("Failed to generate column stats records for metadata table", e);
    }
  }
}
