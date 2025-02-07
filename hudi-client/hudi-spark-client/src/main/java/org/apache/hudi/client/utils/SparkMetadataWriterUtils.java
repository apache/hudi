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
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieUnMergedLogRecordScanner;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.expression.HoodieExpressionIndex;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.JavaScalaConverters;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Function1;

import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.common.config.HoodieCommonConfig.MAX_DFS_STREAM_BUFFER_SIZE;
import static org.apache.hudi.common.util.ConfigUtils.getReaderConfigs;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createBloomFilterMetadataRecord;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createColumnStatsRecords;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getFileSystemView;
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

  public static List<Row> getRowsWithExpressionIndexMetadata(List<Row> rowsForFilePath, String partition, String filePath, long fileSize) {
    return rowsForFilePath.stream().map(row -> {
      scala.collection.immutable.Seq<Object> indexMetadata = JavaScalaConverters.convertJavaListToScalaList(Arrays.asList(partition, filePath, fileSize));
      Row expressionIndexRow = Row.fromSeq(indexMetadata);
      List<Row> rows = new ArrayList<>(2);
      rows.add(row);
      rows.add(expressionIndexRow);
      scala.collection.immutable.Seq<Row> rowSeq = JavaScalaConverters.convertJavaListToScalaList(rows);
      return Row.merge(rowSeq);
    }).collect(Collectors.toList());
  }

  @SuppressWarnings("checkstyle:LineLength")
  public static ExpressionIndexComputationMetadata getExpressionIndexRecordsUsingColumnStats(Dataset<Row> dataset, HoodieExpressionIndex<Column, Column> expressionIndex, String columnToIndex,
                                                                                             Option<Function<HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>>, HoodieData<HoodieRecord>>> partitionRecordsFunctionOpt) {
    // Aggregate col stats related data for the column to index
    Dataset<Row> columnRangeMetadataDataset = dataset
        .select(columnToIndex, SparkMetadataWriterUtils.getExpressionIndexColumnNames())
        .groupBy(SparkMetadataWriterUtils.getExpressionIndexColumns())
        .agg(functions.count(functions.when(functions.col(columnToIndex).isNull(), 1)).alias(COLUMN_STATS_FIELD_NULL_COUNT),
            functions.min(columnToIndex).alias(COLUMN_STATS_FIELD_MIN_VALUE),
            functions.max(columnToIndex).alias(COLUMN_STATS_FIELD_MAX_VALUE),
            functions.count(columnToIndex).alias(COLUMN_STATS_FIELD_VALUE_COUNT));
    // Generate column stat records using the aggregated data
    HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>> rangeMetadataHoodieJavaRDD = HoodieJavaRDD.of(columnRangeMetadataDataset.javaRDD())
        .flatMapToPair((SerializableFunction<Row, Iterator<? extends Pair<String, HoodieColumnRangeMetadata<Comparable>>>>)
            row -> {
              int baseAggregatePosition = SparkMetadataWriterUtils.getExpressionIndexColumnNames().length;
              long nullCount = row.getLong(baseAggregatePosition);
              Comparable minValue = (Comparable) row.get(baseAggregatePosition + 1);
              Comparable maxValue = (Comparable) row.get(baseAggregatePosition + 2);
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
                  totalUncompressedSize
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

  public static ExpressionIndexComputationMetadata getExpressionIndexRecordsUsingBloomFilter(Dataset<Row> dataset, String columnToIndex,
                                                                                             HoodieWriteConfig metadataWriteConfig, String instantTime, String indexName) {
    // Group data using expression index metadata and then create bloom filter on the group
    Dataset<HoodieRecord> bloomFilterRecords = dataset.select(columnToIndex, SparkMetadataWriterUtils.getExpressionIndexColumnNames())
        // row.get(1) refers to partition path value and row.get(2) refers to file name.
        .groupByKey((MapFunction<Row, Pair>) row -> Pair.of(row.getString(1), row.getString(2)), Encoders.kryo(Pair.class))
        .flatMapGroups((FlatMapGroupsFunction<Pair, Row, HoodieRecord>) ((pair, iterator) -> {
          String partition = pair.getLeft().toString();
          String relativeFilePath = pair.getRight().toString();
          String fileName = FSUtils.getFileName(relativeFilePath, partition);
          BloomFilter bloomFilter = HoodieFileWriterFactory.createBloomFilter(metadataWriteConfig);
          iterator.forEachRemaining(row -> {
            byte[] key = row.getAs(columnToIndex).toString().getBytes();
            bloomFilter.add(key);
          });
          ByteBuffer bloomByteBuffer = ByteBuffer.wrap(getUTF8Bytes(bloomFilter.serializeToString()));
          HoodieRecord bloomFilterRecord = createBloomFilterMetadataRecord(partition, fileName, instantTime, metadataWriteConfig.getBloomFilterType(), bloomByteBuffer, false, indexName);
          return Collections.singletonList(bloomFilterRecord).iterator();
        }), Encoders.kryo(HoodieRecord.class));
    return new ExpressionIndexComputationMetadata(HoodieJavaRDD.of(bloomFilterRecords.javaRDD()));
  }

  public static List<Row> readRecordsAsRows(StoragePath[] paths, SQLContext sqlContext,
                                            HoodieTableMetaClient metaClient, Schema schema,
                                            HoodieWriteConfig dataWriteConfig, boolean isBaseFile) {
    List<HoodieRecord> records = isBaseFile ? getBaseFileRecords(new HoodieBaseFile(paths[0].toString()), metaClient, schema)
        : getUnmergedLogFileRecords(Arrays.stream(paths).map(StoragePath::toString).collect(Collectors.toList()), metaClient, schema);
    return toRows(records, schema, dataWriteConfig, sqlContext, paths[0].toString());
  }

  private static List<HoodieRecord> getUnmergedLogFileRecords(List<String> logFilePaths, HoodieTableMetaClient metaClient, Schema readerSchema) {
    List<HoodieRecord> records = new ArrayList<>();
    HoodieUnMergedLogRecordScanner scanner = HoodieUnMergedLogRecordScanner.newBuilder()
        .withStorage(metaClient.getStorage())
        .withBasePath(metaClient.getBasePath())
        .withLogFilePaths(logFilePaths)
        .withBufferSize(MAX_DFS_STREAM_BUFFER_SIZE.defaultValue())
        .withLatestInstantTime(metaClient.getActiveTimeline().getCommitsTimeline().lastInstant().get().requestedTime())
        .withReaderSchema(readerSchema)
        .withTableMetaClient(metaClient)
        .withLogRecordScannerCallback(records::add)
        .build();
    scanner.scan(false);
    return records;
  }

  private static List<HoodieRecord> getBaseFileRecords(HoodieBaseFile baseFile, HoodieTableMetaClient metaClient, Schema readerSchema) {
    List<HoodieRecord> records = new ArrayList<>();
    HoodieRecordMerger recordMerger =
        HoodieRecordUtils.createRecordMerger(metaClient.getBasePath().toString(), EngineType.SPARK, Collections.emptyList(),
            metaClient.getTableConfig().getRecordMergeStrategyId());
    try (HoodieFileReader baseFileReader = HoodieIOFactory.getIOFactory(metaClient.getStorage()).getReaderFactory(recordMerger.getRecordType())
        .getFileReader(getReaderConfigs(metaClient.getStorageConf()), baseFile.getStoragePath())) {
      baseFileReader.getRecordIterator(readerSchema).forEachRemaining((record) -> records.add((HoodieRecord) record));
      return records;
    } catch (IOException e) {
      throw new HoodieIOException("Error reading base file " + baseFile.getFileName(), e);
    }
  }

  private static List<Row> toRows(List<HoodieRecord> records, Schema schema, HoodieWriteConfig dataWriteConfig, SQLContext sqlContext, String path) {
    StructType structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);
    Function1<GenericRecord, Row> converterToRow = AvroConversionUtils.createConverterToRow(schema, structType);
    List<Row> avroRecords = records.stream()
        .map(r -> {
          try {
            return (GenericRecord) (r.getData() instanceof GenericRecord ? r.getData()
                : ((HoodieRecordPayload) r.getData()).getInsertValue(schema, dataWriteConfig.getProps()).get());
          } catch (IOException e) {
            throw new HoodieIOException("Could not fetch record payload");
          }
        })
        .map(converterToRow::apply)
        .collect(Collectors.toList());
    return avroRecords;
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
   * @param metadataWriteConfig             Write config for the metadata table
   * @param partitionRecordsFunctionOpt     Function used to generate partition stat records for the EI. It takes the column range metadata generated for the provided partition files as input
   *                                        and uses those to generate the final partition stats
   * @return ExpressionIndexComputationMetadata containing both EI column stat records and partition stat records if partitionRecordsFunctionOpt is provided
   */
  @SuppressWarnings("checkstyle:LineLength")
  public static ExpressionIndexComputationMetadata getExprIndexRecords(List<Pair<String, Pair<String, Long>>> partitionFilePathAndSizeTriplet, HoodieIndexDefinition indexDefinition,
                                                                       HoodieTableMetaClient metaClient, int parallelism, Schema readerSchema, String instantTime,
                                                                       HoodieEngineContext engineContext, HoodieWriteConfig dataWriteConfig, HoodieWriteConfig metadataWriteConfig,
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
    SQLContext sqlContext = sparkEngineContext.getSqlContext();

    // Read records and append expression index metadata to every row
    HoodieData<Row> rowData = sparkEngineContext.parallelize(partitionFilePathAndSizeTriplet, parallelism)
        .flatMap((SerializableFunction<Pair<String, Pair<String, Long>>, Iterator<Row>>) entry ->
            getExpressionIndexRecordsIterator(metaClient, readerSchema, dataWriteConfig, entry, sqlContext));

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
      return getExpressionIndexRecordsUsingColumnStats(rowDataset, expressionIndex, columnToIndex, partitionRecordsFunctionOpt);
    } else if (indexDefinition.getIndexType().equalsIgnoreCase(PARTITION_NAME_BLOOM_FILTERS)) {
      return getExpressionIndexRecordsUsingBloomFilter(rowDataset, columnToIndex, metadataWriteConfig, instantTime, indexDefinition.getIndexName());
    } else {
      throw new UnsupportedOperationException(indexDefinition.getIndexType() + " is not yet supported");
    }
  }

  private static Iterator<Row> getExpressionIndexRecordsIterator(HoodieTableMetaClient metaClient, Schema readerSchema, HoodieWriteConfig dataWriteConfig,
                                                                 Pair<String, Pair<String, Long>> entry, SQLContext sqlContext) {
    String partition = entry.getKey();
    Pair<String, Long> filePathSizePair = entry.getValue();
    String filePath = filePathSizePair.getKey();
    String relativeFilePath = FSUtils.getRelativePartitionPath(metaClient.getBasePath(), new StoragePath(filePath));
    long fileSize = filePathSizePair.getValue();
    List<Row> rowsForFilePath = readRecordsAsRows(new StoragePath[] {new StoragePath(filePath)}, sqlContext, metaClient, readerSchema, dataWriteConfig,
        FSUtils.isBaseFile(new StoragePath(filePath.substring(filePath.lastIndexOf("/") + 1))));
    List<Row> rowsWithIndexMetadata = getRowsWithExpressionIndexMetadata(rowsForFilePath, partition, relativeFilePath, fileSize);
    return rowsWithIndexMetadata.iterator();
  }

  /**
   * Fetches column range metadata from the EI partition for all the partition files impacted by the commit. This would only take into account completed commits for the partitions
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
  public static HoodiePairData<String, List<HoodieColumnRangeMetadata<Comparable>>> getExpressionIndexPartitionStatUpdates(HoodieCommitMetadata commitMetadata, String indexPartition,
                                                                                                                           HoodieEngineContext engineContext, HoodieTableMetadata tableMetadata,
                                                                                                                           HoodieTableMetaClient dataMetaClient, HoodieMetadataConfig metadataConfig,
                                                                                                                           Option<HoodieRecord.HoodieRecordType> recordTypeOpt) {
    // In this function we iterate over all the partitions modified by the commit and fetch the latest files in those partitions
    // We fetch stored Expression index records for these latest files and return HoodiePairData of partition name and list of column range metadata of these files

    // Step 1: Validate that partition stats is supported for the column data type
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
      List<Pair<String,Schema>> columnsToIndexSchemaMap = columnsToIndex.stream()
          .map(columnToIndex -> Pair.of(columnToIndex, HoodieAvroUtils.getSchemaForField(tableSchema, columnToIndex).getValue().schema())).collect(
          Collectors.toList());
      // filter for supported types
      final List<String> validColumnsToIndex = columnsToIndexSchemaMap.stream()
          .filter(colSchemaPair -> HoodieTableMetadataUtil.SUPPORTED_META_FIELDS_PARTITION_STATS.contains(colSchemaPair.getKey())
              || HoodieTableMetadataUtil.isColumnTypeSupported(colSchemaPair.getValue(), recordTypeOpt))
          .map(entry -> entry.getKey())
          .collect(Collectors.toList());
      if (validColumnsToIndex.isEmpty()) {
        return engineContext.emptyHoodieData().mapToPair(o -> Pair.of("", new ArrayList<>()));
      }

      // Step 2: Compute expression index records for the modified partitions
      LOG.debug("Indexing following columns for partition stats index: {}", validColumnsToIndex);
      List<String> partitionPaths = new ArrayList<>(commitMetadata.getWritePartitionPaths());
      HoodieTableFileSystemView fileSystemView = getFileSystemView(dataMetaClient);
      int parallelism = Math.max(Math.min(partitionPaths.size(), metadataConfig.getPartitionStatsIndexParallelism()), 1);
      return engineContext.parallelize(partitionPaths, parallelism).mapToPair(partitionName -> {
        checkState(tableMetadata != null, "tableMetadata should not be null when scanning metadata table");
        // Collect Column Metadata for Each File part of active file system view of latest snapshot
        // Get all file names, including log files, in a set from the file slices
        Set<String> fileNames = HoodieTableMetadataUtil.getPartitionLatestFileSlicesIncludingInflight(dataMetaClient, Option.of(fileSystemView), partitionName).stream()
            .flatMap(fileSlice -> Stream.concat(
                Stream.of(fileSlice.getBaseFile().map(HoodieBaseFile::getFileName).orElse(null)),
                fileSlice.getLogFiles().map(HoodieLogFile::getFileName)))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
        // Fetch EI column stat records for above files
        List<HoodieColumnRangeMetadata<Comparable>> partitionColumnMetadata =
            tableMetadata.getRecordsByKeyPrefixes(HoodieTableMetadataUtil.generateKeyPrefixes(validColumnsToIndex, partitionName), indexPartition, false)
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
