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
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieUnMergedLogRecordScanner;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.functional.HoodieFunctionalIndex;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieCommonConfig.MAX_DFS_STREAM_BUFFER_SIZE;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;
import static org.apache.hudi.common.util.ConfigUtils.getReaderConfigs;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createBloomFilterMetadataRecord;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createColumnStatsRecords;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;

/**
 * Utility methods for writing metadata for functional index.
 */
public class SparkMetadataWriterUtils {

  public static List<HoodieRecord> getFunctionalIndexRecordsUsingColumnStats(HoodieTableMetaClient metaClient,
                                                                             Schema readerSchema,
                                                                             String filePath,
                                                                             Long fileSize,
                                                                             String partition,
                                                                             HoodieFunctionalIndex<Column, Column> functionalIndex,
                                                                             String columnToIndex,
                                                                             SQLContext sqlContext) {
    List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList = new ArrayList<>();
    buildColumnRangeMetadata(metaClient, readerSchema, functionalIndex, columnToIndex, sqlContext, columnRangeMetadataList, fileSize, new StoragePath(filePath),
        FSUtils.isBaseFile(new StoragePath(filePath.substring(filePath.lastIndexOf("/") + 1))));
    return createColumnStatsRecords(partition, columnRangeMetadataList, false, functionalIndex.getIndexName(), COLUMN_STATS.getRecordType()).collect(Collectors.toList());
  }

  public static List<HoodieRecord> getFunctionalIndexRecordsUsingBloomFilter(HoodieTableMetaClient metaClient,
                                                                             Schema readerSchema,
                                                                             String filePath,
                                                                             String partition,
                                                                             HoodieFunctionalIndex<Column, Column> functionalIndex,
                                                                             String columnToIndex,
                                                                             SQLContext sqlContext,
                                                                             HoodieWriteConfig metadataWriteConfig,
                                                                             String instantTime) {
    List<HoodieRecord> bloomFilterMetadataList = new ArrayList<>();
    // log file handling
    buildBloomFilterMetadata(metaClient, readerSchema, functionalIndex, columnToIndex, sqlContext, bloomFilterMetadataList, new StoragePath(filePath), metadataWriteConfig, partition,
        instantTime, FSUtils.isBaseFile(new StoragePath(filePath.substring(filePath.lastIndexOf("/") + 1))));
    return bloomFilterMetadataList;
  }

  private static void buildColumnRangeMetadata(HoodieTableMetaClient metaClient,
                                               Schema readerSchema,
                                               HoodieFunctionalIndex<Column, Column> functionalIndex,
                                               String columnToIndex,
                                               SQLContext sqlContext,
                                               List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList,
                                               long fileSize,
                                               StoragePath filePath,
                                               boolean isBaseFile) {
    Dataset<Row> fileDf = readRecordsAsRow(new StoragePath[] {filePath}, sqlContext, metaClient, readerSchema, isBaseFile);
    Column indexedColumn = functionalIndex.apply(Collections.singletonList(fileDf.col(columnToIndex)));
    fileDf = fileDf.withColumn(columnToIndex, indexedColumn);
    HoodieColumnRangeMetadata<Comparable> columnRangeMetadata = computeColumnRangeMetadata(fileDf, columnToIndex, filePath.toString(), fileSize);
    columnRangeMetadataList.add(columnRangeMetadata);
  }

  private static void buildBloomFilterMetadata(HoodieTableMetaClient metaClient,
                                               Schema readerSchema,
                                               HoodieFunctionalIndex<Column, Column> functionalIndex,
                                               String columnToIndex,
                                               SQLContext sqlContext,
                                               List<HoodieRecord> bloomFilterMetadataList,
                                               StoragePath filePath,
                                               HoodieWriteConfig writeConfig,
                                               String partitionName,
                                               String instantTime,
                                               boolean isBaseFile) {
    Dataset<Row> fileDf = readRecordsAsRow(new StoragePath[] {filePath}, sqlContext, metaClient, readerSchema, isBaseFile);
    Column indexedColumn = functionalIndex.apply(Collections.singletonList(fileDf.col(columnToIndex)));
    fileDf = fileDf.withColumn(columnToIndex, indexedColumn);
    BloomFilter bloomFilter = HoodieFileWriterFactory.createBloomFilter(writeConfig);
    fileDf.foreach(row -> {
      byte[] key = row.getAs(columnToIndex).toString().getBytes();
      bloomFilter.add(key);
    });
    ByteBuffer bloomByteBuffer = ByteBuffer.wrap(getUTF8Bytes(bloomFilter.serializeToString()));
    bloomFilterMetadataList.add(createBloomFilterMetadataRecord(partitionName, filePath.toString(), instantTime, writeConfig.getBloomFilterType(), bloomByteBuffer, false));
  }

  private static Dataset<Row> readRecordsAsRow(StoragePath[] paths, SQLContext sqlContext,
                                               HoodieTableMetaClient metaClient, Schema schema,
                                               boolean isBaseFile) {
    List<HoodieRecord> records = isBaseFile ? getBaseFileRecords(new HoodieBaseFile(paths[0].toString()), metaClient, schema)
        : getUnmergedLogFileRecords(Arrays.stream(paths).map(StoragePath::toString).collect(Collectors.toList()), metaClient, schema);
    return dropMetaFields(toDataset(records, schema, sqlContext));
  }

  private static List<HoodieRecord> getUnmergedLogFileRecords(List<String> logFilePaths, HoodieTableMetaClient metaClient, Schema readerSchema) {
    List<HoodieRecord> records = new ArrayList<>();
    HoodieUnMergedLogRecordScanner scanner = HoodieUnMergedLogRecordScanner.newBuilder()
        .withStorage(metaClient.getStorage())
        .withBasePath(metaClient.getBasePath())
        .withLogFilePaths(logFilePaths)
        .withBufferSize(MAX_DFS_STREAM_BUFFER_SIZE.defaultValue())
        .withLatestInstantTime(metaClient.getActiveTimeline().getCommitsTimeline().lastInstant().get().getTimestamp())
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
        HoodieRecordUtils.createRecordMerger(metaClient.getBasePath().toString(), EngineType.SPARK, Collections.emptyList(), metaClient.getTableConfig().getRecordMergerStrategy());
    try (HoodieFileReader baseFileReader = HoodieIOFactory.getIOFactory(metaClient.getStorage()).getReaderFactory(recordMerger.getRecordType())
        .getFileReader(getReaderConfigs(metaClient.getStorageConf()), baseFile.getStoragePath())) {
      baseFileReader.getRecordIterator(readerSchema).forEachRemaining((record) -> records.add((HoodieRecord) record));
      return records;
    } catch (IOException e) {
      throw new HoodieIOException("Error reading base file " + baseFile.getFileName(), e);
    }
  }

  private static Dataset<Row> toDataset(List<HoodieRecord> records, Schema schema, SQLContext sqlContext) {
    List<GenericRecord> avroRecords = records.stream()
        .map(r -> (GenericRecord) r.getData())
        .collect(Collectors.toList());
    if (avroRecords.isEmpty()) {
      return sqlContext.emptyDataFrame().toDF();
    }
    JavaSparkContext jsc = new JavaSparkContext(sqlContext.sparkContext());
    JavaRDD<GenericRecord> javaRDD = jsc.parallelize(avroRecords);
    return AvroConversionUtils.createDataFrame(javaRDD.rdd(), schema.toString(), sqlContext.sparkSession());
  }

  private static <T extends Comparable<T>> HoodieColumnRangeMetadata<Comparable> computeColumnRangeMetadata(Dataset<Row> rowDataset,
                                                                                                            String columnName,
                                                                                                            String filePath,
                                                                                                            long fileSize) {
    long totalSize = fileSize;
    // Get nullCount, minValue, and maxValue
    Dataset<Row> aggregated = rowDataset.agg(
        functions.count(functions.when(functions.col(columnName).isNull(), 1)).alias("nullCount"),
        functions.min(columnName).alias("minValue"),
        functions.max(columnName).alias("maxValue"),
        functions.count(columnName).alias("valueCount")
    );

    Row result = aggregated.collectAsList().get(0);
    long nullCount = result.getLong(0);
    @Nullable T minValue = (T) result.get(1);
    @Nullable T maxValue = (T) result.get(2);
    long valueCount = result.getLong(3);

    // Total uncompressed size is harder to get directly. This is just an approximation to maintain the order.
    long totalUncompressedSize = totalSize * 2;

    return HoodieColumnRangeMetadata.<Comparable>create(
        filePath,
        columnName,
        minValue,
        maxValue,
        nullCount,
        valueCount,
        totalSize,
        totalUncompressedSize
    );
  }

  private static Dataset<Row> dropMetaFields(Dataset<Row> df) {
    return df.select(
        Arrays.stream(df.columns())
            .filter(c -> !HOODIE_META_COLUMNS.contains(c))
            .map(df::col).toArray(Column[]::new));
  }
}
