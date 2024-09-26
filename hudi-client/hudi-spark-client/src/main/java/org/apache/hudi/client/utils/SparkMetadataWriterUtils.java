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

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.functional.HoodieFunctionalIndex;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.sources.BaseRelation;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createBloomFilterMetadataRecord;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createColumnStatsRecords;

/**
 * Utility methods for writing metadata for functional index.
 */
public class SparkMetadataWriterUtils {

  /**
   * Configs required to load records from paths as a dataframe
   */
  private static final String QUERY_TYPE_CONFIG = "hoodie.datasource.query.type";
  private static final String QUERY_TYPE_SNAPSHOT = "snapshot";
  private static final String READ_PATHS_CONFIG = "hoodie.datasource.read.paths";
  private static final String GLOB_PATHS_CONFIG = "glob.paths";

  public static List<HoodieRecord> getFunctionalIndexRecordsUsingColumnStats(HoodieTableMetaClient metaClient,
                                                                             Schema readerSchema,
                                                                             List<FileSlice> fileSlices,
                                                                             String partition,
                                                                             HoodieFunctionalIndex<Column, Column> functionalIndex,
                                                                             String columnToIndex,
                                                                             SQLContext sqlContext) {
    List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList = new ArrayList<>();
    for (FileSlice fileSlice : fileSlices) {
      if (fileSlice.getBaseFile().isPresent()) {
        HoodieBaseFile baseFile = fileSlice.getBaseFile().get();
        long fileSize = baseFile.getFileSize();
        buildColumnRangeMetadata(metaClient, readerSchema, functionalIndex, columnToIndex, sqlContext, columnRangeMetadataList, fileSize, baseFile.getStoragePath());
      }
      // Handle log files
      fileSlice.getLogFiles().forEach(logFile -> {
        long fileSize = logFile.getFileSize();
        buildColumnRangeMetadata(metaClient, readerSchema, functionalIndex, columnToIndex, sqlContext, columnRangeMetadataList, fileSize, logFile.getPath());
      });
    }
    return createColumnStatsRecords(partition, columnRangeMetadataList, false).collect(Collectors.toList());
  }

  public static List<HoodieRecord> getFunctionalIndexRecordsUsingBloomFilter(HoodieTableMetaClient metaClient,
                                                                             Schema readerSchema,
                                                                             List<FileSlice> fileSlices,
                                                                             String partition,
                                                                             HoodieFunctionalIndex<Column, Column> functionalIndex,
                                                                             String columnToIndex,
                                                                             SQLContext sqlContext,
                                                                             HoodieWriteConfig metadataWriteConfig) {
    List<HoodieRecord> bloomFilterMetadataList = new ArrayList<>();
    for (FileSlice fileSlice : fileSlices) {
      if (fileSlice.getBaseFile().isPresent()) {
        HoodieBaseFile baseFile = fileSlice.getBaseFile().get();
        buildBloomFilterMetadata(metaClient, readerSchema, functionalIndex, columnToIndex, sqlContext, bloomFilterMetadataList, baseFile.getStoragePath(), metadataWriteConfig, partition,
            baseFile.getCommitTime());
      }
      // Handle log files
      fileSlice.getLogFiles().forEach(
          logFile -> buildBloomFilterMetadata(metaClient, readerSchema, functionalIndex, columnToIndex, sqlContext, bloomFilterMetadataList, logFile.getPath(), metadataWriteConfig, partition,
              logFile.getDeltaCommitTime()));
    }
    return bloomFilterMetadataList;
  }

  private static void buildColumnRangeMetadata(HoodieTableMetaClient metaClient,
                                               Schema readerSchema,
                                               HoodieFunctionalIndex<Column, Column> functionalIndex,
                                               String columnToIndex,
                                               SQLContext sqlContext,
                                               List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList,
                                               long fileSize,
                                               StoragePath filePath) {
    Dataset<Row> fileDf = readRecordsAsRow(new StoragePath[] {filePath}, sqlContext, metaClient, readerSchema);
    Column indexedColumn = functionalIndex.apply(Arrays.asList(fileDf.col(columnToIndex)));
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
                                               String instantTime) {
    Dataset<Row> fileDf = readRecordsAsRow(new StoragePath[] {filePath}, sqlContext, metaClient, readerSchema);
    Column indexedColumn = functionalIndex.apply(Arrays.asList(fileDf.col(columnToIndex)));
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
                                               HoodieTableMetaClient metaClient, Schema schema) {
    String readPathString =
        String.join(",", Arrays.stream(paths).map(StoragePath::toString).toArray(String[]::new));
    String globPathString = String.join(",", Arrays.stream(paths).map(StoragePath::getParent).map(StoragePath::toString).distinct().toArray(String[]::new));
    HashMap<String, String> params = new HashMap<>();
    params.put(QUERY_TYPE_CONFIG, QUERY_TYPE_SNAPSHOT);
    params.put(READ_PATHS_CONFIG, readPathString);
    // Building HoodieFileIndex needs this param to decide query path
    params.put(GLOB_PATHS_CONFIG, globPathString);
    // Let Hudi relations to fetch the schema from the table itself
    BaseRelation relation = SparkAdapterSupport$.MODULE$.sparkAdapter()
        .createRelation(sqlContext, metaClient, schema, paths, params);

    return dropMetaFields(sqlContext.baseRelationToDataFrame(relation));
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
