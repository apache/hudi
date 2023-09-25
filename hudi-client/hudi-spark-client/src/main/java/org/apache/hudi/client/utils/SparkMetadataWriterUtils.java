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
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.functional.HoodieFunctionalIndex;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.metadata.HoodieMetadataPayload;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.sources.BaseRelation;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

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

  public static void buildColumnRangeMetadata(HoodieTableMetaClient metaClient, Schema readerSchema,
                                              HoodieFunctionalIndex<Column, Column> functionalIndex, String columnToIndex, SQLContext sqlContext,
                                              List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList, long fileSize, Path filePath) {
    Dataset<Row> fileDf = readRecordsAsRow(new Path[] {filePath}, sqlContext, metaClient, readerSchema);
    Column indexedColumn = functionalIndex.apply(Arrays.asList(fileDf.col(columnToIndex)));
    fileDf = fileDf.withColumn(columnToIndex, indexedColumn);
    HoodieColumnRangeMetadata<Comparable> columnRangeMetadata = computeColumnRangeMetadata(fileDf, columnToIndex, filePath.toString(), fileSize);
    columnRangeMetadataList.add(columnRangeMetadata);
  }

  public static void buildBloomFilterMetadata(HoodieTableMetaClient metaClient, Schema readerSchema,
                                              HoodieFunctionalIndex<Column, Column> functionalIndex, String columnToIndex,
                                              SQLContext sqlContext, List<HoodieRecord> bloomFilterMetadataList,
                                              Path filePath, HoodieWriteConfig writeConfig, String partitionName, String instantTime) {
    Dataset<Row> fileDf = readRecordsAsRow(new Path[] {filePath}, sqlContext, metaClient, readerSchema);
    Column indexedColumn = functionalIndex.apply(Arrays.asList(fileDf.col(columnToIndex)));
    fileDf = fileDf.withColumn(columnToIndex, indexedColumn);
    BloomFilter bloomFilter = HoodieFileWriterFactory.createBloomFilter(writeConfig);
    fileDf.foreach(row -> {
      byte[] key = row.getAs(columnToIndex).toString().getBytes();
      bloomFilter.add(key);
    });
    ByteBuffer bloomByteBuffer = ByteBuffer.wrap(getUTF8Bytes(bloomFilter.serializeToString()));
    HoodieRecord<HoodieMetadataPayload> bloomFilterMetadataRecord = HoodieMetadataPayload.createBloomFilterMetadataRecord(
        partitionName, filePath.toString(), instantTime, writeConfig.getBloomFilterType(), bloomByteBuffer, false);
    bloomFilterMetadataList.add(bloomFilterMetadataRecord);
  }

  public static Dataset<Row> readRecordsAsRow(Path[] paths, SQLContext sqlContext, HoodieTableMetaClient metaClient, Schema schema) {
    String readPathString = String.join(",", Arrays.stream(paths).map(Path::toString).toArray(String[]::new));
    HashMap<String, String> params = new HashMap<>();
    params.put(QUERY_TYPE_CONFIG, QUERY_TYPE_SNAPSHOT);
    params.put(READ_PATHS_CONFIG, readPathString);
    // Building HoodieFileIndex needs this param to decide query path
    params.put(GLOB_PATHS_CONFIG, readPathString);
    // Let Hudi relations to fetch the schema from the table itself
    BaseRelation relation = SparkAdapterSupport$.MODULE$.sparkAdapter()
        .createRelation(sqlContext, metaClient, schema, paths, params);

    return sqlContext.baseRelationToDataFrame(relation);
  }

  public static <T extends Comparable<T>> HoodieColumnRangeMetadata<Comparable> computeColumnRangeMetadata(Dataset<Row> baseFileDf,
                                                                                                           String columnName,
                                                                                                           String filePath,
                                                                                                           long fileSize) {
    long totalSize = fileSize;
    // Get nullCount, minValue, and maxValue
    Dataset<Row> aggregated = baseFileDf.agg(
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
}
