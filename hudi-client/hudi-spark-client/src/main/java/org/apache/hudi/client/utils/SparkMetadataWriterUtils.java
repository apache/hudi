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
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieUnMergedLogRecordScanner;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.functional.HoodieFunctionalIndex;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
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
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import scala.Function1;

import static org.apache.hudi.common.config.HoodieCommonConfig.MAX_DFS_STREAM_BUFFER_SIZE;
import static org.apache.hudi.common.util.ConfigUtils.getReaderConfigs;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createBloomFilterMetadataRecord;
import static org.apache.hudi.metadata.HoodieMetadataPayload.createColumnStatsRecords;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;

/**
 * Utility methods for writing metadata for functional index.
 */
public class SparkMetadataWriterUtils {

  public static Column[] getFunctionalIndexColumns() {
    return new Column[] {
        functions.col(HoodieFunctionalIndex.HOODIE_FUNCTIONAL_INDEX_PARTITION),
        functions.col(HoodieFunctionalIndex.HOODIE_FUNCTIONAL_INDEX_FILE_PATH),
        functions.col(HoodieFunctionalIndex.HOODIE_FUNCTIONAL_INDEX_FILE_SIZE)
    };
  }

  public static String[] getFunctionalIndexColumnNames() {
    return new String[] {
        HoodieFunctionalIndex.HOODIE_FUNCTIONAL_INDEX_PARTITION,
        HoodieFunctionalIndex.HOODIE_FUNCTIONAL_INDEX_FILE_PATH,
        HoodieFunctionalIndex.HOODIE_FUNCTIONAL_INDEX_FILE_SIZE
    };
  }

  @NotNull
  public static List<Row> getRowsWithFunctionalIndexMetadata(List<Row> rowsForFilePath, String partition, String filePath, long fileSize) {
    return rowsForFilePath.stream().map(row -> {
      scala.collection.immutable.Seq<Object> indexMetadata = JavaScalaConverters.convertJavaListToScalaList(Arrays.asList(partition, filePath, fileSize));
      Row functionalIndexRow = Row.fromSeq(indexMetadata);
      List<Row> rows = new ArrayList<>(2);
      rows.add(row);
      rows.add(functionalIndexRow);
      scala.collection.immutable.Seq<Row> rowSeq = JavaScalaConverters.convertJavaListToScalaList(rows);
      return Row.merge(rowSeq);
    }).collect(Collectors.toList());
  }

  public static HoodieData<HoodieRecord> getFunctionalIndexRecordsUsingColumnStats(Dataset<Row> dataset,
                                                                                   HoodieFunctionalIndex<Column, Column> functionalIndex,
                                                                                   String columnToIndex) {
    // Aggregate col stats related data for the column to index
    Dataset<Row> columnRangeMetadataDataset = dataset
        .select(columnToIndex, SparkMetadataWriterUtils.getFunctionalIndexColumnNames())
        .groupBy(SparkMetadataWriterUtils.getFunctionalIndexColumns())
        .agg(functions.count(functions.when(functions.col(columnToIndex).isNull(), 1)).alias("nullCount"),
            functions.min(columnToIndex).alias("minValue"),
            functions.max(columnToIndex).alias("maxValue"),
            functions.count(columnToIndex).alias("valueCount"));
    // Generate column stat records using the aggregated data
    return HoodieJavaRDD.of(columnRangeMetadataDataset.javaRDD()).flatMap((SerializableFunction<Row, Iterator<HoodieRecord>>)
        row -> {
          int baseAggregatePosition = SparkMetadataWriterUtils.getFunctionalIndexColumnNames().length;
          long nullCount = row.getLong(baseAggregatePosition);
          Comparable minValue = (Comparable) row.get(baseAggregatePosition + 1);
          Comparable maxValue = (Comparable) row.get(baseAggregatePosition + 2);
          long valueCount = row.getLong(baseAggregatePosition + 3);

          String partitionName = row.getString(0);
          String filePath = row.getString(1);
          long totalFileSize = row.getLong(2);
          // Total uncompressed size is harder to get directly. This is just an approximation to maintain the order.
          long totalUncompressedSize = totalFileSize * 2;

          HoodieColumnRangeMetadata<Comparable> rangeMetadata = HoodieColumnRangeMetadata.create(
              filePath,
              columnToIndex,
              minValue,
              maxValue,
              nullCount,
              valueCount,
              totalFileSize,
              totalUncompressedSize
          );
          return createColumnStatsRecords(partitionName, Collections.singletonList(rangeMetadata), false, functionalIndex.getIndexName(),
              COLUMN_STATS.getRecordType()).collect(Collectors.toList()).iterator();
        });
  }

  public static HoodieData<HoodieRecord> getFunctionalIndexRecordsUsingBloomFilter(Dataset<Row> dataset, String columnToIndex,
                                                                                   HoodieWriteConfig metadataWriteConfig, String instantTime) {
    // Group data using functional index metadata and then create bloom filter on the group
    Dataset<HoodieRecord> bloomFilterRecords = dataset.select(columnToIndex, SparkMetadataWriterUtils.getFunctionalIndexColumnNames())
        // row.get(0) refers to partition path value and row.get(1) refers to file name.
        .groupByKey((MapFunction<Row, Pair>) row -> Pair.of(row.getString(0), row.getString(1)), Encoders.kryo(Pair.class))
        .flatMapGroups((FlatMapGroupsFunction<Pair, Row, HoodieRecord>)  ((pair, iterator) -> {
          String partition = pair.getLeft().toString();
          String fileName = pair.getRight().toString();
          BloomFilter bloomFilter = HoodieFileWriterFactory.createBloomFilter(metadataWriteConfig);
          iterator.forEachRemaining(row -> {
            byte[] key = row.getAs(columnToIndex).toString().getBytes();
            bloomFilter.add(key);
          });
          ByteBuffer bloomByteBuffer = ByteBuffer.wrap(getUTF8Bytes(bloomFilter.serializeToString()));
          HoodieRecord bloomFilterRecord = createBloomFilterMetadataRecord(partition, fileName, instantTime, metadataWriteConfig.getBloomFilterType(), bloomByteBuffer, false);
          return Collections.singletonList(bloomFilterRecord).iterator();
        }), Encoders.kryo(HoodieRecord.class));
    return HoodieJavaRDD.of(bloomFilterRecords.javaRDD());
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
}
