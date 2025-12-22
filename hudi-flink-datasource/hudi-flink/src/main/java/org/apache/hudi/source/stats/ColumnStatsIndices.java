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

package org.apache.hudi.source.stats;

import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Tuple3;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.FlinkClientUtil;
import org.apache.hudi.util.RowDataProjection;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Utilities for abstracting away heavy-lifting of interactions with Metadata Table's Column Stats Index,
 * providing convenient interfaces to read it, transpose, etc.
 */
public class ColumnStatsIndices {
  private static final DataType METADATA_DATA_TYPE = getMetadataDataType();
  private static final DataType COL_STATS_DATA_TYPE = getColStatsDataType();
  private static final int[] COL_STATS_TARGET_POS = getColStatsTargetPos();

  // the column schema:
  // |- file_name: string
  // |- min_val: row
  // |- max_val: row
  // |- null_cnt: long
  // |- val_cnt: long
  // |- column_name: string
  private static final int ORD_FILE_NAME = 0;
  private static final int ORD_MIN_VAL = 1;
  private static final int ORD_MAX_VAL = 2;
  private static final int ORD_NULL_CNT = 3;
  private static final int ORD_VAL_CNT = 4;
  private static final int ORD_COL_NAME = 5;

  private ColumnStatsIndices() {
  }

  public static List<RowData> readColumnStatsIndex(String basePath, HoodieMetadataConfig metadataConfig, String[] targetColumns) {
    // NOTE: If specific columns have been provided, we can considerably trim down amount of data fetched
    //       by only fetching Column Stats Index records pertaining to the requested columns.
    //       Otherwise, we fall back to read whole Column Stats Index
    ValidationUtils.checkArgument(targetColumns.length > 0,
        "Column stats is only valid when push down filters have referenced columns");
    final List<RowData> metadataRows = readColumnStatsIndexByColumns(basePath, targetColumns, metadataConfig);
    return projectNestedColStatsColumns(metadataRows);
  }

  private static List<RowData> projectNestedColStatsColumns(List<RowData> rows) {
    int pos = HoodieMetadataRecord.SCHEMA$.getField(HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS).pos();
    RowDataProjection projection = RowDataProjection.instanceV2((RowType) COL_STATS_DATA_TYPE.getLogicalType(), COL_STATS_TARGET_POS);
    return rows.stream().parallel()
        .map(row -> {
          RowData columnStatsField = row.getRow(pos, 9);
          return projection.project(columnStatsField);
        }).collect(Collectors.toList());
  }

  /**
   * Transposes and converts the raw table format of the Column Stats Index representation,
   * where each row/record corresponds to individual (column, file) pair, into the table format
   * where each row corresponds to single file with statistic for individual columns collated
   * w/in such row:
   * <p>
   * Metadata Table Column Stats Index format:
   *
   * <pre>
   *  +---------------------------+------------+------------+------------+-------------+
   *  |        fileName           | columnName |  minValue  |  maxValue  |  num_nulls  |
   *  +---------------------------+------------+------------+------------+-------------+
   *  | one_base_file.parquet     |          A |          1 |         10 |           0 |
   *  | another_base_file.parquet |          A |        -10 |          0 |           5 |
   *  +---------------------------+------------+------------+------------+-------------+
   * </pre>
   * <p>
   * Returned table format
   *
   * <pre>
   *  +---------------------------+------------+------------+-------------+
   *  |          file             | A_minValue | A_maxValue | A_nullCount |
   *  +---------------------------+------------+------------+-------------+
   *  | one_base_file.parquet     |          1 |         10 |           0 |
   *  | another_base_file.parquet |        -10 |          0 |           5 |
   *  +---------------------------+------------+------------+-------------+
   * </pre>
   * <p>
   * NOTE: Column Stats Index might potentially contain statistics for many columns (if not all), while
   * query at hand might only be referencing a handful of those. As such, we collect all the
   * column references from the filtering expressions, and only transpose records corresponding to the
   * columns referenced in those
   *
   * @param colStats     RowData list bearing raw Column Stats Index table
   * @param queryColumns target columns to be included into the final table
   * @param tableSchema  schema of the source data table
   * @return reshaped table according to the format outlined above
   */
  public static Pair<List<RowData>, String[]> transposeColumnStatsIndex(List<RowData> colStats, String[] queryColumns, RowType tableSchema) {

    Map<String, LogicalType> tableFieldTypeMap = tableSchema.getFields().stream()
        .collect(Collectors.toMap(RowType.RowField::getName, RowType.RowField::getType));

    // NOTE: We have to collect list of indexed columns to make sure we properly align the rows
    //       w/in the transposed dataset: since some files might not have all the columns indexed
    //       either due to the Column Stats Index config changes, schema evolution, etc. we have
    //       to make sure that all the rows w/in transposed data-frame are properly padded (with null
    //       values) for such file-column combinations
    Set<String> indexedColumns = colStats.stream().map(row -> row.getString(ORD_COL_NAME)
        .toString()).collect(Collectors.toSet());

    // NOTE: We're sorting the columns to make sure final index schema matches layout
    //       of the transposed table
    TreeSet<String> sortedTargetColumns = Arrays.stream(queryColumns).sorted()
        .filter(indexedColumns::contains)
        .collect(Collectors.toCollection(TreeSet::new));

    final Map<LogicalType, AvroToRowDataConverters.AvroToRowDataConverter> converters = new ConcurrentHashMap<>();
    Map<StringData, List<RowData>> fileNameToRows = colStats.stream().parallel()
        .filter(row -> sortedTargetColumns.contains(row.getString(ORD_COL_NAME).toString()))
        .map(row -> {
          if (row.isNullAt(ORD_MIN_VAL) && row.isNullAt(ORD_MAX_VAL)) {
            // Corresponding row could be null in either of the 2 cases
            //    - Column contains only null values (in that case both min/max have to be nulls)
            //    - This is a stubbed Column Stats record (used as a tombstone)
            return row;
          } else {
            String colName = row.getString(ORD_COL_NAME).toString();
            LogicalType colType = tableFieldTypeMap.get(colName);
            return unpackMinMaxVal(row, colType, converters);
          }
        }).collect(Collectors.groupingBy(rowData -> rowData.getString(ORD_FILE_NAME)));

    return Pair.of(foldRowsByFiles(sortedTargetColumns, fileNameToRows), sortedTargetColumns.toArray(new String[0]));
  }

  private static List<RowData> foldRowsByFiles(
      TreeSet<String> sortedTargetColumns,
      Map<StringData, List<RowData>> fileNameToRows) {
    return fileNameToRows.values().stream().parallel().map(rows -> {
      // Rows seq is always non-empty (otherwise it won't be grouped into)
      StringData fileName = rows.get(0).getString(ORD_FILE_NAME);
      long valueCount = rows.get(0).getLong(ORD_VAL_CNT);

      // To properly align individual rows (corresponding to a file) w/in the transposed projection, we need
      // to align existing column-stats for individual file with the list of expected ones for the
      // whole transposed projection (a superset of all files)
      Map<String, RowData> columnRowsMap = rows.stream()
          .collect(Collectors.toMap(row -> row.getString(ORD_COL_NAME).toString(), row -> row));
      SortedMap<String, RowData> alignedColumnRowsMap = new TreeMap<>();
      sortedTargetColumns.forEach(col -> alignedColumnRowsMap.put(col, columnRowsMap.get(col)));

      List<Tuple3> columnStats = alignedColumnRowsMap.values().stream().map(row -> {
        if (row == null) {
          // NOTE: Since we're assuming missing column to essentially contain exclusively
          //       null values, we set null-count to be equal to value-count (this behavior is
          //       consistent with reading non-existent columns from Parquet)
          return Tuple3.of(null, null, valueCount);
        } else {
          GenericRowData gr = (GenericRowData) row;
          return Tuple3.of(gr.getField(ORD_MIN_VAL), gr.getField(ORD_MAX_VAL), gr.getField(ORD_NULL_CNT));
        }
      }).collect(Collectors.toList());
      GenericRowData foldedRow = new GenericRowData(2 + 3 * columnStats.size());
      foldedRow.setField(0, fileName);
      foldedRow.setField(1, valueCount);
      for (int i = 0; i < columnStats.size(); i++) {
        Tuple3 stats = columnStats.get(i);
        int startPos = 2 + 3 * i;
        foldedRow.setField(startPos, stats.f0);
        foldedRow.setField(startPos + 1, stats.f1);
        foldedRow.setField(startPos + 2, stats.f2);
      }
      return foldedRow;
    }).collect(Collectors.toList());
  }

  private static RowData unpackMinMaxVal(
      RowData row,
      LogicalType colType,
      Map<LogicalType, AvroToRowDataConverters.AvroToRowDataConverter> converters) {

    RowData minValueStruct = row.getRow(ORD_MIN_VAL, 1);
    RowData maxValueStruct = row.getRow(ORD_MAX_VAL, 1);

    checkState(minValueStruct != null && maxValueStruct != null,
        "Invalid Column Stats record: either both min/max have to be null, or both have to be non-null");

    Object minValue = tryUnpackNonNullVal(minValueStruct, colType, converters);
    Object maxValue = tryUnpackNonNullVal(maxValueStruct, colType, converters);

    // the column schema:
    // |- file_name: string
    // |- min_val: row
    // |- max_val: row
    // |- null_cnt: long
    // |- val_cnt: long
    // |- column_name: string

    GenericRowData unpackedRow = new GenericRowData(row.getArity());
    unpackedRow.setField(0, row.getString(0));
    unpackedRow.setField(1, minValue);
    unpackedRow.setField(2, maxValue);
    unpackedRow.setField(3, row.getLong(3));
    unpackedRow.setField(4, row.getLong(4));
    unpackedRow.setField(5, row.getString(5));

    return unpackedRow;
  }

  private static Object tryUnpackNonNullVal(
      RowData rowData,
      LogicalType colType,
      Map<LogicalType, AvroToRowDataConverters.AvroToRowDataConverter> converters) {
    for (int i = 0; i < rowData.getArity(); i++) {
      // row data converted from avro is definitely generic.
      Object nested = ((GenericRowData) rowData).getField(i);
      if (nested != null) {
        return doUnpack(nested, colType, converters);
      }
    }
    return null;
  }

  private static Object doUnpack(
      Object rawVal,
      LogicalType logicalType,
      Map<LogicalType, AvroToRowDataConverters.AvroToRowDataConverter> converters) {
    AvroToRowDataConverters.AvroToRowDataConverter converter =
        converters.computeIfAbsent(logicalType, k -> AvroToRowDataConverters.createConverter(logicalType, true));
    return converter.convert(rawVal);
  }

  private static List<RowData> readColumnStatsIndexByColumns(
      String basePath,
      String[] targetColumns,
      HoodieMetadataConfig metadataConfig) {

    // Read Metadata Table's Column Stats Index into Flink's RowData list by
    //    - Fetching the records from CSI by key-prefixes (encoded column names)
    //    - Deserializing fetched records into [[RowData]]s
    HoodieTableMetadata metadataTable = HoodieTableMetadata.create(
        HoodieFlinkEngineContext.DEFAULT, new HoodieHadoopStorage(basePath, FlinkClientUtil.getHadoopConf()),
        metadataConfig, basePath);

    // TODO encoding should be done internally w/in HoodieBackedTableMetadata
    List<String> encodedTargetColumnNames = Arrays.stream(targetColumns)
        .map(colName -> new ColumnIndexID(colName).asBase64EncodedString()).collect(Collectors.toList());

    HoodieData<HoodieRecord<HoodieMetadataPayload>> records =
        metadataTable.getRecordsByKeyPrefixes(encodedTargetColumnNames, HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, false);

    org.apache.hudi.util.AvroToRowDataConverters.AvroToRowDataConverter converter =
        AvroToRowDataConverters.createRowConverter((RowType) METADATA_DATA_TYPE.getLogicalType());
    return records.collectAsList().stream().parallel().map(record -> {
          // schema and props are ignored for generating metadata record from the payload
          // instead, the underlying file system, or bloom filter, or columns stats metadata (part of payload) are directly used
          GenericRecord genericRecord;
          try {
            genericRecord = (GenericRecord) record.getData().getInsertValue(null, null).orElse(null);
          } catch (IOException e) {
            throw new HoodieException("Exception while getting insert value from metadata payload");
          }
          return (RowData) converter.convert(genericRecord);
        }
    ).collect(Collectors.toList());
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  private static DataType getMetadataDataType() {
    return AvroSchemaConverter.convertToDataType(HoodieMetadataRecord.SCHEMA$);
  }

  private static DataType getColStatsDataType() {
    int pos = HoodieMetadataRecord.SCHEMA$.getField(HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS).pos();
    return METADATA_DATA_TYPE.getChildren().get(pos);
  }

  // the column schema:
  // |- file_name: string
  // |- min_val: row
  // |- max_val: row
  // |- null_cnt: long
  // |- val_cnt: long
  // |- column_name: string
  private static int[] getColStatsTargetPos() {
    RowType colStatsRowType = (RowType) COL_STATS_DATA_TYPE.getLogicalType();
    return Stream.of(
            HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME,
            HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE,
            HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE,
            HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT,
            HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT,
            HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME)
        .mapToInt(colStatsRowType::getFieldIndex)
        .toArray();
  }
}
