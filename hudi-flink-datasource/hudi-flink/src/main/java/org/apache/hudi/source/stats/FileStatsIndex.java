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
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Tuple3;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.ColumnStatsIndexPrefixRawKey;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.source.prune.ColumnStatsProbe;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.RowDataProjection;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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

import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.source.stats.ColumnStatsSchemas.COL_STATS_DATA_TYPE;
import static org.apache.hudi.source.stats.ColumnStatsSchemas.COL_STATS_TARGET_POS;
import static org.apache.hudi.source.stats.ColumnStatsSchemas.METADATA_DATA_TYPE;
import static org.apache.hudi.source.stats.ColumnStatsSchemas.ORD_COL_NAME;
import static org.apache.hudi.source.stats.ColumnStatsSchemas.ORD_FILE_NAME;
import static org.apache.hudi.source.stats.ColumnStatsSchemas.ORD_MAX_VAL;
import static org.apache.hudi.source.stats.ColumnStatsSchemas.ORD_MIN_VAL;
import static org.apache.hudi.source.stats.ColumnStatsSchemas.ORD_NULL_CNT;
import static org.apache.hudi.source.stats.ColumnStatsSchemas.ORD_VAL_CNT;

/**
 * An index support implementation that leverages Column Stats Index to prune files,
 * including utilities for abstracting away heavy-lifting of interactions with the index,
 * providing convenient interfaces to read it, transpose, etc.
 */
public class FileStatsIndex implements ColumnStatsIndex {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(FileStatsIndex.class);
  private final RowType rowType;
  private final String basePath;
  private final Configuration conf;
  private HoodieTableMetaClient metaClient;
  private HoodieTableMetadata metadataTable;

  public FileStatsIndex(
      String basePath,
      RowType rowType,
      Configuration conf,
      @Nullable HoodieTableMetaClient metaClient) {
    this.basePath = basePath;
    this.rowType = rowType;
    this.conf = conf;
    this.metaClient = metaClient;
  }

  @Override
  public String getIndexPartitionName() {
    return HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
  }

  public HoodieTableMetadata getMetadataTable() {
    // initialize the metadata table lazily
    if (this.metadataTable == null) {
      initMetaClient();
      this.metadataTable = metaClient.getTableFormat().getMetadataFactory().create(
          HoodieFlinkEngineContext.DEFAULT,
          metaClient.getStorage(),
          StreamerUtil.metadataConfig(conf),
          basePath);
    }
    return this.metadataTable;
  }

  private void initMetaClient() {
    if (this.metaClient == null) {
      this.metaClient = StreamerUtil.createMetaClient(conf);
    }
  }

  @Override
  public Set<String> computeCandidateFiles(ColumnStatsProbe probe, List<String> allFiles) {
    if (probe == null) {
      return null;
    }
    try {
      String[] targetColumns = probe.getReferencedCols();
      final List<RowData> statsRows = readColumnStatsIndexByColumns(targetColumns);
      return candidatesInMetadataTable(probe, statsRows, allFiles);
    } catch (Throwable t) {
      LOG.error("Failed to read metadata index: {} for data skipping", getIndexPartitionName(), t);
      return null;
    }
  }

  @Override
  public Set<String> computeCandidatePartitions(ColumnStatsProbe probe, List<String> allPartitions) {
    throw new UnsupportedOperationException("This method is not supported by " + this.getClass().getSimpleName());
  }

  /**
   * Computes pruned list of candidates' names based on provided list of data filters.
   * conditions, by leveraging Metadata Table's Column Statistics index (hereon referred as ColStats for brevity)
   * bearing "min", "max", "num_nulls" statistics for all columns.
   *
   * <p>NOTE: This method has to return complete set of the candidates, since only provided candidates will
   * ultimately be scanned as part of query execution. Hence, this method has to maintain the
   * invariant of conservatively including every candidate's name, that is NOT referenced in its index.
   *
   * <p>The {@code filters} must all be simple.
   *
   * @param probe         The column stats probe built from push-down filters.
   * @param indexRows     The raw column stats records.
   * @param oriCandidates The original candidates to be pruned.
   *
   * @return set of pruned (data-skipped) candidate names
   */
  protected Set<String> candidatesInMetadataTable(
      @Nullable ColumnStatsProbe probe,
      List<RowData> indexRows,
      List<String> oriCandidates) {
    if (probe == null) {
      return null;
    }
    String[] referencedCols = probe.getReferencedCols();
    final Pair<List<RowData>, String[]> colStatsTable =
        transposeColumnStatsIndex(indexRows, referencedCols);
    List<RowData> transposedColStats = colStatsTable.getLeft();
    String[] queryCols = colStatsTable.getRight();
    if (queryCols.length == 0) {
      // the indexed columns have no intersection with the referenced columns, returns early
      return null;
    }
    RowType.RowField[] queryFields = DataTypeUtils.projectRowFields(rowType, queryCols);

    Set<String> allIndexedFiles = transposedColStats.stream().parallel()
        .map(row -> row.getString(0).toString())
        .collect(Collectors.toSet());
    Set<String> candidateFiles = transposedColStats.stream().parallel()
        .filter(row -> probe.test(row, queryFields))
        .map(row -> row.getString(0).toString())
        .collect(Collectors.toSet());

    // NOTE: Col-Stats Index isn't guaranteed to have complete set of statistics for every
    //       base-file: since it's bound to clustering, which could occur asynchronously
    //       at arbitrary point in time, and is not likely to be touching all the base files.
    //
    //       To close that gap, we manually compute the difference b/w all indexed (by col-stats-index)
    //       files and all outstanding base-files, and make sure that all base files not
    //       represented w/in the index are included in the output of this method
    oriCandidates.removeAll(allIndexedFiles);
    candidateFiles.addAll(oriCandidates);
    return candidateFiles;
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
   * @return reshaped table according to the format outlined above
   */
  @VisibleForTesting
  public Pair<List<RowData>, String[]> transposeColumnStatsIndex(List<RowData> colStats, String[] queryColumns) {

    Map<String, LogicalType> tableFieldTypeMap = rowType.getFields().stream()
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

  @VisibleForTesting
  public List<RowData> readColumnStatsIndexByColumns(String[] targetColumns) {
    // NOTE: If specific columns have been provided, we can considerably trim down amount of data fetched
    //       by only fetching Column Stats Index records pertaining to the requested columns.
    //       Otherwise, we fall back to read whole Column Stats Index
    ValidationUtils.checkArgument(targetColumns.length > 0,
        "Column stats is only valid when push down filters have referenced columns");

    // Read Metadata Table's column stats Flink's RowData list by
    //    - Fetching the records by key-prefixes (column names)
    //    - Deserializing fetched records into [[RowData]]s
    List<ColumnStatsIndexPrefixRawKey> rawKeys = Arrays.stream(targetColumns)
        .map(ColumnStatsIndexPrefixRawKey::new)  // Just column name, no partition
        .collect(Collectors.toList());

    HoodieData<HoodieRecord<HoodieMetadataPayload>> records =
        getMetadataTable().getRecordsByKeyPrefixes(
            HoodieListData.lazy(rawKeys), getIndexPartitionName(), false);

    org.apache.hudi.util.AvroToRowDataConverters.AvroToRowDataConverter converter =
        AvroToRowDataConverters.createRowConverter((RowType) METADATA_DATA_TYPE.getLogicalType());
    List<RowData> rows = records.collectAsList().stream().parallel().map(record -> {
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
    return projectNestedColStatsColumns(rows);
  }
}
