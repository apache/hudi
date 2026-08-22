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

package org.apache.hudi.common.util;

import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.collection.FlatLists;
import org.apache.hudi.exception.HoodieException;

import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility functions used by BULK_INSERT practitioners while sorting records.
 */
public class SortUtils {

  /**
   * Rejects sort columns whose type cannot serve as a sort key. VARIANT and MAP have no total
   * order (Spark's RowOrdering.isOrderable is false for both), and BLOB and VECTOR are rejected
   * deliberately: their struct/array encodings would compare by raw bytes or elements, which is
   * never a meaningful sort key, and the record-based write path fails on them outright. Without
   * this check the failure surfaces deep in the write job (an AnalysisException from the row
   * partitioner, a ClassCastException from the record-based one) without naming the column.
   *
   * <p>Matching is case-insensitive, mirroring Spark's column resolution. Names absent from the
   * schema (nested paths, meta columns on a data-only schema) are left for the caller to handle.
   *
   * @param sortColumns the configured sort columns, may be null or empty
   * @param schema      schema of the data, with or without metadata fields
   */
  public static void validateSortableColumns(String[] sortColumns, HoodieSchema schema) {
    if (sortColumns == null || sortColumns.length == 0
        || schema == null || schema.getType() != HoodieSchemaType.RECORD) {
      return;
    }
    Map<String, HoodieSchemaField> fieldsByLowerName = schema.getFields().stream()
        .collect(Collectors.toMap(field -> field.name().toLowerCase(Locale.ROOT), Function.identity(), (first, second) -> first));
    for (String sortColumn : sortColumns) {
      HoodieSchemaField field = fieldsByLowerName.get(sortColumn.trim().toLowerCase(Locale.ROOT));
      if (field != null) {
        HoodieSchemaType type = field.schema().getNonNullType().getType();
        if (type == HoodieSchemaType.VARIANT || type == HoodieSchemaType.MAP
            || type == HoodieSchemaType.BLOB || type == HoodieSchemaType.VECTOR) {
          throw new HoodieException(String.format(
              "Sorting by column '%s' of type %s is not supported. Remove it from the sort columns.",
              sortColumn.trim(), type));
        }
      }
    }
  }

  /** Overload for callers holding the write schema as an Avro json string; no-op when either side is absent. */
  public static void validateSortableColumns(String[] sortColumns, String avroSchema) {
    if (sortColumns == null || sortColumns.length == 0 || StringUtils.isNullOrEmpty(avroSchema)) {
      return;
    }
    validateSortableColumns(sortColumns, HoodieSchema.parse(avroSchema));
  }

  /** Overload for callers holding the sort columns as a comma-separated string. */
  public static void validateSortableColumns(String sortColumnsCsv, String avroSchema) {
    if (StringUtils.isNullOrEmpty(sortColumnsCsv)) {
      return;
    }
    validateSortableColumns(sortColumnsCsv.split(","), avroSchema);
  }

  static Object[] prependPartitionPath(String partitionPath, Object[] columnValues) {
    Object[] prependColumnValues = new Object[columnValues.length + 1];
    System.arraycopy(columnValues, 0, prependColumnValues, 1, columnValues.length);
    prependColumnValues[0] = partitionPath;
    return prependColumnValues;
  }

  static Object[] prependPartitionPathAndSuffixRecordKey(String partitionPath, String recordKey, Object[] columnValues) {
    Object[] newColumnValues = new Object[columnValues.length + 2];
    System.arraycopy(columnValues, 0, newColumnValues, 1, columnValues.length);
    newColumnValues[0] = partitionPath;
    newColumnValues[newColumnValues.length - 1] = recordKey;
    return newColumnValues;
  }

  /**
   * Given a hoodie record, returns a comparable list of sorted columns.
   *
   * @param record                            HoodieRecord (Spark or Avro)
   * @param sortColumnNames                   user provided sort columns
   * @param schema                            schema for table
   * @param suffixRecordKey                   HoodieWriteConfig.BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS
   * @param consistentLogicalTimestampEnabled KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED
   */
  public static FlatLists.ComparableList<Comparable<HoodieRecord>> getComparableSortColumns(
      HoodieRecord record,
      String[] sortColumnNames,
      HoodieSchema schema,
      boolean suffixRecordKey,
      boolean consistentLogicalTimestampEnabled
  ) {
    if (record.getRecordType() == HoodieRecord.HoodieRecordType.SPARK) {
      Object[] columnValues = record.getColumnValues(schema, sortColumnNames, consistentLogicalTimestampEnabled);
      if (suffixRecordKey) {
        return FlatLists.ofComparableArray(
            prependPartitionPathAndSuffixRecordKey(record.getPartitionPath(), record.getRecordKey(), columnValues));
      }
      return FlatLists.ofComparableArray(prependPartitionPath(record.getPartitionPath(), columnValues));
    } else if (record.getRecordType() == HoodieRecord.HoodieRecordType.AVRO) {
      return FlatLists.ofComparableArray(
          HoodieAvroUtils.getSortColumnValuesWithPartitionPathAndRecordKey(
              record, sortColumnNames, schema, suffixRecordKey, consistentLogicalTimestampEnabled
          ));
    }
    throw new IllegalArgumentException("Invalid recordType" + record.getRecordType());
  }

  /**
   * Given a hoodie record, returns a comparable list of sorted columns.
   *
   * @param record                            HoodieRecord (Spark or Avro)
   * @param sortColumnNames                   user provided sort columns
   * @param schema                            schema for table
   * @param suffixRecordKey                   HoodieWriteConfig.BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS
   * @param consistentLogicalTimestampEnabled KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED
   * @param wrapUTF8StringFunc                Function to wrap UTF8String-elements of array into HoodieUTF8String (Spark only)
   */
  public static FlatLists.ComparableList<Comparable<HoodieRecord>> getComparableSortColumns(
      HoodieRecord record,
      String[] sortColumnNames,
      HoodieSchema schema,
      boolean suffixRecordKey,
      boolean consistentLogicalTimestampEnabled,
      Function<Object[], Object[]> wrapUTF8StringFunc
  ) {
    if (record.getRecordType() == HoodieRecord.HoodieRecordType.SPARK) {
      Object[] columnValues = record.getColumnValues(schema, sortColumnNames, consistentLogicalTimestampEnabled);
      if (suffixRecordKey) {
        return FlatLists.ofComparableArray(wrapUTF8StringFunc.apply(
            prependPartitionPathAndSuffixRecordKey(record.getPartitionPath(), record.getRecordKey(), columnValues)));
      }
      return FlatLists.ofComparableArray(wrapUTF8StringFunc.apply(prependPartitionPath(record.getPartitionPath(), columnValues)));
    } else if (record.getRecordType() == HoodieRecord.HoodieRecordType.AVRO) {
      return FlatLists.ofComparableArray(wrapUTF8StringFunc.apply(
          HoodieAvroUtils.getSortColumnValuesWithPartitionPathAndRecordKey(
              record, sortColumnNames, schema, suffixRecordKey, consistentLogicalTimestampEnabled
          )));
    }
    throw new IllegalArgumentException("Invalid recordType" + record.getRecordType());
  }
}
