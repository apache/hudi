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
import org.apache.hudi.common.util.collection.Pair;
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
   * Rejects sort columns whose type cannot serve as a sort key. Spark's RowOrdering.isOrderable
   * is false for both VARIANT and MAP, which is the binding constraint on the row path. On the
   * Avro path only MAP is outright uncomparable (GenericData.compare throws "Can't compare
   * maps!"); a variant's {metadata, value} record does compare, but by its bytes, which is never
   * a meaningful sort key. The walk recurses through records and array elements just as
   * isOrderable does, so a struct or an array that merely holds a variant or a map at depth is
   * rejected too, and the error names the nested member that made the column unorderable. Without
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
      String columnName = sortColumn.trim();
      HoodieSchemaField field = fieldsByLowerName.get(columnName.toLowerCase(Locale.ROOT));
      if (field == null) {
        continue;
      }
      validateSortableColumn(columnName, field.schema());
    }
  }

  /**
   * The check behind {@link #validateSortableColumns(String[], HoodieSchema)} for one column whose
   * schema the caller has already resolved, such as a nested path the array overload leaves alone.
   *
   * @param columnName   the column as the caller names it, so a nested path reads as one in the error
   * @param columnSchema schema of the column's value, nullable or not
   */
  public static void validateSortableColumn(String columnName, HoodieSchema columnSchema) {
    Option<Pair<String, HoodieSchemaType>> unorderable = findUnorderableNode(columnSchema, columnName);
    if (unorderable.isPresent()) {
      // Only a nested offender needs pointing at; at the top level the column and its type already say it.
      String nested = unorderable.get().getLeft().equals(columnName) ? ""
          : String.format("it holds a %s at '%s', and ", unorderable.get().getRight(), unorderable.get().getLeft());
      throw new HoodieException(String.format(
          "Sorting by column '%s' of type %s is not supported: %sVARIANT and MAP have no ordering, "
              + "at any depth. Remove it from the sort columns.",
          columnName, columnSchema.getNonNullType().getType(), nested));
    }
  }

  /**
   * Mirrors Spark's RowOrdering.isOrderable, but reports where it fails rather than just that it
   * does: VARIANT and MAP are the unorderable leaves, a record is orderable when every field is,
   * an array when its element type is, and every other type - BLOB (a struct of atomics in Spark)
   * and VECTOR (an array of floats) included - is orderable.
   *
   * @param schema the node to walk
   * @param path   dotted path of {@code schema}, extended with "." + name per record field and
   *               with "[]" per array element
   * @return the path and type of the first unorderable node, or empty when the schema is orderable
   */
  private static Option<Pair<String, HoodieSchemaType>> findUnorderableNode(HoodieSchema schema, String path) {
    HoodieSchema unwrapped = schema.isNullable() ? schema.getNonNullType() : schema;
    switch (unwrapped.getType()) {
      case VARIANT:
      case MAP:
        return Option.of(Pair.of(path, unwrapped.getType()));
      case RECORD:
        for (HoodieSchemaField field : unwrapped.getFields()) {
          Option<Pair<String, HoodieSchemaType>> found = findUnorderableNode(field.schema(), path + "." + field.name());
          if (found.isPresent()) {
            return found;
          }
        }
        return Option.empty();
      case ARRAY:
        return findUnorderableNode(unwrapped.getElementType(), path + "[]");
      default:
        return Option.empty();
    }
  }

  /** Overload for callers holding the write schema as an Avro json string; no-op when either side is absent. */
  public static void validateSortableColumns(String[] sortColumns, String avroSchema) {
    if (sortColumns == null || sortColumns.length == 0 || StringUtils.isNullOrEmpty(avroSchema)) {
      return;
    }
    validateSortableColumns(sortColumns, HoodieSchema.parse(avroSchema));
  }

  /**
   * Overload for callers holding the sort columns as a comma-separated string. The split does not trim;
   * the array overload trims each entry, so spaces around the commas are tolerated either way.
   */
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
