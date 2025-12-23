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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.FlatLists;

import java.util.function.Function;

/**
 * Utility functions used by BULK_INSERT practitioners while sorting records.
 */
public class SortUtils {
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
