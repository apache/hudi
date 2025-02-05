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

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.FlatLists;

import org.apache.avro.Schema;

import static org.apache.hudi.common.util.SortUtils.prependPartitionPath;
import static org.apache.hudi.common.util.SortUtils.prependPartitionPathAndSuffixRecordKey;

/**
 * Utility functions used by BULK_INSERT practitioners while sorting records.
 */
public class SparkSortUtils {
  /**
   * Given a hoodie record, returns a comparable list of sorted columns.
   *
   * @param record                            HoodieRecord (Spark or Avro)
   * @param sortColumnNames                   user provided sort columns
   * @param schema                            schema for table
   * @param suffixRecordKey                   HoodieWriteConfig.BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS
   * @param consistentLogicalTimestampEnabled KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED
   */
  public static FlatLists.ComparableList<Comparable<HoodieRecord<?>>> getComparableSortColumns(
      HoodieRecord<?> record,
      String[] sortColumnNames,
      Schema schema,
      boolean suffixRecordKey,
      boolean consistentLogicalTimestampEnabled
  ) {
    if (record.getRecordType() == HoodieRecord.HoodieRecordType.SPARK) {
      Object[] columnValues = record.getColumnValues(schema, sortColumnNames, consistentLogicalTimestampEnabled);
      if (suffixRecordKey) {
        return SparkAdapterSupport$.MODULE$.sparkAdapter().createComparableList(
            prependPartitionPathAndSuffixRecordKey(record.getPartitionPath(), record.getRecordKey(), columnValues));
      }
      return FlatLists.ofComparableArray(prependPartitionPath(record.getPartitionPath(), columnValues));
    } else if (record.getRecordType() == HoodieRecord.HoodieRecordType.AVRO) {
      return SparkAdapterSupport$.MODULE$.sparkAdapter().createComparableList(
          HoodieAvroUtils.getSortColumnValuesWithPartitionPathAndRecordKey(
              record, sortColumnNames, schema, suffixRecordKey, consistentLogicalTimestampEnabled
          ));
    }
    throw new IllegalArgumentException("Invalid recordType" + record.getRecordType());
  }
}
