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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.SortUtils;
import org.apache.hudi.common.util.collection.FlatLists;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.avro.Schema;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieWriteConfig.BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS;

/**
 * A partitioner that does sorting based on specified column values for Java client.
 *
 * @param <T> HoodieRecordPayload type
 */
public class JavaCustomColumnsSortPartitioner<T>
    implements BulkInsertPartitioner<List<HoodieRecord<T>>> {

  private final String[] sortColumnNames;
  private final Schema schema;
  private final boolean consistentLogicalTimestampEnabled;
  private final boolean suffixRecordKey;

  public JavaCustomColumnsSortPartitioner(String[] columnNames, Schema schema, HoodieWriteConfig config) {
    this.sortColumnNames = columnNames;
    this.schema = schema;
    this.consistentLogicalTimestampEnabled = config.isConsistentLogicalTimestampEnabled();
    this.suffixRecordKey = config.getBoolean(BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS);
  }

  @Override
  public List<HoodieRecord<T>> repartitionRecords(
      List<HoodieRecord<T>> records, int outputPartitions) {
    return records.stream()
        .sorted((o1, o2) -> getComparableSortColumns(o1).compareTo(getComparableSortColumns(o2)))
        .collect(Collectors.toList());
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }

  FlatLists.ComparableList<Comparable<HoodieRecord>> getComparableSortColumns(HoodieRecord record) {
    return SortUtils.getComparableSortColumns(record, sortColumnNames, schema, suffixRecordKey, consistentLogicalTimestampEnabled);
  }
}
