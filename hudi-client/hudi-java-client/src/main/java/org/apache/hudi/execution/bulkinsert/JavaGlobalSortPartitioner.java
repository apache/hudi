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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.table.BulkInsertPartitioner;

import java.util.Comparator;
import java.util.List;

/**
 * A built-in partitioner that does global sorting for the input records across partitions
 * after repartition for bulk insert operation, corresponding to the
 * {@code BulkInsertSortMode.GLOBAL_SORT} mode.
 *
 * @param <T> HoodieRecordPayload type
 */
public class JavaGlobalSortPartitioner<T>
    implements BulkInsertPartitioner<List<HoodieRecord<T>>> {

  @Override
  public List<HoodieRecord<T>> repartitionRecords(List<HoodieRecord<T>> records,
                                                  int outputPartitions) {
    // Now, sort the records and line them up nicely for loading.
    records.sort(new Comparator() {
      @Override
      public int compare(Object o1, Object o2) {
        HoodieRecord o11 = (HoodieRecord) o1;
        HoodieRecord o22 = (HoodieRecord) o2;
        String left = new StringBuilder()
            .append(o11.getPartitionPath())
            .append("+")
            .append(o11.getRecordKey())
            .toString();
        String right = new StringBuilder()
            .append(o22.getPartitionPath())
            .append("+")
            .append(o22.getRecordKey())
            .toString();
        return left.compareTo(right);
      }
    });
    return records;
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
