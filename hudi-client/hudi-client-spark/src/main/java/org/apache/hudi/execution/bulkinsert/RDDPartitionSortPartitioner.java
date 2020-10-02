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
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import scala.Tuple2;

/**
 * A built-in partitioner that does local sorting for each RDD partition
 * after coalesce for bulk insert operation, corresponding to the
 * {@code BulkInsertSortMode.PARTITION_SORT} mode.
 *
 * @param <T> HoodieRecordPayload type
 */
public class RDDPartitionSortPartitioner<T extends HoodieRecordPayload>
    implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
                                                     int outputSparkPartitions) {
    return records.coalesce(outputSparkPartitions)
        .mapToPair(record ->
            new Tuple2<>(
                new StringBuilder()
                    .append(record.getPartitionPath())
                    .append("+")
                    .append(record.getRecordKey())
                    .toString(), record))
        .mapPartitions(partition -> {
          // Sort locally in partition
          List<Tuple2<String, HoodieRecord<T>>> recordList = new ArrayList<>();
          for (; partition.hasNext(); ) {
            recordList.add(partition.next());
          }
          Collections.sort(recordList, (o1, o2) -> o1._1.compareTo(o2._1));
          return recordList.stream().map(e -> e._2).iterator();
        });
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
