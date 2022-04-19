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

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Objects;

/**
 * A built-in partitioner that only does coalesce for input records for bulk insert operation,
 * corresponding to the {@code BulkInsertSortMode.NONE} mode.
 *
 * @param <T> HoodieRecordPayload type
 */
public class NonSortPartitioner<T extends HoodieRecordPayload>
    implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
                                                     int outputSparkPartitionsCount) {
    // TODO handle non-partitioned tables
    // TODO explain
    return records.mapToPair(record -> new Tuple2<>(record.getPartitionPath(), record))
        .partitionBy(new HashingRDDPartitioner(outputSparkPartitionsCount))
        .values();
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return false;
  }

  private static class HashingRDDPartitioner extends Partitioner implements Serializable {
    private final int numPartitions;

    HashingRDDPartitioner(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
      return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
      return Math.abs(Objects.hash(key)) % numPartitions;
    }
  }
}
