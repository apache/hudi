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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import static org.apache.hudi.execution.bulkinsert.BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT;
import static org.apache.hudi.execution.bulkinsert.LSMBulkInsertRecordSorter.KEY_COMPARATOR;
import static org.apache.hudi.execution.bulkinsert.LSMBulkInsertRecordSorter.keyByPartitionAndRecordKey;
import static org.apache.hudi.execution.bulkinsert.LSMBulkInsertRecordSorter.sortWithinPartitions;

/**
 * LSM RDD bulk-insert partitioner for
 * {@link BulkInsertSortMode#PARTITION_PATH_REPARTITION_AND_SORT}.
 *
 * <p>Unlike {@link PartitionPathRepartitionAndSortPartitioner}, which orders partitioned input
 * only by partition path and leaves non-partitioned input unsorted, this implementation orders
 * every output Spark partition by {@code (partition path, record key)} using UTF-8 byte ordering.
 * This stronger ordering is required for records written to LSM base files.
 *
 * <p>For a physically partitioned table, {@link PartitionPathRDDPartitioner} distributes records
 * using only the partition-path component, keeping all records for the same table partition
 * together. {@code repartitionAndSortWithinPartitions} then sorts the composite key with the LSM
 * comparator, so records within each table partition are ordered by record key as well.
 *
 * <p>For a physically non-partitioned table, the input is coalesced to the requested parallelism
 * and each resulting Spark partition is sorted locally with the same LSM comparator. Therefore,
 * both branches produce sorted output and {@link #arePartitionRecordsSorted()} always returns
 * {@code true}.
 */
public class LSMPartitionPathRepartitionAndSortPartitioner<T>
    implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {

  private final boolean isTablePartitioned;
  private final boolean shouldPopulateMetaFields;

  public LSMPartitionPathRepartitionAndSortPartitioner(boolean isTablePartitioned,
                                                       HoodieWriteConfig config) {
    this.isTablePartitioned = isTablePartitioned;
    this.shouldPopulateMetaFields = config.populateMetaFields();
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
                                                     int outputSparkPartitions) {
    if (!shouldPopulateMetaFields) {
      throw new HoodieException(
          PARTITION_PATH_REPARTITION_AND_SORT.name() + " mode requires meta-fields to be enabled");
    }

    if (isTablePartitioned) {
      PartitionPathRDDPartitioner partitioner = new PartitionPathRDDPartitioner(
          key -> ((Tuple2<String, String>) key)._1, outputSparkPartitions);
      return keyByPartitionAndRecordKey(records)
          .repartitionAndSortWithinPartitions(partitioner, KEY_COMPARATOR)
          .values();
    }
    return sortWithinPartitions(records.coalesce(outputSparkPartitions));
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
