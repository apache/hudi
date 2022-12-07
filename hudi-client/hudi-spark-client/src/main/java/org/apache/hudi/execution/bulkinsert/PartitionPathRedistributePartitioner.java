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

import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.Objects;

import scala.Tuple2;

/**
 * A built-in partitioner that does the following for input records for bulk insert operation
 * <p>
 * - For physically partitioned table, repartition the input records based on the partition path,
 * limiting the shuffle parallelism to specified `outputSparkPartitions`
 * <p>
 * - For physically non-partitioned table, simply does coalesce for the input records with
 * `outputSparkPartitions`
 * <p>
 * Corresponding to the {@code BulkInsertSortMode.PARTITION_PATH_REDISTRIBUTE} mode.
 *
 * @param <T> HoodieRecordPayload type
 */
public class PartitionPathRedistributePartitioner<T extends HoodieRecordPayload>
    implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {

  private final boolean isTablePartitioned;

  public PartitionPathRedistributePartitioner(boolean isTablePartitioned) {
    this.isTablePartitioned = isTablePartitioned;
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
                                                     int outputSparkPartitions) {
    if (isTablePartitioned) {
      PartitionPathRDDPartitioner partitioner = new PartitionPathRDDPartitioner(
          (partitionPath) -> (String) partitionPath, outputSparkPartitions);
      return records.mapToPair(record -> new Tuple2<>(record.getPartitionPath(), record))
          .partitionBy(partitioner)
          .values();
    }
    return records.coalesce(outputSparkPartitions);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return false;
  }

  /**
   * A Spark RDD partitioner implementation that determines the Spark partition
   * based on the table partition path, generating targeted number of Spark partitions.
   */
  protected static class PartitionPathRDDPartitioner extends Partitioner implements Serializable {
    private final SerializableFunctionUnchecked<Object, String> partitionPathExtractor;
    private final int numPartitions;

    PartitionPathRDDPartitioner(SerializableFunctionUnchecked<Object, String> partitionPathExtractor, int numPartitions) {
      this.partitionPathExtractor = partitionPathExtractor;
      this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
      return numPartitions;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int getPartition(Object o) {
      return Math.abs(Objects.hash(partitionPathExtractor.apply(o))) % numPartitions;
    }
  }
}
