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

import org.apache.spark.api.java.JavaRDD;

/**
 * A built-in partitioner that avoids expensive sorting for the input records for bulk insert
 * operation, by doing either of the following:
 * <p>
 * - If enforcing the outputSparkPartitions, only does coalesce for input records;
 * <p>
 * - Otherwise, returns input records as is.
 * <p>
 * Corresponds to the {@code BulkInsertSortMode.NONE} mode.
 *
 * @param <T> HoodieRecordPayload type
 */
public class NonSortPartitioner<T>
    implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {

  private final boolean enforceNumOutputPartitions;

  /**
   * Default constructor without enforcing the number of output partitions.
   */
  public NonSortPartitioner() {
    this(false);
  }

  /**
   * Constructor with `enforceNumOutputPartitions` config.
   *
   * @param enforceNumOutputPartitions Whether to enforce the number of output partitions.
   */
  public NonSortPartitioner(boolean enforceNumOutputPartitions) {
    this.enforceNumOutputPartitions = enforceNumOutputPartitions;
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
                                                     int outputSparkPartitions) {
    if (enforceNumOutputPartitions) {
      return records.coalesce(outputSparkPartitions);
    }
    return records;
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return false;
  }
}
