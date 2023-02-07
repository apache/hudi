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
import org.apache.hudi.common.util.collection.Pair;

import org.apache.spark.api.java.JavaRDD;

import static org.apache.hudi.execution.bulkinsert.BulkInsertSortMode.GLOBAL_SORT;

/**
 * A built-in partitioner that does global sorting of the input records across all Spark partitions,
 * corresponding to the {@link BulkInsertSortMode#GLOBAL_SORT} mode.
 *
 * NOTE: Records are sorted by (partitionPath, key) tuple to make sure that physical
 *       partitioning on disk is aligned with logical partitioning of the dataset (by Spark)
 *       as much as possible.
 *       Consider following scenario: dataset is inserted w/ parallelism of N (meaning that Spark
 *       will partition it into N _logical_ partitions while writing), and has M physical partitions
 *       on disk. Without alignment "physical" and "logical" partitions (assuming
 *       here that records are inserted uniformly across partitions), every logical partition,
 *       which might be handled by separate executor, will be inserting into every physical
 *       partition, creating a new file for the records it's writing, entailing that new N x M
 *       files will be added to the table.
 *
 *       Instead, we want no more than N + M files to be created, and therefore sort by
 *       a tuple of (partitionPath, key), which provides for following invariants where every
 *       Spark partition will either
 *          - Hold _all_ record from particular physical partition, or
 *          - Hold _only_ records from that particular physical partition
 *
 *       In other words a single Spark partition will either be hold full set of records for
 *       a few smaller partitions, or it will hold just the records of the larger one. This
 *       allows us to provide a guarantee that no more N + M files will be created.
 *
 * @param <T> {@code HoodieRecordPayload} type
 */
public class GlobalSortPartitioner<T> extends SparkBulkInsertPartitionerBase<JavaRDD<HoodieRecord<T>>> {

  private final boolean shouldPopulateMetaFields;

  public GlobalSortPartitioner(HoodieWriteConfig config) {
    this.shouldPopulateMetaFields = config.populateMetaFields();
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
                                                     int targetPartitionNumHint) {
    if (!shouldPopulateMetaFields) {
      throw new HoodieException(GLOBAL_SORT.name() + " mode requires meta-fields to be enabled");
    }

    return records.sortBy(record ->
        Pair.of(record.getPartitionPath(), record.getRecordKey()), true, handleTargetPartitionNumHint(targetPartitionNumHint));
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
