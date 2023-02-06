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

public enum BulkInsertSortMode {
  /**
   * No sorting, partitioning or coalescing is performed
   */
  NONE,

  /**
   * Performs global sorting (totally ordering records w/in the batch) by the
   * pair of {@code (partition-path, record-key) }
   *
   * This allows to achieve following
   * <ol>
   *   <li>Minimizes number of tasks writing into a single table's partition (since records are
   *   globally ordered by partition-path individual Spark partitions will be holding contiguous
   *   chunks of records from a single partition)
   *   </li>
   *   <li>Records w/in single partition to be sorted w/in the corresponding files, allowing
   *   for more effective pruning when using Hudi's Bloom Index (with range-pruning enabled)</li>
   * </ol>
   */
  GLOBAL_SORT,

  /**
   * Performs local sorting w/in the individual *Spark partitions*
   *
   * NOTE: That this operation doesn't trigger re-shuffling (only coalescing) since sorting
   *       is performed locally w/in individual Spark partitions
   */
  PARTITION_SORT,

  /**
   * Performs re-partitioning of the incoming dataset by table's partition-path
   *
   * This allows to achieve following
   * <ol>
   *   <li>Aligns Spark partitions to be 1:1 mapped to table's actual partitions on disk, therefore
   *   making sure that a single task writes all of the records in a single table partition (in turn
   *   minimizing number of files created for ex)</li>
   * </ol>
   *
   * NOTE: This partitioner should only be used for partitioned tables.
   *       It's also important to keep in mind that this partitioner might be creating skew in the
   *       dataset in cases when particular table's partition might be receiving substantially more
   *       records than the others
   */
  PARTITION_PATH_REPARTITION,

  /**
   * Performs re-partitioning of the incoming dataset by table's partition-path and sorts
   * records locally w/in individual Spark's partitions
   *
   * This allows to achieve following
   * <ol>
   *   <li>Aligns Spark partitions to be 1:1 mapped to table's actual partitions on disk, therefore
   *   making sure that a single task writes all of the records in a single table partition (in turn
   *   minimizing number of files created for ex)</li>
   *   <li>Records w/in single partition to be sorted w/in the corresponding files, allowing
   *   for more effective pruning when using Hudi's Bloom Index (with range-pruning enabled)</li>
   * </ol>
   *
   * This partitioner complements {@link #GLOBAL_SORT} partitioner:
   *
   * <ul>
   *   <li>This is a more-efficient implementation not requiring records to be globally sorted,
   *   sorting them w/in target table's partition instead</li>
   *   <li>This implementation is prone to Spark partitions skew, in cases when incoming dataset
   *   is imbalanced and particular table's partition might be receiving substantially more
   *   records than the others
   * </ul>
   *
   * NOTE: This partitioner should only be used for partitioned tables
   */
  PARTITION_PATH_REPARTITION_AND_SORT
}
