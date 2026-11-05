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

import static org.apache.hudi.execution.bulkinsert.BulkInsertSortMode.PARTITION_SORT;
import static org.apache.hudi.execution.bulkinsert.LSMBulkInsertRecordSorter.sortWithinPartitions;

/**
 * LSM RDD bulk-insert partitioner for {@link BulkInsertSortMode#PARTITION_SORT}.
 *
 * <p>Like {@link RDDPartitionSortPartitioner}, this partitioner first coalesces the input to the
 * requested parallelism and then materializes and sorts the records within each Spark partition.
 * The ordering is intentionally different: {@code RDDPartitionSortPartitioner} compares its
 * combined partition-path and record-key string with Java {@link String#compareTo(String)}, while
 * an LSM table must compare the partition path and record key separately in UTF-8 byte order to
 * preserve the LSM base-file ordering invariant.
 */
public class LSMPartitionSortPartitioner<T>
    implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {

  private final boolean shouldPopulateMetaFields;

  public LSMPartitionSortPartitioner(HoodieWriteConfig config) {
    this.shouldPopulateMetaFields = config.populateMetaFields();
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
                                                     int outputSparkPartitions) {
    if (!shouldPopulateMetaFields) {
      throw new HoodieException(PARTITION_SORT.name() + " mode requires meta-fields to be enabled");
    }

    return sortWithinPartitions(records.coalesce(outputSparkPartitions));
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
