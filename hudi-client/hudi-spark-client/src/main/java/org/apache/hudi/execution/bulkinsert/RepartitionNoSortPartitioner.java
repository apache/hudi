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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 * A built-in partitioner that only does re-partitioning to better align "logical" partitioning
 * of the dataset w/ the "physical" partitioning of the table (effectively a no-op for non-partitioned
 * tables)
 *
 * Corresponds to the {@link BulkInsertSortMode#REPARTITION_NO_SORT} mode.
 *
 * @param <T> {@link HoodieRecordPayload} type
 */
public class RepartitionNoSortPartitioner<T extends HoodieRecordPayload>
    extends RepartitioningBulkInsertPartitionerBase<JavaRDD<HoodieRecord<T>>> {

  public RepartitionNoSortPartitioner(HoodieTableConfig tableConfig) {
    super(tableConfig);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputSparkPartitionsCount) {
    if (isPartitionedTable) {
      PartitionPathRDDPartitioner partitioner =
          new PartitionPathRDDPartitioner((partitionPath) -> (String) partitionPath, outputSparkPartitionsCount);

      return records.mapToPair(record -> new Tuple2<>(record.getPartitionPath(), record))
          .partitionBy(partitioner)
          .values();
    } else {
      return records.coalesce(outputSparkPartitionsCount);
    }
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return false;
  }
}
