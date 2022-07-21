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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A built-in partitioner that only does re-partitioning to better align "logical" partitioning
 * of the dataset w/ the "physical" partitioning of the table (effectively a no-op for non-partitioned
 * tables)
 *
 * Corresponds to the {@link BulkInsertSortMode#PARTITION_NO_SORT} mode.
 */
public class RepartitionNoSortPartitionerWithRows extends RepartitioningBulkInsertPartitionerBase<Dataset<Row>> {

  public RepartitionNoSortPartitionerWithRows(HoodieTableConfig tableConfig) {
    super(tableConfig);
  }

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> dataset, int outputSparkPartitions) {
    if (isPartitionedTable) {
      return dataset.repartition(outputSparkPartitions, new Column(HoodieRecord.PARTITION_PATH_METADATA_FIELD));
    } else {
      return dataset.coalesce(outputSparkPartitions);
    }
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return false;
  }
}
