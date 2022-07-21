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
 * A built-in partitioner that does local sorting w/in the Spark partition,
 * corresponding to the {@code BulkInsertSortMode.PARTITION_SORT} mode.
 */
public class PartitionSortPartitionerWithRows extends RepartitioningBulkInsertPartitionerBase<Dataset<Row>> {

  public PartitionSortPartitionerWithRows(HoodieTableConfig tableConfig) {
    super(tableConfig);
  }

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> dataset, int outputSparkPartitions) {
    Dataset<Row> repartitionedDataset;

    // NOTE: Datasets being ingested into partitioned tables are additionally re-partitioned to better
    //       align dataset's logical partitioning with expected table's physical partitioning to
    //       provide for appropriate file-sizing and better control of the number of files created.
    //
    //       Please check out {@code GlobalSortPartitioner} java-doc for more details
    if (isPartitionedTable) {
      repartitionedDataset = dataset.repartition(outputSparkPartitions, new Column(HoodieRecord.PARTITION_PATH_METADATA_FIELD));
    } else {
      repartitionedDataset = dataset.coalesce(outputSparkPartitions);
    }

    return repartitionedDataset.sortWithinPartitions(HoodieRecord.RECORD_KEY_METADATA_FIELD);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
