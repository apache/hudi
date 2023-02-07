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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner.getOrderByColumnNames;

/**
 * A partitioner that does local sorting for each RDD partition based on the tuple of
 * values of the columns configured for ordering.
 */
public class RowCustomColumnsSortPartitioner extends RepartitioningBulkInsertPartitionerBase<Dataset<Row>> {

  private final String[] orderByColumnNames;

  public RowCustomColumnsSortPartitioner(HoodieWriteConfig config, HoodieTableConfig tableConfig) {
    super(tableConfig);
    this.orderByColumnNames = getOrderByColumnNames(config);

    checkState(orderByColumnNames.length > 0);
  }

  public RowCustomColumnsSortPartitioner(String[] columnNames, HoodieTableConfig tableConfig) {
    super(tableConfig);
    this.orderByColumnNames = columnNames;

    checkState(orderByColumnNames.length > 0);
  }

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> dataset, int targetPartitionNumHint) {
    Dataset<Row> repartitionedDataset;

    // NOTE: In case of partitioned table even "global" ordering (across all RDD partitions) could
    //       not change table's partitioning and therefore there's no point in doing global sorting
    //       across "physical" partitions, and instead we can reduce total amount of data being
    //       shuffled by doing do "local" sorting:
    //          - First, re-partitioning dataset such that "logical" partitions are aligned w/
    //          "physical" ones
    //          - Sorting locally w/in RDD ("logical") partitions
    //
    //       Non-partitioned tables will be globally sorted.
    if (isPartitionedTable) {
      repartitionedDataset = dataset.repartition(handleTargetPartitionNumHint(targetPartitionNumHint),
          new Column(HoodieRecord.PARTITION_PATH_METADATA_FIELD));
    } else {
      repartitionedDataset = tryCoalesce(dataset, targetPartitionNumHint);
    }

    return repartitionedDataset.sortWithinPartitions(
        JavaConverters.asScalaBufferConverter(
            Arrays.stream(orderByColumnNames)
                .map(Column::new)
                .collect(Collectors.toList())).asScala());
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
