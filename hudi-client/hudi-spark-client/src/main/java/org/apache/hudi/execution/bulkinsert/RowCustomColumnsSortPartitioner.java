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

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;

import static org.apache.hudi.table.BulkInsertPartitioner.tryPrependPartitionPathColumns;

/**
 * A partitioner that globally sorts a {@link Dataset<Row>} based on partition path column and custom columns.
 *
 * @see GlobalSortPartitionerWithRows
 * @see BulkInsertSortMode#GLOBAL_SORT
 */
public class RowCustomColumnsSortPartitioner implements BulkInsertPartitioner<Dataset<Row>> {

  private final String[] sortColumnNames;

  public RowCustomColumnsSortPartitioner(HoodieWriteConfig config) {
    this.sortColumnNames = tryPrependPartitionPathColumns(getSortColumnName(config), config);
  }

  public RowCustomColumnsSortPartitioner(String[] columnNames, HoodieWriteConfig config) {
    this.sortColumnNames = tryPrependPartitionPathColumns(columnNames, config);
  }

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> records, int outputSparkPartitions) {
    return records
        .sort(Arrays.stream(sortColumnNames).map(Column::new).toArray(Column[]::new))
        .coalesce(outputSparkPartitions);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }

  private String[] getSortColumnName(HoodieWriteConfig config) {
    return Arrays.stream(config.getUserDefinedBulkInsertPartitionerSortColumns().split(","))
        .map(String::trim).toArray(String[]::new);
  }
}
