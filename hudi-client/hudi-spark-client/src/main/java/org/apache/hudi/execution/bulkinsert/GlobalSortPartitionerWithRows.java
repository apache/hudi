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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.hudi.execution.bulkinsert.BulkInsertSortMode.GLOBAL_SORT;

/**
 * A built-in partitioner that does global sorting for the input Rows across partitions after repartition for bulk insert operation, corresponding to the {@code BulkInsertSortMode.GLOBAL_SORT} mode.
 */
public class GlobalSortPartitionerWithRows implements BulkInsertPartitioner<Dataset<Row>> {

  private final boolean shouldPopulateMetaFields;

  public GlobalSortPartitionerWithRows(HoodieWriteConfig config) {
    this.shouldPopulateMetaFields = config.populateMetaFields();
  }

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> rows, int outputSparkPartitions) {
    if (!shouldPopulateMetaFields) {
      throw new HoodieException(GLOBAL_SORT.name() + " mode requires meta-fields to be enabled");
    }

    // Now, sort the records and line them up nicely for loading.
    // Let's use "partitionPath + key" as the sort key.
    return rows.sort(functions.col(HoodieRecord.PARTITION_PATH_METADATA_FIELD), functions.col(HoodieRecord.RECORD_KEY_METADATA_FIELD))
        .coalesce(outputSparkPartitions);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
