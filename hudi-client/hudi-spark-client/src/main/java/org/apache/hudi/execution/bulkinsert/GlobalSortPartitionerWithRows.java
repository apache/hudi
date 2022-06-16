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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * A built-in partitioner that does global sorting of the input records across all partitions,
 * corresponding to the {@link BulkInsertSortMode#GLOBAL_SORT} mode.
 *
 * This is a {@link GlobalSortPartitioner} counterpart specialized to work on Spark {@link Row}s
 * directly to avoid de-/serialization into intermediate representation. Please check out
 * {@link GlobalSortPartitioner} java-doc for more details regarding its sorting procedure
 */
public class GlobalSortPartitionerWithRows implements BulkInsertPartitioner<Dataset<Row>> {

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> rows, int outputSparkPartitions) {
    return rows.sort(functions.col(HoodieRecord.PARTITION_PATH_METADATA_FIELD), functions.col(HoodieRecord.RECORD_KEY_METADATA_FIELD))
        .coalesce(outputSparkPartitions);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
