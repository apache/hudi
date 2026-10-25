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

import static org.apache.hudi.execution.bulkinsert.BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT;

/**
 * LSM Dataset Row bulk-insert partitioner for
 * {@link BulkInsertSortMode#PARTITION_PATH_REPARTITION_AND_SORT}.
 *
 * <p>Unlike {@link PartitionPathRepartitionAndSortPartitionerWithRows}, which orders partitioned
 * input only by partition path and leaves non-partitioned input unsorted, this implementation
 * orders every output Spark partition by the {@link HoodieRecord#PARTITION_PATH_METADATA_FIELD}
 * and {@link HoodieRecord#RECORD_KEY_METADATA_FIELD} columns. Spark SQL stores these columns as
 * UTF-8 strings, giving LSM base files their required UTF-8 physical ordering without adding
 * temporary sort columns or changing the input schema.
 *
 * <p>For a physically partitioned table, rows are first repartitioned by the partition-path
 * metadata column so that one table partition is not split by the distribution key, and then
 * sorted within each resulting Spark partition by partition path and record key. For a physically
 * non-partitioned table, rows are coalesced before applying the same local sort. Both branches
 * require populated meta fields and produce sorted output, so
 * {@link #arePartitionRecordsSorted()} always returns {@code true}.
 */
public class LSMPartitionPathRepartitionAndSortPartitionerWithRows
    implements BulkInsertPartitioner<Dataset<Row>> {

  private final boolean isTablePartitioned;
  private final boolean shouldPopulateMetaFields;

  public LSMPartitionPathRepartitionAndSortPartitionerWithRows(boolean isTablePartitioned,
                                                               HoodieWriteConfig config) {
    this.isTablePartitioned = isTablePartitioned;
    this.shouldPopulateMetaFields = config.populateMetaFields();
  }

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> rows, int outputSparkPartitions) {
    if (!shouldPopulateMetaFields) {
      throw new HoodieException(
          PARTITION_PATH_REPARTITION_AND_SORT.name() + " mode requires meta-fields to be enabled");
    }

    Dataset<Row> repartitionedRows = isTablePartitioned
        ? rows.repartition(
            outputSparkPartitions, functions.col(HoodieRecord.PARTITION_PATH_METADATA_FIELD))
        : rows.coalesce(outputSparkPartitions);
    return repartitionedRows.sortWithinPartitions(
        functions.col(HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        functions.col(HoodieRecord.RECORD_KEY_METADATA_FIELD));
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
