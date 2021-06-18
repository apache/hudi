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

import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A built-in partitioner that only does coalesce for input Rows for bulk insert operation,
 * corresponding to the {@code BulkInsertSortMode.NONE} mode.
 *
 */
public class NonSortPartitionerWithRows implements BulkInsertPartitioner<Dataset<Row>> {

  private static final Logger LOG = LogManager.getLogger(NonSortPartitionerWithRows.class);

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> rows, int outputSparkPartitions) {
    LOG.warn("TEST_LOG. NonSortPartitionerWithRows");
    return rows.coalesce(outputSparkPartitions);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return false;
  }
}
