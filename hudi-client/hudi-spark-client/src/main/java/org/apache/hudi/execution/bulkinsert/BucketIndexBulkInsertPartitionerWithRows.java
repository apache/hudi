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

import org.apache.hudi.index.bucket.BucketStrategist;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.spark.sql.BucketPartitionUtils$;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Bulk_insert partitioner of Spark row using bucket index.
 */
public class BucketIndexBulkInsertPartitionerWithRows implements BulkInsertPartitioner<Dataset<Row>> {

  private final String indexKeyFields;
  protected final BucketStrategist bucketStrategist;

  public BucketIndexBulkInsertPartitionerWithRows(String indexKeyFields, BucketStrategist bucketStrategist) {
    this.indexKeyFields = indexKeyFields;
    this.bucketStrategist = bucketStrategist;
  }

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> rows, int outputPartitions) {
    return BucketPartitionUtils$.MODULE$.createDataFrame(rows, indexKeyFields, bucketStrategist, outputPartitions, true);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
