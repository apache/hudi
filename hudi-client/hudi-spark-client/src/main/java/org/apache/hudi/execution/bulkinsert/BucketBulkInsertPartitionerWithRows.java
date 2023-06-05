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
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.Tuple2;

/**
 * Bulk_insert partitioner of Spark row using bucket index.
 */
public class BucketBulkInsertPartitionerWithRows implements BulkInsertPartitioner<Dataset<Row>> {

  private final String indexKeyFields;
  private final int bucketNum;

  public BucketBulkInsertPartitionerWithRows(String indexKeyFields, int bucketNum) {
    this.indexKeyFields = indexKeyFields;
    this.bucketNum = bucketNum;
  }

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> rows, int outputPartitions) {
    Partitioner partitioner = new Partitioner() {
      @Override
      public int getPartition(Object key) {
        return (Integer) key;
      }

      @Override
      public int numPartitions() {
        return outputPartitions;
      }
    };

    JavaRDD<Row> rddRows = rows.toJavaRDD()
        .mapToPair(row -> new Tuple2<>(getPartitionKey(row, this.indexKeyFields, this.bucketNum, outputPartitions), row))
        .partitionBy(partitioner)
        .values();
    return rows.sparkSession().createDataFrame(rddRows, rows.schema());
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return false;
  }

  private static int getPartitionKey(Row row, String indexKeyFields, int bucketNum, int partitionNum) {
    int bucketId = BucketIdentifier.getBucketId(row.getString(HoodieRecord.RECORD_KEY_META_FIELD_ORD), indexKeyFields, bucketNum);
    String partition = row.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD);
    if (partition == null || partition.trim().isEmpty()) {
      return bucketId;
    } else {
      int pw = (partition.hashCode() & Integer.MAX_VALUE) % partitionNum;
      return BucketIdentifier.mod(bucketId + pw, partitionNum);
    }
  }
}
