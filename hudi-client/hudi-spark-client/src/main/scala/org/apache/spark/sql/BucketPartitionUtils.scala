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

package org.apache.spark.sql

import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.index.bucket.BucketIdentifier
import org.apache.spark.Partitioner
import org.apache.spark.sql.catalyst.InternalRow

object BucketPartitionUtils {
  def createDataFrame(df: DataFrame, indexKeyFields: String, bucketNum: Int, partitionNum: Int): DataFrame = {
    def getPartitionKeyExtractor(): InternalRow => (String, Int) = row => {
      val kb = BucketIdentifier
        .getBucketId(row.getString(HoodieRecord.RECORD_KEY_META_FIELD_ORD), indexKeyFields, bucketNum)
      val partition = row.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD)
      if (partition == null || partition.trim.isEmpty) {
        ("", kb)
      } else {
        (partition, kb)
      }
    }

    val getPartitionKey = getPartitionKeyExtractor()
    val partitioner = new Partitioner {
      override def numPartitions: Int = partitionNum

      override def getPartition(key: Any): Int = {
        val t = key.asInstanceOf[(String, Int)]
        val pw = (t._1.hashCode & Int.MaxValue) % partitionNum
        BucketIdentifier.mod(t._2 + pw, partitionNum)
      }
    }
    // use internalRow to avoid extra convert.
    val reRdd = df.queryExecution.toRdd
      .keyBy(row => getPartitionKey(row))
      .repartitionAndSortWithinPartitions(partitioner)
      .values
    df.sparkSession.internalCreateDataFrame(reRdd, df.schema)
  }
}
