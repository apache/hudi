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
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig
import org.apache.hudi.common.util.{Functions, RemotePartitionHelper}
import org.apache.hudi.common.util.hash.BucketIndexUtil
import org.apache.hudi.index.bucket.BucketIdentifier
import org.apache.hudi.index.bucket.partition.NumBucketsFunction

import org.apache.spark.Partitioner
import org.apache.spark.sql.catalyst.InternalRow

object BucketPartitionUtils {
  def createDataFrame(df: DataFrame, indexKeyFields: String, numBucketsFunction: NumBucketsFunction, partitioner: Partitioner): DataFrame = {
    def getPartitionKeyExtractor(): InternalRow => (String, Int) = row => {
      val partition = row.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD)
      val kb = BucketIdentifier
        .getBucketId(row.getString(HoodieRecord.RECORD_KEY_META_FIELD_ORD), indexKeyFields, numBucketsFunction.getNumBuckets(partition))

      if (partition == null || partition.trim.isEmpty) {
        ("", kb)
      } else {
        (partition, kb)
      }
    }

    val getPartitionKey = getPartitionKeyExtractor()
    // use internalRow to avoid extra convert.
    val reRdd = df.queryExecution.toRdd
      .keyBy(row => getPartitionKey(row))
      .repartitionAndSortWithinPartitions(partitioner)
      .values
    df.sparkSession.internalCreateDataFrame(reRdd, df.schema)
  }

  def getRemotePartitioner(viewConf: FileSystemViewStorageConfig, numBucketsFunction: NumBucketsFunction, partitionNum: Int): Partitioner = {
    new Partitioner {
      private val helper = new RemotePartitionHelper(viewConf)

      override def numPartitions: Int = partitionNum

      override def getPartition(value: Any): Int = {
        val partitionKeyPair = value.asInstanceOf[(String, Int)]
        helper.getPartition(numBucketsFunction.getNumBuckets(partitionKeyPair._1), partitionKeyPair._1, partitionKeyPair._2, partitionNum)
      }
    }
  }

  def getLocalePartitioner(numBucketsFunction: NumBucketsFunction, partitionNum: Int): Partitioner = {
    new Partitioner {
      private val partitionIndexFunc: Functions.Function3[Integer, String, Integer, Integer] =
        BucketIndexUtil.getPartitionIndexFunc(partitionNum)

      override def numPartitions: Int = partitionNum

      override def getPartition(value: Any): Int = {
        val partitionKeyPair = value.asInstanceOf[(String, Int)]
        partitionIndexFunc.apply(numBucketsFunction.getNumBuckets(partitionKeyPair._1), partitionKeyPair._1, partitionKeyPair._2)
      }
    }
  }
}
