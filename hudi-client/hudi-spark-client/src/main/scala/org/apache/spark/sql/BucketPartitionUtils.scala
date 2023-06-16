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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object BucketPartitionUtils {
  def createDataFrame(df: DataFrame, indexKeyFields: String, bucketNum: Int, partitionNum: Int): DataFrame = {
    def getPartitionKeyExtractor(): InternalRow => String = row => {
      val kb = BucketIdentifier
        .getBucketId(row.getString(HoodieRecord.RECORD_KEY_META_FIELD_ORD), indexKeyFields, bucketNum)
      val partition = row.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD)
      partition + "-" + kb
    }

    val keyExtractor = getPartitionKeyExtractor()
    val keyedSchema = StructType.apply(Seq(StructField("_fg", StringType), StructField("_row", df.schema)))
    // use internalRow to avoid extra convert.
    val reRdd = df.queryExecution.toRdd
      .map(row => InternalRow.fromSeq(Seq(keyExtractor(row), row)))

    // transform back to dataframe first in order to use partition local sort
    df.sparkSession.internalCreateDataFrame(reRdd, keyedSchema)
      .repartition(partitionNum, col("_fg"))
      .sortWithinPartitions("_fg")
      .select(col("_row"))
  }
}
