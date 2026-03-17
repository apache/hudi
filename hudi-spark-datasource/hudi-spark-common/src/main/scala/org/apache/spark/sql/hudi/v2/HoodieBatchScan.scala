/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.v2

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * Batch scan for snapshot reads via DSv2 (COW).
 */
class HoodieBatchScan(readSchema: StructType,
                      inputPartitions: Array[InputPartition],
                      broadcastReader: Broadcast[SparkColumnarFileReader],
                      broadcastConf: Broadcast[SerializableConfiguration],
                      requiredDataSchema: StructType,
                      requiredPartitionSchema: StructType,
                      pushedFilters: Array[Filter] = Array.empty,
                      pushedLimit: Option[Int] = None) extends Scan with Batch with SupportsReportStatistics {

  override def readSchema(): StructType = readSchema

  override def description(): String = {
    val filtersStr = if (pushedFilters.nonEmpty) {
      s", PushedFilters: [${pushedFilters.mkString(", ")}]"
    } else {
      ", PushedFilters: []"
    }
    val limitStr = pushedLimit.map(l => s", PushedLimit: $l").getOrElse("")
    s"HoodieBatchScan${readSchema.catalogString}$filtersStr$limitStr"
  }

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = inputPartitions

  override def createReaderFactory(): PartitionReaderFactory = {
    new HoodiePartitionReaderFactory(
      broadcastReader,
      broadcastConf,
      readSchema,
      requiredDataSchema,
      requiredPartitionSchema,
      pushedLimit)
  }

  override def estimateStatistics(): Statistics = {
    val totalSize = inputPartitions.collect {
      case p: HoodieInputPartition => p.baseFileLength
    }.sum
    new HoodieStatistics(totalSize)
  }
}
