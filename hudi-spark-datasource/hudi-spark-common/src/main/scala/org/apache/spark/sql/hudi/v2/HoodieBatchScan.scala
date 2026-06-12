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

import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.internal.schema.InternalSchema

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * Batch scan for snapshot reads via DSv2 (COW).
 */
class HoodieBatchScan(outputSchema: StructType,
                      inputPartitions: Array[InputPartition],
                      broadcastReader: Broadcast[SparkColumnarFileReader],
                      broadcastConf: Broadcast[SerializableConfiguration],
                      requiredDataSchema: StructType,
                      requiredPartitionSchema: StructType,
                      internalSchemaOpt: HOption[InternalSchema] = HOption.empty(),
                      pushedFilters: Array[Filter] = Array.empty,
                      pushedLimit: Option[Int] = None,
                      tableAvroSchema: HOption[HoodieSchema] = HOption.empty())
  extends Scan with Batch with SupportsReportStatistics {

  override def readSchema(): StructType = outputSchema

  override def description(): String = {
    val filtersStr = s", PushedFilters: [${pushedFilters.mkString(", ")}]"
    val limitStr = pushedLimit.map(l => s", PushedLimit: $l").getOrElse("")
    s"HoodieBatchScan ${outputSchema.catalogString}$filtersStr$limitStr"
  }

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = inputPartitions

  override def createReaderFactory(): PartitionReaderFactory = {
    new HoodiePartitionReaderFactory(
      broadcastReader,
      broadcastConf,
      outputSchema,
      requiredDataSchema,
      requiredPartitionSchema,
      internalSchemaOpt,
      pushedFilters,
      pushedLimit,
      tableAvroSchema)
  }

  override def estimateStatistics(): Statistics = {
    // length is per-split; summing across all splits of one base file equals the
    // file size, so the total remains a faithful byte-size estimate.
    val totalSize = inputPartitions.collect {
      case p: HoodieInputPartition => p.length
    }.sum
    new HoodieStatistics(totalSize)
  }
}
