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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.common.bootstrap.index.BootstrapIndex
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class ShowBootstrapPartitionsProcedure extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "filter", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("indexed_partitions", DataTypes.StringType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val filter = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]

    validateFilter(filter, outputType)
    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val indexReader = createBootstrapIndexReader(metaClient)
    val indexedPartitions = indexReader.getIndexedPartitionPaths

    val results = indexedPartitions.stream().toArray.map(r => Row(r)).toList
    applyFilter(results, filter, outputType)
  }

  private def createBootstrapIndexReader(metaClient: HoodieTableMetaClient) = {
    val index = BootstrapIndex.getBootstrapIndex(metaClient)
    if (!index.useIndex) throw new HoodieException("This is not a bootstrapped Hudi table. Don't have any index info")
    index.createReader
  }

  override def build = new ShowBootstrapPartitionsProcedure()
}

object ShowBootstrapPartitionsProcedure {
  val NAME = "show_bootstrap_partitions"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowBootstrapPartitionsProcedure
  }
}
