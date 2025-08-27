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

import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.util.HoodieTimer
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.metadata.HoodieBackedTableMetadata
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.Collections
import java.util.function.Supplier

import scala.collection.JavaConverters.asScalaIteratorConverter

class ShowMetadataTablePartitionsProcedure() extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "filter", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("partition", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0))
    val path = getArgValueOrDefault(args, PARAMETERS(1))
    val filter = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]

    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.validateFilterExpression(filter, outputType, sparkSession) match {
        case Left(errorMessage) =>
          throw new IllegalArgumentException(s"Invalid filter expression: $errorMessage")
        case Right(_) => // Validation passed, continue
      }
    }
    val basePath = getBasePath(table, path)
    val storage = new HoodieHadoopStorage(basePath, spark.sessionState.newHadoopConf())
    val config = HoodieMetadataConfig.newBuilder.enable(true).build
    val metadata = new HoodieBackedTableMetadata(new HoodieSparkEngineContext(jsc), storage, config, basePath)
    if (!metadata.enabled){
      throw new HoodieException(s"Metadata Table not enabled/initialized.")
    }

    val timer = HoodieTimer.start
    val partitions = metadata.getAllPartitionPaths
    Collections.sort(partitions)
    logDebug("Took " + timer.endTimer + " ms")

    val rows = new util.ArrayList[Row]
    partitions.stream.iterator().asScala.foreach((p: String) => {
        rows.add(Row(p))
    })
    val results = rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.evaluateFilter(results, filter, outputType, sparkSession)
    } else {
      results
    }
  }

  override def build: Procedure = new ShowMetadataTablePartitionsProcedure()
}

object ShowMetadataTablePartitionsProcedure {
  val NAME = "show_metadata_table_partitions"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowMetadataTablePartitionsProcedure()
  }
}
