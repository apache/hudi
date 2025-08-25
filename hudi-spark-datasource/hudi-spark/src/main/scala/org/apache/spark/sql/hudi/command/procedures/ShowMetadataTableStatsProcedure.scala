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

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.metadata.HoodieBackedTableMetadata

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowMetadataTableStatsProcedure() extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "filter", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("stat_key", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("stat_value", DataTypes.StringType, nullable = true, Metadata.empty)
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
    val metaClient = createMetaClient(jsc, basePath)
    val config = HoodieMetadataConfig.newBuilder.enable(true).build
    val metadata = new HoodieBackedTableMetadata(
      new HoodieLocalEngineContext(metaClient.getStorageConf), metaClient.getStorage, config, basePath)
    val stats = metadata.stats

    val rows = new util.ArrayList[Row]
    for (entry <- stats.entrySet.asScala) {
      rows.add(Row(entry.getKey, entry.getValue))
    }
    val results = rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.evaluateFilter(results, filter, outputType, sparkSession)
    } else {
      results
    }
  }

  override def build: Procedure = new ShowMetadataTableStatsProcedure()
}

object ShowMetadataTableStatsProcedure {
  val NAME = "show_metadata_table_stats"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowMetadataTableStatsProcedure()
  }
}
