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
import org.apache.hudi.common.util.{HoodieTimer, StringUtils}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.metadata.HoodieBackedTableMetadata
import org.apache.hudi.storage.{StoragePath, StoragePathInfo}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowMetadataTableFilesProcedure() extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "partition", DataTypes.StringType, ""),
    ProcedureParameter.optional(3, "limit", DataTypes.IntegerType, 100),
    ProcedureParameter.optional(4, "filter", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("file_path", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0))
    val path = getArgValueOrDefault(args, PARAMETERS(1))
    val partition = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val limit = getArgValueOrDefault(args, PARAMETERS(3))
    val filter = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]

    validateFilter(filter, outputType)
    val basePath = getBasePath(table, path)
    val metaClient = createMetaClient(jsc, basePath)
    val config = HoodieMetadataConfig.newBuilder.enable(true).build
    val metaReader = new HoodieBackedTableMetadata(
      new HoodieLocalEngineContext(metaClient.getStorageConf), metaClient.getStorage, config, basePath)
    if (!metaReader.enabled){
      throw new HoodieException(s"Metadata Table not enabled/initialized.")
    }

    var partitionPath = new StoragePath(basePath)
    if (!StringUtils.isNullOrEmpty(partition)) {
      partitionPath = new StoragePath(basePath, partition)
    }

    val timer = HoodieTimer.start
    val statuses = metaReader.getAllFilesInPartition(partitionPath)
    logDebug("Took " + timer.endTimer + " ms")

    val rows = new util.ArrayList[Row]
    statuses.asScala.sortBy(p => p.getPath.getName).foreach((f: StoragePathInfo) => {
      rows.add(Row(f.getPath.getName))
    })
    val results = if (limit.isDefined) {
      rows.stream().limit(limit.get.asInstanceOf[Int]).toArray().map(r => r.asInstanceOf[Row]).toList
    } else {
      rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
    }
    applyFilter(results, filter, outputType)
  }

  override def build: Procedure = new ShowMetadataTableFilesProcedure()
}

object ShowMetadataTableFilesProcedure {
  val NAME = "show_metadata_table_files"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowMetadataTableFilesProcedure()
  }
}
