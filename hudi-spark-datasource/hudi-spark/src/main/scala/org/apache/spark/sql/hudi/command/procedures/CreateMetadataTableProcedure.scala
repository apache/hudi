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

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.util.HoodieTimer
import org.apache.hudi.metadata.{HoodieTableMetadata, SparkMetadataWriterFactory}
import org.apache.hudi.storage.StoragePath

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.io.FileNotFoundException
import java.util.function.Supplier

class CreateMetadataTableProcedure extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))

    val basePath = getBasePath(tableName)
    val metaClient = createMetaClient(jsc, basePath)
    val metadataPath = new StoragePath(HoodieTableMetadata.getMetadataTableBasePath(basePath))

    try {
      val statuses = metaClient.getStorage.listDirectEntries(metadataPath)
      if (!statuses.isEmpty) {
        throw new RuntimeException("Metadata directory (" + metadataPath.toString + ") not empty.")
      }
    } catch {
      case e: FileNotFoundException =>
        // Metadata directory does not exist yet
        metaClient.getStorage.createDirectory(metadataPath)
    }
    val timer = HoodieTimer.start
    val writeConfig = getWriteConfig(basePath)
    SparkMetadataWriterFactory.create(metaClient.getStorageConf, writeConfig, new HoodieSparkEngineContext(jsc), metaClient.getTableConfig)
    Seq(Row("Created Metadata Table in " + metadataPath + " (duration=" + timer.endTimer / 1000.0 + "secs)"))
  }

  override def build = new CreateMetadataTableProcedure()
}

object CreateMetadataTableProcedure {
  val NAME = "create_metadata_table"
  var metadataBaseDirectory: Option[String] = None

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new CreateMetadataTableProcedure()
  }
}
