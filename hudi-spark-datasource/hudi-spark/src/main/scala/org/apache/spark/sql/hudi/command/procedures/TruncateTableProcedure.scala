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

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.Properties
import java.util.function.Supplier
import scala.collection.JavaConverters._

class TruncateTableProcedure extends BaseProcedure
  with ProcedureBuilder
  with Logging {
  override def build: Procedure = new TruncateTableProcedure

  override def parameters: Array[ProcedureParameter] = Array[ProcedureParameter] (
    ProcedureParameter.required(0, "table", DataTypes.StringType)
  )

  override def outputType: StructType = StructType(Array[StructField](
    StructField("result", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("time_cost", DataTypes.LongType, nullable = true, Metadata.empty)
  ))

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(parameters, args)

    val tableName = getArgValueOrDefault(args, parameters(0)).get.asInstanceOf[String]
    logInfo(s"start execute truncate table procedure for $tableName")

    val basePath: String = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, tableName).tableLocation

    val context = sparkSession.sparkContext
    val conf = context.hadoopConfiguration

    val targetPath = new StoragePath(basePath)
    val engineContext = new HoodieSparkEngineContext(context)

    val metaClient = HoodieTableMetaClient
      .builder()
      .setBasePath(basePath)
      .setConf(HadoopFSUtils.getStorageConfWithCopy(conf))
      .build()

    val tableConfig = metaClient.getTableConfig

    val properties: Properties = generateProperties(tableConfig)

    val startTime = System.currentTimeMillis()
    try {
      val storage = HoodieStorageUtils.getStorage(
        basePath, HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf))

      FSUtils.deleteDir(engineContext, storage, targetPath, context.defaultParallelism)

      // ReInit hoodie.properties
      HoodieTableMetaClient.newTableBuilder()
        .fromProperties(properties)
        .initTable(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration), basePath)

      logInfo(s"Success to execute truncate table procedure for ${tableName}")
      Seq(Row("SUCCESS", System.currentTimeMillis() - startTime))
    } catch {
      case e: Exception =>
        logError(s"Fail to execute truncate table procedure for ${tableName}", e)
        Seq(Row("FAILED", System.currentTimeMillis() - startTime))
    }
  }

  private def generateProperties(config: HoodieTableConfig): Properties = {
    val properties = new Properties()
    config.getProps.entrySet().asScala.foreach { entry =>
      properties.setProperty(entry.getKey.toString, entry.getValue.toString)
    }
    properties
  }
}

object TruncateTableProcedure {
  val NAME = "truncate_table"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new TruncateTableProcedure
  }
}
