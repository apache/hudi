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

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.hive.HiveSyncTool
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier
import scala.util.{Failure, Success, Try}

class SimpleToExtensibleBucketProcedure extends BaseProcedure with ProcedureBuilder
  with ProvidesHoodieConfig with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "partition_initial_bucket_number", DataTypes.IntegerType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.BooleanType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)
    val table = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val basePath = getBasePath(Option(table), Option.empty)
    val initialBucketNum = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Integer]
    var hiveSyncTool: HiveSyncTool = null
    var client: SparkRDDWriteClient[_] = null
    val result = Try {
      client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, Map.empty, Option(table))
      val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, table)
      val tableConfig = hoodieCatalogTable.tableConfig
      val hiveSyncConfig = buildHiveSyncConfig(sparkSession, hoodieCatalogTable, tableConfig)
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
      val hiveConf = new HiveConf
      hiveConf.addResource(hadoopConf)
      hiveSyncTool = new HiveSyncTool(hiveSyncConfig.getProps, hiveConf)
      // TODO: check if this table can be converted to extensible bucket
      // TODO: initialize the partition-level-extensible-bucket-metadata for each partition
      hiveSyncTool.updateSimpleToExtensibleBucket(initialBucketNum)
    } match {
      case Success(_) =>
        logInfo(s"Finished to sync heterogeneous metadata to hive metastore of $table.")
        true
      case Failure(e) =>
        logWarning(s"Failed: sync heterogeneous metadata to hive metastore of $table.", e)
        false
    }
    Seq(Row(result))
  }

  override def build = new SimpleToExtensibleBucketProcedure()
}

object SimpleToExtensibleBucketProcedure {
  val NAME: String = "simple_to_extensible_bucket"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): SimpleToExtensibleBucketProcedure = new SimpleToExtensibleBucketProcedure()
  }
}

