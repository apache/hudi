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
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.{HoodieCleanConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}
import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class UpgradeOrDowngradeProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "to_version", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.BooleanType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val toVersion = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val config = getWriteConfigWithTrue(tableName)
    val basePath = config.getBasePath
    val metaClient = HoodieTableMetaClient.builder
      .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration))
      .setBasePath(config.getBasePath)
      .setLoadActiveTimelineOnLoad(false)
      .setConsistencyGuardConfig(config.getConsistencyGuardConfig)
      .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion)))
      .setFileSystemRetryConfig(config.getFileSystemRetryConfig)
      .build

    val result = Try {
      new UpgradeDowngrade(metaClient, config, new HoodieSparkEngineContext(jsc), SparkUpgradeDowngradeHelper.getInstance)
        .run(HoodieTableVersion.valueOf(toVersion), null)
    } match {
      case Success(_) =>
        logInfo(s"Table at $basePath upgraded / downgraded to version $toVersion.")
        true
      case Failure(e) =>
        logWarning(s"Failed: Could not upgrade/downgrade table at $basePath to version $toVersion.", e)
        false
    }

    Seq(Row(result))
  }

  private def getWriteConfigWithTrue(tableOpt: scala.Option[Any]) = {
    val basePath = getBasePath(tableOpt)
    val (tableName, database) = HoodieCLIUtils.getTableIdentifier(tableOpt.get.asInstanceOf[String])
    HoodieWriteConfig.newBuilder
      .forTable(tableName)
      .withPath(basePath)
      .withRollbackUsingMarkers(true)
      .withCleanConfig(HoodieCleanConfig.newBuilder.withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build)
      .withIndexConfig(HoodieIndexConfig.newBuilder.withIndexType(HoodieIndex.IndexType.BLOOM).build)
      .withProps(Map(HoodieTableConfig.DATABASE_NAME.key -> database.getOrElse(sparkSession.sessionState.catalog.getCurrentDatabase)).asJava)
      .build
  }

  override def build = new UpgradeOrDowngradeProcedure()
}

object UpgradeTableProcedure {
  val NAME = "upgrade_table"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new UpgradeOrDowngradeProcedure()
  }
}

object DowngradeTableProcedure {
  val NAME = "downgrade_table"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new UpgradeOrDowngradeProcedure()
  }
}
