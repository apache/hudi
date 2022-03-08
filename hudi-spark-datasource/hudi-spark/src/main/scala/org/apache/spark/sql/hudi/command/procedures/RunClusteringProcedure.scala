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

import org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.{ClusteringUtils, Option => HOption}
import org.apache.hudi.config.HoodieClusteringConfig
import org.apache.hudi.exception.HoodieClusteringException
import org.apache.hudi.metadata.HoodieTableMetadata
import org.apache.hudi.{HoodieCommonUtils, SparkAdapterSupport}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

import java.util.Properties
import java.util.function.Supplier
import scala.collection.JavaConverters
import scala.collection.JavaConverters._

class RunClusteringProcedure extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport with Logging {
  /**
   * OPTIMIZE table_name|table_path [WHERE predicate]
   * [ORDER BY (col_name1 [, ...] ) ]
   */
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "path", DataTypes.StringType, None),
    ProcedureParameter.optional(2, "predicate", DataTypes.StringType, None),
    ProcedureParameter.optional(3, "order", DataTypes.StringType, None)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("timestamp", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("groups", DataTypes.IntegerType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val predicate = getArgValueOrDefault(args, PARAMETERS(2))
    val orderColumns = getArgValueOrDefault(args, PARAMETERS(3))

    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    var conf: Map[String, String] = Map.empty
    predicate match {
      case Some(p) =>
        val partitionColumnsSchema = HoodieCommonUtils.getPartitionSchemaFromProperty(metaClient, None)
        val partitionPredicate = HoodieCommonUtils.resolveFilterExpr(
          spark, p.asInstanceOf[String], partitionColumnsSchema)
        val partitionSelected = prunePartition(metaClient, partitionPredicate)
        conf = conf ++ Map(
          HoodieClusteringConfig.PLAN_PARTITION_FILTER_MODE_NAME.key() -> "SELECTED_PARTITIONS",
          HoodieClusteringConfig.PARTITION_SELECTED.key() -> partitionSelected
        )
        logInfo(s"Partition predicates: ${p}, partition selected: ${partitionSelected}")
      case _ =>
        logInfo("No partition predicates")
    }

    // Construct sort column info
    orderColumns match {
      case Some(o) =>
        validateOrderColumns(o.asInstanceOf[String], metaClient)
        conf = conf ++ Map(
          HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key() -> o.asInstanceOf[String]
        )
        logInfo(s"Order columns: ${o}")
      case _ =>
        logInfo("No order columns")
    }

    // Get all pending clustering instants
    var pendingClustering = ClusteringUtils.getAllPendingClusteringPlans(metaClient)
      .iterator().asScala.map(_.getLeft.getTimestamp).toSeq.sortBy(f => f)
    logInfo(s"Pending clustering instants: ${pendingClustering.mkString(",")}")

    val client = HoodieCommonUtils.createHoodieClientFromPath(sparkSession, basePath, conf)
    val instantTime = HoodieActiveTimeline.createNewInstantTime
    if (client.scheduleClusteringAtInstant(instantTime, HOption.empty())) {
      pendingClustering ++= Seq(instantTime)
    }
    logInfo(s"Clustering instants to run: ${pendingClustering.mkString(",")}.")

    val startTs = System.currentTimeMillis()
    pendingClustering.foreach(client.cluster(_, true))
    logInfo(s"Finish clustering all the instants: ${pendingClustering.mkString(",")}," +
      s" time cost: ${System.currentTimeMillis() - startTs}ms.")
    Seq.empty[Row]
  }

  override def build: Procedure = new RunClusteringProcedure()

  def prunePartition(metaClient: HoodieTableMetaClient, partitionPredicate: Expression): String = {
    val partitionSchema = HoodieCommonUtils.getPartitionSchemaFromProperty(metaClient, None)

    // Get tableName meta data
    val engineContext = new HoodieSparkEngineContext(new JavaSparkContext(sparkSession.sparkContext))
    val properties = new Properties()
    properties.putAll(JavaConverters.mapAsJavaMapConverter(sparkSession.sessionState.conf.getAllConfs).asJava)
    val metadataConfig = HoodieMetadataConfig.newBuilder().fromProperties(properties).build()
    val tableMetadata = HoodieTableMetadata.create(engineContext, metadataConfig, metaClient.getBasePath,
      FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue)

    val sparkParsePartitionUtil = sparkAdapter.createSparkParsePartitionUtil(sparkSession.sessionState.conf)
    val typedProperties = HoodieCommonUtils.getConfigProperties(sparkSession, Map.empty)

    val partitionColumns = metaClient.getTableConfig.getPartitionFields.orElse(Array[String]())

    // Translate all partition path to {@code org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath}
    val partitionPaths = tableMetadata.getAllPartitionPaths.asScala.map(partitionPath => {
      val partitionColumnValues = HoodieCommonUtils.parsePartitionColumnValues(
        sparkParsePartitionUtil, typedProperties, metaClient.getBasePath,
        partitionSchema, partitionColumns, partitionPath)
      new PartitionPath(partitionPath, partitionColumnValues)
    })

    // Filter partition by predicates
    val selectedPartitions = HoodieCommonUtils.prunePartition(
      partitionSchema, partitionPaths, partitionPredicate)
    selectedPartitions.map(partitionPath => partitionPath.getPath).toSet.mkString(",")
  }

  def validateOrderColumns(orderColumns: String, metaClient: HoodieTableMetaClient): Unit = {
    if (orderColumns == null) {
      throw new HoodieClusteringException("Order columns is null")
    }

    val tableSchemaResolver = new TableSchemaResolver(metaClient)
    val fields = tableSchemaResolver.getTableAvroSchema(false)
      .getFields.asScala.map(_.name().toLowerCase)
    orderColumns.split(",").foreach(col => {
      if (!fields.contains(col.toLowerCase)) {
        throw new HoodieClusteringException("Order column not exist:" + col)
      }
    })
  }

}

object RunClusteringProcedure {
  val NAME = "run_clustering"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new RunClusteringProcedure
  }
}
