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

import org.apache.hudi.{AvroConversionUtils, HoodieCLIUtils, HoodieFileIndex}
import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.{ClusteringUtils, HoodieTimer, Option => HOption}
import org.apache.hudi.common.util.ValidationUtils.checkArgument
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieLockConfig}
import org.apache.hudi.exception.HoodieClusteringException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.HoodieCatalystExpressionUtils.{resolveExpr, splitPartitionAndDataPredicates}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.execution.datasources.FileStatusCache
import org.apache.spark.sql.types._

import java.util.function.Supplier

import scala.collection.JavaConverters._

class RunClusteringProcedure extends BaseProcedure
  with ProcedureBuilder
  with PredicateHelper
  with Logging {

  /**
   * OPTIMIZE table_name|table_path [WHERE predicate]
   * [ORDER BY (col_name1 [, ...] ) ]
   */
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "predicate", DataTypes.StringType),
    ProcedureParameter.optional(3, "order", DataTypes.StringType),
    ProcedureParameter.optional(4, "show_involved_partition", DataTypes.BooleanType, false),
    ProcedureParameter.optional(5, "op", DataTypes.StringType),
    ProcedureParameter.optional(6, "order_strategy", DataTypes.StringType),
    // params => key=value, key2=value2
    ProcedureParameter.optional(7, "options", DataTypes.StringType),
    ProcedureParameter.optional(8, "instants", DataTypes.StringType),
    ProcedureParameter.optional(9, "selected_partitions", DataTypes.StringType),
    ProcedureParameter.optional(10, "selected_partitions_reg", DataTypes.StringType),
    ProcedureParameter.optional(11, "limit", DataTypes.IntegerType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("timestamp", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("input_group_size", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("involved_partitions", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val predicate = getArgValueOrDefault(args, PARAMETERS(2))
    val orderColumns = getArgValueOrDefault(args, PARAMETERS(3))
    val showInvolvedPartitions = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[Boolean]
    val op = getArgValueOrDefault(args, PARAMETERS(5))
    val orderStrategy = getArgValueOrDefault(args, PARAMETERS(6))
    val options = getArgValueOrDefault(args, PARAMETERS(7))
    val specificInstants = getArgValueOrDefault(args, PARAMETERS(8))
    val parts = getArgValueOrDefault(args, PARAMETERS(9))
    val partsReg = getArgValueOrDefault(args, PARAMETERS(10))
    val limit = getArgValueOrDefault(args, PARAMETERS(11))

    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)
    var confs: Map[String, String] = Map.empty

    val selectedPartitions: String = (parts, predicate) match {
      case (_, Some(p)) => prunePartition(metaClient, p.asInstanceOf[String])
      case (Some(o), _) => o.asInstanceOf[String]
      case _ => null
    }

    if (selectedPartitions == null) {
      logInfo("No partition selected")
      if (partsReg.isDefined) {
        confs = confs ++ Map(
          HoodieClusteringConfig.PARTITION_REGEX_PATTERN.key() -> partsReg.get.asInstanceOf[String]
        )
      }
    } else if (selectedPartitions.isEmpty) {
      logInfo("No partition matched")
      // scalastyle:off return
      return Seq()
      // scalastyle:on return
    } else {
      confs = confs ++ Map(
        HoodieClusteringConfig.PLAN_PARTITION_FILTER_MODE_NAME.key() -> "SELECTED_PARTITIONS",
        HoodieClusteringConfig.PARTITION_SELECTED.key() -> selectedPartitions
      )
      logInfo(s"Partition selected: $selectedPartitions")
    }

    // Construct sort column info
    orderColumns match {
      case Some(o) =>
        validateOrderColumns(o.asInstanceOf[String], metaClient)
        confs = confs ++ Map(
          HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key() -> o.asInstanceOf[String]
        )
        logInfo(s"Order columns: $o")
      case _ =>
        logInfo("No order columns")
    }

    orderStrategy match {
      case Some(o) =>
        val strategy = HoodieClusteringConfig.resolveLayoutOptimizationStrategy(o.asInstanceOf[String])
        confs = confs ++ Map(
          HoodieClusteringConfig.LAYOUT_OPTIMIZE_STRATEGY.key() -> strategy.name()
        )
      case _ =>
        logInfo("No order strategy")
    }

    options match {
      case Some(p) =>
        confs = confs ++ HoodieCLIUtils.extractOptions(p.asInstanceOf[String])
      case _ =>
        logInfo("No options")
    }

    val pendingClusteringInstants = ClusteringUtils.getAllPendingClusteringPlans(metaClient)
      .iterator().asScala.map(_.getLeft.requestedTime).toSeq.sortBy(f => f)

    var (filteredPendingClusteringInstants, operation) = HoodieProcedureUtils.filterPendingInstantsAndGetOperation(
      pendingClusteringInstants, specificInstants.asInstanceOf[Option[String]], op.asInstanceOf[Option[String]], limit.asInstanceOf[Option[Int]])

    var client: SparkRDDWriteClient[_] = null
    try {
      client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, confs,
        tableName.asInstanceOf[Option[String]])
      if (metaClient.getTableConfig.isMetadataTableAvailable) {
        if (!confs.contains(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key)) {
          confs = confs ++ HoodieCLIUtils.getLockOptions(basePath, metaClient.getBasePath.toUri.getScheme, client.getConfig.getCommonConfig.getProps())
        }
      }
      if (operation.isSchedule) {
        val instantTime = client.scheduleClustering(HOption.empty())
        instantTime.ifPresent(instant => {
          filteredPendingClusteringInstants = Seq(instant)
        })
      }
      logInfo(s"Clustering instants to run: ${filteredPendingClusteringInstants.mkString(",")}.")

      if (operation.isExecute) {
        val timer = HoodieTimer.start
        filteredPendingClusteringInstants.foreach(client.cluster(_, true))
        logInfo(s"Finish clustering at instants: ${filteredPendingClusteringInstants.mkString(",")}," +
          s" spend: ${timer.endTimer()}ms.")
      }

      val clusteringInstants = metaClient.reloadActiveTimeline().getInstants.iterator().asScala
        .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION && filteredPendingClusteringInstants.contains(p.requestedTime))
        .toSeq
        .sortBy(f => f.requestedTime)
        .reverse

      val clusteringPlans = clusteringInstants.map(instant =>
        ClusteringUtils.getClusteringPlan(metaClient, instant)
      )

      if (showInvolvedPartitions) {
        clusteringPlans.map { p =>
          Row(p.get().getLeft.requestedTime, p.get().getRight.getInputGroups.size(),
            p.get().getLeft.getState.name(), HoodieCLIUtils.extractPartitions(p.get().getRight.getInputGroups.asScala.toSeq))
        }
      } else {
        clusteringPlans.map { p =>
          Row(p.get().getLeft.requestedTime, p.get().getRight.getInputGroups.size(), p.get().getLeft.getState.name(), "*")
        }
      }
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  override def build: Procedure = new RunClusteringProcedure()

  def prunePartition(metaClient: HoodieTableMetaClient, predicate: String): String = {
    val options = Map(QUERY_TYPE.key() -> QUERY_TYPE_SNAPSHOT_OPT_VAL, "path" -> metaClient.getBasePath.toString)
    val hoodieFileIndex = HoodieFileIndex(sparkSession, metaClient, None, options,
      FileStatusCache.getOrCreate(sparkSession))

    // Resolve partition predicates
    val schemaResolver = new TableSchemaResolver(metaClient)
    val tableSchema = AvroConversionUtils.convertAvroSchemaToStructType(schemaResolver.getTableAvroSchema)
    val condition = resolveExpr(sparkSession, predicate, tableSchema)
    val partitionColumns = metaClient.getTableConfig.getPartitionFields.orElse(Array[String]())
    val (partitionPredicates, dataPredicates) = splitPartitionAndDataPredicates(
      sparkSession, splitConjunctivePredicates(condition).toArray, partitionColumns)
    checkArgument(dataPredicates.isEmpty, "Only partition predicates are allowed")

    // Get all partitions and prune partition by predicates
    val prunedPartitions = hoodieFileIndex.getPartitionPaths(partitionPredicates)
    prunedPartitions.map(partitionPath => partitionPath.getPath).toSet.mkString(",")
  }

  private def validateOrderColumns(orderColumns: String, metaClient: HoodieTableMetaClient): Unit = {
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
