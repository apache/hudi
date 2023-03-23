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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.{DataSourceReadOptions, DefaultSource, SparkAdapterSupport}
import org.apache.spark.sql.HoodieSpark3CatalystPlanUtils.MatchResolvedTable
import org.apache.spark.sql.catalyst.analysis.UnresolvedPartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.logcal.HoodieQuery
import org.apache.spark.sql.catalyst.plans.logcal.HoodieQuery.parseOptions
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.{Table, V1Table}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.analysis.HoodieSpark32PlusAnalysis.{HoodieV1OrV2Table, ResolvesToHudiTable}
import org.apache.spark.sql.hudi.catalog.HoodieInternalV2Table
import org.apache.spark.sql.hudi.command.{AlterHoodieTableDropPartitionCommand, ShowHoodieTablePartitionsCommand, TruncateHoodieTableCommand}
import org.apache.spark.sql.{AnalysisException, SQLContext, SparkSession}

/**
 * NOTE: PLEASE READ CAREFULLY
 *
 * Since Hudi relations don't currently implement DS V2 Read API, we have to fallback to V1 here.
 * Such fallback will have considerable performance impact, therefore it's only performed in cases
 * where V2 API have to be used. Currently only such use-case is using of Schema Evolution feature
 *
 * Check out HUDI-4178 for more details
 */
case class HoodieDataSourceV2ToV1Fallback(sparkSession: SparkSession) extends Rule[LogicalPlan]
  with ProvidesHoodieConfig {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // The only place we're avoiding fallback is in [[AlterTableCommand]]s since
    // current implementation relies on DSv2 features
    case _: AlterTableCommand => plan

    // NOTE: Unfortunately, [[InsertIntoStatement]] is implemented in a way that doesn't expose
    //       target relation as a child (even though there's no good reason for that)
    case iis @ InsertIntoStatement(rv2 @ DataSourceV2Relation(v2Table: HoodieInternalV2Table, _, _, _, _), _, _, _, _, _) =>
      iis.copy(table = convertToV1(rv2, v2Table))

    case _ =>
      plan.resolveOperatorsDown {
        case rv2 @ DataSourceV2Relation(v2Table: HoodieInternalV2Table, _, _, _, _) => convertToV1(rv2, v2Table)
      }
  }

  private def convertToV1(rv2: DataSourceV2Relation, v2Table: HoodieInternalV2Table) = {
    val output = rv2.output
    val catalogTable = v2Table.catalogTable.map(_ => v2Table.v1Table)
    val relation = new DefaultSource().createRelation(new SQLContext(sparkSession),
      buildHoodieConfig(v2Table.hoodieCatalogTable), v2Table.hoodieCatalogTable.tableSchema)

    LogicalRelation(relation, output, catalogTable, isStreaming = false)
  }
}

/**
 * Rule for resolve hoodie's extended syntax or rewrite some logical plan.
 */
case class HoodieSpark32PlusResolveReferences(spark: SparkSession) extends Rule[LogicalPlan]
  with SparkAdapterSupport with ProvidesHoodieConfig {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    case TimeTravelRelation(ResolvesToHudiTable(table), timestamp, version) =>
      if (timestamp.isEmpty && version.nonEmpty) {
        throw new AnalysisException("Version expression is not supported for time travel")
      }

      val pathOption = table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
      val dataSource =
        DataSource(
          spark,
          userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
          partitionColumns = table.partitionColumnNames,
          bucketSpec = table.bucketSpec,
          className = table.provider.get,
          options = table.storage.properties ++ pathOption ++ Map(
            DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key -> timestamp.get.toString()),
          catalogTable = Some(table))

      val relation = dataSource.resolveRelation(checkFilesExist = false)

      LogicalRelation(relation, table)

    case q: HoodieQuery =>
      val (tableName, opts) = parseOptions(q.args)

      val tableId = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
      val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)

      val hoodieDataSource = new DefaultSource
      val relation = hoodieDataSource.createRelation(spark.sqlContext, opts ++ Map("path" ->
        catalogTable.location.toString))

      LogicalRelation(relation, catalogTable)
  }
}

/**
 * Rule replacing resolved Spark's commands (not working for Hudi tables out-of-the-box) with
 * corresponding Hudi implementations
 */
case class HoodieSpark32PlusPostAnalysisRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case ShowPartitions(MatchResolvedTable(_, id, HoodieV1OrV2Table(_)), specOpt, _) =>
        ShowHoodieTablePartitionsCommand(
          id.asTableIdentifier, specOpt.map(s => s.asInstanceOf[UnresolvedPartitionSpec].spec))

      // Rewrite TruncateTableCommand to TruncateHoodieTableCommand
      case TruncateTable(MatchResolvedTable(_, id, HoodieV1OrV2Table(_))) =>
        TruncateHoodieTableCommand(id.asTableIdentifier, None)

      case TruncatePartition(MatchResolvedTable(_, id, HoodieV1OrV2Table(_)), partitionSpec: UnresolvedPartitionSpec) =>
        TruncateHoodieTableCommand(id.asTableIdentifier, Some(partitionSpec.spec))

      case DropPartitions(MatchResolvedTable(_, id, HoodieV1OrV2Table(_)), specs, ifExists, purge) =>
        AlterHoodieTableDropPartitionCommand(
          id.asTableIdentifier,
          specs.seq.map(f => f.asInstanceOf[UnresolvedPartitionSpec]).map(s => s.spec),
          ifExists,
          purge,
          retainData = true
        )

      case _ => plan
    }
  }
}

object HoodieSpark32PlusAnalysis extends SparkAdapterSupport {

  private[sql] object HoodieV1OrV2Table {
    def unapply(table: Table): Option[CatalogTable] = table match {
      case V1Table(catalogTable) if sparkAdapter.isHoodieTable(catalogTable) => Some(catalogTable)
      case v2: HoodieInternalV2Table => v2.catalogTable
      case _ => None
    }
  }

  // TODO dedup w/ HoodieAnalysis
  private[sql] object ResolvesToHudiTable {
    def unapply(plan: LogicalPlan): Option[CatalogTable] =
      sparkAdapter.resolveHoodieTable(plan)
  }
}


