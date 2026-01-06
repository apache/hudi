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
import org.apache.hudi.storage.StoragePath

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.HoodieSpark3CatalystPlanUtils.MatchResolvedTable
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.resolveExpressionByPlanChildren
import org.apache.spark.sql.catalyst.analysis.{AnalysisErrorAt, EliminateSubqueryAliases, NamedRelation, UnresolvedAttribute, UnresolvedPartitionSpec}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logcal.{HoodieQuery, HoodieTableChanges, HoodieTableChangesOptionsParser}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.{Table, V1Table}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isMetaField
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.analysis.HoodieSpark32PlusAnalysis.{HoodieV1OrV2Table, ResolvesToHudiTable}
import org.apache.spark.sql.hudi.catalog.HoodieInternalV2Table
import org.apache.spark.sql.hudi.command.{AlterHoodieTableDropPartitionCommand, ShowHoodieTablePartitionsCommand, TruncateHoodieTableCommand}

/**
 * NOTE: PLEASE READ CAREFULLY
 *
 * Since Hudi relations don't currently implement DS V2 Read API, we have to fallback to V1 here.
 * Such fallback will have considerable performance impact, therefore it's only performed in cases
 * where V2 API have to be used. Currently only such use-case is using of Schema Evolution feature
 *
 * Check out HUDI-4178 for more details
 */

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

    case HoodieQuery(args) =>
      val (tableName, opts) = HoodieQuery.parseOptions(args)

      val tableId = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
      val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)

      val hoodieDataSource = new DefaultSource
      val relation = hoodieDataSource.createRelation(spark.sqlContext, opts ++ Map("path" ->
        catalogTable.location.toString))

      LogicalRelation(relation, catalogTable)

    case HoodieTableChanges(args) =>
      val (tablePath, opts) = HoodieTableChangesOptionsParser.parseOptions(args, HoodieTableChanges.FUNC_NAME)
      val hoodieDataSource = new DefaultSource
      if (tablePath.contains(StoragePath.SEPARATOR)) {
        // the first param is table path
        val relation = hoodieDataSource.createRelation(spark.sqlContext, opts ++ Map("path" -> tablePath))
        LogicalRelation(relation)
      } else {
        // the first param is table identifier
        val tableId = spark.sessionState.sqlParser.parseTableIdentifier(tablePath)
        val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)
        val relation = hoodieDataSource.createRelation(spark.sqlContext, opts ++ Map("path" ->
          catalogTable.location.toString))
        LogicalRelation(relation, catalogTable)
      }
    case mO@MatchMergeIntoTable(targetTableO, sourceTableO, _)
      // START: custom Hudi change: don't want to go to the spark mit resolution so we resolve the source and target
      // if they haven't been
      if !mO.resolved =>
      lazy val analyzer = spark.sessionState.analyzer
      val targetTable = if (targetTableO.resolved) targetTableO else analyzer.execute(targetTableO)
      val sourceTable = if (sourceTableO.resolved) sourceTableO else analyzer.execute(sourceTableO)
      val originalMergeInto = mO.asInstanceOf[MergeIntoTable]
      val m = originalMergeInto.copy(
        targetTable = targetTable,
        sourceTable = sourceTable,
        mergeCondition = originalMergeInto.mergeCondition,
        matchedActions = originalMergeInto.matchedActions,
        notMatchedActions = originalMergeInto.notMatchedActions)
      // END: custom Hudi change
      EliminateSubqueryAliases(targetTable) match {
        case r: NamedRelation if r.skipSchemaResolution =>
          // Do not resolve the expression if the target table accepts any schema.
          // This allows data sources to customize their own resolution logic using
          // custom resolution rules.
          m

        case _ =>
          val newMatchedActions = m.matchedActions.map {
            case DeleteAction(deleteCondition) =>
              val resolvedDeleteCondition = deleteCondition.map(
                resolveExpressionByPlanChildren(_, m))
              DeleteAction(resolvedDeleteCondition)
            case UpdateAction(updateCondition, assignments) =>
              val resolvedUpdateCondition = updateCondition.map(
                resolveExpressionByPlanChildren(_, m))
              UpdateAction(
                resolvedUpdateCondition,
                // The update value can access columns from both target and source tables.
                resolveAssignments(assignments, m, resolveValuesWithSourceOnly = false))
            case UpdateStarAction(updateCondition) =>
              // START: custom Hudi change: filter out meta fields
              val assignments = targetTable.output.filter(a => !isMetaField(a.name)).map { attr =>
                Assignment(attr, UnresolvedAttribute(Seq(attr.name)))
              }
              // END: custom Hudi change
              UpdateAction(
                updateCondition.map(resolveExpressionByPlanChildren(_, m)),
                // For UPDATE *, the value must from source table.
                resolveAssignments(assignments, m, resolveValuesWithSourceOnly = true))
            case o => o
          }
          val newNotMatchedActions = m.notMatchedActions.map {
            case InsertAction(insertCondition, assignments) =>
              // The insert action is used when not matched, so its condition and value can only
              // access columns from the source table.
              val resolvedInsertCondition = insertCondition.map(
                resolveExpressionByPlanChildren(_, Project(Nil, m.sourceTable)))
              InsertAction(
                resolvedInsertCondition,
                resolveAssignments(assignments, m, resolveValuesWithSourceOnly = true))
            case InsertStarAction(insertCondition) =>
              // The insert action is used when not matched, so its condition and value can only
              // access columns from the source table.
              val resolvedInsertCondition = insertCondition.map(
                resolveExpressionByPlanChildren(_, Project(Nil, m.sourceTable)))
              // START: custom Hudi change: filter out meta fields
              val assignments = targetTable.output.filter(a => !isMetaField(a.name)).map { attr =>
                Assignment(attr, UnresolvedAttribute(Seq(attr.name)))
              }
              // END: custom Hudi change
              InsertAction(
                resolvedInsertCondition,
                resolveAssignments(assignments, m, resolveValuesWithSourceOnly = true))
            case o => o
          }
          val resolvedMergeCondition = resolveExpressionByPlanChildren(m.mergeCondition, m)
          m.copy(mergeCondition = resolvedMergeCondition,
            matchedActions = newMatchedActions,
            notMatchedActions = newNotMatchedActions)
      }
  }

  def resolveAssignments(
                          assignments: Seq[Assignment],
                          mergeInto: MergeIntoTable,
                          resolveValuesWithSourceOnly: Boolean): Seq[Assignment] = {
    assignments.map { assign =>
      val resolvedKey = assign.key match {
        case c if !c.resolved =>
          resolveMergeExprOrFail(c, Project(Nil, mergeInto.targetTable))
        case o => o
      }
      val resolvedValue = assign.value match {
        // The update values may contain target and/or source references.
        case c if !c.resolved =>
          if (resolveValuesWithSourceOnly) {
            resolveMergeExprOrFail(c, Project(Nil, mergeInto.sourceTable))
          } else {
            resolveMergeExprOrFail(c, mergeInto)
          }
        case o => o
      }
      Assignment(resolvedKey, resolvedValue)
    }
  }

  private def resolveMergeExprOrFail(e: Expression, p: LogicalPlan): Expression = {
    try {
      val resolved = resolveExpressionByPlanChildren(e, p)
      resolved.references.filter(!_.resolved).foreach { a =>
        // Note: This will throw error only on unresolved attribute issues,
        // not other resolution errors like mismatched data types.
        val cols = p.inputSet.toSeq.map(_.sql).mkString(", ")
        // START: custom Hudi change from spark because spark 3.4 constructor is different for fail analysis
        sparkAdapter.getCatalystPlanUtils.failAnalysisForMIT(a, cols)
        // END: custom Hudi change
      }
      resolved
    } catch {
      case x: AnalysisException => throw x
    }
  }

  private[sql] object MatchMergeIntoTable {
    def unapply(plan: LogicalPlan): Option[(LogicalPlan, LogicalPlan, Expression)] =
      sparkAdapter.getCatalystPlanUtils.unapplyMergeIntoTable(plan)
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


