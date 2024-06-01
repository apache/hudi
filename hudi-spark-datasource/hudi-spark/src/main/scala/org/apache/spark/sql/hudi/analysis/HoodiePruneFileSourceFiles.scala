/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.HoodieFileIndex.DataSkippingFailureMode
import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.{HoodieBaseRelation, HoodieFileIndex}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, AttributeSet, Expression, ExpressionSet, NamedExpression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.hudi.analysis.HoodiePruneFileSourceFiles.{HoodieRelationMatcher, exprUtils, getPartitionFiltersAndDataFilters, rebuildPhysicalOperation}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success}

case class HoodiePruneFileSourceFiles(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case op@PhysicalOperation(projects, filters, lr@LogicalRelation(HoodieRelationMatcher(fileIndex), _, _, _))
      if !fileIndex.hasPredicatesPushedDown =>

      val (_, normalizedFilters) = prepareFilters(filters, lr)

      val (partitionFilters, dataFilters) =
        getPartitionFiltersAndDataFilters(fileIndex.partitionSchema, normalizedFilters)

      // Apply partition pruning as in the existing rule
      fileIndex.listFiles(partitionFilters, dataFilters)

      // Now apply data filters
      val candidateFilesNamesOpt: Option[Set[String]] =
        fileIndex.lookupCandidateFilesInMetadataTable(dataFilters, Seq(), shouldPushDownFilesFilter = true) match {
          case Success(opt) => opt
          case Failure(e) =>
            logError("Failed to lookup candidate files in File Index", e)

            spark.sqlContext.getConf(DataSkippingFailureMode.configName, DataSkippingFailureMode.Fallback.value) match {
              case DataSkippingFailureMode.Fallback.value => Option.empty
              case DataSkippingFailureMode.Strict.value => throw new HoodieException(e);
            }
        }

      if (candidateFilesNamesOpt.nonEmpty) {
        // Change table stats based on the sizeInBytes of pruned files
        val prunedLogicalRelation = updateLogicalRelation(lr, dataFilters, fileIndex)
        // Keep partition-pruning predicates so that they are visible in physical planning
        rebuildPhysicalOperation(projects, filters, prunedLogicalRelation)
      } else {
        op
      }
  }

  private def prepareFilters(filters: Seq[Expression], lr: LogicalRelation): (Seq[Expression], Seq[Expression]) = {
    val deterministicFilters = filters.filter(f => f.deterministic && !SubqueryExpression.hasSubquery(f))
    val normalizedFilters = exprUtils.normalizeExprs(deterministicFilters, lr.output)
    (deterministicFilters, normalizedFilters)
  }

  private def updateLogicalRelation(lr: LogicalRelation, dataFilters: Seq[Expression], fileIndex: HoodieFileIndex): LogicalRelation = {
    // Update the LogicalRelation with new stats or file list as needed
    val filteredStats = FilterEstimation(Filter(dataFilters.reduce(And), lr)).estimate
    val colStats = filteredStats.map {
      _.attributeStats.map { case (attr, colStat) =>
        (attr.name, colStat.toCatalogColumnStat(attr.name, attr.dataType))
      }
    }

    val tableWithStats = lr.catalogTable.map(_.copy(
      stats = Some(
        CatalogStatistics(
          sizeInBytes = BigInt(fileIndex.sizeInBytes),
          rowCount = filteredStats.flatMap(_.rowCount),
          colStats = colStats.getOrElse(Map.empty)))
    ))

    lr.copy(catalogTable = tableWithStats)
  }

}

private object HoodiePruneFileSourceFiles extends PredicateHelper {

  private val exprUtils = sparkAdapter.getCatalystExpressionUtils

  private object HoodieRelationMatcher {
    def unapply(relation: BaseRelation): Option[HoodieFileIndex] = relation match {
      case HadoopFsRelation(fileIndex: HoodieFileIndex, _, _, _, _, _) => Some(fileIndex)
      case r: HoodieBaseRelation => Some(r.fileIndex)
      case _ => None
    }
  }

  private def rebuildPhysicalOperation(projects: Seq[NamedExpression],
                                       filters: Seq[Expression],
                                       relation: LeafNode): Project = {
    val withFilter = if (filters.nonEmpty) {
      val filterExpression = filters.reduceLeft(And)
      Filter(filterExpression, relation)
    } else {
      relation
    }
    Project(projects, withFilter)
  }

  def getPartitionFiltersAndDataFilters(partitionSchema: StructType,
                                        normalizedFilters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    val partitionColumns = normalizedFilters.flatMap { expr =>
      expr.collect {
        case attr: AttributeReference if partitionSchema.names.contains(attr.name) =>
          attr
      }
    }
    val partitionSet = AttributeSet(partitionColumns)
    val (partitionFilters, dataFilters) = normalizedFilters.partition(f =>
      f.references.subsetOf(partitionSet)
    )
    val extraPartitionFilter =
      dataFilters.flatMap(exprUtils.extractPredicatesWithinOutputSet(_, partitionSet))
    (ExpressionSet(partitionFilters ++ extraPartitionFilter).toSeq, dataFilters)
  }

}
