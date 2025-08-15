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

import org.apache.hudi.{HoodieBaseRelation, HoodieFileIndex}
import org.apache.hudi.SparkAdapterSupport.sparkAdapter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, AttributeSet, Expression, ExpressionSet, NamedExpression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.hudi.analysis.Spark4HoodiePruneFileSourcePartitions.{exprUtils, getPartitionFiltersAndDataFilters, rebuildPhysicalOperation, HoodieRelationMatcher}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

/**
 * Prune the partitions of Hudi table based relations by the means of pushing down the
 * partition filters
 *
 * NOTE: [[HoodiePruneFileSourcePartitions]] is a replica in kind to Spark's [[PruneFileSourcePartitions]]
 */
case class Spark4HoodiePruneFileSourcePartitions(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case op @ PhysicalOperation(projects, filters, lr @ LogicalRelation(HoodieRelationMatcher(fileIndex), _, _, _, _))
      if !fileIndex.hasPredicatesPushedDown =>

      val deterministicFilters = filters.filter(f => f.deterministic && !SubqueryExpression.hasSubquery(f))
      val normalizedFilters = exprUtils.normalizeExprs(deterministicFilters, lr.output)

      val (partitionPruningFilters, dataFilters) =
        getPartitionFiltersAndDataFilters(fileIndex.partitionSchema, normalizedFilters)

      // [[HudiFileIndex]] is a caching one, therefore we don't need to reconstruct new relation,
      // instead we simply just refresh the index and update the stats
      fileIndex.filterFileSlices(dataFilters, partitionPruningFilters, isPartitionPruned = true)

      if (partitionPruningFilters.nonEmpty) {
        // Change table stats based on the sizeInBytes of pruned files
        val filteredStats = FilterEstimation(Filter(partitionPruningFilters.reduce(And), lr)).estimate
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

        val prunedLogicalRelation = lr.copy(catalogTable = tableWithStats)
        // Keep partition-pruning predicates so that they are visible in physical planning
        rebuildPhysicalOperation(projects, filters, prunedLogicalRelation)
      } else {
        op
      }
  }

}

private object Spark4HoodiePruneFileSourcePartitions extends PredicateHelper {

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
