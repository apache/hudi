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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.hudi.HoodieBaseRelation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, BinaryComparison, DynamicPruning, DynamicPruningSubquery, EqualTo, Expression, In, InSet, MultiLikeBase, Not, Or, PredicateHelper, StringPredicate, StringRegexExpression}
import org.apache.spark.sql.catalyst.optimizer.{PruneFilters, PushDownPredicates}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{DYNAMIC_PRUNING_EXPRESSION, DYNAMIC_PRUNING_SUBQUERY}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

class Spark32PartitionPruning(spark: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  private var pruned = false

  /**
   * MODIFIED to match on hoodieBaseRelation
   */
  def getFilterableTableScan(a: Expression, plan: LogicalPlan): Option[LogicalPlan] = {
    val srcInfo: Option[(Expression, LogicalPlan)] = findExpressionAndTrackLineageDown(a, plan)
    srcInfo.flatMap {
      case (resExp, l: LogicalRelation) =>
        l.relation match {
          case hudi: HoodieBaseRelation =>
            val partitionColumns = AttributeSet(
              l.resolve(hudi.partitionSchema, spark.sessionState.analyzer.resolver))
            if (resExp.references.subsetOf(partitionColumns)) {
              Some(l)
            } else {
              None
            }
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Insert a dynamic partition pruning predicate on one side of the join using the filter on the
   * other side of the join.
   *  - to be able to identify this filter during query planning, we use a custom
   *    DynamicPruning expression that wraps a regular In expression
   *  - we also insert a flag that indicates if the subquery duplication is worthwhile and it
   *  should run regardless of the join strategy, or is too expensive and it should be run only if
   *  we can reuse the results of a broadcast
   */
  private def insertPredicate(
                               pruningKey: Expression,
                               pruningPlan: LogicalPlan,
                               filteringKey: Expression,
                               filteringPlan: LogicalPlan,
                               joinKeys: Seq[Expression],
                               partScan: LogicalPlan): LogicalPlan = {
    val reuseEnabled = conf.exchangeReuseEnabled
    val index = joinKeys.indexOf(filteringKey)
    lazy val hasBenefit = pruningHasBenefit(pruningKey, partScan, filteringKey, filteringPlan)
    if (reuseEnabled || hasBenefit) {
      // insert a DynamicPruning wrapper to identify the subquery during query planning
      Filter(
        DynamicPruningSubquery(
          pruningKey,
          filteringPlan,
          joinKeys,
          index,
          conf.dynamicPartitionPruningReuseBroadcastOnly || !hasBenefit),
        pruningPlan)
    } else {
      // abort dynamic partition pruning
      pruningPlan
    }
  }

  /**
   * Given an estimated filtering ratio we assume the partition pruning has benefit if
   * the size in bytes of the partitioned plan after filtering is greater than the size
   * in bytes of the plan on the other side of the join. We estimate the filtering ratio
   * using column statistics if they are available, otherwise we use the config value of
   * `spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio`.
   */
  private def pruningHasBenefit(
                                 partExpr: Expression,
                                 partPlan: LogicalPlan,
                                 otherExpr: Expression,
                                 otherPlan: LogicalPlan): Boolean = {

    // get the distinct counts of an attribute for a given table
    def distinctCounts(attr: Attribute, plan: LogicalPlan): Option[BigInt] = {
      plan.stats.attributeStats.get(attr).flatMap(_.distinctCount)
    }

    // the default filtering ratio when CBO stats are missing, but there is a
    // predicate that is likely to be selective
    val fallbackRatio = conf.dynamicPartitionPruningFallbackFilterRatio
    // the filtering ratio based on the type of the join condition and on the column statistics
    val filterRatio = (partExpr.references.toList, otherExpr.references.toList) match {
      // filter out expressions with more than one attribute on any side of the operator
      case (leftAttr :: Nil, rightAttr :: Nil)
        if conf.dynamicPartitionPruningUseStats =>
        // get the CBO stats for each attribute in the join condition
        val partDistinctCount = distinctCounts(leftAttr, partPlan)
        val otherDistinctCount = distinctCounts(rightAttr, otherPlan)
        val availableStats = partDistinctCount.isDefined && partDistinctCount.get > 0 &&
          otherDistinctCount.isDefined
        if (!availableStats) {
          fallbackRatio
        } else if (partDistinctCount.get.toDouble <= otherDistinctCount.get.toDouble) {
          // there is likely an estimation error, so we fallback
          fallbackRatio
        } else {
          1 - otherDistinctCount.get.toDouble / partDistinctCount.get.toDouble
        }
      case _ => fallbackRatio
    }

    val estimatePruningSideSize = filterRatio * partPlan.stats.sizeInBytes.toFloat
    // the pruning overhead is the total size in bytes of all scan relations
    val overhead = otherPlan.collectLeaves().map(_.stats.sizeInBytes).sum.toFloat
    estimatePruningSideSize > overhead
  }

  /**
   * Returns whether an expression is likely to be selective
   */
  private def isLikelySelective(e: Expression): Boolean = e match {
    case Not(expr) => isLikelySelective(expr)
    case And(l, r) => isLikelySelective(l) || isLikelySelective(r)
    case Or(l, r) => isLikelySelective(l) && isLikelySelective(r)
    case _: StringRegexExpression => true
    case _: BinaryComparison => true
    case _: In | _: InSet => true
    case _: StringPredicate => true
    case _: MultiLikeBase => true
    case _ => false
  }

  /**
   * Search a filtering predicate in a given logical plan
   */
  private def hasSelectivePredicate(plan: LogicalPlan): Boolean = {
    plan.find {
      case f: Filter => isLikelySelective(f.condition)
      case _ => false
    }.isDefined
  }

  /**
   * To be able to prune partitions on a join key, the filtering side needs to
   * meet the following requirements:
   *   (1) it can not be a stream
   *   (2) it needs to contain a selective predicate used for filtering
   */
  private def hasPartitionPruningFilter(plan: LogicalPlan): Boolean = {
    !plan.isStreaming && hasSelectivePredicate(plan)
  }

  private def canPruneLeft(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | RightOuter => true
    case _ => false
  }

  private def canPruneRight(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | LeftOuter => true
    case _ => false
  }

  private def prune(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {
      // skip this rule if there's already a DPP subquery on the LHS of a join
      case j @ Join(Filter(_: DynamicPruningSubquery, _), _, _, _, _) => j
      case j @ Join(_, Filter(_: DynamicPruningSubquery, _), _, _, _) => j
      //Start: modified to only hit once
      case j @ Join(left, right, joinType, Some(condition), hint) if !pruned =>
        pruned = true
        //End: modified to only hit once
        var newLeft = left
        var newRight = right

        // extract the left and right keys of the join condition
        val (leftKeys, rightKeys) = j match {
          case ExtractEquiJoinKeys(_, lkeys, rkeys, _, _, _, _) => (lkeys, rkeys)
          case _ => (Nil, Nil)
        }

        // checks if two expressions are on opposite sides of the join
        def fromDifferentSides(x: Expression, y: Expression): Boolean = {
          def fromLeftRight(x: Expression, y: Expression) =
            !x.references.isEmpty && x.references.subsetOf(left.outputSet) &&
              !y.references.isEmpty && y.references.subsetOf(right.outputSet)
          fromLeftRight(x, y) || fromLeftRight(y, x)
        }

        splitConjunctivePredicates(condition).foreach {
          case EqualTo(a: Expression, b: Expression)
            if fromDifferentSides(a, b) =>
            val (l, r) = if (a.references.subsetOf(left.outputSet) &&
              b.references.subsetOf(right.outputSet)) {
              a -> b
            } else {
              b -> a
            }

            // there should be a partitioned table and a filter on the dimension table,
            // otherwise the pruning will not trigger
            var filterableScan = getFilterableTableScan(l, left)
            if (filterableScan.isDefined && canPruneLeft(joinType) &&
              hasPartitionPruningFilter(right)) {
              newLeft = insertPredicate(l, newLeft, r, right, rightKeys, filterableScan.get)
            } else {
              filterableScan = getFilterableTableScan(r, right)
              if (filterableScan.isDefined && canPruneRight(joinType) &&
                hasPartitionPruningFilter(left) ) {
                newRight = insertPredicate(r, newRight, l, left, leftKeys, filterableScan.get)
              }
            }
          case _ =>
        }
        Join(newLeft, newRight, joinType, Some(condition), hint)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // Do not rewrite subqueries.
    case s: Subquery if s.correlated => plan
    case _ if !conf.dynamicPartitionPruningEnabled => plan
    case _ =>
      val appliedPP = prune(plan)
      if (pruned) {
        val appliedPDP = PushDownPredicates.apply(appliedPP)
        if (appliedPP.fastEquals(appliedPDP)) {
          //Start: copied from CleanupDynamicPruningFilters
          val appliedCDPF = appliedPDP.transformWithPruning(
            // No-op for trees that do not contain dynamic pruning.
            _.containsAnyPattern(DYNAMIC_PRUNING_EXPRESSION, DYNAMIC_PRUNING_SUBQUERY)) {
            // pass through anything that is pushed down into PhysicalOperation
            case p@PhysicalOperation(_, _, LogicalRelation(_: HadoopFsRelation, _, _, _)) => p
            case p@PhysicalOperation(_, _, _: DataSourceV2ScanRelation) => p
            //Added: this line which I copied from 2 lines above and replaced hadoopfs relation with hoodiebase
            case p@PhysicalOperation(_, _, LogicalRelation(_: HoodieBaseRelation, _, _, _)) => p
            // remove any Filters with DynamicPruning that didn't get pushed down to PhysicalOperation.
            case f@Filter(condition, _) =>
              val newCondition = condition.transformWithPruning(
                _.containsAnyPattern(DYNAMIC_PRUNING_EXPRESSION, DYNAMIC_PRUNING_SUBQUERY)) {
                case _: DynamicPruning => TrueLiteral
              }
              f.copy(condition = newCondition)
          }
          //End: copied from CleanupDynamicPruningFilters
          PruneFilters.apply(appliedCDPF)
        } else {
          appliedPDP
        }
      } else {
        appliedPP
      }
  }
}

object Spark32PartitionPruning {
  var prevPlan: LogicalPlan = null
}
