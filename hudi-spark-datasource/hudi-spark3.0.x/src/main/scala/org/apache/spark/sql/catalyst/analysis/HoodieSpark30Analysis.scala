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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, ResolveLambdaVariables, UnresolvedAttribute, UnresolvedExtractValue, caseInsensitiveResolution, withPosition}
import org.apache.spark.sql.catalyst.expressions.{Alias, CurrentDate, CurrentTimestamp, Expression, ExtractValue, GetStructField, LambdaFunction}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils

/**
 * NOTE: Taken from HoodieSpark2Analysis applied to Spark version 3.0.3 and modified to resolve source and target tables
 * if not already resolved
 *
 *       PLEASE REFRAIN MAKING ANY CHANGES TO THIS CODE UNLESS ABSOLUTELY NECESSARY
 */
object HoodieSpark30Analysis {

  case class ResolveReferences(spark: SparkSession) extends Rule[LogicalPlan] {

    private val resolver = spark.sessionState.conf.resolver

    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case mO @ MergeIntoTable(targetTableO, sourceTableO, _, _, _)
        // START: custom Hudi change: don't want to go to the spark mit resolution so we resolve the source and target if they haven't been
        if !mO.resolved || containsUnresolvedStarAssignments(mO) =>
        lazy val analyzer = spark.sessionState.analyzer
        val targetTable = if (targetTableO.resolved) targetTableO else analyzer.execute(targetTableO)
        val sourceTable = if (sourceTableO.resolved) sourceTableO else analyzer.execute(sourceTableO)
        val m = mO.copy(targetTable = targetTable, sourceTable = sourceTable)
        // END: custom Hudi change
        EliminateSubqueryAliases(targetTable) match {
          case _ =>
            val newMatchedActions = m.matchedActions.map {
              case DeleteAction(deleteCondition) =>
                val resolvedDeleteCondition = deleteCondition.map(resolveExpressionTopDown(_, m))
                DeleteAction(resolvedDeleteCondition)
              case UpdateAction(updateCondition, assignments) =>
                val resolvedUpdateCondition = updateCondition.map(resolveExpressionTopDown(_, m))
                // The update value can access columns from both target and source tables.
                UpdateAction(
                  resolvedUpdateCondition,
                  resolveAssignments(assignments, m, resolveValuesWithSourceOnly = false))
              case o => o
            }
            val newNotMatchedActions = m.notMatchedActions.map {
              case InsertAction(insertCondition, assignments) =>
                // The insert action is used when not matched, so its condition and value can only
                // access columns from the source table.
                val resolvedInsertCondition =
                  insertCondition.map(resolveExpressionTopDown(_, Project(Nil, m.sourceTable)))
                InsertAction(
                  resolvedInsertCondition,
                  resolveAssignments(assignments, m, resolveValuesWithSourceOnly = true))
              case o => o
            }
            val resolvedMergeCondition = resolveExpressionTopDown(m.mergeCondition, m)
            m.copy(mergeCondition = resolvedMergeCondition,
              matchedActions = newMatchedActions,
              notMatchedActions = newNotMatchedActions)
        }
    }

    private def resolveAssignments(assignments: Seq[Assignment],
                                   mergeInto: MergeIntoTable,
                                   resolveValuesWithSourceOnly: Boolean): Seq[Assignment] = {
      if (assignments.isEmpty) {
        // START: custom Hudi change: filter out meta fields
        val expandedColumns = HoodieSqlCommonUtils.removeMetaFields(mergeInto.targetTable.output)
        // END: custom Hudi change
        val expandedValues = mergeInto.sourceTable.output
        expandedColumns.zip(expandedValues).map(kv => Assignment(kv._1, kv._2))
      } else {
        assignments.map { assign =>
          val resolvedKey = assign.key match {
            case c if !c.resolved =>
              resolveExpressionTopDown(c, Project(Nil, mergeInto.targetTable))
            case o => o
          }
          val resolvedValue = assign.value match {
            // The update values may contain target and/or source references.
            case c if !c.resolved =>
              if (resolveValuesWithSourceOnly) {
                resolveExpressionTopDown(c, Project(Nil, mergeInto.sourceTable))
              } else {
                resolveExpressionTopDown(c, mergeInto)
              }
            case o => o
          }
          Assignment(resolvedKey, resolvedValue)
        }
      }
    }

    /**
     * Resolves the attribute and extract value expressions(s) by traversing the
     * input expression in top down manner. The traversal is done in top-down manner as
     * we need to skip over unbound lambda function expression. The lambda expressions are
     * resolved in a different rule [[ResolveLambdaVariables]]
     *
     * Example :
     * SELECT transform(array(1, 2, 3), (x, i) -> x + i)"
     *
     * In the case above, x and i are resolved as lambda variables in [[ResolveLambdaVariables]]
     *
     * Note : In this routine, the unresolved attributes are resolved from the input plan's
     * children attributes.
     */
    private def resolveExpressionTopDown(e: Expression, q: LogicalPlan): Expression = {
      // scalastyle:off return
      if (e.resolved) return e
      // scalastyle:on return
      e match {
        case f: LambdaFunction if !f.bound => f
        case u@UnresolvedAttribute(nameParts) =>
          // Leave unchanged if resolution fails. Hopefully will be resolved next round.
          val result =
            withPosition(u) {
              q.resolveChildren(nameParts, resolver)
                .orElse(resolveLiteralFunction(nameParts, u, q))
                .getOrElse(u)
            }
          logDebug(s"Resolving $u to $result")
          result
        case UnresolvedExtractValue(child, fieldExpr) if child.resolved =>
          ExtractValue(child, fieldExpr, resolver)
        case _ => e.mapChildren(resolveExpressionTopDown(_, q))
      }
    }

    /**
     * Literal functions do not require the user to specify braces when calling them
     * When an attributes is not resolvable, we try to resolve it as a literal function.
     */
    private def resolveLiteralFunction(nameParts: Seq[String],
                                       attribute: UnresolvedAttribute,
                                       plan: LogicalPlan): Option[Expression] = {
      // scalastyle:off return
      if (nameParts.length != 1) return None
      // scalastyle:on return
      val isNamedExpression = plan match {
        case Aggregate(_, aggregateExpressions, _) => aggregateExpressions.contains(attribute)
        case Project(projectList, _) => projectList.contains(attribute)
        case Window(windowExpressions, _, _, _) => windowExpressions.contains(attribute)
        case _ => false
      }
      val wrapper: Expression => Expression =
        if (isNamedExpression) f => Alias(f, toPrettySQL(f))() else identity
      // support CURRENT_DATE and CURRENT_TIMESTAMP
      val literalFunctions = Seq(CurrentDate(), CurrentTimestamp())
      val name = nameParts.head
      val func = literalFunctions.find(e => caseInsensitiveResolution(e.prettyName, name))
      func.map(wrapper)
    }

    // START: custom Hudi change. Following section is amended to the original (Spark's) implementation
    private def containsUnresolvedStarAssignments(mit: MergeIntoTable): Boolean = {
      val containsUnresolvedInsertStar = mit.notMatchedActions.exists {
        case InsertAction(_, assignments) => assignments.isEmpty
        case _ => false
      }
      val containsUnresolvedUpdateStar = mit.matchedActions.exists {
        case UpdateAction(_, assignments) => assignments.isEmpty
        case _ => false
      }

      containsUnresolvedInsertStar || containsUnresolvedUpdateStar
    }
    // END: custom Hudi change.
  }

}
