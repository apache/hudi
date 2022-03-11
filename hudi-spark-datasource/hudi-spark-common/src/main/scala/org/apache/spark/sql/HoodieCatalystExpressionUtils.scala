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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{ResolveTimeZone, UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType

object HoodieCatalystExpressionUtils {

  /**
   * Resolve filter expression from string expr with given table schema, for example:
   * <pre>
   *   ts > 1000 and ts <= 1500
   * </pre>
   * will be resolved as
   * <pre>
   *   And(GreaterThan(ts#590L > 1000), LessThanOrEqual(ts#590L <= 1500))
   * </pre>
   *
   * @param spark       The spark session
   * @param exprString  String to be resolved
   * @param tableSchema The table schema
   * @return Resolved filter expression
   */
  def resolveExpr(spark: SparkSession, exprString: String, tableSchema: StructType): Expression = {
    val expr = spark.sessionState.sqlParser.parseExpression(exprString)
    resolveExpr(spark, expr, tableSchema)
  }

  def resolveExpr(spark: SparkSession, expr: Expression, tableSchema: StructType): Expression = {
    val analyzer = spark.sessionState.analyzer
    val schemaFields = tableSchema.fields

    val resolvedExpr = {
      val plan: LogicalPlan = Filter(expr, LocalRelation(schemaFields.head, schemaFields.drop(1): _*))
      val rules: Seq[Rule[LogicalPlan]] =
        ResolveTimeZone ::
        analyzer.ResolveFunctions ::
        analyzer.ResolveReferences ::
        Nil

      def resetUnresolved(plan: LogicalPlan): LogicalPlan = {
        @inline def copy(expr: Expression) =
          expr.makeCopy(expr.productIterator.map(x => x.asInstanceOf[AnyRef]).toArray)

        plan.transformExpressionsUp {
          case expr if !expr.resolved => copy(expr)
        }
      }

      var effective = true
      var current: LogicalPlan = plan
      // NOTE: This is a workaround for Spark bug which makes it impossible
      //       to resolve functions in a single pass (and collaterally to that it also erroneously marks
      //       this rule as ineffective, making its subsequent application impossible)
      //       To work it around we
      //          - Clone the plan (cloning all of its expressions) this way clearing the internal metadata state
      //            (blocking subsequent rule application)
      //          - Apply ResolveFunctions rule multiple times
      // TODO(SPARK-38512) cleanup after resolved
      while (effective) {
        val newPlan = rules.foldRight(current) {
          case (rule, plan) =>
            rule.apply(plan)
        }
        effective = !current.fastEquals(newPlan)
        // Make a copy to reset bitset with ineffective rules
        current = resetUnresolved(newPlan)
      }

      current.asInstanceOf[Filter].condition
    }

    if (!hasUnresolvedRefs(resolvedExpr)) {
      resolvedExpr
    } else {
      throw new IllegalStateException("unresolved attribute")
    }
  }

  private def hasUnresolvedRefs(resolvedExpr: Expression): Boolean =
    resolvedExpr.collectFirst {
      case _: UnresolvedAttribute | _: UnresolvedFunction => true
    }.isDefined

  /**
   * Split the given predicates into two sequence predicates:
   * - predicates that references partition columns only(and involves no sub-query);
   * - other predicates.
   *
   * @param sparkSession     The spark session
   * @param predicates       The predicates to be split
   * @param partitionColumns The partition columns
   * @return (partitionFilters, dataFilters)
   */
  def splitPartitionAndDataPredicates(sparkSession: SparkSession,
                                      predicates: Array[Expression],
                                      partitionColumns: Array[String]): (Array[Expression], Array[Expression]) = {
    // Validates that the provided names both resolve to the same entity
    val resolvedNameEquals = sparkSession.sessionState.analyzer.resolver

    predicates.partition(expr => {
      // Checks whether given expression only references partition columns(and involves no sub-query)
      expr.references.forall(r => partitionColumns.exists(resolvedNameEquals(r.name, _))) &&
        !SubqueryExpression.hasSubquery(expr)
    })
  }
}
