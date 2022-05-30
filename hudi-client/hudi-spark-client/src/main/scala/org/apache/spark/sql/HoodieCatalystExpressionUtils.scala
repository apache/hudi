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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateMutableProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, MutableProjection, SubqueryExpression, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.types.StructType

trait HoodieCatalystExpressionUtils {

  /**
   * Generates instance of [[UnsafeProjection]] projecting row of one [[StructType]] into another [[StructType]]
   *
   * NOTE: No safety checks are executed to validate that this projection is actually feasible,
   *       it's up to the caller to make sure that such projection is possible.
   *
   * NOTE: Projection of the row from [[StructType]] A to [[StructType]] B is only possible, if
   *       B is a subset of A
   */
  def generateUnsafeProjection(from: StructType, to: StructType): UnsafeProjection = {
    val attrs = from.toAttributes
    val attrsMap = attrs.map(attr => (attr.name, attr)).toMap
    val targetExprs = to.fields.map(f => attrsMap(f.name))

    GenerateUnsafeProjection.generate(targetExprs, attrs)
  }

  /**
   * Generates instance of [[MutableProjection]] projecting row of one [[StructType]] into another [[StructType]]
   *
   * NOTE: No safety checks are executed to validate that this projection is actually feasible,
   *       it's up to the caller to make sure that such projection is possible.
   *
   * NOTE: Projection of the row from [[StructType]] A to [[StructType]] B is only possible, if
   *       B is a subset of A
   */
  def generateMutableProjection(from: StructType, to: StructType): MutableProjection = {
    val attrs = from.toAttributes
    val attrsMap = attrs.map(attr => (attr.name, attr)).toMap
    val targetExprs = to.fields.map(f => attrsMap(f.name))

    GenerateMutableProjection.generate(targetExprs, attrs)
  }

  /**
   * Parses and resolves expression against the attributes of the given table schema.
   *
   * For example:
   * <pre>
   * ts > 1000 and ts <= 1500
   * </pre>
   * will be resolved as
   * <pre>
   * And(GreaterThan(ts#590L > 1000), LessThanOrEqual(ts#590L <= 1500))
   * </pre>
   *
   * Where <pre>ts</pre> is a column of the provided [[tableSchema]]
   *
   * @param spark       spark session
   * @param exprString  string representation of the expression to parse and resolve
   * @param tableSchema table schema encompassing attributes to resolve against
   * @return Resolved filter expression
   */
  def resolveExpr(spark: SparkSession, exprString: String, tableSchema: StructType): Expression = {
    val expr = spark.sessionState.sqlParser.parseExpression(exprString)
    resolveExpr(spark, expr, tableSchema)
  }

  /**
   * Resolves provided expression (unless already resolved) against the attributes of the given table schema.
   *
   * For example:
   * <pre>
   * ts > 1000 and ts <= 1500
   * </pre>
   * will be resolved as
   * <pre>
   * And(GreaterThan(ts#590L > 1000), LessThanOrEqual(ts#590L <= 1500))
   * </pre>
   *
   * Where <pre>ts</pre> is a column of the provided [[tableSchema]]
   *
   * @param spark       spark session
   * @param expr        Catalyst expression to be resolved (if not yet)
   * @param tableSchema table schema encompassing attributes to resolve against
   * @return Resolved filter expression
   */
  def resolveExpr(spark: SparkSession, expr: Expression, tableSchema: StructType): Expression = {
    val analyzer = spark.sessionState.analyzer
    val schemaFields = tableSchema.fields

    val resolvedExpr = {
      val plan: LogicalPlan = Filter(expr, LocalRelation(schemaFields.head, schemaFields.drop(1): _*))
      analyzer.execute(plan).asInstanceOf[Filter].condition
    }

    if (!hasUnresolvedRefs(resolvedExpr)) {
      resolvedExpr
    } else {
      throw new IllegalStateException("unresolved attribute")
    }
  }

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

  /**
   * Matches an expression iff
   *
   * <ol>
   *   <li>It references exactly one [[AttributeReference]]</li>
   *   <li>It contains only whitelisted transformations that preserve ordering of the source column [1]</li>
   * </ol>
   *
   * [1] Preserving ordering is defined as following: transformation T is defined as ordering preserving in case
   *     values of the source column A values being ordered as a1, a2, a3 ..., will map into column B = T(A) which
   *     will keep the same ordering b1, b2, b3, ... with b1 = T(a1), b2 = T(a2), ...
   */
  def tryMatchAttributeOrderingPreservingTransformation(expr: Expression): Option[AttributeReference]

  private def hasUnresolvedRefs(resolvedExpr: Expression): Boolean =
    resolvedExpr.collectFirst {
      case _: UnresolvedAttribute | _: UnresolvedFunction => true
    }.isDefined
}
