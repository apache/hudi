/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.util.ValidationUtils.checkState

import org.apache.spark.sql.HoodieCatalystExpressionUtils
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, GreaterThanOrEqual, LessThanOrEqual, SubqueryExpression}

trait ColumnStatsExpressionUtils {

  @inline def genColMinValueExpr(colName: String): Expression
  @inline def genColMaxValueExpr(colName: String): Expression
  @inline def genColNumNullsExpr(colName: String): Expression
  @inline def genColValueCountExpr: Expression

  @inline def genColumnValuesEqualToExpression(colName: String,
                                               value: Expression,
                                               targetExprBuilder: Function[Expression, Expression] = Predef.identity): Expression = {
    val minValueExpr = targetExprBuilder.apply(genColMinValueExpr(colName))
    val maxValueExpr = targetExprBuilder.apply(genColMaxValueExpr(colName))
    // Only case when column C contains value V is when min(C) <= V <= max(c)
    And(LessThanOrEqual(minValueExpr, value), GreaterThanOrEqual(maxValueExpr, value))
  }

  def genColumnOnlyValuesEqualToExpression(colName: String,
                                           value: Expression,
                                           targetExprBuilder: Function[Expression, Expression] = Predef.identity): Expression = {
    val minValueExpr = targetExprBuilder.apply(genColMinValueExpr(colName))
    val maxValueExpr = targetExprBuilder.apply(genColMaxValueExpr(colName))
    // Only case when column C contains _only_ value V is when min(C) = V AND max(c) = V
    And(EqualTo(minValueExpr, value), EqualTo(maxValueExpr, value))
  }

  def swapAttributeRefInExpr(sourceExpr: Expression, from: AttributeReference, to: Expression): Expression = {
    checkState(sourceExpr.references.size == 1)
    sourceExpr.transformDown {
      case attrRef: AttributeReference if attrRef.sameRef(from) => to
    }
  }

  /**
   * This check is used to validate that the expression that target column is compared against
   * <pre>
   *    a) Has no references to other attributes (for ex, columns)
   *    b) Does not contain sub-queries
   * </pre>
   *
   * This in turn allows us to be certain that Spark will be able to evaluate such expression
   * against Column Stats Index as well
   */
  def isValueExpression(expr: Expression): Boolean =
    expr.references.isEmpty && !SubqueryExpression.hasSubquery(expr)

  /**
   * This utility pattern-matches an expression iff
   *
   * <ol>
   *   <li>It references *exactly* 1 attribute (column)</li>
   *   <li>It does NOT contain sub-queries</li>
   *   <li>It contains only whitelisted transformations that preserve ordering of the source column [1]</li>
   * </ol>
   *
   * [1] This is required to make sure that we can correspondingly map Column Stats Index values as well. Applying
   * transformations that do not preserve the ordering might lead to incorrect results being returned by Data
   * Skipping flow.
   *
   * Returns only [[AttributeReference]] contained as a sub-expression
   */
  object AllowedTransformationExpression extends SparkAdapterSupport {
    val exprUtils: HoodieCatalystExpressionUtils = sparkAdapter.getCatalystExpressionUtils

    def unapply(expr: Expression): Option[AttributeReference] = {
      // First step, we check that expression
      //    - Does NOT contain sub-queries
      //    - Does contain exactly 1 attribute
      if (SubqueryExpression.hasSubquery(expr) || expr.references.size != 1) {
        None
      } else {
        // Second step, we validate that holding expression is an actually permitted
        // transformation
        // NOTE: That transformation composition is permitted
        exprUtils.tryMatchAttributeOrderingPreservingTransformation(expr)
      }
    }
  }
}
