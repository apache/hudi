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

package org.apache.spark.sql.hudi

import org.apache.hudi.ColumnStatsIndexSupport.{getMaxColumnNameFor, getMinColumnNameFor, getNullCountColumnNameFor, getValueCountColumnNameFor}
import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, EqualNullSafe, EqualTo, Expression, ExtractValue, GetStructField, GreaterThan, GreaterThanOrEqual, In, InSet, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or, StartsWith, SubqueryExpression}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hudi.ColumnStatsExpressionUtils._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, HoodieCatalystExpressionUtils}
import org.apache.spark.unsafe.types.UTF8String

object DataSkippingUtils extends Logging {

  /**
   * Translates provided {@link filterExpr} into corresponding filter-expression for column-stats index index table
   * to filter out candidate files that would hold records matching the original filter
   *
   * @param dataTableFilterExpr source table's query's filter expression
   * @param indexSchema index table schema
   * @return filter for column-stats index's table
   */
  def translateIntoColumnStatsIndexFilterExpr(dataTableFilterExpr: Expression, indexSchema: StructType): Expression = {
    try {
      createColumnStatsIndexFilterExprInternal(dataTableFilterExpr, indexSchema)
    } catch {
      case e: AnalysisException =>
        logDebug(s"Failed to translated provided data table filter expr into column stats one ($dataTableFilterExpr)", e)
        throw e
    }
  }

  private def createColumnStatsIndexFilterExprInternal(dataTableFilterExpr: Expression, indexSchema: StructType): Expression = {
    // Try to transform original Source Table's filter expression into
    // Column-Stats Index filter expression
    tryComposeIndexFilterExpr(dataTableFilterExpr, indexSchema) match {
      case Some(e) => e
      // NOTE: In case we can't transform source filter expression, we fallback
      // to {@code TrueLiteral}, to essentially avoid pruning any indexed files from scanning
      case None => TrueLiteral
    }
  }

  private def tryComposeIndexFilterExpr(sourceFilterExpr: Expression, indexSchema: StructType): Option[Expression] = {
    //
    // For translation of the Filter Expression for the Data Table into Filter Expression for Column Stats Index, we're
    // assuming that
    //    - The column A is queried in the Data Table (hereafter referred to as "colA")
    //    - Filter Expression is a relational expression (ie "=", "<", "<=", ...) of the following form
    //
    //      ```transform_expr(colA) = value_expr```
    //
    //      Where
    //        - "transform_expr" is an expression of the _transformation_ which preserve ordering of the "colA"
    //        - "value_expr" is an "value"-expression (ie one NOT referring to other attributes/columns or containing sub-queries)
    //
    // We translate original Filter Expr into the one querying Column Stats Index like following: let's consider
    // equality Filter Expr referred to above:
    //
    //   ```transform_expr(colA) = value_expr```
    //
    // This expression will be translated into following Filter Expression for the Column Stats Index:
    //
    //   ```(transform_expr(colA_minValue) <= value_expr) AND (value_expr <= transform_expr(colA_maxValue))```
    //
    // Which will enable us to match files with the range of values in column A containing the target ```value_expr```
    //
    // NOTE: That we can apply ```transform_expr``` transformation precisely b/c it preserves the ordering of the
    //       values of the source column, ie following holds true:
    //
    //       colA_minValue = min(colA)  =>  transform_expr(colA_minValue) = min(transform_expr(colA))
    //       colA_maxValue = max(colA)  =>  transform_expr(colA_maxValue) = max(transform_expr(colA))
    //
    sourceFilterExpr match {
      // If Expression is not resolved, we can't perform the analysis accurately, bailing
      case expr if !expr.resolved => None

      // Filter "expr(colA) = B" and "B = expr(colA)"
      // Translates to "(expr(colA_minValue) <= B) AND (B <= expr(colA_maxValue))" condition for index lookup
      case EqualTo(sourceExpr @ AllowedTransformationExpression(attrRef), valueExpr: Expression) if isValueExpression(valueExpr) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            // NOTE: Since we're supporting (almost) arbitrary expressions of the form `f(colA) = B`, we have to
            //       appropriately translate such original expression targeted at Data Table, to corresponding
            //       expression targeted at Column Stats Index Table. For that, we take original expression holding
            //       [[AttributeReference]] referring to the Data Table, and swap it w/ expression referring to
            //       corresponding column in the Column Stats Index
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            genColumnValuesEqualToExpression(colName, valueExpr, targetExprBuilder)
          }

      case EqualTo(valueExpr: Expression, sourceExpr @ AllowedTransformationExpression(attrRef)) if isValueExpression(valueExpr) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            genColumnValuesEqualToExpression(colName, valueExpr, targetExprBuilder)
          }

      // Filter "expr(colA) != B" and "B != expr(colA)"
      // Translates to "NOT(expr(colA_minValue) = B AND expr(colA_maxValue) = B)"
      // NOTE: This is NOT an inversion of `colA = b`, instead this filter ONLY excludes files for which `colA = B`
      //       holds true
      case Not(EqualTo(sourceExpr @ AllowedTransformationExpression(attrRef), value: Expression)) if isValueExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            Not(genColumnOnlyValuesEqualToExpression(colName, value, targetExprBuilder))
          }

      case Not(EqualTo(value: Expression, sourceExpr @ AllowedTransformationExpression(attrRef))) if isValueExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            Not(genColumnOnlyValuesEqualToExpression(colName, value, targetExprBuilder))
          }

      // Filter "colA = null"
      // Translates to "colA_nullCount = null" for index lookup
      case EqualNullSafe(attrRef: AttributeReference, litNull @ Literal(null, _)) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map(colName => EqualTo(genColNumNullsExpr(colName), litNull))

      // Filter "expr(colA) < B" and "B > expr(colA)"
      // Translates to "expr(colA_minValue) < B" for index lookup
      case LessThan(sourceExpr @ AllowedTransformationExpression(attrRef), value: Expression) if isValueExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            LessThan(targetExprBuilder.apply(genColMinValueExpr(colName)), value)
          }

      case GreaterThan(value: Expression, sourceExpr @ AllowedTransformationExpression(attrRef)) if isValueExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            LessThan(targetExprBuilder.apply(genColMinValueExpr(colName)), value)
          }

      // Filter "B < expr(colA)" and "expr(colA) > B"
      // Translates to "B < colA_maxValue" for index lookup
      case LessThan(value: Expression, sourceExpr @ AllowedTransformationExpression(attrRef)) if isValueExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            GreaterThan(targetExprBuilder.apply(genColMaxValueExpr(colName)), value)
          }

      case GreaterThan(sourceExpr @ AllowedTransformationExpression(attrRef), value: Expression) if isValueExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            GreaterThan(targetExprBuilder.apply(genColMaxValueExpr(colName)), value)
          }

      // Filter "expr(colA) <= B" and "B >= expr(colA)"
      // Translates to "colA_minValue <= B" for index lookup
      case LessThanOrEqual(sourceExpr @ AllowedTransformationExpression(attrRef), value: Expression) if isValueExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            LessThanOrEqual(targetExprBuilder.apply(genColMinValueExpr(colName)), value)
          }

      case GreaterThanOrEqual(value: Expression, sourceExpr @ AllowedTransformationExpression(attrRef)) if isValueExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            LessThanOrEqual(targetExprBuilder.apply(genColMinValueExpr(colName)), value)
          }

      // Filter "B <= expr(colA)" and "expr(colA) >= B"
      // Translates to "B <= colA_maxValue" for index lookup
      case LessThanOrEqual(value: Expression, sourceExpr @ AllowedTransformationExpression(attrRef)) if isValueExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            GreaterThanOrEqual(targetExprBuilder.apply(genColMaxValueExpr(colName)), value)
          }

      case GreaterThanOrEqual(sourceExpr @ AllowedTransformationExpression(attrRef), value: Expression) if isValueExpression(value) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            GreaterThanOrEqual(targetExprBuilder.apply(genColMaxValueExpr(colName)), value)
          }

      // Filter "colA is null"
      // Translates to "colA_nullCount > 0" for index lookup
      case IsNull(attribute: AttributeReference) =>
        getTargetIndexedColumnName(attribute, indexSchema)
          .map(colName => GreaterThan(genColNumNullsExpr(colName), Literal(0)))

      // Filter "colA is not null"
      // Translates to "colA_nullCount < colA_valueCount" for index lookup
      case IsNotNull(attribute: AttributeReference) =>
        getTargetIndexedColumnName(attribute, indexSchema)
          .map(colName => LessThan(genColNumNullsExpr(colName), genColValueCountExpr))

      // Filter "expr(colA) in (B1, B2, ...)"
      // Translates to "(colA_minValue <= B1 AND colA_maxValue >= B1) OR (colA_minValue <= B2 AND colA_maxValue >= B2) ... "
      // for index lookup
      // NOTE: This is equivalent to "colA = B1 OR colA = B2 OR ..."
      case In(sourceExpr @ AllowedTransformationExpression(attrRef), list: Seq[Expression]) if list.forall(isValueExpression) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            list.map(lit => genColumnValuesEqualToExpression(colName, lit, targetExprBuilder)).reduce(Or)
          }

      // Filter "expr(colA) in (B1, B2, ...)"
      // NOTE: [[InSet]] is an optimized version of the [[In]] expression, where every sub-expression w/in the
      //       set is a static literal
      case InSet(sourceExpr @ AllowedTransformationExpression(attrRef), hset: Set[Any]) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            hset.map(value => genColumnValuesEqualToExpression(colName, Literal(value), targetExprBuilder)).reduce(Or)
          }

      // Filter "expr(colA) not in (B1, B2, ...)"
      // Translates to "NOT((colA_minValue = B1 AND colA_maxValue = B1) OR (colA_minValue = B2 AND colA_maxValue = B2))" for index lookup
      // NOTE: This is NOT an inversion of `in (B1, B2, ...)` expr, this is equivalent to "colA != B1 AND colA != B2 AND ..."
      case Not(In(sourceExpr @ AllowedTransformationExpression(attrRef), list: Seq[Expression])) if list.forall(_.foldable) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            Not(list.map(lit => genColumnOnlyValuesEqualToExpression(colName, lit, targetExprBuilder)).reduce(Or))
          }

      // Filter "colA like 'xxx%'"
      // Translates to "colA_minValue <= xxx AND xxx <= colA_maxValue" for index lookup
      //
      // NOTE: Since a) this operator matches strings by prefix and b) given that this column is going to be ordered
      //       lexicographically, we essentially need to check that provided literal falls w/in min/max bounds of the
      //       given column
      case StartsWith(sourceExpr @ AllowedTransformationExpression(attrRef), v @ Literal(_: UTF8String, _)) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            genColumnValuesEqualToExpression(colName, v, targetExprBuilder)
          }

      // Filter "expr(colA) not like 'xxx%'"
      // Translates to "NOT(expr(colA_minValue) like 'xxx%' AND expr(colA_maxValue) like 'xxx%')" for index lookup
      // NOTE: This is NOT an inversion of "colA like xxx"
      case Not(StartsWith(sourceExpr @ AllowedTransformationExpression(attrRef), value @ Literal(_: UTF8String, _))) =>
        getTargetIndexedColumnName(attrRef, indexSchema)
          .map { colName =>
            val targetExprBuilder: Expression => Expression = swapAttributeRefInExpr(sourceExpr, attrRef, _)
            val minValueExpr = targetExprBuilder.apply(genColMinValueExpr(colName))
            val maxValueExpr = targetExprBuilder.apply(genColMaxValueExpr(colName))
            Not(And(StartsWith(minValueExpr, value), StartsWith(maxValueExpr, value)))
          }

      case or: Or =>
        val resLeft = createColumnStatsIndexFilterExprInternal(or.left, indexSchema)
        val resRight = createColumnStatsIndexFilterExprInternal(or.right, indexSchema)

        Option(Or(resLeft, resRight))

      case and: And =>
        val resLeft = createColumnStatsIndexFilterExprInternal(and.left, indexSchema)
        val resRight = createColumnStatsIndexFilterExprInternal(and.right, indexSchema)

        Option(And(resLeft, resRight))

      //
      // Pushing Logical NOT inside the AND/OR expressions
      // NOTE: This is required to make sure we're properly handling negations in
      //       cases like {@code NOT(colA = 0)}, {@code NOT(colA in (a, b, ...)}
      //

      case Not(And(left: Expression, right: Expression)) =>
        Option(createColumnStatsIndexFilterExprInternal(Or(Not(left), Not(right)), indexSchema))

      case Not(Or(left: Expression, right: Expression)) =>
        Option(createColumnStatsIndexFilterExprInternal(And(Not(left), Not(right)), indexSchema))

      case _: Expression => None
    }
  }

  private def checkColIsIndexed(colName: String, indexSchema: StructType): Boolean = {
    Set.apply(
      getMinColumnNameFor(colName),
      getMaxColumnNameFor(colName),
      getNullCountColumnNameFor(colName)
    )
      .forall(stat => indexSchema.exists(_.name == stat))
  }

  private def getTargetIndexedColumnName(resolvedExpr: AttributeReference, indexSchema: StructType): Option[String] = {
    val colName = UnresolvedAttribute(getTargetColNameParts(resolvedExpr)).name

    // Verify that the column is indexed
    if (checkColIsIndexed(colName, indexSchema)) {
      Option.apply(colName)
    } else {
      None
    }
  }

  private def getTargetColNameParts(resolvedTargetCol: Expression): Seq[String] = {
    resolvedTargetCol match {
      case attr: Attribute => Seq(attr.name)
      case Alias(c, _) => getTargetColNameParts(c)
      case GetStructField(c, _, Some(name)) => getTargetColNameParts(c) :+ name
      case ex: ExtractValue =>
        throw new AnalysisException(s"convert reference to name failed, Updating nested fields is only supported for StructType: ${ex}.")
      case other =>
        throw new AnalysisException(s"convert reference to name failed,  Found unsupported expression ${other}")
    }
  }
}

object ColumnStatsExpressionUtils {

  @inline def genColMinValueExpr(colName: String): Expression = col(getMinColumnNameFor(colName)).expr
  @inline def genColMaxValueExpr(colName: String): Expression = col(getMaxColumnNameFor(colName)).expr
  @inline def genColNumNullsExpr(colName: String): Expression = col(getNullCountColumnNameFor(colName)).expr
  @inline def genColValueCountExpr: Expression = col(getValueCountColumnNameFor).expr

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

