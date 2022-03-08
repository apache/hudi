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

import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.index.columnstats.ColumnStatsIndexHelper.{getMaxColumnNameFor, getMinColumnNameFor, getNumNullsColumnNameFor}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, EqualNullSafe, EqualTo, Expression, ExtractValue, GetStructField, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or, StartsWith}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
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

  private def tryComposeIndexFilterExpr(sourceExpr: Expression, indexSchema: StructType): Option[Expression] = {
    def minValue(colName: String) = col(getMinColumnNameFor(colName)).expr
    def maxValue(colName: String) = col(getMaxColumnNameFor(colName)).expr
    def numNulls(colName: String) = col(getNumNullsColumnNameFor(colName)).expr

    def colContainsValuesEqualTo(colName: String, value: Expression): Expression = {
      // TODO clean up
      checkState(value.foldable)
      // Only case when column C contains value V is when min(C) <= V <= max(c)
      And(LessThanOrEqual(minValue(colName), value), GreaterThanOrEqual(maxValue(colName), value))
    }

    def colContainsOnlyValuesEqualTo(colName: String, value: Expression): Expression = {
      // TODO clean up
      checkState(value.foldable)
      // Only case when column C contains _only_ value V is when min(C) = V AND max(c) = V
      And(EqualTo(minValue(colName), value), EqualTo(maxValue(colName), value))
    }

    sourceExpr match {
      // Filter "colA = B"
      // NOTE: B could be an arbitrary _foldable_ expression (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "colA_minValue <= B AND B <= colA_maxValue" condition for index lookup
      case EqualTo(attribute: AttributeReference, value: Expression) if value.foldable =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => colContainsValuesEqualTo(colName, value))

      // Filter "B = colA"
      // NOTE: B could be an arbitrary _foldable_ expression (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "colA_minValue <= B AND B <= colA_maxValue" condition for index lookup
      case EqualTo(value: Expression, attribute: AttributeReference) if value.foldable =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => colContainsValuesEqualTo(colName, value))

      // Filter "colA != B"
      // NOTE: B could be an arbitrary _foldable_ expression (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "NOT(colA_minValue = B AND colA_maxValue = B)"
      // NOTE: This is NOT an inversion of `colA = b`
      case Not(EqualTo(attribute: AttributeReference, value: Expression)) if value.foldable =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => Not(colContainsOnlyValuesEqualTo(colName, value)))

      // Filter "B != colA"
      // NOTE: B could be an arbitrary _foldable_ expression (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "NOT(colA_minValue = B AND colA_maxValue = B)"
      // NOTE: This is NOT an inversion of `colA = b`
      case Not(EqualTo(value: Expression, attribute: AttributeReference)) if value.foldable =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => Not(colContainsOnlyValuesEqualTo(colName, value)))

      // Filter "colA = null"
      // Translates to "colA_num_nulls = null" for index lookup
      case equalNullSafe @ EqualNullSafe(_: AttributeReference, _ @ Literal(null, _)) =>
        getTargetIndexedColName(equalNullSafe.left, indexSchema)
          .map(colName => EqualTo(numNulls(colName), equalNullSafe.right))

      // Filter "colA < B"
      // NOTE: B could be an arbitrary _foldable_ expression (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "colA_minValue < B" for index lookup
      case LessThan(attribute: AttributeReference, value: Expression) if value.foldable =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => LessThan(minValue(colName), value))

      // Filter "B > colA"
      // NOTE: B could be an arbitrary _foldable_ expression (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "B > colA_minValue" for index lookup
      case GreaterThan(value: Expression, attribute: AttributeReference) if value.foldable =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => LessThan(minValue(colName), value))

      // Filter "B < colA"
      // NOTE: B could be an arbitrary _foldable_ expression (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "B < colA_maxValue" for index lookup
      case LessThan(value: Expression, attribute: AttributeReference) if value.foldable =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => GreaterThan(maxValue(colName), value))

      // Filter "colA > B"
      // NOTE: B could be an arbitrary _foldable_ expression (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "colA_maxValue > B" for index lookup
      case GreaterThan(attribute: AttributeReference, value: Expression) if value.foldable =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => GreaterThan(maxValue(colName), value))

      // Filter "colA <= B"
      // NOTE: B could be an arbitrary _foldable_ expression (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "colA_minValue <= B" for index lookup
      case LessThanOrEqual(attribute: AttributeReference, value: Expression) if value.foldable =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => LessThanOrEqual(minValue(colName), value))

      // Filter "B >= colA"
      // NOTE: B could be an arbitrary _foldable_ expression (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "B >= colA_minValue" for index lookup
      case GreaterThanOrEqual(value: Expression, attribute: AttributeReference) if value.foldable =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => LessThanOrEqual(minValue(colName), value))

      // Filter "B <= colA"
      // NOTE: B could be an arbitrary _foldable_ expression (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "B <= colA_maxValue" for index lookup
      case LessThanOrEqual(value: Expression, attribute: AttributeReference) if value.foldable =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => GreaterThanOrEqual(maxValue(colName), value))

      // Filter "colA >= B"
      // NOTE: B could be an arbitrary _foldable_ expression (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "colA_maxValue >= B" for index lookup
      case GreaterThanOrEqual(attribute: AttributeReference, value: Expression) if value.foldable =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => GreaterThanOrEqual(maxValue(colName), value))

      // Filter "colA is null"
      // Translates to "colA_num_nulls > 0" for index lookup
      case IsNull(attribute: AttributeReference) =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => GreaterThan(numNulls(colName), Literal(0)))

      // Filter "colA is not null"
      // Translates to "colA_num_nulls = 0" for index lookup
      case IsNotNull(attribute: AttributeReference) =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => EqualTo(numNulls(colName), Literal(0)))

      // Filter "colA in (B1, B2, ...)"
      // NOTE: B1, ... , BN could be an arbitrary _foldable_ expressions (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "(colA_minValue <= B1 AND colA_maxValue >= B1) OR (colA_minValue <= B2 AND colA_maxValue >= B2) ... "
      // for index lookup
      // NOTE: This is equivalent to "colA = B1 OR colA = B2 OR ..."
      case In(attribute: AttributeReference, list: Seq[Expression]) if list.forall(_.foldable) =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName =>
            list.map { lit => colContainsValuesEqualTo(colName, lit) }.reduce(Or)
          )

      // Filter "colA not in (B1, B2, ...)"
      // NOTE: B1, ... , BN could be an arbitrary _foldable_ expressions (ie expression that is defined as the one
      //       [[Expression#foldable]] returns true for)
      //
      // Translates to "NOT((colA_minValue = B1 AND colA_maxValue = B1) OR (colA_minValue = B2 AND colA_maxValue = B2))" for index lookup
      // NOTE: This is NOT an inversion of `in (B1, B2, ...)` expr, this is equivalent to "colA != B1 AND colA != B2 AND ..."
      case Not(In(attribute: AttributeReference, list: Seq[Expression])) if list.forall(_.foldable) =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName =>
            Not(
              list.map { lit => colContainsOnlyValuesEqualTo(colName, lit) }.reduce(Or)
            )
          )

      // Filter "colA like 'xxx%'"
      // Translates to "colA_minValue <= xxx AND xxx <= colA_maxValue" for index lookup
      //
      // NOTE: Since a) this operator matches strings by prefix and b) given that this column is going to be ordered
      //       lexicographically, we essentially need to check that provided literal falls w/in min/max bounds of the
      //       given column
      case StartsWith(attribute, v @ Literal(_: UTF8String, _)) =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName => colContainsValuesEqualTo(colName, v))

      // Filter "colA not like 'xxx%'"
      // Translates to "NOT(colA_minValue like 'xxx%' AND colA_maxValue like 'xxx%')" for index lookup
      // NOTE: This is NOT an inversion of "colA like xxx"
      case Not(StartsWith(attribute, value @ Literal(_: UTF8String, _))) =>
        getTargetIndexedColName(attribute, indexSchema)
          .map(colName =>
            Not(And(StartsWith(minValue(colName), value), StartsWith(maxValue(colName), value)))
          )

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
      getNumNullsColumnNameFor(colName)
    )
      .forall(stat => indexSchema.exists(_.name == stat))
  }

  private def getTargetIndexedColName(resolvedExpr: Expression, indexSchema: StructType): Option[String] = {
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
