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
import org.apache.hudi.SparkAdapterSupport.sparkAdapter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, EqualNullSafe, Expression, ExtractValue, GetStructField, IsNotNull, IsNull, Literal, Not, Or, SubqueryExpression}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.hudi.dataskipping.{ColumnStatsSource, Compiler}

import java.util.concurrent.atomic.AtomicBoolean

object DataSkippingUtils extends Logging {

  /**
   * Translates provided {@link filterExpr} into corresponding filter-expression for column-stats index table
   * to filter out candidate files that would hold records matching the original filter.
   * In case the column stats were creating using expression index, the index filter must also account for the expression.
   *
   * @param dataTableFilterExpr source table's query's filter expression
   * @param indexSchema index table schema
   * @param isExpressionIndex whether the index is an expression index
   * @return filter for column-stats index's table
   */
  def translateIntoColumnStatsIndexFilterExpr(dataTableFilterExpr: Expression, isExpressionIndex: Boolean = false, indexedCols : Seq[String] = Seq.empty,
                                              hasNonIndexedCols : AtomicBoolean = new AtomicBoolean(false)): Expression = {
    try {
      Compiler.compile(dataTableFilterExpr, ColumnStatsSource(indexedCols), strictResolution = !isExpressionIndex)
    } catch {
      case e: AnalysisException =>
        logDebug(s"Failed to translated provided data table filter expr into column stats one ($dataTableFilterExpr)", e)
        throw e
    }
  }

  private[hudi] def getTargetIndexedColumnName(resolvedExpr: AttributeReference, indexedCols: Seq[String]): Option[String] = {
    val colName = UnresolvedAttribute(getTargetColNameParts(resolvedExpr)).name

    // Verify that the column is indexed
    if (indexedCols.contains(colName)) {
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
        throw new HoodieAnalysisException(s"convert reference to name failed, Updating nested fields is only supported for StructType: ${ex}.")
      case other =>
        throw new HoodieAnalysisException(s"convert reference to name failed,  Found unsupported expression ${other}")
    }
  }

  /**
   * Deduces if filter contains Null count or value count based filters. Partitions filters for MOR table cannot be looked up in
   * Partition Stats Index if they contain these filters.
   *
   * @param sourceFilterExpr source expr filter of interest.
   * @param indexedCols      list of indexed cols
   * @return {@code true} if source filters contains null or value count based filters. {@code false} otherwise.
   */
  def containsNullOrValueCountBasedFilters(sourceFilterExpr: Expression, indexedCols: Seq[String]): Boolean = {
    // To handle Not(IsNull(...)) automatically as IsNotNull(...), or Not(IsNotNull(...)) as IsNull(...),
    // we normalize the expression to push down the Not operator to transform once. Then, we
    // only match on Or, And, IsNull, IsNotNull, etc. on the "not-pushed-down" expression.
    val normalizedNotExpr = pushDownNot(sourceFilterExpr)

    normalizedNotExpr match {
      // If Expression is not resolved, we can't perform the analysis accurately, bailing
      case expr if !expr.resolved => false

      // Filter "colA <=> null"
      // Translates to "colA_nullCount IS NULL OR colA_nullCount > 0" for index lookup
      case EqualNullSafe(attrRef: AttributeReference, litNull@Literal(null, _)) =>
        getTargetIndexedColumnName(attrRef, indexedCols).isDefined

      // Filter "colA is null"
      // Translates to "colA_nullCount IS NULL OR colA_nullCount > 0" for index lookup
      case IsNull(attribute: AttributeReference) =>
        getTargetIndexedColumnName(attribute, indexedCols).isDefined

      // Filter "colA is not null"
      // Translates to "colA_nullCount IS NULL OR colA_valueCount IS NULL OR colA_nullCount < colA_valueCount" for index lookup
      // "colA_nullCount IS NULL OR colA_valueCount IS NULL" means we are not certain whether the column is null or not,
      // hence we return True to ensure this does not affect the query.
      case IsNotNull(attribute: AttributeReference) =>
        getTargetIndexedColumnName(attribute, indexedCols).isDefined

      case or: Or =>
        val resLeft = containsNullOrValueCountBasedFilters(or.left, indexedCols = indexedCols)
        val resRight = containsNullOrValueCountBasedFilters(or.right, indexedCols = indexedCols)
        resLeft || resRight

      case and: And =>
        val resLeft = containsNullOrValueCountBasedFilters(and.left, indexedCols = indexedCols)
        val resRight = containsNullOrValueCountBasedFilters(and.right, indexedCols = indexedCols)
        resLeft || resRight

      case _: Expression => false
    }
  }

  private def pushDownNot(expr: Expression): Expression = expr match {
    case Not(And(l, r)) => Or(pushDownNot(Not(l)), pushDownNot(Not(r)))
    case Not(Or(l, r)) => And(pushDownNot(Not(l)), pushDownNot(Not(r)))
    case Not(IsNull(a)) => IsNotNull(a)
    case Not(IsNotNull(a)) => IsNull(a)
    case Not(child) => pushDownNot(child)
    case other => other
  }
}

object ColumnStatsExpressionUtils {

  @inline def genColMinValueExpr(colName: String): Expression = sparkAdapter.getExpressionFromColumn(col(getMinColumnNameFor(colName)))
  @inline def genColMaxValueExpr(colName: String): Expression = sparkAdapter.getExpressionFromColumn(col(getMaxColumnNameFor(colName)))
  @inline def genColNumNullsExpr(colName: String): Expression = sparkAdapter.getExpressionFromColumn(col(getNullCountColumnNameFor(colName)))
  @inline def genColValueCountExpr: Expression = sparkAdapter.getExpressionFromColumn(col(getValueCountColumnNameFor))

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
}
