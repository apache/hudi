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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, ExtractValue, GetStructField}
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.hudi.dataskipping.{ColumnStatsSource, ExpressionIndexSource, StatsFilterCompiler}

import java.util.concurrent.atomic.AtomicBoolean

object DataSkippingUtils extends Logging {

  /**
   * Translates provided {@link filterExpr} into corresponding filter-expression for column-stats index table
   * to filter out candidate files that would hold records matching the original filter.
   *
   * @param dataTableFilterExpr source table's query's filter expression
   * @param indexSchema index table schema
   * @return filter for column-stats index's table
   */
  def translateIntoColumnStatsIndexFilterExpr(dataTableFilterExpr: Expression, indexedCols : Seq[String] = Seq.empty,
                                              hasNonIndexedCols : AtomicBoolean = new AtomicBoolean(false)): Expression = {
    try {
      StatsFilterCompiler.compile(dataTableFilterExpr, ColumnStatsSource(indexedCols))
    } catch {
      case e: AnalysisException =>
        logDebug(s"Failed to translated provided data table filter expr into column stats one ($dataTableFilterExpr)", e)
        throw e
    }
  }

  /**
   * Same as [[translateIntoColumnStatsIndexFilterExpr]], against one expression index: only a term that is the
   * index's `f(col)` reads its stats, every other leaf keeps the file.
   */
  def translateIntoExpressionIndexFilterExpr(dataTableFilterExpr: Expression, source: ExpressionIndexSource): Expression =
    StatsFilterCompiler.compile(dataTableFilterExpr, source)

  /** Returns the column name of `resolvedExpr` if it is one of `indexedCols`, else None. */
  def getTargetIndexedColumnName(resolvedExpr: AttributeReference, indexedCols: Seq[String]): Option[String] = {
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
}
