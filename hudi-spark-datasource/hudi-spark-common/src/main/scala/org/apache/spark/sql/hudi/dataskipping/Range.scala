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

package org.apache.spark.sql.hudi.dataskipping

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.hudi.ColumnStatsExpressionUtils.{genColMaxValueExpr, genColMinValueExpr, genColNumNullsExpr, genColValueCountExpr}
import org.apache.spark.sql.hudi.DataSkippingUtils.getTargetIndexedColumnName

/**
 * Stage 3 (Range): what values can the term take in a file?
 *
 * `lo` / `hi` are expressions over the stats table bounding the term within one file; `nulls` / `rows`
 * are its null and value counts, when known. A range is read from a [[RangeSource]] for a column,
 * or derived through a chain of [[Step]]s from the range of the step's child.
 *
 * Invariant: no range means unknown, and unknown keeps the file.
 */
case class Range(lo: Expression, hi: Expression, nulls: Option[Expression], rows: Option[Expression])

/**
 * Where ranges come from. Returns `None` for anything it does not hold stats for.
 */
sealed trait RangeSource {
  def rangeOf(term: Expression): Option[Range]
}

/**
 * Column stats index: a column maps to its min / max / null-count / value-count columns.
 *
 * NOTE: An expression index stores f(col) bounds, yet today it is read through this same source, so
 *       the term's f is re-applied to the stored bounds. An `ExpressionIndexSource` would read the
 *       stored bounds as is for the indexed f(col) term
 */
case class ColumnStatsSource(indexedCols: Seq[String]) extends RangeSource {
  override def rangeOf(term: Expression): Option[Range] = term match {
    case column: AttributeReference =>
      getTargetIndexedColumnName(column, indexedCols).map { colName =>
        Range(
          lo = genColMinValueExpr(colName),
          hi = genColMaxValueExpr(colName),
          nulls = Some(genColNumNullsExpr(colName)),
          rows = Some(genColValueCountExpr))
      }
    case _ => None
  }
}

object Range {

  def derive(term: Expression, source: RangeSource): Option[Range] =
    viaRules(term, source).map(rebindOffChainReferences(term, source, _))

  /**
   * The source first; otherwise the first rule step whose child derives a range. Several steps
   * only arise for operators accepted on either side (for ex, `BitwiseOr`), tried in today's order.
   */
  private def viaRules(term: Expression, source: RangeSource): Option[Range] =
    source.rangeOf(term).orElse(
      RangeRules.of(term).view.flatMap(step => viaRules(step.child, source).map(step.apply)).headOption)

  /**
   * Re-binds EVERY reference to the column, including ones in operands off the rule chain (for ex,
   * the days operand of `date_add(d, dayofmonth(d))`), matching the pre-stage translator. A [[Step]]
   * only rebuilds its chain slot, so the leftover references are re-bound to the same bound here.
   * Such terms are not order-preserving in general; kept as is so the output stays identical.
   */
  private def rebindOffChainReferences(term: Expression, source: RangeSource, r: Range): Range = {
    if (term.references.size != 1) {
      r
    } else {
      val column = term.references.head.exprId
      def rebind(bound: Expression, pick: Range => Expression): Expression = bound.transformDown {
        case a: AttributeReference if a.exprId == column => source.rangeOf(a).map(pick).getOrElse(a)
      }
      r.copy(lo = rebind(r.lo, _.lo), hi = rebind(r.hi, _.hi))
    }
  }
}
