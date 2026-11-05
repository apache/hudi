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

import org.apache.hudi.ColumnStatsIndexSupport.{getMaxColumnNameFor, getMinColumnNameFor, getNullCountColumnNameFor, getValueCountColumnNameFor}
import org.apache.hudi.SparkAdapterSupport.sparkAdapter

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.functions.col
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
case class TermRange(lo: Expression, hi: Expression, nulls: Option[Expression], rows: Option[Expression])

/**
 * Where ranges come from. Returns `None` for anything it does not hold stats for.
 */
sealed trait RangeSource {
  def rangeOf(term: Expression): Option[TermRange]
}

/**
 * Column stats index: a column maps to its min / max / null-count / value-count columns.
 *
 * NOTE: Expression indexes go through this source too, but only with the bare column:
 *       `ExpressionIndexSupport` strips the indexed f(col) down to col before translating, so the
 *       stored f(col) bounds are compared as is
 */
case class ColumnStatsSource(indexedCols: Seq[String]) extends RangeSource {
  override def rangeOf(term: Expression): Option[TermRange] = term match {
    case column: AttributeReference =>
      getTargetIndexedColumnName(column, indexedCols).map { colName =>
        TermRange(
          lo = statsColumn(getMinColumnNameFor(colName)),
          hi = statsColumn(getMaxColumnNameFor(colName)),
          nulls = Some(statsColumn(getNullCountColumnNameFor(colName))),
          rows = Some(statsColumn(getValueCountColumnNameFor)))
      }
    case _ => None
  }

  /** A column of the transposed column stats table, for ex `qty_minValue`. */
  private def statsColumn(name: String): Expression = sparkAdapter.getExpressionFromColumn(col(name))
}

object TermRange {

  def derive(term: Expression, source: RangeSource): Option[TermRange] =
    derive(term, source, RangeRules.all)

  /**
   * Same as [[derive]], over the given rule table; tests pass their own rules here.
   *
   * A chain with a [[Decreasing]] step whose bounds still reference the column off the chain
   * derives no range (the file is kept): [[rebindOffChainReferences]] pairs the column's min
   * with the low bound, but past a [[Decreasing]] step the low bound is built from the max.
   */
  private[dataskipping] def derive(term: Expression, source: RangeSource, rules: Seq[RangeRule]): Option[TermRange] =
    viaRules(term, source, rules).flatMap { chain =>
      if (chain.hasDecreasingStep && hasOffChainReferences(term, chain.range)) {
        None
      } else {
        Some(rebindOffChainReferences(term, source, chain.range))
      }
    }

  /**
   * A derived range, and whether any step of the chain that derived it is [[Decreasing]].
   */
  private case class Chain(range: TermRange, hasDecreasingStep: Boolean)

  /**
   * The source first; otherwise the first rule step whose child derives a range. Several steps
   * only arise for operators accepted on either side (for ex, `BitwiseOr`), tried in today's order.
   */
  private def viaRules(term: Expression, source: RangeSource, rules: Seq[RangeRule]): Option[Chain] =
    source.rangeOf(term).map(r => Chain(r, hasDecreasingStep = false)).orElse(
      rules.flatMap(_.steps(term)).view.flatMap { step =>
        viaRules(step.child, source, rules).map { chain =>
          Chain(step.apply(chain.range), chain.hasDecreasingStep || step.direction == Decreasing)
        }
      }.headOption)

  /**
   * Whether a bound still holds the column once the chain rebuilt its slots, i.e. the term
   * references the column off the rule chain.
   */
  private def hasOffChainReferences(term: Expression, r: TermRange): Boolean =
    term.references.size == 1 && {
      val column = term.references.head.exprId
      def referencesColumn(bound: Expression): Boolean = bound.find {
        case a: AttributeReference => a.exprId == column
        case _ => false
      }.isDefined
      referencesColumn(r.lo) || referencesColumn(r.hi)
    }

  /**
   * Re-binds EVERY reference to the column, including ones in operands off the rule chain (for ex,
   * the days operand of `date_add(d, dayofmonth(d))`), matching the pre-stage translator. A [[Step]]
   * only rebuilds its chain slot, so the leftover references are re-bound to the same bound here.
   * Such terms are not order-preserving in general; kept as is so the output stays identical.
   */
  private def rebindOffChainReferences(term: Expression, source: RangeSource, r: TermRange): TermRange = {
    if (term.references.size != 1) {
      r
    } else {
      val column = term.references.head.exprId
      def rebind(bound: Expression, pick: TermRange => Expression): Expression = bound.transformDown {
        case a: AttributeReference if a.exprId == column => source.rangeOf(a).map(pick).getOrElse(a)
      }
      r.copy(lo = rebind(r.lo, _.lo), hi = rebind(r.hi, _.hi))
    }
  }
}
