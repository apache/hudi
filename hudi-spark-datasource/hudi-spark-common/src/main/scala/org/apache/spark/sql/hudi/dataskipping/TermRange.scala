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
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hudi.DataSkippingUtils.getTargetIndexedColumnName

/**
 * Stage 3 (Range): what values can the term take in a file?
 *
 * `lo` / `hi` are expressions over the stats table bounding the term within one file; `nulls` / `rows`
 * are its null and value counts, when known. A range is read from a [[RangeSource]] for a column,
 * or derived through a chain of [[Step]]s from the range of the step's child.
 *
 * `nullBoundIsUnknown` says what a NULL `lo` / `hi` means. False for a column, and for a chain of
 * total, unguarded steps: the bound is NULL only when every value of the file is NULL, and no row of
 * such a file passes `term OP value`, so the NULL verdict may drop it. True once a `partial` step
 * (the term is undefined for some values) or a `fileGuard` step (the mapping is unknown outside the
 * guard) derived the range: the bound may be NULL in a file holding matching rows, and
 * [[StatsFilterCompiler]] keeps the file on a NULL verdict.
 *
 * Invariant: no range means unknown, and unknown keeps the file.
 */
case class TermRange(lo: Expression,
                     hi: Expression,
                     nulls: Option[Expression],
                     rows: Option[Expression],
                     nullBoundIsUnknown: Boolean)

/**
 * Where ranges come from. Returns `None` for anything it does not hold stats for.
 */
sealed trait RangeSource {
  def rangeOf(term: Expression): Option[TermRange]
}

/**
 * Column stats index: a column maps to its min / max / null-count / value-count columns.
 */
case class ColumnStatsSource(indexedCols: Seq[String]) extends RangeSource {
  override def rangeOf(term: Expression): Option[TermRange] = term match {
    case column: AttributeReference => getTargetIndexedColumnName(column, indexedCols).map(TermRange.ofStatsColumns)
    case _ => None
  }
}

/**
 * Expression index: the stats of one `f(col)`, stored under the source column's name. Only a term that is
 * that `f(col)` has a range; any other term, including the bare column of a non-identity index, has none.
 * A rule step may still reach the indexed `f(col)` as its child, for ex `CAST(hour(ts) AS BIGINT)`.
 */
case class ExpressionIndexSource(indexed: IndexedExpression) extends RangeSource {
  override def rangeOf(term: Expression): Option[TermRange] =
    if (indexed.matches(term)) Some(TermRange.ofStatsColumns(indexed.sourceColumn)) else None
}

object TermRange {

  /** The range read from the transposed stats table, for ex `qty_minValue` / `qty_maxValue`. */
  private[dataskipping] def ofStatsColumns(colName: String): TermRange =
    TermRange(
      lo = statsColumn(getMinColumnNameFor(colName)),
      hi = statsColumn(getMaxColumnNameFor(colName)),
      nulls = Some(statsColumn(getNullCountColumnNameFor(colName))),
      rows = Some(statsColumn(getValueCountColumnNameFor)),
      nullBoundIsUnknown = false)

  private def statsColumn(name: String): Expression = sparkAdapter.getExpressionFromColumn(col(quoteIdentifier(name)))

  def derive(term: Expression, source: RangeSource): Option[TermRange] =
    derive(term, source, RangeRules.all)

  /**
   * Same as [[derive]], over the given rule table; tests pass their own rules here.
   *
   * A chain with a [[Decreasing]] step whose bounds still reference the column off the chain derives
   * no range (the file is kept): [[rebindOffChainReferences]] pairs the column's min with the low
   * bound, but past a [[Decreasing]] step the low bound is built from the max.
   *
   * A chain with a `fileGuard` or `partial` step derives a range whose `nullBoundIsUnknown` is set.
   */
  def derive(term: Expression, source: RangeSource, rules: Seq[RangeRule]): Option[TermRange] =
    viaRules(term, source, rules).flatMap { chain =>
      if (chain.hasDecreasingStep && hasOffChainReferences(term, chain.range)) {
        None
      } else {
        rebindOffChainReferences(term, source, chain.range)
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
          Chain(
            step.apply(chain.range),
            hasDecreasingStep = chain.hasDecreasingStep || step.direction == Decreasing)
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
   *
   * No range when the source holds no stats for the bare column (an expression index holds only its
   * `f(col)`): the bound would otherwise keep a column the stats table does not have.
   */
  private def rebindOffChainReferences(term: Expression, source: RangeSource, r: TermRange): Option[TermRange] = {
    if (!hasOffChainReferences(term, r)) {
      Some(r)
    } else {
      val column = term.references.head.exprId
      val references = Seq(r.lo, r.hi).flatMap(_.collect { case a: AttributeReference if a.exprId == column => a })
      val columnRanges = references.map(a => a -> source.rangeOf(a)).toMap
      if (columnRanges.values.exists(_.isEmpty)) {
        None
      } else {
        def rebind(bound: Expression, pick: TermRange => Expression): Expression = bound.transformDown {
          case a: AttributeReference if a.exprId == column => pick(columnRanges(a).get)
        }
        Some(r.copy(lo = rebind(r.lo, _.lo), hi = rebind(r.hi, _.hi)))
      }
    }
  }
}
