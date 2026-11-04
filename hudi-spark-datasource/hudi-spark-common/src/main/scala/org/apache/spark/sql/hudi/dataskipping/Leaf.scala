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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, InSet, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, StartsWith, SubqueryExpression}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Stage 2 (Leaf): is this predicate a single `term OP constant` the index can answer?
 *
 * Invariants:
 *  - The term references exactly one column and holds no subquery; the values reference no column.
 *  - The term is put on the left in exactly one place (`comparison`); nothing downstream flips.
 *  - IS NULL / IS NOT NULL only take a bare column.
 *  - The only NOTs understood here are NotEq, NotIn and NotStartsWith; any other NOT is not a leaf.
 */
sealed trait Op

object Op {
  case object Eq extends Op
  case object NotEq extends Op
  case object Lt extends Op
  case object LtEq extends Op
  case object Gt extends Op
  case object GtEq extends Op
  case object In extends Op
  case object NotIn extends Op
  case object StartsWith extends Op
  case object NotStartsWith extends Op
  case object IsNull extends Op
  case object IsNotNull extends Op
}

case class Leaf(term: Expression, op: Op, values: Seq[Expression])

object Leaf {

  def parse(e: Expression): Option[Leaf] = e match {
    case EqualTo(left, right) => comparison(left, Op.Eq, right)
    case Not(EqualTo(left, right)) => comparison(left, Op.NotEq, right)

    // "colA <=> null" is "colA IS NULL"
    case EqualNullSafe(column: AttributeReference, Literal(null, _)) => Some(Leaf(column, Op.IsNull, Nil))

    case LessThan(left, right) => comparison(left, Op.Lt, right)
    case LessThanOrEqual(left, right) => comparison(left, Op.LtEq, right)
    case GreaterThan(left, right) => comparison(left, Op.Gt, right)
    case GreaterThanOrEqual(left, right) => comparison(left, Op.GtEq, right)

    case IsNull(column: AttributeReference) => Some(Leaf(column, Op.IsNull, Nil))
    case IsNotNull(column: AttributeReference) => Some(Leaf(column, Op.IsNotNull, Nil))

    case In(term, list) if isTerm(term) && list.forall(isValueExpression) => Some(Leaf(term, Op.In, list))
    // NOTE: [[InSet]] is the optimized [[In]] over static values. [[Literal#apply]] does not accept
    //       [[UTF8String]], so strings are converted first
    case InSet(term, hset) if isTerm(term) => Some(Leaf(term, Op.In, hset.toSeq.map(toLiteral)))
    // NOTE: NotIn checks `foldable` rather than `isValueExpression`, as today
    case Not(In(term, list)) if isTerm(term) && list.forall(_.foldable) => Some(Leaf(term, Op.NotIn, list))

    case StartsWith(term, prefix @ Literal(_: UTF8String, _)) if isTerm(term) =>
      Some(Leaf(term, Op.StartsWith, Seq(prefix)))
    case Not(StartsWith(term, prefix @ Literal(_: UTF8String, _))) if isTerm(term) =>
      Some(Leaf(term, Op.NotStartsWith, Seq(prefix)))

    case _ => None
  }

  /**
   * The one place a comparison is put in canonical form: `term OP value` is kept, `value OP term`
   * becomes `term flip(OP) value`. A side cannot be both a term (one column) and a value (no column),
   * so at most one orientation applies.
   */
  private def comparison(left: Expression, op: Op, right: Expression): Option[Leaf] =
    if (isTerm(left) && isValueExpression(right)) {
      Some(Leaf(left, op, Seq(right)))
    } else if (isTerm(right) && isValueExpression(left)) {
      Some(Leaf(right, flip(op), Seq(left)))
    } else {
      None
    }

  private def flip(op: Op): Op = op match {
    case Op.Eq => Op.Eq
    case Op.NotEq => Op.NotEq
    case Op.Lt => Op.Gt
    case Op.LtEq => Op.GtEq
    case Op.Gt => Op.Lt
    case Op.GtEq => Op.LtEq
    case other => throw new IllegalArgumentException(s"Not a comparison: $other")
  }

  /**
   * A term has no subquery and references exactly one column.
   * Whether the term is an order-preserving function of that column is decided in stage 3.
   */
  private def isTerm(e: Expression): Boolean =
    !SubqueryExpression.hasSubquery(e) && e.references.size == 1

  /**
   * A value references no column and holds no subquery, so Spark can evaluate it against the stats
   * table as well.
   */
  private def isValueExpression(e: Expression): Boolean =
    e.references.isEmpty && !SubqueryExpression.hasSubquery(e)

  private def toLiteral(value: Any): Literal = value match {
    case str: UTF8String => Literal(str.toString)
    case _ => Literal(value)
  }
}
