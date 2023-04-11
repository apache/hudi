/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi

import org.apache.hudi.expression.{Predicates, AttributeReference => HAttributeReference, Expression => HExpression, Literal => HLiteral}
import org.apache.hudi.internal.schema.{Type, Types}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BinaryComparison, Expression, In, InSet, Literal, Not, Or, StartsWith}
import org.apache.spark.sql.types.{BooleanType, ByteType, CharType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType, VarcharType}

import scala.jdk.CollectionConverters.seqAsJavaListConverter

object ExpressionConverter {

  def convertFilters(filters: Seq[Expression], timeZoneId: String): Option[HExpression] =
    filters.foldLeft(Option.empty[HExpression]) { (left, expr) =>
      val result = convertFilter(expr, timeZoneId)
      (left, result) match {
        case (None, None) => None
        case (None, Some(r)) => Some(r)
        case (Some(l), None) => Some(l)
        case (Some(l), Some(r)) => Some(Predicates.and(l, r))
      }
    }

  def convertFilter(filter: Expression, timeZoneId: String): Option[HExpression] = filter match {
    case In(value, list) =>
      val convertedValue = convertFilter(value, timeZoneId)
      val convertedList = list.map(convertFilter(_, timeZoneId))
      if (convertedList.forall(_.isDefined) && convertedValue.isDefined) {
        Some(Predicates.in(convertedValue.get, convertedList.map(_.get).asJava))
      } else {
        None
      }

    case InSet(child, values) =>
      val dataType = convertDataType(child.dataType)
      val literals = values.map(new HLiteral(_, dataType).asInstanceOf[HExpression]).toList.asJava
      val convertedChild = convertFilter(child, timeZoneId)
      if (convertedChild.isDefined) {
        Some(Predicates.in(convertedChild.get, literals))
      } else {
        None
      }

    case op@BinaryComparison(left, right) => op.symbol match {
      case "=" =>
        for {
          convertedLeft <- convertFilter(left, timeZoneId)
          convertedRight <- convertFilter(right, timeZoneId)
        } yield Predicates.eq(convertedLeft, convertedRight)
      case ">" =>
        for {
          convertedLeft <- convertFilter(left, timeZoneId)
          convertedRight <- convertFilter(right, timeZoneId)
        } yield Predicates.gt(convertedLeft, convertedRight)
      case ">=" =>
        for {
          convertedLeft <- convertFilter(left, timeZoneId)
          convertedRight <- convertFilter(right, timeZoneId)
        } yield Predicates.gteq(convertedLeft, convertedRight)
      case "<" =>
        for {
          convertedLeft <- convertFilter(left, timeZoneId)
          convertedRight <- convertFilter(right, timeZoneId)
        } yield Predicates.lt(convertedLeft, convertedRight)
      case "<=" =>
        for {
          convertedLeft <- convertFilter(left, timeZoneId)
          convertedRight <- convertFilter(right, timeZoneId)
        } yield Predicates.lteq(convertedLeft, convertedRight)
    }

    case StartsWith(left, right) =>
      for {
        left <- convertFilter(left, timeZoneId)
        right <- convertFilter(right, timeZoneId)
      } yield Predicates.startsWith(left, right)

    case And(expr1, expr2) =>
      val convertedExpr1 = convertFilter(expr1, timeZoneId)
      val convertedExpr2 = convertFilter(expr2, timeZoneId)
      if (convertedExpr1.isDefined && convertedExpr2.isDefined) {
        Some(Predicates.and(convertedExpr1.get, convertedExpr2.get))
      } else if (convertedExpr1.isDefined) {
        convertedExpr1
      } else if (convertedExpr2.isDefined) {
        convertedExpr2
      } else {
        None
      }

    case Or(expr1, expr2) =>
      for {
        left <- convertFilter(expr1, timeZoneId)
        right <- convertFilter(expr2, timeZoneId)
      } yield Predicates.or(left, right)

    case Not(child) =>
      val convertedChild = convertFilter(child, timeZoneId)
      if (convertedChild.isDefined) {
        Some(Predicates.not(convertedChild.get))
      } else {
        None
      }

    case AttributeReference(name, dataType, nullable, _) =>
      Some(new HAttributeReference(name, convertDataType(dataType), nullable))

    case Literal(value, dataType) =>
      Some(new HLiteral(value, convertDataType(dataType)))

    case _ =>
      None
  }

  def convertDataType(sparkType: DataType): Type = sparkType match {
    case BooleanType =>
      Types.BooleanType.get()
    case IntegerType | ShortType | ByteType =>
      Types.IntType.get()
    case LongType =>
      Types.LongType.get()
    case FloatType =>
      Types.FloatType.get()
    case DoubleType =>
      Types.DoubleType.get()
    case StringType | CharType(_) | VarcharType(_) =>
      Types.StringType.get()
    case DateType =>
      Types.DateType.get()
    case TimestampType =>
      Types.TimestampType.get()
    case _type: DecimalType =>
      Types.DecimalType.get(_type.precision, _type.scale)
    case _ =>
      throw new UnsupportedOperationException(s"Cannot convert spark type $sparkType to the relate HUDI type")
  }
}
