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

package org.apache.spark.sql

import HoodieSparkTypeUtils.isCastPreservingOrdering
import org.apache.spark.sql.catalyst.expressions.{Add, AnsiCast, AttributeReference, BitwiseOr, Cast, DateAdd, DateDiff, DateFormatClass, DateSub, Divide, Exp, Expm1, Expression, FromUTCTimestamp, FromUnixTime, Log, Log10, Log1p, Log2, Lower, Multiply, ParseToDate, ParseToTimestamp, ShiftLeft, ShiftRight, ToUTCTimestamp, ToUnixTimestamp, Upper}
import org.apache.spark.sql.types.DataType

object HoodieSpark31CatalystExpressionUtils extends HoodieCatalystExpressionUtils {

  override def tryMatchAttributeOrderingPreservingTransformation(expr: Expression): Option[AttributeReference] = {
    expr match {
      case OrderPreservingTransformation(attrRef) => Some(attrRef)
      case _ => None
    }
  }

  def canUpCast(fromType: DataType, toType: DataType): Boolean =
    Cast.canUpCast(fromType, toType)

  override def unapplyCastExpression(expr: Expression): Option[(Expression, DataType, Option[String], Boolean)] =
    expr match {
      case Cast(castedExpr, dataType, timeZoneId) => Some((castedExpr, dataType, timeZoneId, false))
      case AnsiCast(castedExpr, dataType, timeZoneId) => Some((castedExpr, dataType, timeZoneId, true))
      case _ => None
    }

  private object OrderPreservingTransformation {
    def unapply(expr: Expression): Option[AttributeReference] = {
      expr match {
        // Date/Time Expressions
        case DateFormatClass(OrderPreservingTransformation(attrRef), _, _) => Some(attrRef)
        case DateAdd(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case DateSub(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case DateDiff(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case DateDiff(_, OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case FromUnixTime(OrderPreservingTransformation(attrRef), _, _) => Some(attrRef)
        case FromUTCTimestamp(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case ParseToDate(OrderPreservingTransformation(attrRef), _, _) => Some(attrRef)
        case ParseToTimestamp(OrderPreservingTransformation(attrRef), _, _) => Some(attrRef)
        case ToUnixTimestamp(OrderPreservingTransformation(attrRef), _, _, _) => Some(attrRef)
        case ToUTCTimestamp(OrderPreservingTransformation(attrRef), _) => Some(attrRef)

        // String Expressions
        case Lower(OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case Upper(OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case org.apache.spark.sql.catalyst.expressions.Left(OrderPreservingTransformation(attrRef), _, _) => Some(attrRef)

        // Math Expressions
        // Binary
        case Add(OrderPreservingTransformation(attrRef), _, _) => Some(attrRef)
        case Add(_, OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case Multiply(OrderPreservingTransformation(attrRef), _, _) => Some(attrRef)
        case Multiply(_, OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case Divide(OrderPreservingTransformation(attrRef), _, _) => Some(attrRef)
        case BitwiseOr(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case BitwiseOr(_, OrderPreservingTransformation(attrRef)) => Some(attrRef)
        // Unary
        case Exp(OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case Expm1(OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case Log(OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case Log10(OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case Log1p(OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case Log2(OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case ShiftLeft(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case ShiftRight(OrderPreservingTransformation(attrRef), _) => Some(attrRef)

        // Other
        case cast @ Cast(OrderPreservingTransformation(attrRef), _, _)
          if isCastPreservingOrdering(cast.child.dataType, cast.dataType) => Some(attrRef)

        // Identity transformation
        case attrRef: AttributeReference => Some(attrRef)
        // No match
        case _ => None
      }
    }
  }
}
