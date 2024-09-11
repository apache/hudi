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

package org.apache.spark.sql

import HoodieSparkTypeUtils.isCastPreservingOrdering
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Add, And, Attribute, AttributeReference, AttributeSet, BitwiseOr, Cast, DateAdd, DateDiff, DateFormatClass, DateSub, Divide, Exp, Expm1, Expression, FromUTCTimestamp, FromUnixTime, Like, Log, Log10, Log1p, Log2, Lower, Multiply, Or, ParseToDate, ParseToTimestamp, ShiftLeft, ShiftRight, ToUTCTimestamp, ToUnixTimestamp, Upper}
import org.apache.spark.sql.types.{DataType, StructType}

object HoodieSpark2CatalystExpressionUtils extends HoodieCatalystExpressionUtils {

  override def getEncoder(schema: StructType): ExpressionEncoder[Row] = {
    RowEncoder.apply(schema).resolveAndBind()
  }

  // NOTE: This method has been borrowed from Spark 3.1
  override def extractPredicatesWithinOutputSet(condition: Expression,
                                                outputSet: AttributeSet): Option[Expression] = condition match {
    case And(left, right) =>
      val leftResultOptional = extractPredicatesWithinOutputSet(left, outputSet)
      val rightResultOptional = extractPredicatesWithinOutputSet(right, outputSet)
      (leftResultOptional, rightResultOptional) match {
        case (Some(leftResult), Some(rightResult)) => Some(And(leftResult, rightResult))
        case (Some(leftResult), None) => Some(leftResult)
        case (None, Some(rightResult)) => Some(rightResult)
        case _ => None
      }

    // The Or predicate is convertible when both of its children can be pushed down.
    // That is to say, if one/both of the children can be partially pushed down, the Or
    // predicate can be partially pushed down as well.
    //
    // Here is an example used to explain the reason.
    // Let's say we have
    // condition: (a1 AND a2) OR (b1 AND b2),
    // outputSet: AttributeSet(a1, b1)
    // a1 and b1 is convertible, while a2 and b2 is not.
    // The predicate can be converted as
    // (a1 OR b1) AND (a1 OR b2) AND (a2 OR b1) AND (a2 OR b2)
    // As per the logical in And predicate, we can push down (a1 OR b1).
    case Or(left, right) =>
      for {
        lhs <- extractPredicatesWithinOutputSet(left, outputSet)
        rhs <- extractPredicatesWithinOutputSet(right, outputSet)
      } yield Or(lhs, rhs)

    // Here we assume all the `Not` operators is already below all the `And` and `Or` operators
    // after the optimization rule `BooleanSimplification`, so that we don't need to handle the
    // `Not` operators here.
    case other =>
      if (other.references.subsetOf(outputSet)) {
        Some(other)
      } else {
        None
      }
  }

  // NOTE: This method has been borrowed from Spark 3.1
  override def normalizeExprs(exprs: Seq[Expression], attributes: Seq[Attribute]): Seq[Expression] = {
    exprs.map {
      _.transform {
        case a: AttributeReference =>
          a.withName(attributes.find(_.semanticEquals(a)).getOrElse(a).name)
      }
    }
  }

  override def matchCast(expr: Expression): Option[(Expression, DataType, Option[String])] =
    expr match {
      case Cast(child, dataType, timeZoneId) => Some((child, dataType, timeZoneId))
      case _ => None
    }

  override def tryMatchAttributeOrderingPreservingTransformation(expr: Expression): Option[AttributeReference] = {
    expr match {
      case OrderPreservingTransformation(attrRef) => Some(attrRef)
      case _ => None
    }
  }

  def canUpCast(fromType: DataType, toType: DataType): Boolean =
    // Spark 2.x does not support up-casting, hence we simply check whether types are
    // actually the same
    fromType.sameType(toType)

  override def unapplyCastExpression(expr: Expression): Option[(Expression, DataType, Option[String], Boolean)] =
    expr match {
      case Cast(castedExpr, dataType, timeZoneId) => Some((castedExpr, dataType, timeZoneId, false))
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
        case ToUnixTimestamp(OrderPreservingTransformation(attrRef), _, _) => Some(attrRef)
        case ToUTCTimestamp(OrderPreservingTransformation(attrRef), _) => Some(attrRef)

        // String Expressions
        case Lower(OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case Upper(OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case org.apache.spark.sql.catalyst.expressions.Left(OrderPreservingTransformation(attrRef), _, _) => Some(attrRef)

        // Math Expressions
        // Binary
        case Add(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case Add(_, OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case Multiply(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case Multiply(_, OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case Divide(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
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
