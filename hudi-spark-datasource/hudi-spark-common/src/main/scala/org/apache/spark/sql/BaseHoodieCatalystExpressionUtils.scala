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

import org.apache.spark.sql.HoodieSparkTypeUtils.isCastPreservingOrdering
import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, AttributeReference, AttributeSet, BitwiseOr, Cast, DateAdd, DateDiff, DateFormatClass, DateSub, Divide, Exp, Expm1, Expression, FromUnixTime, FromUTCTimestamp, Log, Log10, Log1p, Log2, Multiply, PredicateHelper, ShiftRight, ToUnixTimestamp, ToUTCTimestamp}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}

/**
 * Base implementation of [[HoodieCatalystExpressionUtils]] carrying the method bodies that are
 * identical across all supported Spark versions. Methods relying on Spark APIs that changed
 * across versions are implemented in the per-version `HoodieSparkXXCatalystExpressionUtils`
 * objects (or in [[HoodieSpark4CatalystExpressionUtils]] when shared within a Spark major version).
 */
abstract class BaseHoodieCatalystExpressionUtils extends HoodieCatalystExpressionUtils with PredicateHelper {

  override def normalizeExprs(exprs: Seq[Expression], attributes: Seq[Attribute]): Seq[Expression] = {
    DataSourceStrategy.normalizeExprs(exprs, attributes)
  }

  override def extractPredicatesWithinOutputSet(condition: Expression,
                                                outputSet: AttributeSet): Option[Expression] = {
    super[PredicateHelper].extractPredicatesWithinOutputSet(condition, outputSet)
  }

  override def tryMatchAttributeOrderingPreservingTransformation(expr: Expression): Option[AttributeReference] = {
    expr match {
      case OrderPreservingTransformation(attrRef) => Some(attrRef)
      case _ => None
    }
  }

  def canUpCast(fromType: DataType, toType: DataType): Boolean =
    Cast.canUpCast(fromType, toType)

  /**
   * Matches order-preserving date/time parsing expressions whose case-class shapes differ across
   * Spark versions (currently [[org.apache.spark.sql.catalyst.expressions.ParseToDate]] and
   * [[org.apache.spark.sql.catalyst.expressions.ParseToTimestamp]]), returning the source child
   * expression that the order-preserving transformation matching should recurse into
   */
  protected def unapplyOrderPreservingDateParsing(expr: Expression): Option[Expression]

  private object OrderPreservingTransformation {
    def unapply(expr: Expression): Option[AttributeReference] = {
      expr match {
        // Date/Time Expressions
        case DateFormatClass(OrderPreservingTransformation(attrRef), _, _) => Some(attrRef)
        case DateAdd(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case DateSub(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        // NOTE: DateDiff(end, start) is end - start, so it reverses the ordering of its start date,
        //       which hence must not be matched
        case DateDiff(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case FromUnixTime(OrderPreservingTransformation(attrRef), _, _) => Some(attrRef)
        case FromUTCTimestamp(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case ToUnixTimestamp(OrderPreservingTransformation(attrRef), _, _, _) => Some(attrRef)
        case ToUTCTimestamp(OrderPreservingTransformation(attrRef), _) => Some(attrRef)

        // String Expressions
        // NOTE: Lower/Upper are not matched: case folding does not preserve the binary ordering
        //       of strings (for ex, "Zebra" < "apple", but lower("Zebra") = "zebra" > "apple")
        // Left API change: Improve RuntimeReplaceable
        // https://issues.apache.org/jira/browse/SPARK-38240
        case org.apache.spark.sql.catalyst.expressions.Left(OrderPreservingTransformation(attrRef), _) => Some(attrRef)

        // Math Expressions
        // Binary
        // NOTE: Integral Add/Multiply wrap on overflow in non-ANSI mode, so re-applying them to
        //       min/max stats can invert the bound and prune matching files (#20460). Decimal can
        //       overflow into NULL. IEEE float/double saturate to +/- Inf and keep ordering, so
        //       only those result types are matched. ShiftLeft is integral-only and always wraps.
        case add @ Add(OrderPreservingTransformation(attrRef), _, _)
          if isNonWrappingNumericType(add.dataType) => Some(attrRef)
        case add @ Add(_, OrderPreservingTransformation(attrRef), _)
          if isNonWrappingNumericType(add.dataType) => Some(attrRef)
        case mul @ Multiply(OrderPreservingTransformation(attrRef), _, _)
          if isNonWrappingNumericType(mul.dataType) => Some(attrRef)
        case mul @ Multiply(_, OrderPreservingTransformation(attrRef), _)
          if isNonWrappingNumericType(mul.dataType) => Some(attrRef)
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
        case ShiftRight(OrderPreservingTransformation(attrRef), _) => Some(attrRef)

        // Other
        case cast @ Cast(OrderPreservingTransformation(attrRef), _, _, _)
          if isCastPreservingOrdering(cast.child.dataType, cast.dataType) => Some(attrRef)

        // Identity transformation
        case attrRef: AttributeReference => Some(attrRef)
        // Date/time parsing expressions whose shapes are Spark-version-specific
        case _ => unapplyOrderPreservingDateParsing(expr) match {
          case Some(child) => unapply(child)
          case None => None
        }
      }
    }

    // IEEE float/double saturate to +/- Inf on overflow and keep ordering; integral wraps and
    // decimal can overflow into NULL, so those result types must not translate into stats filters
    private def isNonWrappingNumericType(dt: DataType): Boolean = dt match {
      case FloatType | DoubleType => true
      case _ => false
    }
  }
}
