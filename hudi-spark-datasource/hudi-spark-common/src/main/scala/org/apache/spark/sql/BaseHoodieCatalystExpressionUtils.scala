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
import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, AttributeReference, AttributeSet, BitwiseOr, Cast, DateAdd, DateDiff, DateFormatClass, DateSub, Divide, Exp, Expm1, Expression, FromUnixTime, FromUTCTimestamp, Log, Log10, Log1p, Log2, Lower, Multiply, PredicateHelper, ShiftLeft, ShiftRight, ToUnixTimestamp, ToUTCTimestamp, Upper}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.types.DataType

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
        case DateDiff(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case DateDiff(_, OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case FromUnixTime(OrderPreservingTransformation(attrRef), _, _) => Some(attrRef)
        case FromUTCTimestamp(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        case ToUnixTimestamp(OrderPreservingTransformation(attrRef), _, _, _) => Some(attrRef)
        case ToUTCTimestamp(OrderPreservingTransformation(attrRef), _) => Some(attrRef)

        // String Expressions
        case Lower(OrderPreservingTransformation(attrRef)) => Some(attrRef)
        case Upper(OrderPreservingTransformation(attrRef)) => Some(attrRef)
        // Left API change: Improve RuntimeReplaceable
        // https://issues.apache.org/jira/browse/SPARK-38240
        case org.apache.spark.sql.catalyst.expressions.Left(OrderPreservingTransformation(attrRef), _) => Some(attrRef)

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
  }
}
