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
import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, AttributeReference, AttributeSet, BitwiseOr, Cast, DateAdd, DateDiff, DateFormatClass, DateSub, Divide, Exp, Expm1, Expression, FromUnixTime, FromUTCTimestamp, GetTimestamp, Literal, Log, Log10, Log1p, Log2, Lower, Multiply, PredicateHelper, ShiftLeft, ShiftRight, ToUnixTimestamp, ToUTCTimestamp, Upper}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.types.{DataType, StringType}

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
   * Date/time format patterns for which lexicographic ordering of the formatted strings
   * coincides with the chronological ordering of the values they parse to: fixed-width,
   * year-first patterns. Only these make it sound to re-apply a string-to-date/timestamp
   * parse on top of string min/max column stats (see [[OrderPreservingTransformation]]).
   */
  private val ORDER_PRESERVING_DATE_FORMATS: Set[String] = Set(
    "yyyy",
    "yyyy-MM",
    "yyyy-MM-dd",
    "yyyy-MM-dd HH",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyyMMdd",
    "yyyyMMddHHmmss",
    "yyyy/MM/dd",
    "yyyy/MM/dd HH:mm:ss")

  private def isOrderPreservingDateFormat(fmt: Expression): Boolean =
    fmt match {
      case Literal(value, StringType) if value != null => ORDER_PRESERVING_DATE_FORMATS.contains(value.toString)
      case _ => false
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
        case ToUnixTimestamp(OrderPreservingTransformation(attrRef), _, _, _) => Some(attrRef)
        case ToUTCTimestamp(OrderPreservingTransformation(attrRef), _) => Some(attrRef)
        // Post-ReplaceExpressions shape of to_date/to_timestamp with an explicit format: since
        // SPARK-38240 the optimizer rewrites those RuntimeReplaceable nodes into
        // GetTimestamp(source, fmt) (Cast-wrapped to date for to_date) before filters reach
        // file pruning. Matched only when
        //   (a) the format is a literal, fixed-width, year-first pattern: the translation
        //       re-applies the parse on top of string min/max column stats, which is only
        //       sound when lexicographic ordering of parseable strings agrees with
        //       chronological ordering, and
        //   (b) failOnError is false (non-ANSI), so probing a stat value that does not parse
        //       can never throw from the index lookup; it yields null instead, which the
        //       null-tolerant bounds in DataSkippingUtils turn into "keep the file".
        // GetTimestamp's constructor arity differs across Spark versions but its left()/right()
        // accessors are stable, hence the typed match
        case gt: GetTimestamp if !gt.failOnError && isOrderPreservingDateFormat(gt.right) =>
          unapply(gt.left)

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
        case _ => None
      }
    }
  }
}
