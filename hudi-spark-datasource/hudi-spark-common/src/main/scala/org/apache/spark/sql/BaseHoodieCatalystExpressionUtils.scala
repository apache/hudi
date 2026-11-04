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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Cast, Expression, PredicateHelper}
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

  def canUpCast(fromType: DataType, toType: DataType): Boolean =
    Cast.canUpCast(fromType, toType)

  /**
   * Matches order-preserving date/time parsing expressions whose case-class shapes differ across
   * Spark versions (currently [[org.apache.spark.sql.catalyst.expressions.ParseToDate]] and
   * [[org.apache.spark.sql.catalyst.expressions.ParseToTimestamp]]), returning the source child
   * expression that data skipping's `DateTimeFormatRule` recurses into
   */
  protected def unapplyOrderPreservingDateParsing(expr: Expression): Option[Expression]

  override def unapplyDateParsingChild(expr: Expression): Option[Expression] =
    unapplyOrderPreservingDateParsing(expr)
}
