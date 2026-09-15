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

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Cast, EvalMode, Expression, ParseToDate, ParseToTimestamp}
import org.apache.spark.sql.types.{DataType, StructType}

object HoodieSpark34CatalystExpressionUtils extends BaseHoodieCatalystExpressionUtils {

  override def getEncoder(schema: StructType): ExpressionEncoder[Row] = {
    RowEncoder.apply(schema).resolveAndBind()
  }

  override def matchCast(expr: Expression): Option[(Expression, DataType, Option[String])] = {
    expr match {
      case Cast(child, dataType, timeZoneId, _) => Some((child, dataType, timeZoneId))
      case _ => None
    }
  }

  override def unapplyCastExpression(expr: Expression): Option[(Expression, DataType, Option[String], Boolean)] =
    expr match {
      case Cast(castedExpr, dataType, timeZoneId, ansiEnabled) =>
        Some((castedExpr, dataType, timeZoneId, if (ansiEnabled == EvalMode.ANSI) true else false))
      case _ => None
    }

  override protected def unapplyOrderPreservingDateParsing(expr: Expression): Option[Expression] =
    expr match {
      case ParseToDate(child, _, _) => Some(child)
      case ParseToTimestamp(child, _, _, _, _) => Some(child)
      case _ => None
    }
}
