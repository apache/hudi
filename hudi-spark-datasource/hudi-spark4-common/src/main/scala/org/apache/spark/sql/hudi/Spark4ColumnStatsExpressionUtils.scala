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

package org.apache.spark.sql.hudi

import org.apache.hudi.ColumnStatsIndexSupport.{getMaxColumnNameFor, getMinColumnNameFor, getNullCountColumnNameFor, getValueCountColumnNameFor}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.ColumnConversions.toRichColumn
import org.apache.spark.sql.functions.col

object Spark4ColumnStatsExpressionUtils extends ColumnStatsExpressionUtils {

  @inline override def genColMinValueExpr(colName: String): Expression = col(getMinColumnNameFor(colName)).expr
  @inline override def genColMaxValueExpr(colName: String): Expression = col(getMaxColumnNameFor(colName)).expr
  @inline override def genColNumNullsExpr(colName: String): Expression = col(getNullCountColumnNameFor(colName)).expr
  @inline override def genColValueCountExpr: Expression = col(getValueCountColumnNameFor).expr

}
