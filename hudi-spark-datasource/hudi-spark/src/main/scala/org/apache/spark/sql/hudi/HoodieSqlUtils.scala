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

package org.apache.spark.sql.hudi

import org.apache.hudi.SparkAdapterSupport
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{MergeIntoTable, SubqueryAlias}

object HoodieSqlUtils extends SparkAdapterSupport {

  /**
   * Get the TableIdentifier of the target table in MergeInto.
   */
  def getMergeIntoTargetTableId(mergeInto: MergeIntoTable): TableIdentifier = {
    val aliaId = mergeInto.targetTable match {
      case SubqueryAlias(_, SubqueryAlias(tableId, _)) => tableId
      case SubqueryAlias(tableId, _) => tableId
      case plan => throw new IllegalArgumentException(s"Illegal plan $plan in target")
    }
    sparkAdapter.getCatalystPlanUtils.toTableIdentifier(aliaId)
  }

  /**
   * Split the expression to a sub expression seq by the AND operation.
   * @param expression
   * @return
   */
  def splitByAnd(expression: Expression): Seq[Expression] = {
    expression match {
      case And(left, right) =>
        splitByAnd(left) ++ splitByAnd(right)
      case exp => Seq(exp)
    }
  }
}
