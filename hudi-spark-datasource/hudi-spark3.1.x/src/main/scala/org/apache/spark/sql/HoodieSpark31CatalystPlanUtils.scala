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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, ProjectionOverSchema}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.AlterTableRecoverPartitionsCommand
import org.apache.spark.sql.types.StructType

object HoodieSpark31CatalystPlanUtils extends HoodieSpark3CatalystPlanUtils {

  override def isRelationTimeTravel(plan: LogicalPlan): Boolean = false

  override def getRelationTimeTravel(plan: LogicalPlan): Option[(LogicalPlan, Option[Expression], Option[String])] = {
    throw new IllegalStateException(s"Should not call getRelationTimeTravel for Spark <= 3.2.x")
  }

  override def projectOverSchema(schema: StructType, output: AttributeSet): ProjectionOverSchema = ProjectionOverSchema(schema)

  override def isRepairTable(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[AlterTableRecoverPartitionsCommand]
  }

  override def getRepairTableChildren(plan: LogicalPlan): Option[(TableIdentifier, Boolean, Boolean, String)] = {
    plan match {
      // For Spark >= 3.2.x, AlterTableRecoverPartitionsCommand was renamed RepairTableCommand, and added two new
      // parameters: enableAddPartitions and enableDropPartitions. By setting them to true and false, can restore
      // AlterTableRecoverPartitionsCommand's behavior
      case c: AlterTableRecoverPartitionsCommand =>
        Some((c.tableName, true, false, c.cmd))
    }
  }
}
