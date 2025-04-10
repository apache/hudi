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

import org.apache.hudi.internal.schema.action.TableChange.ColumnChangeID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hudi.catalog.HoodieInternalV2Table
import org.apache.spark.sql.hudi.command.{AlterTableCommand => HudiAlterTableCommand}

/**
  * Rule to mostly resolve, normalize and rewrite column names based on case sensitivity.
  * for alter table column commands.
  */
class Spark33ResolveHudiAlterTableCommand(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (ProvidesHoodieConfig.isSchemaEvolutionEnabled(sparkSession)) {
      plan.resolveOperatorsUp {
        case set@SetTableProperties(ResolvedHoodieV2TablePlan(t), _) if set.resolved =>
          HudiAlterTableCommand(t.v1Table, set.changes, ColumnChangeID.PROPERTY_CHANGE)
        case unSet@UnsetTableProperties(ResolvedHoodieV2TablePlan(t), _, _) if unSet.resolved =>
          HudiAlterTableCommand(t.v1Table, unSet.changes, ColumnChangeID.PROPERTY_CHANGE)
        case drop@DropColumns(ResolvedHoodieV2TablePlan(t), _, _) if drop.resolved =>
          HudiAlterTableCommand(t.v1Table, drop.changes, ColumnChangeID.DELETE)
        case add@AddColumns(ResolvedHoodieV2TablePlan(t), _) if add.resolved =>
          HudiAlterTableCommand(t.v1Table, add.changes, ColumnChangeID.ADD)
        case renameColumn@RenameColumn(ResolvedHoodieV2TablePlan(t), _, _) if renameColumn.resolved =>
          HudiAlterTableCommand(t.v1Table, renameColumn.changes, ColumnChangeID.UPDATE)
        case alter@AlterColumn(ResolvedHoodieV2TablePlan(t), _, _, _, _, _) if alter.resolved =>
          HudiAlterTableCommand(t.v1Table, alter.changes, ColumnChangeID.UPDATE)
        case replace@ReplaceColumns(ResolvedHoodieV2TablePlan(t), _) if replace.resolved =>
          HudiAlterTableCommand(t.v1Table, replace.changes, ColumnChangeID.REPLACE)
      }
    } else {
      plan
    }
  }

  object ResolvedHoodieV2TablePlan {
    def unapply(plan: LogicalPlan): Option[HoodieInternalV2Table] = {
      plan match {
        case ResolvedTable(_, _, v2Table: HoodieInternalV2Table, _) => Some(v2Table)
        case _ => None
      }
    }
  }
}

