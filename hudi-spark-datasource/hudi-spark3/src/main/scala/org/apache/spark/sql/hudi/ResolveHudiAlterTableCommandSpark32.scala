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

import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.internal.schema.action.TableChange.ColumnChangeID
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumn, DropColumns, LogicalPlan, RenameColumn, ReplaceColumns, SetTableProperties, UnsetTableProperties}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hudi.catalog.HoodieInternalV2Table
import org.apache.spark.sql.hudi.command.{AlterTableCommand => HudiAlterTableCommand}

/**
  * Rule to mostly resolve, normalize and rewrite column names based on case sensitivity.
  * for alter table column commands.
  */
class ResolveHudiAlterTableCommandSpark32(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case set @ SetTableProperties(asTable(table), _) if schemaEvolutionEnabled && set.resolved =>
      HudiAlterTableCommand(table, set.changes, ColumnChangeID.PROPERTY_CHANGE)
    case unSet @ UnsetTableProperties(asTable(table), _, _) if schemaEvolutionEnabled && unSet.resolved =>
      HudiAlterTableCommand(table, unSet.changes, ColumnChangeID.PROPERTY_CHANGE)
    case drop @ DropColumns(asTable(table), _) if schemaEvolutionEnabled && drop.resolved =>
      HudiAlterTableCommand(table, drop.changes, ColumnChangeID.DELETE)
    case add @ AddColumns(asTable(table), _) if schemaEvolutionEnabled  && add.resolved =>
      HudiAlterTableCommand(table, add.changes, ColumnChangeID.ADD)
    case renameColumn @ RenameColumn(asTable(table), _, _) if schemaEvolutionEnabled && renameColumn.resolved=>
      HudiAlterTableCommand(table, renameColumn.changes, ColumnChangeID.UPDATE)
    case alter @ AlterColumn(asTable(table), _, _, _, _, _) if schemaEvolutionEnabled && alter.resolved =>
      HudiAlterTableCommand(table, alter.changes, ColumnChangeID.UPDATE)
    case replace @ ReplaceColumns(asTable(table), _) if schemaEvolutionEnabled && replace.resolved =>
      HudiAlterTableCommand(table, replace.changes, ColumnChangeID.REPLACE)
  }

  private def schemaEvolutionEnabled(): Boolean = sparkSession
    .sessionState.conf.getConfString(HoodieWriteConfig.SCHEMA_EVOLUTION_ENABLE.key(), "false").toBoolean

  object asTable {
    def unapply(a: LogicalPlan): Option[CatalogTable] = {
      a match {
        case ResolvedTable(_, _, table: HoodieInternalV2Table, _) =>
          table.catalogTable
        case _ =>
          None
      }
    }
  }
}

