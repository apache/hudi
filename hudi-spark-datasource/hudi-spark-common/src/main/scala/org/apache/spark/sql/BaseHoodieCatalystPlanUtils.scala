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

import org.apache.hudi.SparkAdapterSupport

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, TableOutputResolver}
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, CreateIndex, DropIndex, HoodieShowIndexes, InsertIntoStatement, Join, JoinHint, LogicalPlan, RefreshIndex}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.execution.{ExtendedMode, SimpleMode}
import org.apache.spark.sql.execution.command.{CreateTableLikeCommand, ExplainCommand, RepairTableCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.internal.SQLConf

trait BaseHoodieCatalystPlanUtils extends HoodieCatalystPlansUtils {

  /**
   * Un-applies [[ResolvedTable]] that had its signature changed in Spark 3.2
   */
  def unapplyResolvedTable(plan: LogicalPlan): Option[(TableCatalog, Identifier, Table)] =
    plan match {
      case ResolvedTable(catalog, identifier, table, _) => Some((catalog, identifier, table))
      case _ => None
    }

  def resolveOutputColumns(tableName: String,
                           expected: Seq[Attribute],
                           query: LogicalPlan,
                           byName: Boolean,
                           conf: SQLConf): LogicalPlan =
    TableOutputResolver.resolveOutputColumns(tableName, expected, query, byName, conf)

  override def createExplainCommand(plan: LogicalPlan, extended: Boolean): LogicalPlan =
    ExplainCommand(plan, mode = if (extended) ExtendedMode else SimpleMode)

  override def createJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType): Join = {
    Join(left, right, joinType, None, JoinHint.NONE)
  }

  override def unapplyCreateTableLikeCommand(plan: LogicalPlan): Option[(TableIdentifier, TableIdentifier, CatalogStorageFormat, Option[String], Map[String, String], Boolean)] = {
    plan match {
      case CreateTableLikeCommand(targetTable, sourceTable, fileFormat, provider, properties, ifNotExists) =>
        Some(targetTable, sourceTable, fileFormat, provider, properties, ifNotExists)
      case _ => None
    }
  }

  def rebaseInsertIntoStatement(iis: LogicalPlan, targetTable: LogicalPlan, query: LogicalPlan): LogicalPlan =
    iis.asInstanceOf[InsertIntoStatement].copy(table = targetTable, query = query)

  override def createMITJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType, condition: Option[Expression], hint: String): LogicalPlan = {
    Join(left, right, joinType, condition, JoinHint.NONE)
  }

  override def produceSameOutput(a: LogicalPlan, b: LogicalPlan): Boolean = {
    a.sameOutput(b)
  }

  override def isRepairTable(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[RepairTableCommand]
  }

  override def getRepairTableChildren(plan: LogicalPlan): Option[(TableIdentifier, Boolean, Boolean, String)] = {
    plan match {
      case rtc: RepairTableCommand =>
        Some((rtc.tableName, rtc.enableAddPartitions, rtc.enableDropPartitions, rtc.cmd))
      case _ =>
        None
    }
  }

  override def unapplyCreateIndex(plan: LogicalPlan): Option[(LogicalPlan, String, String, Boolean, Seq[(Seq[String], Map[String, String])], Map[String, String])] = {
    plan match {
      case ci@CreateIndex(table, indexName, indexType, ignoreIfExists, columns, properties) =>
        Some((table, indexName, indexType, ignoreIfExists, columns.map(col => (col._1.name, col._2)), properties))
      case _ =>
        None
    }
  }

  override def unapplyDropIndex(plan: LogicalPlan): Option[(LogicalPlan, String, Boolean)] = {
    plan match {
      case ci@DropIndex(table, indexName, ignoreIfNotExists) =>
        Some((table, indexName, ignoreIfNotExists))
      case _ =>
        None
    }
  }

  override def unapplyShowIndexes(plan: LogicalPlan): Option[(LogicalPlan, Seq[Attribute])] = {
    plan match {
      case ci@HoodieShowIndexes(table, output) =>
        Some((table, output))
      case _ =>
        None
    }
  }

  override def unapplyRefreshIndex(plan: LogicalPlan): Option[(LogicalPlan, String)] = {
    plan match {
      case ci@RefreshIndex(table, indexName) =>
        Some((table, indexName))
      case _ =>
        None
    }
  }

  override def unapplyInsertIntoStatement(plan: LogicalPlan): Option[(LogicalPlan, Seq[String], Map[String, Option[String]], LogicalPlan, Boolean, Boolean)] = {
    plan match {
      case insert: InsertIntoStatement =>
        Some((insert.table, insert.userSpecifiedCols, insert.partitionSpec, insert.query, insert.overwrite, insert.ifPartitionNotExists))
      case _ =>
        None
    }
  }

  override def resolveRowLevelAssignments(attrs: Seq[Attribute],
                                          assignments: Seq[Assignment],
                                          isInsert: Boolean): Seq[Assignment] = {
    val nested = assignments.find(!_.key.isInstanceOf[Attribute])
    // A key that references none of the attributes, as in a MERGE INTO of a table into itself, would
    // be dropped by the alignment, so such assignments are left to the command as they are
    val alignable = assignments.forall(_.key.references.exists(ref => attrs.exists(_.semanticEquals(ref))))
    if (nested.isEmpty || !alignable) {
      assignments
    } else {
      alignRowLevelAssignments(attrs, assignments.map(a => a.copy(key = trimAliases(a.key))), isInsert) match {
        case Some(aligned) =>
          aligned
            .filter(assignment => assignments.exists(_.key.references.exists(_.semanticEquals(assignment.key))))
            .map(assignment => assignment.copy(value = trimAliases(assignment.value)))
        case None =>
          throw new HoodieAnalysisException(
            s"Assigning a nested field is not supported on Spark $SPARK_VERSION; Spark 3.5 and later support " +
              s"assignments to fields of struct columns (provided: `${nested.get.sql}`)")
      }
    }
  }

  private def trimAliases(expr: Expression): Expression = expr.transformUp { case alias: Alias => alias.child }

  /**
   * Aligns the assignments with the table attributes through Spark's own alignment, one assignment
   * per attribute; [[None]] for a Spark version without it.
   */
  protected def alignRowLevelAssignments(attrs: Seq[Attribute],
                                         assignments: Seq[Assignment],
                                         isInsert: Boolean): Option[Seq[Assignment]] = None

  override def createProjectForByNameQuery(lr: LogicalRelation, plan: LogicalPlan): Option[LogicalPlan] = {
    plan match {
      case insert: InsertIntoStatement =>
        Some(ResolveInsertionBase.createProjectForByNameQuery(lr.catalogTable.get.qualifiedName, insert))
      case _ =>
        None
    }
  }
}

object BaseHoodieCatalystPlanUtils extends SparkAdapterSupport {

  /**
   * This is an extractor to accommodate for [[ResolvedTable]] signature change in Spark 3.2
   */
  object MatchResolvedTable {
    def unapply(plan: LogicalPlan): Option[(TableCatalog, Identifier, Table)] =
      sparkAdapter.getCatalystPlanUtils match {
        case sparkCatalystPlanUtils: BaseHoodieCatalystPlanUtils => sparkCatalystPlanUtils.unapplyResolvedTable(plan)
        case _ => None
      }
  }
}
