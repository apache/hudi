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

import org.apache.hudi.SparkHoodieTableFileIndex
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, ProjectionOverSchema}
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MergeIntoTable, Project}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.execution.command.AlterTableRecoverPartitionsCommand
import org.apache.spark.sql.execution.datasources.parquet.NewHoodieParquetFileFormat
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.StructType

object HoodieSpark30CatalystPlanUtils extends HoodieSpark3CatalystPlanUtils {

  def unapplyResolvedTable(plan: LogicalPlan): Option[(TableCatalog, Identifier, Table)] =
    plan match {
      case ResolvedTable(catalog, identifier, table) => Some((catalog, identifier, table))
      case _ => None
    }

  override def unapplyMergeIntoTable(plan: LogicalPlan): Option[(LogicalPlan, LogicalPlan, Expression)] = {
    plan match {
      case MergeIntoTable(targetTable, sourceTable, mergeCondition, _, _) =>
        Some((targetTable, sourceTable, mergeCondition))
      case _ => None
    }
  }

  override def applyNewHoodieParquetFileFormatProjection(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case s@ScanOperation(_, _,
      l@LogicalRelation(fs: HadoopFsRelation, _, _, _)) if fs.fileFormat.isInstanceOf[NewHoodieParquetFileFormat] && !fs.fileFormat.asInstanceOf[NewHoodieParquetFileFormat].isProjected =>
        fs.fileFormat.asInstanceOf[NewHoodieParquetFileFormat].isProjected = true
        Project(l.resolve(fs.location.asInstanceOf[SparkHoodieTableFileIndex].schema, fs.sparkSession.sessionState.analyzer.resolver), s)
      case _ => plan
    }
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

  /**
   * Commands of managing indexes are not supported for Spark3.0
   */
  override def unapplyCreateIndex(plan: LogicalPlan): Option[(LogicalPlan, String, String, Boolean, Seq[(Seq[String], Map[String, String])], Map[String, String])] = None

  override def unapplyDropIndex(plan: LogicalPlan): Option[(LogicalPlan, String, Boolean)] = None

  override def unapplyShowIndexes(plan: LogicalPlan): Option[(LogicalPlan, Seq[Attribute])] = None

  override def unapplyRefreshIndex(plan: LogicalPlan): Option[(LogicalPlan, String)] = None
}
