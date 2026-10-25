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

import org.apache.spark.sql.catalyst.analysis.AnalysisErrorAt
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, MergeIntoTable}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.{HoodieFormatTrait, ParquetFileFormat}
import org.apache.spark.sql.internal.SQLConf

object HoodieSpark34CatalystPlanUtils extends HoodieSpark3CatalystPlanUtils {

  override def unapplyMergeIntoTable(plan: LogicalPlan): Option[(LogicalPlan, LogicalPlan, Expression)] = {
    plan match {
      case MergeIntoTable(targetTable, sourceTable, mergeCondition, _, _, _) =>
        Some((targetTable, sourceTable, mergeCondition))
      case _ => None
    }
  }

  override def maybeApplyForNewFileFormat(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case s@ScanOperation(_, _, _,
      l@LogicalRelation(fs: HadoopFsRelation, _, _, _))
        if fs.fileFormat.isInstanceOf[ParquetFileFormat with HoodieFormatTrait]
          && !fs.fileFormat.asInstanceOf[ParquetFileFormat with HoodieFormatTrait].isProjected =>
        FileFormatUtilsForFileGroupReader.applyNewFileFormatChanges(s, l, fs)
      case _ => plan
    }
  }

  override def failAnalysisForMIT(a: Attribute, cols: String): Unit = {
    a.failAnalysis(
      errorClass = "_LEGACY_ERROR_TEMP_2309",
      messageParameters = Map(
        "sqlExpr" -> a.sql,
        "cols" -> cols))
  }

  override def failTableNotFound(tableName: String): Unit = {
    throw new AnalysisException(
      errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      messageParameters = Map("relationName" -> s"`$tableName`"))
  }

  override def unapplyInsertIntoStatement(plan: LogicalPlan): Option[(LogicalPlan, Seq[String], Map[String, Option[String]], LogicalPlan, Boolean, Boolean)] = {
    plan match {
      case insert: InsertIntoStatement =>
        // https://github.com/apache/spark/pull/36077
        // first: in this pr, spark34 support default value for insert into, it will regenerate the user specified cols
        //        so, no need deal with it in hudi side
        // second: in this pr, it will append hoodie meta field with default value, has some bug, it look like be fixed
        //         in spark35(https://github.com/apache/spark/pull/41262), so if user want specified cols, need disable default feature.
        if (SQLConf.get.enableDefaultColumns) {
          if (insert.userSpecifiedCols.nonEmpty) {
            throw new AnalysisException("hudi not support specified cols when enable default columns, " +
              "please disable 'spark.sql.defaultColumn.enabled'")
          }
          Some((insert.table, Seq.empty, insert.partitionSpec, insert.query, insert.overwrite, insert.ifPartitionNotExists))
        } else {
          Some((insert.table, insert.userSpecifiedCols, insert.partitionSpec, insert.query, insert.overwrite, insert.ifPartitionNotExists))
        }
      case _ =>
        None
    }
  }
}
