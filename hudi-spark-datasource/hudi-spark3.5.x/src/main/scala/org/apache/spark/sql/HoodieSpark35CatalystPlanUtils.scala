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

import org.apache.spark.sql.catalyst.analysis.{AnalysisErrorAt, AssignmentUtils}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression}
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, LogicalPlan, MergeIntoTable}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.{HoodieFormatTrait, ParquetFileFormat}

object HoodieSpark35CatalystPlanUtils extends HoodieSpark3CatalystPlanUtils {

  override def unapplyMergeIntoTable(plan: LogicalPlan): Option[(LogicalPlan, LogicalPlan, Expression)] = {
    plan match {
      case MergeIntoTable(targetTable, sourceTable, mergeCondition, _, _, _) =>
        Some((targetTable, sourceTable, mergeCondition))
      case _ => None
    }
  }

  override protected def alignRowLevelAssignments(attrs: Seq[Attribute],
                                                  assignments: Seq[Assignment],
                                                  isInsert: Boolean): Option[Seq[Assignment]] = {
    Some(if (isInsert) {
      AssignmentUtils.alignInsertAssignments(attrs, assignments)
    } else {
      AssignmentUtils.alignUpdateAssignments(attrs, assignments)
    })
  }

  override protected def isStoreAssignmentCast(cast: Cast): Boolean = cast.getTagValue(Cast.BY_TABLE_INSERTION).isDefined

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
      errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      messageParameters = Map(
        "objectName" -> a.sql,
        "proposal" -> cols))
  }

  override def failTableNotFound(tableName: String): Unit = {
    throw new AnalysisException(
      errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      messageParameters = Map("relationName" -> s"`$tableName`"))
  }
}
