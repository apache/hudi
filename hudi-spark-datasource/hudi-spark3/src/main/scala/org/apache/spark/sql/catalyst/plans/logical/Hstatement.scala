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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.analysis.{FieldName, FieldPosition}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.types.DataType


/**
  * The logical plan that defines or changes the comment of an TABLE for v2 catalogs.
  * The logical plan of the ALTER TABLE ... SET TBLPROPERTIES command.
  */
case class HoodieSetTableProperties(
                                     table: LogicalPlan,
                                     properties: Map[String, String]) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    properties.map { case (key, value) =>
      TableChange.setProperty(key, value)
    }.toSeq
  }
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
  * The logical plan of the ALTER TABLE ... UNSET TBLPROPERTIES command.
  */
case class HoodieUnsetTableProperties(
                                       table: LogicalPlan,
                                       propertyKeys: Seq[String],
                                       ifExists: Boolean) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    propertyKeys.map(key => TableChange.removeProperty(key))
  }
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
  * The logical plan of the ALTER TABLE ... ADD COLUMNS command.
  */
case class HoodieAddColumns(
                             table: LogicalPlan,
                             columnsToAdd: Seq[QualifiedColType]) extends AlterTableCommand {
  columnsToAdd.foreach { c =>
    TypeUtils.failWithIntervalType(c.dataType)
  }

  override lazy val resolved: Boolean = table.resolved && columnsToAdd.forall(_.resolved)

  override def changes: Seq[TableChange] = {
    columnsToAdd.map { col =>
      require(col.path.forall(_.resolved),
        "FieldName should be resolved before it's converted to TableChange.")
      require(col.position.forall(_.resolved),
        "FieldPosition should be resolved before it's converted to TableChange.")
      TableChange.addColumn(
        col.name.toArray,
        col.dataType,
        col.nullable,
        col.comment.orNull,
        col.position.map(_.position).orNull)
    }
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
  * The logical plan of the ALTER TABLE ... REPLACE COLUMNS command.
  */
case class HoodieReplaceColumns(
                                 table: LogicalPlan,
                                 columnsToAdd: Seq[QualifiedColType]) extends AlterTableCommand {
  columnsToAdd.foreach { c =>
    TypeUtils.failWithIntervalType(c.dataType)
  }

  override lazy val resolved: Boolean = table.resolved && columnsToAdd.forall(_.resolved)

  override def changes: Seq[TableChange] = {
    // REPLACE COLUMNS deletes all the existing columns and adds new columns specified.
    require(table.resolved)
    val deleteChanges = table.schema.fieldNames.map { name =>
      TableChange.deleteColumn(Array(name))
    }
    val addChanges = columnsToAdd.map { col =>
      assert(col.path.isEmpty)
      assert(col.position.isEmpty)
      TableChange.addColumn(
        col.name.toArray,
        col.dataType,
        col.nullable,
        col.comment.orNull,
        null)
    }
    deleteChanges ++ addChanges
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
  * The logical plan of the ALTER TABLE ... DROP COLUMNS command.
  */
case class HoodieDropColumns(
                              table: LogicalPlan,
                              columnsToDrop: Seq[FieldName]) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    columnsToDrop.map { col =>
      require(col.resolved, "FieldName should be resolved before it's converted to TableChange.")
      TableChange.deleteColumn(col.name.toArray)
    }
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
  * The logical plan of the ALTER TABLE ... RENAME COLUMN command.
  */
case class HoodieRenameColumn(
                               table: LogicalPlan,
                               column: FieldName,
                               newName: String) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    require(column.resolved, "FieldName should be resolved before it's converted to TableChange.")
    Seq(TableChange.renameColumn(column.name.toArray, newName))
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
  * The logical plan of the ALTER TABLE ... ALTER COLUMN command.
  */
case class HoodieAlterColumn(
                              table: LogicalPlan,
                              column: FieldName,
                              dataType: Option[DataType],
                              nullable: Option[Boolean],
                              comment: Option[String],
                              position: Option[FieldPosition]) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    require(column.resolved, "FieldName should be resolved before it's converted to TableChange.")
    val colName = column.name.toArray
    val typeChange = dataType.map { newDataType =>
      TableChange.updateColumnType(colName, newDataType)
    }
    val nullabilityChange = nullable.map { nullable =>
      TableChange.updateColumnNullability(colName, nullable)
    }
    val commentChange = comment.map { newComment =>
      TableChange.updateColumnComment(colName, newComment)
    }
    val positionChange = position.map { newPosition =>
      require(newPosition.resolved,
        "FieldPosition should be resolved before it's converted to TableChange.")
      TableChange.updateColumnPosition(colName, newPosition.position)
    }
    typeChange.toSeq ++ nullabilityChange ++ commentChange ++ positionChange
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

