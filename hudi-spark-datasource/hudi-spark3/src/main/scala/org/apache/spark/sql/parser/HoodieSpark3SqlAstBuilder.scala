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

package org.apache.spark.sql.parser

import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumn, DropColumns, HoodieAddColumns, HoodieAlterColumn, HoodieDropColumns, HoodieRenameColumn, HoodieReplaceColumns, HoodieSetTableProperties, HoodieUnsetTableProperties, LogicalPlan, RenameColumn, ReplaceColumns, SetTableProperties, SetViewProperties, UnsetTableProperties, UnsetViewProperties}
import org.apache.spark.sql.execution.SparkSqlAstBuilder

class HoodieSpark3SqlAstBuilder extends SparkSqlAstBuilder {

  /**
    * Parse a [[AlterTableAddColumns]] command.
    * For example:
    * {{{
    * ALTER TABLE table1
    * ADD COLUMNS (col_name data_type [COMMENT col_comment], ...);
    * }}}
    */
  override def visitAddTableColumns(ctx: AddTableColumnsContext): LogicalPlan = withOrigin(ctx) {
    val addColumns = super.visitAddTableColumns(ctx).asInstanceOf[AddColumns]
    HoodieAddColumns(addColumns.table, addColumns.columnsToAdd)
  }

  /**
    * Parse a [[AlterTableRenameColumn]] command.
    * For example:
    * {{{
    * ALTER TABLE table1 RENAME COLUMN a.b.c TO x
    * }}}
    */
  override def visitRenameTableColumn(
                                       ctx: RenameTableColumnContext): LogicalPlan = withOrigin(ctx) {
    val renameColumn = super.visitRenameTableColumn(ctx).asInstanceOf[RenameColumn]
    HoodieRenameColumn(renameColumn.table, renameColumn.column, renameColumn.newName)
  }

  /**
    * Parse a [[AlterTableAlterColumn]] command to alter a column's property.
    * For example:
    * {{{
    * ALTER TABLE table1 ALTER COLUMN a.b.c TYPE bigint
    * ALTER TABLE table1 ALTER COLUMN a.b.c SET NOT NULL
    * ALTER TABLE table1 ALTER COLUMN a.b.c DROP NOT NULL
    * ALTER TABLE table1 ALTER COLUMN a.b.c COMMENT 'new comment'
    * ALTER TABLE table1 ALTER COLUMN a.b.c FIRST
    * ALTER TABLE table1 ALTER COLUMN a.b.c AFTER x
    * }}}
    */
  override def visitAlterTableAlterColumn(
                                           ctx: AlterTableAlterColumnContext): LogicalPlan = withOrigin(ctx) {
    val alterColumn = super.visitAlterTableAlterColumn(ctx).asInstanceOf[AlterColumn]
    HoodieAlterColumn(
      alterColumn.table,
      alterColumn.column,
      alterColumn.dataType,
      alterColumn.nullable,
      alterColumn.comment,
      alterColumn.position)
  }

  /**
    * Parse a [[AlterTableAlterColumn]] command. This is Hive SQL syntax.
    * For example:
    * {{{
    * ALTER TABLE table [PARTITION partition_spec]
    * CHANGE [COLUMN] column_old_name column_new_name column_dataType [COMMENT column_comment]
    * [FIRST | AFTER column_name];
    * }}}
    */
  override def visitHiveChangeColumn(ctx: HiveChangeColumnContext): LogicalPlan = withOrigin(ctx) {
    val alterColumn = super.visitHiveChangeColumn(ctx).asInstanceOf[AlterColumn]
    HoodieAlterColumn(
      alterColumn.table,
      alterColumn.column,
      alterColumn.dataType,
      alterColumn.nullable,
      alterColumn.comment,
      alterColumn.position)
  }
  override def visitHiveReplaceColumns(
                                        ctx: HiveReplaceColumnsContext): LogicalPlan = withOrigin(ctx) {
    val hiveReplaceColumns = super.visitHiveReplaceColumns(ctx).asInstanceOf[ReplaceColumns]
    HoodieReplaceColumns(hiveReplaceColumns.table, hiveReplaceColumns.columnsToAdd)
  }

  /**
    * Parse a [[AlterTableDropColumns]] command.
    * For example:
    * {{{
    * ALTER TABLE table1 DROP COLUMN a.b.c
    * ALTER TABLE table1 DROP COLUMNS a.b.c, x, y
    * }}}
    */
  override def visitDropTableColumns(
                                      ctx: DropTableColumnsContext): LogicalPlan = withOrigin(ctx) {
    val dropColumns = super.visitDropTableColumns(ctx).asInstanceOf[DropColumns]
    HoodieDropColumns(dropColumns.table, dropColumns.columnsToDrop)
  }

  /**
    * Parse [[SetViewProperties]] or [[SetTableProperties]] commands.
    * For example:
    * {{{
    * ALTER TABLE table SET TBLPROPERTIES ('table_property' = 'property_value');
    * ALTER VIEW view SET TBLPROPERTIES ('table_property' = 'property_value');
    * }}}
    */
  override def visitSetTableProperties(
                                        ctx: SetTablePropertiesContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.VIEW != null) {
      super.visitSetTableProperties(ctx)
    } else {
      val setTableProperties = super.visitSetTableProperties(ctx).asInstanceOf[SetTableProperties]
      HoodieSetTableProperties(setTableProperties.table, setTableProperties.properties)
    }
  }

  /**
    * Parse [[UnsetViewProperties]] or [[UnsetTableProperties]] commands.
    * For example:
    * {{{
    * ALTER TABLE table UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
    * ALTER VIEW view UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
    * }}}
    */
  override def visitUnsetTableProperties(
                                          ctx: UnsetTablePropertiesContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.VIEW != null) {
      super.visitUnsetTableProperties(ctx)
    } else {
      val unsetTableProperties = super.visitUnsetTableProperties(ctx).asInstanceOf[UnsetTableProperties]
      HoodieUnsetTableProperties(unsetTableProperties.table, unsetTableProperties.propertyKeys, unsetTableProperties.ifExists)
    }
  }
}

