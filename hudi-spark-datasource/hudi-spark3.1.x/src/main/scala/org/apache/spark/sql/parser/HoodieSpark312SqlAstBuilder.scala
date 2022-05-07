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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkSqlAstBuilder

// TODO: we should remove this file when we support datasourceV2 for hoodie on spark3.1x
class HoodieSpark312SqlAstBuilder(sparkSession: SparkSession) extends SparkSqlAstBuilder {

  /**
    * Parse a [[AlterTableAlterColumnStatement]] command to alter a column's property.
    *
    * For example:
    * {{{
    *   ALTER TABLE table1 ALTER COLUMN a.b.c TYPE bigint
    *   ALTER TABLE table1 ALTER COLUMN a.b.c SET NOT NULL
    *   ALTER TABLE table1 ALTER COLUMN a.b.c DROP NOT NULL
    *   ALTER TABLE table1 ALTER COLUMN a.b.c COMMENT 'new comment'
    *   ALTER TABLE table1 ALTER COLUMN a.b.c FIRST
    *   ALTER TABLE table1 ALTER COLUMN a.b.c AFTER x
    * }}}
    */
  override def visitAlterTableAlterColumn(ctx: AlterTableAlterColumnContext): LogicalPlan = withOrigin(ctx) {
    val alter = super.visitAlterTableAlterColumn(ctx).asInstanceOf[AlterTableAlterColumnStatement]
    HoodieAlterTableAlterColumnStatement(alter.tableName, alter.column, alter.dataType, alter.nullable, alter.comment, alter.position)
  }

  /**
    * Parse a [[org.apache.spark.sql.catalyst.plans.logical.AlterTableAddColumnsStatement]] command.
    *
    * For example:
    * {{{
    *   ALTER TABLE table1
    *   ADD COLUMNS (col_name data_type [COMMENT col_comment], ...);
    * }}}
    */
  override def visitAddTableColumns(ctx: AddTableColumnsContext): LogicalPlan = withOrigin(ctx) {
    val add = super.visitAddTableColumns(ctx).asInstanceOf[AlterTableAddColumnsStatement]
    HoodieAlterTableAddColumnsStatement(add.tableName, add.columnsToAdd)
  }

  /**
    * Parse a [[org.apache.spark.sql.catalyst.plans.logical.AlterTableRenameColumnStatement]] command.
    *
    * For example:
    * {{{
    *   ALTER TABLE table1 RENAME COLUMN a.b.c TO x
    * }}}
    */
  override def visitRenameTableColumn(
                                       ctx: RenameTableColumnContext): LogicalPlan = withOrigin(ctx) {
    val rename = super.visitRenameTableColumn(ctx).asInstanceOf[AlterTableRenameColumnStatement]
    HoodieAlterTableRenameColumnStatement(rename.tableName, rename.column, rename.newName)
  }

  /**
    * Parse a [[AlterTableDropColumnsStatement]] command.
    *
    * For example:
    * {{{
    *   ALTER TABLE table1 DROP COLUMN a.b.c
    *   ALTER TABLE table1 DROP COLUMNS a.b.c, x, y
    * }}}
    */
  override def visitDropTableColumns(
                                      ctx: DropTableColumnsContext): LogicalPlan = withOrigin(ctx) {
    val drop = super.visitDropTableColumns(ctx).asInstanceOf[AlterTableDropColumnsStatement]
    HoodieAlterTableDropColumnsStatement(drop.tableName, drop.columnsToDrop)
  }

  /**
    * Parse [[AlterViewSetPropertiesStatement]] or [[AlterTableSetPropertiesStatement]] commands.
    *
    * For example:
    * {{{
    *   ALTER TABLE table SET TBLPROPERTIES ('table_property' = 'property_value');
    *   ALTER VIEW view SET TBLPROPERTIES ('table_property' = 'property_value');
    * }}}
    */
  override def visitSetTableProperties(
                                        ctx: SetTablePropertiesContext): LogicalPlan = withOrigin(ctx) {
    val set = super.visitSetTableProperties(ctx)
    set match {
      case s: AlterTableSetPropertiesStatement => HoodieAlterTableSetPropertiesStatement(s.tableName, s.properties)
      case other => other
    }
  }

  /**
    * Parse [[AlterViewUnsetPropertiesStatement]] or [[AlterTableUnsetPropertiesStatement]] commands.
    *
    * For example:
    * {{{
    *   ALTER TABLE table UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
    *   ALTER VIEW view UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
    * }}}
    */
  override def visitUnsetTableProperties(
                                          ctx: UnsetTablePropertiesContext): LogicalPlan = withOrigin(ctx) {
    val unset = super.visitUnsetTableProperties(ctx)
    unset match {
      case us: AlterTableUnsetPropertiesStatement => HoodieAlterTableUnsetPropertiesStatement(us.tableName, us.propertyKeys, us.ifExists)
      case other => other
    }
  }
}
