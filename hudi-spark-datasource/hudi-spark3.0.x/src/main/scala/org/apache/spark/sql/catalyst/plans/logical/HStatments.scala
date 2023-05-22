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

import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition
import org.apache.spark.sql.types.DataType

/**
  * ALTER TABLE ... ADD COLUMNS command, as parsed from SQL.
  */
case class HoodieAlterTableAddColumnsStatement(
                                                tableName: Seq[String],
                                                columnsToAdd: Seq[QualifiedColType]) extends ParsedStatement

/**
  * ALTER TABLE ... CHANGE COLUMN command, as parsed from SQL.
  */
case class HoodieAlterTableAlterColumnStatement(
                                                 tableName: Seq[String],
                                                 column: Seq[String],
                                                 dataType: Option[DataType],
                                                 nullable: Option[Boolean],
                                                 comment: Option[String],
                                                 position: Option[ColumnPosition]) extends ParsedStatement


/**
  * ALTER TABLE ... RENAME COLUMN command, as parsed from SQL.
  */
case class HoodieAlterTableRenameColumnStatement(
                                                  tableName: Seq[String],
                                                  column: Seq[String],
                                                  newName: String) extends ParsedStatement

/**
  * ALTER TABLE ... DROP COLUMNS command, as parsed from SQL.
  */
case class HoodieAlterTableDropColumnsStatement(
                                                 tableName: Seq[String], columnsToDrop: Seq[Seq[String]]) extends ParsedStatement

/**
  * ALTER TABLE ... SET TBLPROPERTIES command, as parsed from SQL.
  */
case class HoodieAlterTableSetPropertiesStatement(
                                                   tableName: Seq[String], properties: Map[String, String]) extends ParsedStatement

/**
  * ALTER TABLE ... UNSET TBLPROPERTIES command, as parsed from SQL.
  */
case class HoodieAlterTableUnsetPropertiesStatement(
                                                     tableName: Seq[String], propertyKeys: Seq[String], ifExists: Boolean) extends ParsedStatement
