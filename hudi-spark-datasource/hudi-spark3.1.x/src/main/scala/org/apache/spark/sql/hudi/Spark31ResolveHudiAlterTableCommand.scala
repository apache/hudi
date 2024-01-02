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

import org.apache.hudi.common.config.HoodieCommonConfig
import org.apache.hudi.internal.schema.action.TableChange.ColumnChangeID
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Util.failNullType
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, TableChange}
import org.apache.spark.sql.hudi.command.Spark31AlterTableCommand
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, SparkSession}

import java.util.Locale
import scala.collection.mutable

/**
  * Rule to mostly resolve, normalize and rewrite column names based on case sensitivity
  * for alter table column commands.
  * TODO: we should remove this file when we support datasourceV2 for hoodie on spark3.1x
  */
case class Spark31ResolveHudiAlterTableCommand(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case add @ HoodieAlterTableAddColumnsStatement(asTable(table), cols) =>
      if (isHoodieTable(table) && schemaEvolutionEnabled){
        cols.foreach(c => CatalogV2Util.failNullType(c.dataType))
        val changes = cols.map { col =>
          TableChange.addColumn(
            col.name.toArray,
            col.dataType,
            col.nullable,
            col.comment.orNull,
            col.position.orNull)
        }
        val newChanges = normalizeChanges(changes, table.schema)
        Spark31AlterTableCommand(table, newChanges, ColumnChangeID.ADD)
      } else {
        // throw back to spark
        AlterTableAddColumnsStatement(add.tableName, add.columnsToAdd)
      }
    case a @ HoodieAlterTableAlterColumnStatement(asTable(table), _, _, _, _, _) =>
      if (isHoodieTable(table) && schemaEvolutionEnabled){
        a.dataType.foreach(failNullType)
        val colName = a.column.toArray
        val typeChange = a.dataType.map { newDataType =>
          TableChange.updateColumnType(colName, newDataType)
        }
        val nullabilityChange = a.nullable.map { nullable =>
          TableChange.updateColumnNullability(colName, nullable)
        }
        val commentChange = a.comment.map { newComment =>
          TableChange.updateColumnComment(colName, newComment)
        }
        val positionChange = a.position.map { newPosition =>
          TableChange.updateColumnPosition(colName, newPosition)
        }
        Spark31AlterTableCommand(table, normalizeChanges(typeChange.toSeq ++ nullabilityChange ++ commentChange ++ positionChange, table.schema), ColumnChangeID.UPDATE)
      } else {
        // throw back to spark
        AlterTableAlterColumnStatement(a.tableName, a.column, a.dataType, a.nullable, a.comment, a.position)
      }
    case rename @ HoodieAlterTableRenameColumnStatement(asTable(table), col, newName) =>
      if (isHoodieTable(table) && schemaEvolutionEnabled){
        val changes = Seq(TableChange.renameColumn(col.toArray, newName))
        Spark31AlterTableCommand(table, normalizeChanges(changes, table.schema), ColumnChangeID.UPDATE)
      } else {
        // throw back to spark
        AlterTableRenameColumnStatement(rename.tableName, rename.column, rename.newName)
      }
    case drop @ HoodieAlterTableDropColumnsStatement(asTable(table), cols) =>
      if (isHoodieTable(table) && schemaEvolutionEnabled) {
        val changes = cols.map(col => TableChange.deleteColumn(col.toArray))
        Spark31AlterTableCommand(table, normalizeChanges(changes, table.schema), ColumnChangeID.DELETE)
      } else {
        // throw back to spark
        AlterTableDropColumnsStatement(drop.tableName, drop.columnsToDrop)
      }
    case set @ HoodieAlterTableSetPropertiesStatement(asTable(table), props) =>
      if (isHoodieTable(table) && schemaEvolutionEnabled) {
        val changes = props.map { case (key, value) =>
          TableChange.setProperty(key, value)
        }.toSeq
        Spark31AlterTableCommand(table, normalizeChanges(changes, table.schema), ColumnChangeID.PROPERTY_CHANGE)
      } else {
        // throw back to spark
        AlterTableSetPropertiesStatement(set.tableName, set.properties)
      }
    case unset @ HoodieAlterTableUnsetPropertiesStatement(asTable(table), keys, _) =>
      if (isHoodieTable(table) && schemaEvolutionEnabled) {
        val changes = keys.map(key => TableChange.removeProperty(key))
        Spark31AlterTableCommand(table, normalizeChanges(changes, table.schema), ColumnChangeID.PROPERTY_CHANGE)
      } else {
        // throw back to spark
        AlterTableUnsetPropertiesStatement(unset.tableName, unset.propertyKeys, unset.ifExists)
      }
  }

  private def schemaEvolutionEnabled(): Boolean =
    sparkSession.sessionState.conf.getConfString(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key,
      HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.defaultValue.toString).toBoolean

  private def isHoodieTable(table: CatalogTable): Boolean = table.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "hudi"

  def normalizeChanges(changes: Seq[TableChange], schema: StructType): Seq[TableChange] = {
    val colsToAdd = mutable.Map.empty[Seq[String], Seq[String]]
    changes.flatMap {
      case add: AddColumn =>
        def addColumn(parentSchema: StructType, parentName: String, normalizedParentName: Seq[String]): TableChange = {
          val fieldsAdded = colsToAdd.getOrElse(normalizedParentName, Nil)
          val pos = findColumnPosition(add.position(), parentName, parentSchema, fieldsAdded)
          val field = add.fieldNames().last
          colsToAdd(normalizedParentName) = fieldsAdded :+ field
          TableChange.addColumn(
            (normalizedParentName :+ field).toArray,
            add.dataType(),
            add.isNullable,
            add.comment,
            pos)
        }
        val parent = add.fieldNames().init
        if (parent.nonEmpty) {
          // Adding a nested field, need to normalize the parent column and position
          val target = schema.findNestedField(parent, includeCollections = true, conf.resolver)
          if (target.isEmpty) {
            // Leave unresolved. Throws error in CheckAnalysis
            Some(add)
          } else {
            val (normalizedName, sf) = target.get
            sf.dataType match {
              case struct: StructType =>
                Some(addColumn(struct, parent.quoted, normalizedName :+ sf.name))
              case other =>
                Some(add)
            }
          }
        } else {
          // Adding to the root. Just need to normalize position
          Some(addColumn(schema, "root", Nil))
        }

      case typeChange: UpdateColumnType =>
        // Hive style syntax provides the column type, even if it may not have changed
        val fieldOpt = schema.findNestedField(
          typeChange.fieldNames(), includeCollections = true, conf.resolver)

        if (fieldOpt.isEmpty) {
          // We couldn't resolve the field. Leave it to CheckAnalysis
          Some(typeChange)
        } else {
          val (fieldNames, field) = fieldOpt.get
          if (field.dataType == typeChange.newDataType()) {
            // The user didn't want the field to change, so remove this change
            None
          } else {
            Some(TableChange.updateColumnType(
              (fieldNames :+ field.name).toArray, typeChange.newDataType()))
          }
        }
      case n: UpdateColumnNullability =>
        // Need to resolve column
        resolveFieldNames(
          schema,
          n.fieldNames(),
          TableChange.updateColumnNullability(_, n.nullable())).orElse(Some(n))

      case position: UpdateColumnPosition =>
        position.position() match {
          case after: After =>
            // Need to resolve column as well as position reference
            val fieldOpt = schema.findNestedField(
              position.fieldNames(), includeCollections = true, conf.resolver)

            if (fieldOpt.isEmpty) {
              Some(position)
            } else {
              val (normalizedPath, field) = fieldOpt.get
              val targetCol = schema.findNestedField(
                normalizedPath :+ after.column(), includeCollections = true, conf.resolver)
              if (targetCol.isEmpty) {
                // Leave unchanged to CheckAnalysis
                Some(position)
              } else {
                Some(TableChange.updateColumnPosition(
                  (normalizedPath :+ field.name).toArray,
                  ColumnPosition.after(targetCol.get._2.name)))
              }
            }
          case _ =>
            // Need to resolve column
            resolveFieldNames(
              schema,
              position.fieldNames(),
              TableChange.updateColumnPosition(_, position.position())).orElse(Some(position))
        }

      case comment: UpdateColumnComment =>
        resolveFieldNames(
          schema,
          comment.fieldNames(),
          TableChange.updateColumnComment(_, comment.newComment())).orElse(Some(comment))

      case rename: RenameColumn =>
        resolveFieldNames(
          schema,
          rename.fieldNames(),
          TableChange.renameColumn(_, rename.newName())).orElse(Some(rename))

      case delete: DeleteColumn =>
        resolveFieldNames(schema, delete.fieldNames(), TableChange.deleteColumn)
          .orElse(Some(delete))

      case column: ColumnChange =>
        // This is informational for future developers
        throw new UnsupportedOperationException(
          "Please add an implementation for a column change here")
      case other => Some(other)
    }
  }

  /**
    * Returns the table change if the field can be resolved, returns None if the column is not
    * found. An error will be thrown in CheckAnalysis for columns that can't be resolved.
    */
  private def resolveFieldNames(
                                 schema: StructType,
                                 fieldNames: Array[String],
                                 copy: Array[String] => TableChange): Option[TableChange] = {
    val fieldOpt = schema.findNestedField(
      fieldNames, includeCollections = true, conf.resolver)
    fieldOpt.map { case (path, field) => copy((path :+ field.name).toArray) }
  }

  private def findColumnPosition(
                                  position: ColumnPosition,
                                  parentName: String,
                                  struct: StructType,
                                  fieldsAdded: Seq[String]): ColumnPosition = {
    position match {
      case null => null
      case after: After =>
        (struct.fieldNames ++ fieldsAdded).find(n => conf.resolver(n, after.column())) match {
          case Some(colName) =>
            ColumnPosition.after(colName)
          case None =>
            throw new AnalysisException("Couldn't find the reference column for " +
              s"$after at $parentName")
        }
      case other => other
    }
  }

  object asTable {
    def unapply(parts: Seq[String]): Option[CatalogTable] = {
      val identifier = parts match {
        case Seq(tblName) => TableIdentifier(tblName)
        case Seq(dbName, tblName) => TableIdentifier(tblName, Some(dbName))
        case _ =>
          throw new AnalysisException(
            s"${parts} is not a valid TableIdentifier as it has more than 2 name parts.")
      }
      Some(sparkSession.sessionState.catalog.getTableMetadata(identifier))
    }
  }
}
