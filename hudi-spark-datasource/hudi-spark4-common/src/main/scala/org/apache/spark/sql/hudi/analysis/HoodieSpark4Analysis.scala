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

package org.apache.spark.sql.hudi.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{ResolveInsertionBase, TableOutputResolver}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.PreprocessTableInsertion
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.PartitioningUtils.normalizePartitionSpec

/**
 * In Spark 3.5, the following Resolution rules are removed,
 * [[ResolveUserSpecifiedColumns]] and [[ResolveDefaultColumns]]
 * (see code changes in [[org.apache.spark.sql.catalyst.analysis.Analyzer]]
 * from https://github.com/apache/spark/pull/41262).
 * The same logic of resolving the user specified columns and default values,
 * which are required for a subset of columns as user specified compared to the table
 * schema to work properly, are deferred to [[PreprocessTableInsertion]] for v1 INSERT.
 *
 * Note that [[HoodieAnalysis]] intercepts the [[InsertIntoStatement]] after Spark's built-in
 * Resolution rules are applies, the logic of resolving the user specified columns and default
 * values may no longer be applied. To make INSERT with a subset of columns specified by user
 * to work, the custom resolution rules `HoodieSpark4XResolveColumnsForInsertInto` extending
 * this base class are added to achieve the same, before converting [[InsertIntoStatement]]
 * into [[InsertIntoHoodieTableCommand]].
 *
 * The implementation is copied and adapted from [[PreprocessTableInsertion]]
 * https://github.com/apache/spark/blob/d061aadf25fd258d2d3e7332a489c9c24a2b5530/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/rules.scala#L373
 *
 * Also note that, the project logic in [[ResolveImplementationsEarly]] for INSERT is still
 * needed in the case of INSERT with all columns in a different ordering.
 *
 * This base class carries the preprocessing logic shared by all supported Spark 4.x versions;
 * the per-version subclasses keep only the plan matching that depends on version-specific
 * case-class shapes of [[InsertIntoStatement]].
 */
abstract class HoodieSpark4ResolveColumnsForInsertInto extends ResolveInsertionBase {

  protected def preprocess(insert: InsertIntoStatement,
                           catalogTable: Option[CatalogTable]): InsertIntoStatement = {
    preprocess(insert, catalogTable, catalogTable.map(_.partitionSchema).getOrElse(new StructType()))
  }

  protected def preprocess(insert: InsertIntoStatement,
                           catalogTable: Option[CatalogTable],
                           partitionSchema: StructType): InsertIntoStatement = {
    val tblName = catalogTable.map(_.identifier.quotedString).getOrElse("unknown")
    preprocess(insert, tblName, partitionSchema, catalogTable)
  }

  // NOTE: this is copied from [[PreprocessTableInsertion]] with additional logic
  // to unset user-specified columns at the end
  protected def preprocess(insert: InsertIntoStatement,
                           tblName: String,
                           partColNames: StructType,
                           catalogTable: Option[CatalogTable]): InsertIntoStatement = {

    val normalizedPartSpec = normalizePartitionSpec(
      insert.partitionSpec, partColNames, tblName, conf.resolver)

    val staticPartCols = normalizedPartSpec.filter(_._2.isDefined).keySet
    val expectedColumns = insert.table.output.filterNot(a => staticPartCols.contains(a.name))

    val partitionsTrackedByCatalog = catalogTable.isDefined &&
      catalogTable.get.partitionColumnNames.nonEmpty &&
      catalogTable.get.tracksPartitionsInCatalog
    if (partitionsTrackedByCatalog && normalizedPartSpec.nonEmpty) {
      // empty partition column value
      if (normalizedPartSpec.values.flatten.exists(v => v != null && v.isEmpty)) {
        val spec = normalizedPartSpec.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
        throw QueryCompilationErrors.invalidPartitionSpecError(
          s"The spec ($spec) contains an empty partition column value")
      }
    }

    // Create a project if this INSERT has a user-specified column list.
    val hasColumnList = insert.userSpecifiedCols.nonEmpty
    val query = if (hasColumnList) {
      createProjectForByNameQuery(tblName, insert)
    } else {
      insert.query
    }
    val newQuery = try {
      TableOutputResolver.resolveOutputColumns(
        tblName,
        expectedColumns,
        query,
        byName = hasColumnList || insert.byName,
        conf,
        supportColDefaultValue = true)
    } catch {
      case e: AnalysisException if staticPartCols.nonEmpty &&
        (e.getErrorClass == "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS" ||
          e.getErrorClass == "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS") =>
        val newException = e.copy(
          errorClass = Some("INSERT_PARTITION_COLUMN_ARITY_MISMATCH"),
          messageParameters = e.messageParameters ++ Map(
            "tableColumns" -> insert.table.output.map(c => toSQLId(c.name)).mkString(", "),
            "staticPartCols" -> staticPartCols.toSeq.sorted.map(c => toSQLId(c)).mkString(", ")
          ))
        newException.setStackTrace(e.getStackTrace)
        throw newException
    }
    if (normalizedPartSpec.nonEmpty) {
      if (normalizedPartSpec.size != partColNames.length) {
        throw QueryCompilationErrors.requestedPartitionsMismatchTablePartitionsError(
          tblName, normalizedPartSpec, partColNames)
      }

      // NOTE: Hudi converts [[InsertIntoStatement]] to [[InsertIntoHoodieTableCommand]]
      // and the user specified is no longer need after resolution
      // (`userSpecifiedCols = Seq()`)
      insert.copy(query = newQuery, partitionSpec = normalizedPartSpec, userSpecifiedCols = Seq())
    } else {
      // All partition columns are dynamic because the InsertIntoTable command does
      // not explicitly specify partitioning columns.
      // NOTE: Hudi converts [[InsertIntoStatement]] to [[InsertIntoHoodieTableCommand]]
      // and the user specified is no longer need after resolution
      // (`userSpecifiedCols = Seq()`)
      insert.copy(query = newQuery, partitionSpec = partColNames.map(_.name).map(_ -> None).toMap,
        userSpecifiedCols = Seq())
    }
  }
}
