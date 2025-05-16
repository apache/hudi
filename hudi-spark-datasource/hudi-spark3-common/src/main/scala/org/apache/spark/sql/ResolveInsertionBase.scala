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

import org.apache.hudi.SparkAdapterSupport

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * this class is similar with spark::ResolveInsertionBase in spark3.4
 */
object ResolveInsertionBase extends SparkAdapterSupport {
  def resolver: Resolver = SQLConf.get.resolver

  /** Add a project to use the table column names for INSERT INTO BY NAME */
  def createProjectForByNameQuery(tblName: String,
                                  i: InsertIntoStatement): LogicalPlan = {
    sparkAdapter.getSchemaUtils.checkColumnNameDuplication(i.userSpecifiedCols,
      "in the table definition of " + tblName, SQLConf.get.caseSensitiveAnalysis)

    if (i.userSpecifiedCols.size != i.query.output.size) {
      if (i.userSpecifiedCols.size > i.query.output.size) {
        throw cannotWriteNotEnoughColumnsToTableError(
          tblName, i.userSpecifiedCols, i.query.output)
      } else {
        throw cannotWriteNotEnoughColumnsToTableError(
          tblName, i.userSpecifiedCols, i.query.output)
      }
    }
    val projectByName = i.userSpecifiedCols.zip(i.query.output)
      .map { case (userSpecifiedCol, queryOutputCol) =>
        val resolvedCol = i.table.resolve(Seq(userSpecifiedCol), resolver)
          .getOrElse(
            throw unresolvedAttributeError(
              "UNRESOLVED_COLUMN", userSpecifiedCol, i.table.output.map(_.name)))
        (queryOutputCol.dataType, resolvedCol.dataType) match {
          case (input: StructType, expected: StructType) =>
            // Rename inner fields of the input column to pass the by-name INSERT analysis.
            Alias(Cast(queryOutputCol, renameFieldsInStruct(input, expected)), resolvedCol.name)()
          case _ =>
            Alias(queryOutputCol, resolvedCol.name)()
        }
      }
    Project(projectByName, i.query)
  }

  private def renameFieldsInStruct(input: StructType, expected: StructType): StructType = {
    if (input.length == expected.length) {
      val newFields = input.zip(expected).map { case (f1, f2) =>
        (f1.dataType, f2.dataType) match {
          case (s1: StructType, s2: StructType) =>
            f1.copy(name = f2.name, dataType = renameFieldsInStruct(s1, s2))
          case _ =>
            f1.copy(name = f2.name)
        }
      }
      StructType(newFields)
    } else {
      input
    }
  }

  def cannotWriteNotEnoughColumnsToTableError(tableName: String,
                                              expected: Seq[String],
                                              queryOutput: Seq[Attribute]): Throwable = {
    new AnalysisException("table: " + tableName + "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS, expect: " +
      expected.mkString(",") + " queryOutput: " + queryOutput.map(attr => attr.name).mkString(","))
  }

  def unresolvedAttributeError(errorClass: String,
                               colName: String,
                               candidates: Seq[String]): Throwable = {
    new AnalysisException(errorClass + " expect col is : " +
      colName + " table output is : " + candidates.mkString(","))
  }
}
