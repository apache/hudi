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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.SparkAdapterSupport
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig

case class DeleteHoodieTableCommand(deleteTable: DeleteFromTable) extends HoodieLeafRunnableCommand
  with SparkAdapterSupport with ProvidesHoodieConfig {

  private val table = deleteTable.table

  private val tableId = getTableIdentifier(table)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logInfo(s"start execute delete command for $tableId")

    // Remove meta fields from the data frame
    var df = removeMetaFields(Dataset.ofRows(sparkSession, table))
    // SPARK-38626 DeleteFromTable.condition is changed from Option[Expression] to Expression in Spark 3.3
    val condition = sparkAdapter.extractCondition(deleteTable)
    if (condition != null) df = df.filter(Column(condition))

    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableId)
    val config = buildHoodieDeleteTableConfig(hoodieCatalogTable, sparkSession)
    df.write
      .format("hudi")
      .mode(SaveMode.Append)
      .options(config)
      .save()
    sparkSession.catalog.refreshTable(tableId.unquotedString)
    logInfo(s"Finish execute delete command for $tableId")
    Seq.empty[Row]
  }
}
