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

import org.apache.hudi.DataSourceWriteOptions.{SPARK_SQL_OPTIMIZED_WRITES, SPARK_SQL_WRITES_PREPPED_KEY}
import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.table.HoodieTableConfig

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, Filter, LogicalPlan, Project}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isMetaField
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.command.HoodieLeafRunnableCommand.stripMetaFieldAttributes

case class DeleteHoodieTableCommand(dft: DeleteFromTable) extends HoodieLeafRunnableCommand
  with SparkAdapterSupport
  with ProvidesHoodieConfig {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalogTable = sparkAdapter.resolveHoodieTable(dft.table)
      .map(HoodieCatalogTable(sparkSession, _))
      .get

    val tableId = catalogTable.table.qualifiedName

    logInfo(s"Executing 'DELETE FROM' command for $tableId")

    val condition = sparkAdapter.extractDeleteCondition(dft)

    val config = if (sparkSession.sqlContext.conf.getConfString(SPARK_SQL_OPTIMIZED_WRITES.key()
      , SPARK_SQL_OPTIMIZED_WRITES.defaultValue()) == "true") {
      buildHoodieDeleteTableConfig(catalogTable, sparkSession) + (SPARK_SQL_WRITES_PREPPED_KEY -> "true")
    } else {
      buildHoodieDeleteTableConfig(catalogTable, sparkSession)
    }

    val recordKeysStr = config.getOrElse(HoodieTableConfig.RECORDKEY_FIELDS.key(), "")
    val recordKeys = recordKeysStr.split(",").filter(_.nonEmpty)

    // get all columns which are used in condition
    val conditionColumns = if (condition == null) {
      Seq.empty[String]
    } else {
      condition.references.map(_.name).toSeq
    }

    val requiredCols = recordKeys ++ conditionColumns

    val targetLogicalPlan = if (sparkSession.sqlContext.conf.getConfString(SPARK_SQL_OPTIMIZED_WRITES.key()
      , SPARK_SQL_OPTIMIZED_WRITES.defaultValue()) == "true") {
      tryPruningDeleteRecordSchema(dft.table, requiredCols)
    } else {
      stripMetaFieldAttributes(dft.table)
    }

    val filteredPlan = if (condition != null) {
      Filter(condition, targetLogicalPlan)
    } else {
      targetLogicalPlan
    }

    val df = Dataset.ofRows(sparkSession, filteredPlan)

    df.write.format("hudi")
      .mode(SaveMode.Append)
      .options(config)
      .save()

    sparkSession.catalog.refreshTable(tableId)

    logInfo(s"Finished executing 'DELETE FROM' command for $tableId")

    Seq.empty[Row]
  }

  def tryPruningDeleteRecordSchema(query: LogicalPlan, requiredColNames: Seq[String]): LogicalPlan = {
    val filteredOutput = query.output.filter(attr => isMetaField(attr.name) || requiredColNames.contains(attr.name))
    if (filteredOutput == query.output) {
      query
    } else {
      Project(filteredOutput, query)
    }
  }
}
