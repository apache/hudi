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

import org.apache.hudi.{HoodieSparkSqlWriter, SparkAdapterSupport}
import org.apache.hudi.DataSourceWriteOptions.{SPARK_SQL_OPTIMIZED_WRITES, SPARK_SQL_WRITES_PREPPED_KEY}
import org.apache.hudi.common.table.HoodieTableConfig

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, Filter, LogicalPlan, Project, UpdateTable}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isMetaField
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.command.HoodieCommandMetrics.updateCommitMetrics
import org.apache.spark.sql.hudi.command.HoodieLeafRunnableCommand.stripMetaFieldAttributes

case class DeleteHoodieTableCommand(catalogTable: HoodieCatalogTable, query: LogicalPlan, config: Map[String, String]) extends DataWritingCommand
  with SparkAdapterSupport
  with ProvidesHoodieConfig {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def outputColumnNames: Seq[String] = {
    query.output.map(_.name)
  }

  override lazy val metrics: Map[String, SQLMetric] = HoodieCommandMetrics.metrics

  override def run(sparkSession: SparkSession, queryPlan: SparkPlan): Seq[Row] = {
    val tableId = catalogTable.table.qualifiedName
    logInfo(s"Executing 'DELETE FROM' command for $tableId")
    val df = sparkSession.internalCreateDataFrame(queryPlan.execute(), queryPlan.schema)
    val (success, commitInstantTime, _, _, _, _) = HoodieSparkSqlWriter.write(sparkSession.sqlContext, SaveMode.Append, config, df)
    if (success && commitInstantTime.isPresent) {
      updateCommitMetrics(metrics, catalogTable.metaClient, commitInstantTime.get())
      DataWritingCommand.propogateMetrics(sparkSession.sparkContext, this, metrics)
    }
    sparkSession.catalog.refreshTable(tableId)
    logInfo(s"Finished executing 'DELETE FROM' command for $tableId")
    Seq.empty[Row]
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(query = newChild)
}

object DeleteHoodieTableCommand extends SparkAdapterSupport with ProvidesHoodieConfig{

  def inputPlan(sparkSession: SparkSession, dft: DeleteFromTable, catalogTable: HoodieCatalogTable): (LogicalPlan, Map[String, String]) = {
    val condition = sparkAdapter.extractDeleteCondition(dft)

    val config = if (sparkSession.sessionState.conf.getConfString(SPARK_SQL_OPTIMIZED_WRITES.key()
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

    val targetLogicalPlan = if (sparkSession.sessionState.conf.getConfString(SPARK_SQL_OPTIMIZED_WRITES.key()
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
    (filteredPlan, config)
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
