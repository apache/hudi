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

import org.apache.hudi.DataSourceWriteOptions.{SPARK_SQL_WRITES_PREPPED_KEY, SPARK_SQL_OPTIMIZED_WRITES}
import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.HoodieCatalystExpressionUtils.attributeEquals
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Filter, Project, UpdateTable}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig

case class UpdateHoodieTableCommand(ut: UpdateTable) extends HoodieLeafRunnableCommand
  with SparkAdapterSupport with ProvidesHoodieConfig {

  private def buildUpdateConfig( sparkSession: SparkSession,
                                    hoodieCatalogTable: HoodieCatalogTable): Map[String, String] = {
    val optimizedWrite = sparkSession.sqlContext.conf.getConfString(ENABLE_OPTIMIZED_SQL_WRITES.key()
      , ENABLE_OPTIMIZED_SQL_WRITES.defaultValue()) == "true"

    // as for MERGE_ON_READ precombine field is required but it's not enforced during table creation
    val shouldCombine = !StringUtils.isNullOrEmpty(hoodieCatalogTable.preCombineKey.getOrElse(""))

    if (!shouldCombine && hoodieCatalogTable.tableType.name().equals("MERGE_ON_READ")) {
      throw new HoodieException("Precombine field must be set for MERGE_ON_READ table type to execute UPDATE statement.")
    }

    // Set config to show that this is a prepped write.
    val preppedWriteConfig = if (optimizedWrite) Map(DATASOURCE_WRITE_PREPPED_KEY -> "true") else Map().empty
    val combineBeforeUpsertConfig = Map(HoodieWriteConfig.COMBINE_BEFORE_UPSERT.key() -> shouldCombine.toString)

    buildHoodieConfig(hoodieCatalogTable) ++ preppedWriteConfig ++ combineBeforeUpsertConfig
  }
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalogTable = sparkAdapter.resolveHoodieTable(ut.table)
      .map(HoodieCatalogTable(sparkSession, _))
      .get

    val tableId = catalogTable.table.qualifiedName

    logInfo(s"Executing 'UPDATE' command for $tableId")

    val assignedAttributes = ut.assignments.map {
      case Assignment(attr: AttributeReference, value) => attr -> value
    }

    val filteredOutput = if (sparkSession.sqlContext.conf.getConfString(SPARK_SQL_OPTIMIZED_WRITES.key()
      , SPARK_SQL_OPTIMIZED_WRITES.defaultValue()) == "true") {
      ut.table.output
    } else {
      removeMetaFields(ut.table.output)
    }

    val targetExprs = filteredOutput.map { targetAttr =>
      // NOTE: [[UpdateTable]] permits partial updates and therefore here we correlate assigned
      //       assigned attributes to the ones of the target table. Ones not being assigned
      //       will simply be carried over (from the old record)
      assignedAttributes.find(p => attributeEquals(p._1, targetAttr))
        .map { case (_, expr) => Alias(castIfNeeded(expr, targetAttr.dataType), targetAttr.name)() }
        .getOrElse(targetAttr)
    }

    val condition = ut.condition.getOrElse(TrueLiteral)
    val filteredPlan = Filter(condition, Project(targetExprs, ut.table))

    val config = buildUpdateConfig(sparkSession, catalogTable)

    val df = Dataset.ofRows(sparkSession, filteredPlan)

    df.write.format("hudi")
      .mode(SaveMode.Append)
      .options(config)
      .save()

    sparkSession.catalog.refreshTable(tableId)

    logInfo(s"Finished executing 'UPDATE' command for $tableId")

    Seq.empty[Row]
  }

}
