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

import org.apache.spark.sql._
import org.apache.spark.sql.HoodieCatalystExpressionUtils.attributeEquals
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Filter, Project, UpdateTable}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.analysis.HoodieAnalysis.failAnalysis

case class UpdateHoodieTableCommand(ut: UpdateTable) extends HoodieLeafRunnableCommand
  with SparkAdapterSupport with ProvidesHoodieConfig {

  private var sparkSession: SparkSession = _

  private lazy val hoodieCatalogTable = sparkAdapter.resolveHoodieTable(ut.table) match {
    case Some(catalogTable) => HoodieCatalogTable(sparkSession, catalogTable)
    case _ =>
      failAnalysis(s"Failed to resolve update statement into the Hudi table. Got instead: ${ut.table}")
  }

  /**
   * Validate there is no assignment clause for the given attribute in the given table.
   *
   * @param resolver    The resolver to use
   * @param fields      The fields from the target table who should not have any assignment clause
   * @param tableId     Table identifier (for error messages)
   * @param fieldType   Type of the attribute to be validated (for error messages)
   * @param assignments The assignments clause
   *
   * @throws AnalysisException if assignment clause for the given target table attribute is found
   */
  private def validateNoAssignmentsToTargetTableAttr(resolver: Resolver,
                                                     fields: Seq[String],
                                                     tableId: String,
                                                     fieldType: String,
                                                     assignments: Seq[(AttributeReference, Expression)]
                                                     ): Unit = {
    fields.foreach(field => if (assignments.exists {
      case (attr, _) => resolver(attr.name, field)
    }) {
      throw new AnalysisException(s"Detected disallowed assignment clause in UPDATE statement for $fieldType " +
        s"`$field` for table `$tableId`. Please remove the assignment clause to avoid the error.")
    })
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    this.sparkSession = sparkSession
    val catalogTable = sparkAdapter.resolveHoodieTable(ut.table)
      .map(HoodieCatalogTable(sparkSession, _))
      .get

    val tableId = catalogTable.table.qualifiedName

    logInfo(s"Executing 'UPDATE' command for $tableId")

    val assignedAttributes = ut.assignments.map {
      case Assignment(attr: AttributeReference, value) => attr -> value
    }

    // We don't support update queries changing partition column value.
    validateNoAssignmentsToTargetTableAttr(
      sparkSession.sessionState.conf.resolver,
      hoodieCatalogTable.tableConfig.getPartitionFields.orElse(Array.empty),
      tableId,
      "partition field",
      assignedAttributes
    )

    // We don't support update queries changing the primary key column value.
    validateNoAssignmentsToTargetTableAttr(
      sparkSession.sessionState.conf.resolver,
      hoodieCatalogTable.tableConfig.getRecordKeyFields.orElse(Array.empty),
      tableId,
      "record key field",
      assignedAttributes
    )

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

    val config = if (sparkSession.sqlContext.conf.getConfString(SPARK_SQL_OPTIMIZED_WRITES.key()
      , SPARK_SQL_OPTIMIZED_WRITES.defaultValue()) == "true") {
      // Set config to show that this is a prepped write.
      buildHoodieConfig(catalogTable) + (SPARK_SQL_WRITES_PREPPED_KEY -> "true")
    } else {
      buildHoodieConfig(catalogTable)
    }

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
