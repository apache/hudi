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

import org.apache.hudi.{HoodieSchemaConversionUtils, HoodieSparkSqlWriter, SparkAdapterSupport}
import org.apache.hudi.DataSourceWriteOptions.{SPARK_SQL_OPTIMIZED_WRITES, SPARK_SQL_WRITES_PREPPED_KEY, SPARK_SQL_WRITES_PREPPED_MOVES_RECORDS_KEY}

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.HoodieCatalystExpressionUtils.attributeEquals
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, If, Literal}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Filter, LogicalPlan, Project, UpdateTable}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.ProvidesHoodieConfig.combineOptions
import org.apache.spark.sql.hudi.command.HoodieCommandMetrics.updateCommitMetrics
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException

import scala.collection.JavaConverters._

case class UpdateHoodieTableCommand(ut: UpdateTable, query: LogicalPlan) extends DataWritingCommand
  with SparkAdapterSupport with ProvidesHoodieConfig {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def outputColumnNames: Seq[String] = {
    query.output.map(_.name)
  }

  override lazy val metrics: Map[String, SQLMetric] = HoodieCommandMetrics.metrics

  override def run(sparkSession: SparkSession, plan: SparkPlan): Seq[Row] = {
    val catalogTable = UpdateHoodieTableCommand.catalogTable(sparkSession, ut)
    val tableId = catalogTable.table.qualifiedName

    logInfo(s"Executing 'UPDATE' command for $tableId")
    val config = if (sparkSession.sessionState.conf.getConfString(SPARK_SQL_OPTIMIZED_WRITES.key()
      , SPARK_SQL_OPTIMIZED_WRITES.defaultValue()) == "true") {
      // Set config to show that this is a prepped write.
      buildHoodieConfig(catalogTable) + (SPARK_SQL_WRITES_PREPPED_KEY -> "true") +
        (SPARK_SQL_WRITES_PREPPED_MOVES_RECORDS_KEY -> UpdateHoodieTableCommand.assignsPartitionField(sparkSession, catalogTable, ut).toString)
    } else {
      buildHoodieConfig(catalogTable)
    }

    val enrichedSchema = HoodieSchemaConversionUtils.alignSchemaWithCatalog(
      plan.schema,
      catalogTable.tableSchemaWithoutMetaFields,
      sparkSession.sessionState.conf.caseSensitiveAnalysis,
      alignNullability = true)
    val df = sparkSession.internalCreateDataFrame(plan.execute(), enrichedSchema)
    val (success, commitInstantTime, _, _, _, _) = HoodieSparkSqlWriter.write(sparkSession.sqlContext, SaveMode.Append, config, df)
    if (success && commitInstantTime.isPresent) {
      updateCommitMetrics(metrics, catalogTable.metaClient, commitInstantTime.get())
      DataWritingCommand.propogateMetrics(sparkSession.sparkContext, this, metrics)
    }

    sparkSession.catalog.refreshTable(tableId)

    logInfo(s"Finished executing 'UPDATE' command for $tableId")

    Seq.empty[Row]
  }

  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {copy(query = newChild)}
}

object UpdateHoodieTableCommand extends SparkAdapterSupport with Logging {

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
      throw new HoodieAnalysisException(s"Detected disallowed assignment clause in UPDATE statement for $fieldType " +
        s"`$field` for table `$tableId`. Please remove the assignment clause to avoid the error.")
    })
  }

  /**
   * Whether the write moves a record that the statement gives other partition values to the partition of those
   * values. With a record key that takes a global index that updates the partition path, as for MERGE INTO. Without
   * one, the prepped write deletes the record where it was read and inserts it into its new partition.
   */
  private def movesRecordsAcrossPartitions(sparkSession: SparkSession, catalogTable: HoodieCatalogTable): Boolean = {
    if (catalogTable.primaryKeys.isEmpty) {
      sparkSession.sessionState.conf.getConfString(SPARK_SQL_OPTIMIZED_WRITES.key(), SPARK_SQL_OPTIMIZED_WRITES.defaultValue()) == "true"
    } else {
      MergeIntoHoodieTableCommand.useGlobalIndex(combineOptions(catalogTable, catalogTable.tableConfig,
        sparkSession.sessionState.conf, defaultOpts = Map.empty))
    }
  }

  /**
   * Whether the prepped write moves a record that the statement gives other partition values: only when the write
   * applies the update, as for a record that MERGE INTO moves. The move deletes the record where it was read and
   * inserts it into its new partition, bypassing the merge of the update with the record, so a record whose update
   * the merge would not apply keeps its partition values and takes the update in place, where the merge keeps the
   * record as it is. Refuses the statement when the merge mode does not tell whether the update applies.
   */
  private def movesOnlyAppliedUpdates(catalogTable: HoodieCatalogTable,
                                      resolver: Resolver,
                                      targetAttrs: Seq[Attribute],
                                      updatedValue: Attribute => Expression): Expression = {
    val orderingAttrs = catalogTable.orderingFields.asScala.toSeq.flatMap { field =>
      targetAttrs.find(attr => resolver(attr.name, field))
    }
    MergeIntoHoodieTableCommand.appliesUpdate(catalogTable.tableConfig.getRecordMergeMode,
      orderingAttrs, orderingAttrs.map(updatedValue)) match {
      case Literal(null, _) =>
        val partitionFields = catalogTable.partitionFields.map(field => s"`$field`").mkString(", ")
        throw new HoodieAnalysisException(s"UPDATE cannot change the partition field(s) $partitionFields of table " +
          s"`${catalogTable.table.qualifiedName}` under the ${catalogTable.tableConfig.getRecordMergeMode} merge mode, " +
          "as it cannot tell whether the merge applies the update to a record.")
      case updateApplies => updateApplies
    }
  }

  private def assignsPartitionField(sparkSession: SparkSession, catalogTable: HoodieCatalogTable, ut: UpdateTable): Boolean = {
    val resolver = sparkSession.sessionState.conf.resolver
    val partitionFields = catalogTable.tableConfig.getPartitionFields.orElse(Array.empty)
    ut.assignments.exists {
      case Assignment(attr: AttributeReference, _) => partitionFields.exists(resolver(attr.name, _))
      case _ => false
    }
  }

  private def catalogTable(sparkSession: SparkSession, ut: UpdateTable): HoodieCatalogTable = {
    sparkAdapter.resolveHoodieTable(ut.table) match {
      case Some(catalogTable) => HoodieCatalogTable(sparkSession, catalogTable)
      case _ =>
        throw new HoodieAnalysisException(s"Failed to resolve update statement into the Hudi table. Got instead: ${ut.table}")
    }
  }

  def inputPlan(sparkSession: SparkSession, ut: UpdateTable): LogicalPlan = {
    val hoodieCatalogTable = catalogTable(sparkSession, ut)
    val tableId = hoodieCatalogTable.table.qualifiedName

    logInfo(s"Executing 'UPDATE' command for $tableId")

    val assignedAttributes = ut.assignments.map {
      case Assignment(attr: AttributeReference, value) => attr -> value
    }

    if (!movesRecordsAcrossPartitions(sparkSession, hoodieCatalogTable)) {
      validateNoAssignmentsToTargetTableAttr(
        sparkSession.sessionState.conf.resolver,
        hoodieCatalogTable.tableConfig.getPartitionFields.orElse(Array.empty),
        tableId,
        "partition field",
        assignedAttributes
      )
    }

    // We don't support update queries changing the primary key column value.
    validateNoAssignmentsToTargetTableAttr(
      sparkSession.sessionState.conf.resolver,
      hoodieCatalogTable.tableConfig.getRecordKeyFields.orElse(Array.empty),
      tableId,
      "record key field",
      assignedAttributes
    )

    val optimizedWrites = sparkSession.sessionState.conf.getConfString(SPARK_SQL_OPTIMIZED_WRITES.key()
      , SPARK_SQL_OPTIMIZED_WRITES.defaultValue()) == "true"
    val filteredOutput = if (optimizedWrites) {
      ut.table.output
    } else {
      removeMetaFields(ut.table.output)
    }

    def updatedValue(targetAttr: Attribute): Expression = assignedAttributes.find(p => attributeEquals(p._1, targetAttr))
      .map { case (_, expr) => castIfNeeded(expr, targetAttr.dataType) }
      .getOrElse(targetAttr)
    val resolver = sparkSession.sessionState.conf.resolver
    val partitionAttrs = hoodieCatalogTable.partitionFields.flatMap { field =>
      ut.table.output.find(attr => resolver(attr.name, field))
    }
    val movesRecord = if (optimizedWrites && partitionAttrs.exists(attr => !updatedValue(attr).semanticEquals(attr))) {
      Some(movesOnlyAppliedUpdates(hoodieCatalogTable, resolver, ut.table.output, updatedValue))
    } else {
      None
    }

    val targetExprs = filteredOutput.map { targetAttr =>
      // NOTE: [[UpdateTable]] permits partial updates and therefore here we correlate assigned
      //       assigned attributes to the ones of the target table. Ones not being assigned
      //       will simply be carried over (from the old record)
      if (assignedAttributes.exists(p => attributeEquals(p._1, targetAttr))) {
        val value = movesRecord.filter(_ => partitionAttrs.exists(attributeEquals(_, targetAttr)))
          .map(If(_, updatedValue(targetAttr), targetAttr))
          .getOrElse(updatedValue(targetAttr))
        Alias(value, targetAttr.name)()
      } else {
        targetAttr
      }
    }

    val condition = ut.condition.getOrElse(TrueLiteral)
    Project(targetExprs, Filter(condition, ut.table))
  }
}
