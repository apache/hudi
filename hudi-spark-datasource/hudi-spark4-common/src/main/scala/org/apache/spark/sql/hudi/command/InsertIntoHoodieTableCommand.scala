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

import org.apache.hudi.{AvroConversionUtils, HoodieSparkSqlWriter, SparkAdapterSupport}
import org.apache.hudi.AvroConversionUtils.convertStructTypeToAvroSchema
import org.apache.hudi.exception.HoodieException

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.command.HoodieCommandMetrics.updateCommitMetrics
import org.apache.spark.sql.hudi.command.HoodieLeafRunnableCommand.stripMetaFieldAttributes
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Command for insert into Hudi table.
 *
 * This is correspondent to Spark's native [[InsertIntoStatement]]
 *
 * @param logicalRelation the [[LogicalRelation]] representing the table to be writing into.
 * @param query           the logical plan representing data to be written
 * @param partitionSpec   a map from the partition key to the partition value (optional).
 *                        If the value is missing, dynamic partition insert will be performed.
 *                        As an example, `INSERT INTO tbl PARTITION (a=1, b=2) AS` would have
 *                        Map('a' -> Some('1'), 'b' -> Some('2')),
 *                        and `INSERT INTO tbl PARTITION (a=1, b) AS ...`
 *                        would have Map('a' -> Some('1'), 'b' -> None).
 * @param overwrite       overwrite existing table or partitions.
 */
case class InsertIntoHoodieTableCommand(logicalRelation: LogicalRelation,
                                        query: LogicalPlan,
                                        partitionSpec: Map[String, Option[String]],
                                        overwrite: Boolean)
  extends DataWritingCommand {
  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)
  override lazy val metrics: Map[String, SQLMetric] = HoodieCommandMetrics.metrics

  override def outputColumnNames: Seq[String] = {
    query.output.map(_.name)
  }

  override def run(sparkSession: SparkSession, plan: SparkPlan): Seq[Row] = {
    assert(logicalRelation.catalogTable.isDefined, "Missing catalog table")

    val table = logicalRelation.catalogTable.get
    InsertIntoHoodieTableCommand.run(sparkSession, table, plan, partitionSpec, overwrite, metrics = metrics)
    DataWritingCommand.propogateMetrics(sparkSession.sparkContext, this, metrics)
    Seq.empty[Row]
  }

  override def withNewChildInternal(newChild: LogicalPlan) : LogicalPlan = copy(query = newChild)
}

object InsertIntoHoodieTableCommand extends Logging with ProvidesHoodieConfig with SparkAdapterSupport {

  /**
   * Run the insert query. We support both dynamic partition insert and static partition insert.
   * @param sparkSession The spark session.
   * @param table The insert table.
   * @param query The insert query.
   * @param partitionSpec The specified insert partition map.
   *                         e.g. "insert into h(dt = '2021') select id, name from src"
   *                         "dt" is the key in the map and "2021" is the partition value. If the
   *                         partition value has not specified(in the case of dynamic partition)
   *                         , it is None in the map.
   * @param overwrite Whether to overwrite the table.
   * @param refreshTable Whether to refresh the table after insert finished.
   * @param extraOptions Extra options for insert.
   */
  def run(sparkSession: SparkSession,
          table: CatalogTable,
          query: SparkPlan,
          partitionSpec: Map[String, Option[String]],
          overwrite: Boolean,
          refreshTable: Boolean = true,
          extraOptions: Map[String, String] = Map.empty,
          metrics: Map[String, SQLMetric]): Boolean = {
    val catalogTable = new HoodieCatalogTable(sparkSession, table)

    val (mode, isOverWriteTable, isOverWritePartition, staticOverwritePartitionPathOpt) = if (overwrite) {
      deduceOverwriteConfig(sparkSession, catalogTable, partitionSpec, extraOptions)
    } else {
      (SaveMode.Append, false, false, Option.empty)
    }
    val config = buildHoodieInsertConfig(catalogTable, sparkSession, isOverWritePartition, isOverWriteTable, partitionSpec, extraOptions, staticOverwritePartitionPathOpt)

    val df = sparkSession.internalCreateDataFrame(query.execute(), query.schema)
    val (structName, namespace) = AvroConversionUtils.getAvroRecordNameAndNamespace(catalogTable.tableName)
    val schema = convertStructTypeToAvroSchema(catalogTable.tableSchema, structName, namespace)
    val (success, commitInstantTime, _, _, _, _) = HoodieSparkSqlWriter.write(sparkSession.sqlContext, mode, config, df,
      schemaFromCatalog = Option.apply(schema))

    if (!success) {
      throw new HoodieException("Insert Into to Hudi table failed")
    }

    if (success && commitInstantTime.isPresent) {
      updateCommitMetrics(metrics, catalogTable.metaClient, commitInstantTime.get())
    }

    if (success && refreshTable) {
      sparkSession.catalog.refreshTable(table.identifier.unquotedString)
    }

    success
  }

  /**
   * Align provided [[query]]'s output with the expected [[catalogTable]] schema by
   *
   * <ul>
   *   <li>Performing type coercion (casting corresponding outputs, where needed)</li>
   *   <li>Adding aliases (matching column names) to corresponding outputs </li>
   * </ul>
   *
   * @param query target query whose output is to be inserted
   * @param catalogTable catalog table
   * @param partitionsSpec partition spec specifying static/dynamic partition values
   * @param conf Spark's [[SQLConf]]
   */
  def alignQueryOutput(query: LogicalPlan,
                               catalogTable: HoodieCatalogTable,
                               partitionsSpec: Map[String, Option[String]],
                               conf: SQLConf): LogicalPlan = {

    val targetPartitionSchema = catalogTable.partitionSchema
    val staticPartitionValues = filterStaticPartitionValues(partitionsSpec)

    // Make sure we strip out meta-fields from the incoming dataset (these will have to be discarded anyway)
    val cleanedQuery = stripMetaFieldAttributes(query)
    // To validate and align properly output of the query, we simply filter out partition columns with already
    // provided static values from the table's schema
    //
    // NOTE: This is a crucial step: since coercion might rely on either of a) name-based or b) positional-based
    //       matching it's important to strip out partition columns, having static values provided in the partition spec,
    //       since such columns wouldn't be otherwise specified w/in the query itself and therefore couldn't be matched
    //       positionally for example
    val expectedQueryColumns = catalogTable.tableSchemaWithoutMetaFields.filterNot(f => staticPartitionValues.contains(f.name))
    val coercedQueryOutput = coerceQueryOutputColumns(StructType(expectedQueryColumns), cleanedQuery, catalogTable, conf)
    // After potential reshaping validate that the output of the query conforms to the table's schema
    validate(removeMetaFields(coercedQueryOutput.schema), partitionsSpec, catalogTable)

    val staticPartitionValuesExprs = createStaticPartitionValuesExpressions(staticPartitionValues, targetPartitionSchema, conf)

    Project(coercedQueryOutput.output ++ staticPartitionValuesExprs, coercedQueryOutput)
  }

  private def coerceQueryOutputColumns(expectedSchema: StructType,
                                       query: LogicalPlan,
                                       catalogTable: HoodieCatalogTable,
                                       conf: SQLConf): LogicalPlan = {
    val planUtils = sparkAdapter.getCatalystPlanUtils
    try {
      planUtils.resolveOutputColumns(
        catalogTable.catalogTableName, sparkAdapter.getSchemaUtils.toAttributes(expectedSchema), query, byName = true, conf)
    } catch {
      // NOTE: In case matching by name didn't match the query output, we will attempt positional matching
      // SPARK-42309 Error message changed in Spark 3.5.0 so we need to match two strings here
      case ae: AnalysisException if (ae.getMessage().startsWith("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data for the table")
        || ae.getMessage().startsWith("Cannot write incompatible data to table")) =>
        planUtils.resolveOutputColumns(
          catalogTable.catalogTableName, sparkAdapter.getSchemaUtils.toAttributes(expectedSchema), query, byName = false, conf)
    }
  }

  private def validate(queryOutputSchema: StructType, partitionsSpec: Map[String, Option[String]], catalogTable: HoodieCatalogTable): Unit = {
    // Validate that partition-spec has proper format (it could be empty if all of the partition values are dynamic,
    // ie there are no static partition-values specified)
    if (partitionsSpec.nonEmpty && partitionsSpec.size != catalogTable.partitionSchema.size) {
      throw new HoodieException(s"Required partition schema is: ${catalogTable.partitionSchema.fieldNames.mkString("[", ", ", "]")}, " +
        s"partition spec is: ${partitionsSpec.mkString("[", ", ", "]")}")
    }

    val staticPartitionValues = filterStaticPartitionValues(partitionsSpec)
    val fullQueryOutputSchema = StructType(queryOutputSchema.fields ++ staticPartitionValues.keys.map(StructField(_, StringType)))

    // Assert that query provides all the required columns
    if (!conforms(fullQueryOutputSchema, catalogTable.tableSchemaWithoutMetaFields)) {
      throw new HoodieException(s"Expected table's schema: ${catalogTable.tableSchemaWithoutMetaFields.fields.mkString("[", ", ", "]")}, " +
        s"query's output (including static partition values): ${fullQueryOutputSchema.fields.mkString("[", ", ", "]")}")
    }
  }

  private def createStaticPartitionValuesExpressions(staticPartitionValues: Map[String, String],
                                                     partitionSchema: StructType,
                                                     conf: SQLConf): Seq[NamedExpression] = {
    partitionSchema.fields
      .filter(pf => staticPartitionValues.contains(pf.name))
      .map(pf => {
        val staticPartitionValue = staticPartitionValues(pf.name)
        val castExpr = castIfNeeded(Literal.create(staticPartitionValue), pf.dataType)

        Alias(castExpr, pf.name)()
      })
  }

  private def conforms(sourceSchema: StructType, targetSchema: StructType): Boolean = {
    if (sourceSchema.fields.length != targetSchema.fields.length) {
      false
    } else {
      targetSchema.fields.zip(sourceSchema).forall {
        case (targetColumn, sourceColumn) =>
          // Make sure we can cast source column to the target column type
          Cast.canCast(sourceColumn.dataType, targetColumn.dataType)
      }
    }
  }

  private def filterStaticPartitionValues(partitionsSpec: Map[String, Option[String]]): Map[String, String] =
    partitionsSpec.filter(p => p._2.isDefined).mapValues(_.get).toMap
}
