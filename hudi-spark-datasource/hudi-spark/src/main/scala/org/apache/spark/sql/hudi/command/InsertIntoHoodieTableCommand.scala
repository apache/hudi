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

import org.apache.hudi.HoodieSparkSqlWriter
import org.apache.hudi.exception.HoodieException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.canCast
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

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
  extends HoodieLeafRunnableCommand {
  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(logicalRelation.catalogTable.isDefined, "Missing catalog table")

    val table = logicalRelation.catalogTable.get
    InsertIntoHoodieTableCommand.run(sparkSession, table, query, partitionSpec, overwrite)
    Seq.empty[Row]
  }
}

object InsertIntoHoodieTableCommand extends Logging with ProvidesHoodieConfig {
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
          query: LogicalPlan,
          partitionSpec: Map[String, Option[String]],
          overwrite: Boolean,
          refreshTable: Boolean = true,
          extraOptions: Map[String, String] = Map.empty): Boolean = {
    val catalogTable = new HoodieCatalogTable(sparkSession, table)
    val config = buildHoodieInsertConfig(catalogTable, sparkSession, overwrite, partitionSpec, extraOptions)

    // NOTE: In case of partitioned table we override specified "overwrite" parameter
    //       to instead append to the dataset
    val mode = if (overwrite && catalogTable.partitionFields.isEmpty) {
      SaveMode.Overwrite
    } else {
      SaveMode.Append
    }

    val alignedQuery = alignQueryOutput(query, catalogTable, partitionSpec, sparkSession.sessionState.conf)

    val (success, _, _, _, _, _) = HoodieSparkSqlWriter.write(sparkSession.sqlContext, mode, config, Dataset.ofRows(sparkSession, alignedQuery))

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
  private def alignQueryOutput(query: LogicalPlan,
                               catalogTable: HoodieCatalogTable,
                               partitionsSpec: Map[String, Option[String]],
                               conf: SQLConf): LogicalPlan = {

    val targetPartitionSchema = catalogTable.partitionSchema
    val staticPartitionValues = filterStaticPartitionValues(partitionsSpec)

    validate(removeMetaFields(query.schema), partitionsSpec, catalogTable)

    // To validate and align properly output of the query, we simply filter out partition columns with already
    // provided static values from the table's schema
    val expectedColumns = catalogTable.tableSchemaWithoutMetaFields.filterNot(f => staticPartitionValues.contains(f.name))

    val queryOutputWithoutMetaFields = removeMetaFields(query.output)

    val transformedQueryOutput = coerceQueryOutput(queryOutputWithoutMetaFields, expectedColumns, conf)
    val staticPartitionValuesExprs = createStaticPartitionValuesExpressions(staticPartitionValues, targetPartitionSchema, conf)

    Project(transformedQueryOutput ++ staticPartitionValuesExprs, query)
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
        val castExpr = castIfNeeded(Literal.create(staticPartitionValue), pf.dataType, conf)

        Alias(castExpr, pf.name)()
      })
  }

  private def coerceQueryOutput(queryOutput: Seq[Attribute],
                                expectedColumns: Seq[StructField],
                                conf: SQLConf): Seq[Alias] = {
    queryOutput.zip(expectedColumns).map { case (attr, field) =>
      // Lookup (by name) corresponding column from the table definition. In case there's
      // no match, assume the column by the relative ordering
      val targetColumn = expectedColumns.find(_.name.equals(attr.name)).getOrElse(field)
      // Since query output might be providing data in a different format we might need to wrap
      // output reference into the cast expression
      val castExpr = castIfNeeded(attr.withNullability(targetColumn.nullable), targetColumn.dataType, conf)

      Alias(castExpr, targetColumn.name)()
    }
  }

  private def conforms(sourceSchema: StructType, targetSchema: StructType): Boolean = {
    if (sourceSchema.fields.length != targetSchema.fields.length) {
      false
    } else {
      targetSchema.fields.zip(sourceSchema).forall {
        case (targetColumn, correspondingColumn) =>
          // Determine matching source column by either a name or corresponding ordering
          val matchingSourceColumn = sourceSchema.find(_.name == targetColumn.name).getOrElse(correspondingColumn)
          // Make sure we can cast source column to the target column type
          canCast(matchingSourceColumn.dataType, targetColumn.dataType)
      }
    }
  }

  private def filterStaticPartitionValues(partitionsSpec: Map[String, Option[String]]): Map[String, String] =
    partitionsSpec.filter(p => p._2.isDefined).mapValues(_.get)
}
