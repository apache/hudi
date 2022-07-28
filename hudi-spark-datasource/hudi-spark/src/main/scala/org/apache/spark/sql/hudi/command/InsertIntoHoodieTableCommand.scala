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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

/**
 * Command for insert into hoodie table.
 */
case class InsertIntoHoodieTableCommand(
    logicalRelation: LogicalRelation,
    query: LogicalPlan,
    partition: Map[String, Option[String]],
    overwrite: Boolean)
  extends HoodieLeafRunnableCommand {
  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(logicalRelation.catalogTable.isDefined, "Missing catalog table")

    val table = logicalRelation.catalogTable.get
    InsertIntoHoodieTableCommand.run(sparkSession, table, query, partition, overwrite)
    Seq.empty[Row]
  }
}

object InsertIntoHoodieTableCommand extends Logging with ProvidesHoodieConfig {
  /**
   * Run the insert query. We support both dynamic partition insert and static partition insert.
   * @param sparkSession The spark session.
   * @param table The insert table.
   * @param query The insert query.
   * @param insertPartitions The specified insert partition map.
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
      insertPartitions: Map[String, Option[String]],
      overwrite: Boolean,
      refreshTable: Boolean = true,
      extraOptions: Map[String, String] = Map.empty): Boolean = {

    val hoodieCatalogTable = new HoodieCatalogTable(sparkSession, table)
    val config = buildHoodieInsertConfig(hoodieCatalogTable, sparkSession, overwrite, insertPartitions, extraOptions)

    val mode = if (overwrite && hoodieCatalogTable.partitionFields.isEmpty) {
      // insert overwrite non-partition table
      SaveMode.Overwrite
    } else {
      // for insert into or insert overwrite partition we use append mode.
      SaveMode.Append
    }
    val conf = sparkSession.sessionState.conf
    val alignedQuery = alignOutputFields(query, hoodieCatalogTable, insertPartitions, conf)
    // If we create dataframe using the Dataset.ofRows(sparkSession, alignedQuery),
    // The nullable attribute of fields will lost.
    // In order to pass the nullable attribute to the inputDF, we specify the schema
    // of the rdd.
    val inputDF = sparkSession.createDataFrame(
      Dataset.ofRows(sparkSession, alignedQuery).rdd, alignedQuery.schema)
    val success =
      HoodieSparkSqlWriter.write(sparkSession.sqlContext, mode, config, inputDF)._1
    if (success) {
      if (refreshTable) {
        sparkSession.catalog.refreshTable(table.identifier.unquotedString)
      }
      true
    } else {
      false
    }
  }

  /**
   * Aligned the type and name of query's output fields with the result table's fields.
   * @param query The insert query which to aligned.
   * @param hoodieCatalogTable The result hoodie catalog table.
   * @param insertPartitions The insert partition map.
   * @param conf The SQLConf.
   * @return
   */
  private def alignOutputFields(
    query: LogicalPlan,
    hoodieCatalogTable: HoodieCatalogTable,
    insertPartitions: Map[String, Option[String]],
    conf: SQLConf): LogicalPlan = {

    val targetPartitionSchema = hoodieCatalogTable.partitionSchema

    val staticPartitionValues = insertPartitions.filter(p => p._2.isDefined).mapValues(_.get)
    assert(staticPartitionValues.isEmpty ||
      insertPartitions.size == targetPartitionSchema.size,
      s"Required partition columns is: ${targetPartitionSchema.json}, Current input partitions " +
        s"is: ${staticPartitionValues.mkString("," + "")}")

    val queryOutputWithoutMetaFields = removeMetaFields(query.output)
    assert(staticPartitionValues.size + queryOutputWithoutMetaFields.size
      == hoodieCatalogTable.tableSchemaWithoutMetaFields.size,
      s"Required select columns count: ${hoodieCatalogTable.tableSchemaWithoutMetaFields.size}, " +
        s"Current select columns(including static partition column) count: " +
        s"${staticPartitionValues.size + queryOutputWithoutMetaFields.size}，columns: " +
        s"(${(queryOutputWithoutMetaFields.map(_.name) ++ staticPartitionValues.keys).mkString(",")})")

    val dataAndDynamicPartitionSchemaWithoutMetaFields = StructType(
      hoodieCatalogTable.tableSchemaWithoutMetaFields.filterNot(f => staticPartitionValues.contains(f.name)))
    val dataProjectsWithoutMetaFields = getTableFieldsAlias(queryOutputWithoutMetaFields,
      dataAndDynamicPartitionSchemaWithoutMetaFields.fields, conf)

    val partitionProjects = targetPartitionSchema.fields.filter(f => staticPartitionValues.contains(f.name))
      .map(f => {
        val staticPartitionValue = staticPartitionValues.getOrElse(f.name,
          s"Missing static partition value for: ${f.name}")
        val castAttr = castIfNeeded(Literal.create(staticPartitionValue), f.dataType, conf)
        Alias(castAttr, f.name)()
      })

    Project(dataProjectsWithoutMetaFields ++ partitionProjects, query)
  }

  private def getTableFieldsAlias(
     queryOutputWithoutMetaFields: Seq[Attribute],
     schemaWithoutMetaFields: Seq[StructField],
     conf: SQLConf): Seq[Alias] = {
    queryOutputWithoutMetaFields.zip(schemaWithoutMetaFields).map { case (dataAttr, dataField) =>
      val targetAttrOption = if (dataAttr.name.startsWith("col")) {
        None
      } else {
        queryOutputWithoutMetaFields.find(_.name.equals(dataField.name))
      }
      val targetAttr = targetAttrOption.getOrElse(dataAttr)
      val castAttr = castIfNeeded(targetAttr.withNullability(dataField.nullable),
        dataField.dataType, conf)
      Alias(castAttr, dataField.name)()
    }
  }
}
