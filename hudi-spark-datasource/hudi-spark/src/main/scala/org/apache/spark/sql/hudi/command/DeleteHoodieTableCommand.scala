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

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.hudi.DataSourceWriteOptions.{SPARK_SQL_OPTIMIZED_WRITES, SPARK_SQL_WRITES_PREPPED_KEY}
import org.apache.hudi.HoodieBaseRelation.convertToAvroSchema
import org.apache.hudi.HoodieCatalystUtils.toUnresolved
import org.apache.hudi.{HoodieSparkUtils, SparkAdapterSupport}
import org.apache.hudi.avro.AvroSchemaUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, Filter, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.removeMetaFields
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.types.LongType

import scala.collection.JavaConverters

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
    val attributeSeq = dft.table.output

    val filteredOutput = if (sparkSession.sqlContext.conf.getConfString(SPARK_SQL_OPTIMIZED_WRITES.key()
      , SPARK_SQL_OPTIMIZED_WRITES.defaultValue()) == "true") {
      attributeSeq
    } else {
      removeMetaFields(attributeSeq)
    }

    // Include temporary row index column name in the attribute refs of logical plan
    var attributeRefs = attributeSeq.map(expr => AttributeReference(expr.name, expr.dataType, nullable = expr.nullable)())
    var targetAttributes: Seq[NamedExpression] = filteredOutput.map(attr => toUnresolved(attr).asInstanceOf[NamedExpression])
    if (HoodieSparkUtils.gteqSpark3_5) {
      val rowIndexAttributeRef = AttributeReference(SparkAdapterSupport.sparkAdapter.getTemporaryRowIndexColumnName(), LongType, nullable = true)()
      attributeRefs = attributeRefs :+ rowIndexAttributeRef
      targetAttributes = targetAttributes :+ rowIndexAttributeRef.toAttribute
    }

    val schema = if (HoodieSparkUtils.gteqSpark3_5) {
      AvroSchemaUtils.projectSchema(
        convertToAvroSchema(catalogTable.tableSchema, catalogTable.tableName),
        JavaConverters.seqAsJavaList(attributeSeq.map(e => e.name)),
        new Schema.Field(
          SparkAdapterSupport.sparkAdapter.getTemporaryRowIndexColumnName(),
          Schema.createUnion(
            Schema.create(Schema.Type.NULL), SchemaBuilder.builder().longType())
        ))
    } else {
      convertToAvroSchema(catalogTable.tableSchema, catalogTable.tableName)
    }
    val options = Map(
      "hoodie.datasource.query.type" -> "snapshot",
      "path" -> catalogTable.metaClient.getBasePath.toString)
    val relation = sparkAdapter.createRelation(
      sparkSession.sqlContext, catalogTable.metaClient, schema, Array.empty, JavaConverters.mapAsJavaMap(options))
    val filteredPlan = Filter(toUnresolved(condition), Project(targetAttributes, new LogicalRelation(relation, attributeRefs, None, false)))

    val config = if (sparkSession.sqlContext.conf.getConfString(SPARK_SQL_OPTIMIZED_WRITES.key()
      , SPARK_SQL_OPTIMIZED_WRITES.defaultValue()) == "true") {
      buildHoodieDeleteTableConfig(catalogTable, sparkSession) + (SPARK_SQL_WRITES_PREPPED_KEY -> "true")
    } else {
      buildHoodieDeleteTableConfig(catalogTable, sparkSession)
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
}
