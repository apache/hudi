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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.{DefaultSource, SparkAdapterSupport}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedPartitionSpec}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.{castIfNeeded, tableExistsInPath}
import org.apache.spark.sql.hudi.catalog.{HoodieCatalog, HoodieConfigHelper, HoodieInternalV2Table}
import org.apache.spark.sql.hudi.command.{AlterHoodieTableDropPartitionCommand, ShowHoodieTablePartitionsCommand, TruncateHoodieTableCommand}
import org.apache.spark.sql.hudi.{HoodieSqlCommonUtils, SparkSqlUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, SQLContext, SparkSession}

/**
 * Rule for convert the logical plan to command.
 * @param sparkSession
 */
case class HoodieSpark3Analysis(sparkSession: SparkSession) extends Rule[LogicalPlan]
  with SparkAdapterSupport with HoodieConfigHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case dsv2 @ DataSourceV2Relation(d: HoodieInternalV2Table, _, _, _, _) =>
      val output = dsv2.output
      val catalogTable = if (d.catalogTable.isDefined) {
        Some(d.v1Table)
      } else {
        None
      }
      val relation = new DefaultSource().createRelation(new SQLContext(sparkSession),
        buildHoodieConfig(d.hoodieCatalogTable))
      LogicalRelation(relation, output, catalogTable, isStreaming = false)
    case a @ InsertIntoStatement(r, d, _, _, _, _) if a.query.resolved
      && r.isInstanceOf[DataSourceV2Relation] && needsSchemaAdjustment(a.query, r.schema) =>
      val projection = resolveQueryColumnsByOrdinal(a.query, r.output)
      if (projection != a.query) {
        a.copy(query = projection)
      } else {
        a
      }
  }

  private def needsSchemaAdjustment(query: LogicalPlan,
                                    schema: StructType): Boolean = {
    val output = query.output
    // static partition insert.
    if (output.length < schema.length) {
      // scalastyle:off
      return false
      // scalastyle:on
    }

    val existingSchemaOutput = output.take(schema.length)
    existingSchemaOutput.map(_.name) != schema.map(_.name)
  }

  private def resolveQueryColumnsByOrdinal(query: LogicalPlan,
                                           targetAttrs: Seq[Attribute]): LogicalPlan = {
    // always add a Cast. it will be removed in the optimizer if it is unnecessary.
    val project = query.output.zipWithIndex.map { case (attr, i) =>
      if (i < targetAttrs.length) {
        val targetAttr = targetAttrs(i)
        val castAttr = castIfNeeded(attr.withNullability(targetAttr.nullable), targetAttr.dataType, conf)
        Alias(castAttr, targetAttr.name)()
      } else {
        attr
      }
    }
    Project(project, query)
  }


}

/**
 * Rule for resolve hoodie's extended syntax or rewrite some logical plan.
 * @param sparkSession
 */
case class HoodieSpark3ResolveReferences(sparkSession: SparkSession) extends Rule[LogicalPlan]
  with SparkAdapterSupport with HoodieConfigHelper {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    // Fill schema for Create Table without specify schema info
    case c @ CreateV2Table(tableCatalog, tableName, schema, _, properties, _)
      if SparkSqlUtils.isHoodieTable(properties) =>
      val hoodieCatalog = tableCatalog.asInstanceOf[HoodieCatalog]
      val tablePath = SparkSqlUtils.getTableLocation(properties,
        TableIdentifier(tableName.name(), tableName.namespace().lastOption), sparkSession)

      val tableExistInCatalog = hoodieCatalog.tableExists(tableName)
      // Only when the table has not exist in catalog, we need to fill the schema info for creating table.
      if (!tableExistInCatalog && tableExistsInPath(tablePath, sparkSession.sessionState.newHadoopConf())) {
        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(tablePath)
          .setConf(sparkSession.sessionState.newHadoopConf())
          .build()
        val tableSchema = HoodieSqlCommonUtils.getTableSqlSchema(metaClient)
        if (tableSchema.isDefined && schema.isEmpty) {
          // Fill the schema with the schema from the table
          c.copy(tableSchema = tableSchema.get)
        } else if (tableSchema.isDefined && schema != tableSchema.get) {
          throw new AnalysisException(s"Specified schema in create table statement is not equal to the table schema." +
            s"You should not specify the schema for an exist table: $tableName ")
        } else {
          c
        }
      } else {
        c
      }
    case DropPartitions(child, specs, ifExists, purge)
      if child.resolved && child.isInstanceOf[ResolvedTable] && child.asInstanceOf[ResolvedTable].table.isInstanceOf[HoodieInternalV2Table] =>
        AlterHoodieTableDropPartitionCommand(
          child.asInstanceOf[ResolvedTable].identifier.asTableIdentifier,
          specs.seq.map(f => f.asInstanceOf[UnresolvedPartitionSpec]).map(s => s.spec),
          ifExists,
          purge,
          retainData = true
        )
    case p => p
  }
}

/**
 * Rule for rewrite some spark commands to hudi's implementation.
 * @param sparkSession
 */
case class HoodieSpark3PostAnalysisRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case ShowPartitions(child, specOpt, _)
        if child.isInstanceOf[ResolvedTable] &&
          child.asInstanceOf[ResolvedTable].table.isInstanceOf[HoodieInternalV2Table] =>
        ShowHoodieTablePartitionsCommand(child.asInstanceOf[ResolvedTable].identifier.asTableIdentifier, specOpt.map(s => s.asInstanceOf[UnresolvedPartitionSpec].spec))

      // Rewrite TruncateTableCommand to TruncateHoodieTableCommand
      case TruncateTable(child)
        if child.isInstanceOf[ResolvedTable] &&
          child.asInstanceOf[ResolvedTable].table.isInstanceOf[HoodieInternalV2Table] =>
        new TruncateHoodieTableCommand(child.asInstanceOf[ResolvedTable].identifier.asTableIdentifier, None)
      case _ => plan
    }
  }
}

object AppendHoodie {
  def unapply(a: AppendData): Option[(DataSourceV2Relation, HoodieInternalV2Table)] = {
    if (a.query.resolved) {
      a.table match {
        case r: DataSourceV2Relation if r.table.isInstanceOf[HoodieInternalV2Table] =>
          Some((r, r.table.asInstanceOf[HoodieInternalV2Table]))
        case _ => None
      }
    } else {
      None
    }
  }
}
