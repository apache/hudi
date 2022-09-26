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

import org.apache.hudi.{DefaultSource, SparkAdapterSupport}
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedPartitionSpec}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.{Table, V1Table}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.{castIfNeeded, removeMetaFields}
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.catalog.HoodieInternalV2Table
import org.apache.spark.sql.hudi.command.{AlterHoodieTableDropPartitionCommand, ShowHoodieTablePartitionsCommand, TruncateHoodieTableCommand}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * NOTE: PLEASE READ CAREFULLY
 *
 * Since Hudi relations don't currently implement DS V2 Read API, we have to fallback to V1 here.
 * Such fallback will have considerable performance impact, therefore it's only performed in cases
 * where V2 API have to be used. Currently only such use-case is using of Schema Evolution feature
 *
 * Check out HUDI-4178 for more details
 */
class HoodieDataSourceV2ToV1Fallback(sparkSession: SparkSession) extends Rule[LogicalPlan]
  with ProvidesHoodieConfig {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case v2r @ DataSourceV2Relation(v2Table: HoodieInternalV2Table, _, _, _, _) =>
      val output = v2r.output
      val catalogTable = v2Table.catalogTable.map(_ => v2Table.v1Table)
      val relation = new DefaultSource().createRelation(new SQLContext(sparkSession),
        buildHoodieConfig(v2Table.hoodieCatalogTable), v2Table.hoodieCatalogTable.tableSchema)

      LogicalRelation(relation, output, catalogTable, isStreaming = false)
  }
}

class HoodieSpark3Analysis(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case s @ InsertIntoStatement(r @ DataSourceV2Relation(v2Table: HoodieInternalV2Table, _, _, _, _), partitionSpec, _, _, _, _)
      if s.query.resolved && needsSchemaAdjustment(s.query, v2Table.hoodieCatalogTable.table, partitionSpec, r.schema) =>
      val projection = resolveQueryColumnsByOrdinal(s.query, r.output)
      if (projection != s.query) {
        s.copy(query = projection)
      } else {
        s
      }
  }

  /**
   * Need to adjust schema based on the query and relation schema, for example,
   * if using insert into xx select 1, 2 here need to map to column names
   */
  private def needsSchemaAdjustment(query: LogicalPlan,
                                    table: CatalogTable,
                                    partitionSpec: Map[String, Option[String]],
                                    schema: StructType): Boolean = {
    val output = query.output
    val queryOutputWithoutMetaFields = removeMetaFields(output)
    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, table)

    val partitionFields = hoodieCatalogTable.partitionFields
    val partitionSchema = hoodieCatalogTable.partitionSchema
    val staticPartitionValues = partitionSpec.filter(p => p._2.isDefined).mapValues(_.get)

    assert(staticPartitionValues.isEmpty ||
      staticPartitionValues.size == partitionSchema.size,
      s"Required partition columns is: ${partitionSchema.json}, Current static partitions " +
        s"is: ${staticPartitionValues.mkString("," + "")}")

    assert(staticPartitionValues.size + queryOutputWithoutMetaFields.size
      == hoodieCatalogTable.tableSchemaWithoutMetaFields.size,
      s"Required select columns count: ${hoodieCatalogTable.tableSchemaWithoutMetaFields.size}, " +
        s"Current select columns(including static partition column) count: " +
        s"${staticPartitionValues.size + queryOutputWithoutMetaFields.size}，columns: " +
        s"(${(queryOutputWithoutMetaFields.map(_.name) ++ staticPartitionValues.keys).mkString(",")})")

    // static partition insert.
    if (staticPartitionValues.nonEmpty) {
      // drop partition fields in origin schema to align fields.
      schema.dropWhile(p => partitionFields.contains(p.name))
    }

    val existingSchemaOutput = output.take(schema.length)
    existingSchemaOutput.map(_.name) != schema.map(_.name) ||
      existingSchemaOutput.map(_.dataType) != schema.map(_.dataType)
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
 * Rule replacing resolved Spark's commands (not working for Hudi tables out-of-the-box) with
 * corresponding Hudi implementations
 */
case class HoodieSpark3PostAnalysisRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case ShowPartitions(ResolvedTable(_, id, HoodieV1OrV2Table(_), _), specOpt, _) =>
        ShowHoodieTablePartitionsCommand(
          id.asTableIdentifier, specOpt.map(s => s.asInstanceOf[UnresolvedPartitionSpec].spec))

      // Rewrite TruncateTableCommand to TruncateHoodieTableCommand
      case TruncateTable(ResolvedTable(_, id, HoodieV1OrV2Table(_), _)) =>
        TruncateHoodieTableCommand(id.asTableIdentifier, None)

      case TruncatePartition(ResolvedTable(_, id, HoodieV1OrV2Table(_), _), partitionSpec: UnresolvedPartitionSpec) =>
        TruncateHoodieTableCommand(id.asTableIdentifier, Some(partitionSpec.spec))

      case DropPartitions(ResolvedTable(_, id, HoodieV1OrV2Table(_), _), specs, ifExists, purge) =>
        AlterHoodieTableDropPartitionCommand(
          id.asTableIdentifier,
          specs.seq.map(f => f.asInstanceOf[UnresolvedPartitionSpec]).map(s => s.spec),
          ifExists,
          purge,
          retainData = true
        )

      case _ => plan
    }
  }
}

private[sql] object HoodieV1OrV2Table extends SparkAdapterSupport {
  def unapply(table: Table): Option[CatalogTable] = table match {
    case V1Table(catalogTable) if sparkAdapter.isHoodieTable(catalogTable) => Some(catalogTable)
    case v2: HoodieInternalV2Table => v2.catalogTable
    case _ => None
  }
}
