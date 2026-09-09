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

import org.apache.hudi.{DefaultSource, EmptyRelation, HoodieBaseRelation}
import org.apache.hudi.SparkAdapterSupport.sparkAdapter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.catalog.HoodieInternalV2Table
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType

/**
 * NOTE: PLEASE READ CAREFULLY
 *
 * Since Hudi relations don't currently implement DS V2 Read API, we have to fallback to V1 here.
 * Such fallback will have considerable performance impact, therefore it's only performed in cases
 * where V2 API have to be used. Currently only such use-case is using of Schema Evolution feature
 *
 * Check out HUDI-4178 for more details
 */
case class HoodieSpark40DataSourceV2ToV1Fallback(sparkSession: SparkSession) extends Rule[LogicalPlan]
  with ProvidesHoodieConfig {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // The only place we're avoiding fallback is in [[AlterTableCommand]]s since
    // current implementation relies on DSv2 features
    case _: AlterTableCommand => plan

    // NOTE: Unfortunately, [[InsertIntoStatement]] is implemented in a way that doesn't expose
    //       target relation as a child (even though there's no good reason for that)
    case iis@InsertIntoStatement(rv2@DataSourceV2Relation(v2Table: HoodieInternalV2Table, _, _, _, _), _, _, _, _, _, _) =>
      iis.copy(table = convertToV1(rv2, v2Table))

    case _ =>
      plan.resolveOperatorsDown {
        case rv2@DataSourceV2Relation(v2Table: HoodieInternalV2Table, _, _, _, _) => convertToV1(rv2, v2Table)
      }
  }

  private def convertToV1(rv2: DataSourceV2Relation, v2Table: HoodieInternalV2Table) = {
    val output = rv2.output
    val catalogTable = v2Table.catalogTable.map(_ => v2Table.v1Table)
    val relation = new DefaultSource().createRelation(sparkSession.sqlContext,
      buildHoodieConfig(v2Table.hoodieCatalogTable), v2Table.hoodieCatalogTable.tableSchema)

    LogicalRelation(relation, output, catalogTable, isStreaming = false, Option.empty)
  }
}

/**
 * Resolution rule resolving the user specified columns and default values of
 * [[InsertIntoStatement]] for Spark 4.0; see [[HoodieSpark4ResolveColumnsForInsertInto]]
 * for the shared preprocessing logic and the rationale.
 */
case class HoodieSpark40ResolveColumnsForInsertInto() extends HoodieSpark4ResolveColumnsForInsertInto {
  // NOTE: This is copied from [[PreprocessTableInsertion]] with additional handling of Hudi relations
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case i@InsertIntoStatement(table, _, _, query, _, _, _)
        if table.resolved && query.resolved
          && i.userSpecifiedCols.nonEmpty && i.table.isInstanceOf[LogicalRelation]
          && sparkAdapter.isHoodieTable(i.table.asInstanceOf[LogicalRelation].catalogTable.get) =>
        table match {
          case relation: HiveTableRelation =>
            val metadata = relation.tableMeta
            preprocess(i, metadata.identifier.quotedString, metadata.partitionSchema,
              Some(metadata))
          case LogicalRelation(h: HadoopFsRelation, _, catalogTable, _, _) =>
            preprocess(i, catalogTable, h.partitionSchema)
          case LogicalRelation(_: InsertableRelation, _, catalogTable, _, _) =>
            preprocess(i, catalogTable, new StructType())
          // The two conditions below are adapted to Hudi relations
          case LogicalRelation(_: EmptyRelation, _, catalogTable, _, _) =>
            preprocess(i, catalogTable)
          case LogicalRelation(_: HoodieBaseRelation, _, catalogTable, _, _) =>
            preprocess(i, catalogTable)
          case _ => i
        }
      case _ => plan
    }
  }
}
