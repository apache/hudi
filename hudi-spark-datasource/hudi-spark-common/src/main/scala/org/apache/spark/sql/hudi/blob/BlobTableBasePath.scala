/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.blob

import org.apache.hudi.{HoodieBaseRelation, HoodieFileIndex}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Union}
import org.apache.spark.sql.execution.adaptive.LogicalQueryStage
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 * Finds the base path of the Hudi table that produces a blob column, so that managed blob
 * references stored relative to the table base path can be resolved by [[BatchedBlobReader]].
 *
 * Walks the (optimized) logical plan from the attribute's `exprId` down to the leaf producing it:
 *  - a [[LogicalRelation]] over a `HadoopFsRelation` whose location is a [[HoodieFileIndex]], or over a
 *    [[HoodieBaseRelation]], whose output holds the exprId yields that table's base path;
 *  - a node (e.g. `Project`, `Aggregate`) that defines the exprId as `Alias(attr: Attribute, _)` is followed through `attr`;
 *  - a `LogicalQueryStage` (adaptive execution re-plans the remaining query with each materialized stage
 *    as such a leaf) is followed into the logical plan it stands for;
 *  - a `Union` yields a base path only if every child yields the same one for the column at the same position;
 *  - any other node is followed into the single child whose output holds the exprId (none or several: unknown);
 *  - any other leaf (a local relation, a non-Hudi file source) is unknown.
 *
 * Attributes are matched by `exprId`, never by name. Runs on the driver during physical planning only.
 */
object BlobTableBasePath {

  /**
   * @param plan Logical plan producing `attr`
   * @param attr The blob column attribute
   * @return The base path of the Hudi table the column comes from, or None if unknown or ambiguous
   */
  def resolve(plan: LogicalPlan, attr: Attribute): Option[String] =
    resolveTables(plan, attr.exprId).flatMap { tables =>
      val paths = tables.flatten.map(_.getBasePath.toString).distinct
      if (tables.forall(_.isDefined) && paths.size == 1) paths.headOption else None
    }

  /** Unknown lineage and mixed versions cannot reinterpret a scheme-less path as a legacy reference. */
  def resolveLayoutVersion(plan: LogicalPlan, attr: Attribute): Option[String] =
    resolveTables(plan, attr.exprId).map { tables =>
      val versions = tables.flatten.map(t => Option(t.getTableConfig.getProps.getProperty(HoodieTableConfig.BLOB_LAYOUT_VERSION.key()))).distinct
      if (versions.exists(_.isDefined) && tables.exists(_.isEmpty)) {
        Some("mixed")
      } else if (tables.exists(_.isEmpty)) {
        Some("unknown")
      } else if (versions.size == 1) {
        versions.head
      } else if (versions.isEmpty) {
        Some("unknown")
      } else {
        Some("mixed")
      }
    }.getOrElse(Some("unknown"))

  private def resolveTables(plan: LogicalPlan, exprId: ExprId): Option[Seq[Option[HoodieTableMetaClient]]] = plan match {
    case lr: LogicalRelation =>
      if (lr.output.exists(_.exprId == exprId)) Some(Seq(hudiMetaClient(lr))) else None

    case stage: LogicalQueryStage =>
      resolveTables(stage.logicalPlan, exprId)

    case union: Union =>
      val index = union.output.indexWhere(_.exprId == exprId)
      if (index < 0) {
        None
      } else {
        Some(union.children.flatMap { child =>
          resolveTables(child, child.output(index).exprId).getOrElse(Seq(None))
        })
      }

    case node =>
      aliasedAttribute(node, exprId) match {
        case Some(source) =>
          node.children match {
            case Seq(child) => resolveTables(child, source.exprId)
            case _ => None
          }
        case None =>
          node.children.filter(_.outputSet.exists(_.exprId == exprId)) match {
            case Seq(child) => resolveTables(child, exprId)
            case _ => None
          }
      }
  }

  /** The attribute that `node` renames to `exprId` through `Alias(attr: Attribute, _)`, if any. */
  private def aliasedAttribute(node: LogicalPlan, exprId: ExprId): Option[Attribute] =
    node.expressions.collectFirst {
      case a @ Alias(source: Attribute, _) if a.exprId == exprId => source
    }

  private def hudiMetaClient(lr: LogicalRelation): Option[HoodieTableMetaClient] = lr.relation match {
    case fs: HadoopFsRelation => fs.location match {
      case fileIndex: HoodieFileIndex => Some(fileIndex.metaClient)
      case _ => None
    }
    case rel: HoodieBaseRelation => Some(rel.metaClient)
    case _ => None
  }
}
