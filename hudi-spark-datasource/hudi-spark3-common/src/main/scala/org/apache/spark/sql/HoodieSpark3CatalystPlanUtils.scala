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

package org.apache.spark.sql

import org.apache.hudi.spark3.internal.ReflectUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, TableOutputResolver, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Like}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogV2Util, LookupCatalog, V1Table, V2TableWithV1Fallback}
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.execution.{ExtendedMode, SimpleMode}
import org.apache.spark.sql.internal.SQLConf

abstract class HoodieSpark3CatalystPlanUtils extends HoodieCatalystPlansUtils
  with LookupCatalog with Logging {

  override protected val catalogManager: CatalogManager = {
    val catalog = spark.sessionState.catalog
    val v2SessionCatalog = new V2SessionCatalog(catalog)
    new CatalogManager(v2SessionCatalog, catalog)
  }

  def resolveOutputColumns(
      tableName: String,
      expected: Seq[Attribute],
      query: LogicalPlan,
      byName: Boolean,
      conf: SQLConf): LogicalPlan =
    TableOutputResolver.resolveOutputColumns(tableName, expected, query, byName, conf)

  def createExplainCommand(plan: LogicalPlan, extended: Boolean): LogicalPlan =
    ExplainCommand(plan, mode = if (extended) ExtendedMode else SimpleMode)

  override def toTableIdentifier(aliasId: AliasIdentifier): TableIdentifier = {
    aliasId match {
      case AliasIdentifier(name, Seq(database)) =>
        TableIdentifier(name, Some(database))
      case AliasIdentifier(name, Seq(_, database)) =>
        TableIdentifier(name, Some(database))
      case AliasIdentifier(name, Seq()) =>
        TableIdentifier(name, None)
      case _ => throw new IllegalArgumentException(s"Cannot cast $aliasId to TableIdentifier")
    }
  }

  override def resolve(relation: UnresolvedRelation): Option[CatalogTable] = {
    val nameParts = relation.multipartIdentifier
    nameParts match {
      case CatalogAndIdentifier(catalog, ident) =>
        CatalogV2Util.loadTable(catalog, ident) match {
          case Some(table) =>
            table match {
              case v1Table: V1Table =>
                Some(v1Table.v1Table)
              case withFallback: V2TableWithV1Fallback =>
                Some(withFallback.v1Table)
              case _ =>
                logWarning("It's not a hoodie table: " + table.getClass.getName)
                None
            }
        }
      case _ =>
        import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
        Some(spark.sessionState.catalog.getTableMetadata(nameParts.asTableIdentifier))
    }
  }

  protected def expandIdentifier(nameParts: Seq[String]): Seq[String] = {
    // scalastyle:off return
    if (!isResolvingView || isReferredTempViewName(nameParts)) return nameParts
    // scalastyle:on return

    if (nameParts.length == 1) {
      AnalysisContext.get.catalogAndNamespace :+ nameParts.head
    } else if (catalogManager.isCatalogRegistered(nameParts.head)) {
      nameParts
    } else {
      AnalysisContext.get.catalogAndNamespace.head +: nameParts
    }
  }

  private def isResolvingView: Boolean = AnalysisContext.get.catalogAndNamespace.nonEmpty

  private def isReferredTempViewName(nameParts: Seq[String]): Boolean = {
    val resolver = spark.sessionState.conf.resolver
    AnalysisContext.get.referredTempViewNames.exists { n =>
      (n.length == nameParts.length) && n.zip(nameParts).forall {
        case (a, b) => resolver(a, b)
      }
    }
  }

  override def createJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType): Join = {
    Join(left, right, joinType, None, JoinHint.NONE)
  }

  override def isInsertInto(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[InsertIntoStatement]
  }

  override def getInsertIntoChildren(plan: LogicalPlan):
  Option[(LogicalPlan, Map[String, Option[String]], LogicalPlan, Boolean, Boolean)] = {
    plan match {
      case insert: InsertIntoStatement =>
        Some((insert.table, insert.partitionSpec, insert.query, insert.overwrite, insert.ifPartitionNotExists))
      case _ =>
        None
    }
  }

  override def createInsertInto(
      table: LogicalPlan,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean): LogicalPlan = {
    ReflectUtil.createInsertInto(table, partition, Seq.empty[String], query, overwrite, ifPartitionNotExists)
  }

  override def createLike(left: Expression, right: Expression): Expression = {
    new Like(left, right)
  }
}

