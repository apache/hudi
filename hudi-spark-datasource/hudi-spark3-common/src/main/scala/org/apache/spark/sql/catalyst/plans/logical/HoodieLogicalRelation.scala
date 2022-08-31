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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.HoodieLogicalRelation.{resolveHudiTable, resolvesToMetaField}
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.JavaConverters._

// TODO elaborate
case class HoodieLogicalRelation(override val child: LogicalRelation) extends UnaryNode
  with MultiInstanceRelation
  with HoodieUnaryLikeSham[LogicalPlan] {

  val targetTable: CatalogTable = resolveHudiTable(child) match {
    case Some(table) => table
    case None =>
      throw new AnalysisException("Failed to resolve to Hudi table")
  }

  private val (metaOutput, dataOutput) = child.output.partition(attr => resolvesToMetaField(attr))

  override def metadataOutput: Seq[Attribute] = metaOutput

  override def output: Seq[Attribute] = dataOutput

  override def newInstance(): LogicalPlan = this.copy(child = child.newInstance())

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = this.copy(child = newChild.asInstanceOf[LogicalRelation])
}

object HoodieLogicalRelation extends SparkAdapterSupport {

  private lazy val metaFieldNames = HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet

  // TODO rebase to use resolver
  def resolvesToMetaField(attr: Attribute): Boolean =
    metaFieldNames.contains(attr.name)

  def resolveHudiTable(plan: LogicalPlan): Option[CatalogTable] =
    sparkAdapter.resolveHoodieTable(plan)
}
