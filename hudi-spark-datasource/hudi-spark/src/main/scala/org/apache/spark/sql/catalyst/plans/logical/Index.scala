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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType

/**
 * The logical plan of the CREATE INDEX command.
 */
case class CreateIndex(
    table: LogicalPlan,
    indexName: String,
    indexType: String,
    ignoreIfExists: Boolean,
    columns: Seq[(Attribute, Map[String, String])],
    properties: Map[String, String],
    override val output: Seq[Attribute] = CreateIndex.getOutputAttrs) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  override lazy val resolved: Boolean = table.resolved && columns.forall(_._1.resolved)

  def withNewChildrenInternal(newChild: IndexedSeq[LogicalPlan]): CreateIndex = {
    copy(table = newChild.head)
  }
}

object CreateIndex {
  def getOutputAttrs: Seq[Attribute] = Seq.empty
}

/**
 * The logical plan of the DROP INDEX command.
 */
case class DropIndex(
    table: LogicalPlan,
    indexName: String,
    ignoreIfNotExists: Boolean,
    override val output: Seq[Attribute] = DropIndex.getOutputAttrs) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  def withNewChildrenInternal(newChild: IndexedSeq[LogicalPlan]): DropIndex = {
    copy(table = newChild.head)
  }
}

object DropIndex {
  def getOutputAttrs: Seq[Attribute] = Seq.empty
}

/**
 * The logical plan of the SHOW INDEXES command.
 */
case class ShowIndexes(
    table: LogicalPlan,
    override val output: Seq[Attribute] = ShowIndexes.getOutputAttrs) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  def withNewChildrenInternal(newChild: IndexedSeq[LogicalPlan]): ShowIndexes = {
    copy(table = newChild.head)
  }
}

object ShowIndexes {
  def getOutputAttrs: Seq[Attribute] = Seq(
    AttributeReference("index_name", StringType, nullable = false)(),
    AttributeReference("col_name", StringType, nullable = false)(),
    AttributeReference("index_type", StringType, nullable = false)(),
    AttributeReference("col_options", StringType, nullable = true)(),
    AttributeReference("options", StringType, nullable = true)()
  )
}

/**
 * The logical plan of the REFRESH INDEX command.
 */
case class RefreshIndex(
    table: LogicalPlan,
    indexName: String,
    override val output: Seq[Attribute] = RefreshIndex.getOutputAttrs) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  def withNewChildrenInternal(newChild: IndexedSeq[LogicalPlan]): RefreshIndex = {
    copy(table = newChild.head)
  }
}

object RefreshIndex {
  def getOutputAttrs: Seq[Attribute] = Seq.empty
}
