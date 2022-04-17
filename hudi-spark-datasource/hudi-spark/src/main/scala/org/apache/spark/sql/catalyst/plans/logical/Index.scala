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

import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.spark.sql.hudi.command.index.HoodieTableIndexColumn

case class CreateIndexCommand(
    indexName: String,
    table: LogicalPlan,
    indexColumns: Array[HoodieTableIndexColumn],
    allowExists: Boolean,
    indexType: IndexType,
    indexProperties: Option[Map[String, String]]) extends Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): CreateIndexCommand = {
    this
  }
}

case class DropIndexCommand(
    indexName: String,
    table: LogicalPlan,
    allowExists: Boolean) extends Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): DropIndexCommand = {
    this
  }
}

case class RefreshIndexCommand(
    indexName: String,
    table: LogicalPlan) extends Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): RefreshIndexCommand = {
    this
  }
}

case class ShowIndexCommand(
     indexName: Option[String],
     table: LogicalPlan) extends Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): ShowIndexCommand = {
    this
  }
}
