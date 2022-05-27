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

import org.apache.spark.sql.catalyst.expressions.Attribute

/**
 * The logical plan for CREATE INDEX command
 *
 * The syntax of this command is:
 * {{{
 *   CREATE INDEX indexName ON table (col) AS indexType
 * }}}
 *
 */
case class CreateIndex(
    indexName: String,
    table: LogicalPlan,
    indexColumn: String,
    indexType: String,
    override val output: Seq[Attribute] = CreateIndex.getOutputAttrs)
    extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): CreateIndex = {
    copy(table = newChildren.head)
  }
}

object CreateIndex {
  def getOutputAttrs: Seq[Attribute] = Seq.empty
}
