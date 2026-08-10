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

import org.apache.spark.sql.catalyst.plans.logical.CompactionOperation.CompactionOperation

case class CompactionTable(table: LogicalPlan, operation: CompactionOperation, instantTimestamp: Option[Long])
  extends Command {
  override def children: Seq[LogicalPlan] = Seq(table)

  def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): CompactionTable = {
    copy(table = newChildren.head)
  }
}

case class CompactionPath(path: String, operation: CompactionOperation, instantTimestamp: Option[Long])
  extends Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): CompactionPath = {
    this
  }
}

case class CompactionShowOnTable(table: LogicalPlan, limit: Int = 20)
  extends Command {
  override def children: Seq[LogicalPlan] = Seq(table)

  def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): CompactionShowOnTable = {
     copy(table = newChildren.head)
  }
}

case class CompactionShowOnPath(path: String, limit: Int = 20) extends Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): CompactionShowOnPath = {
    this
  }
}

object CompactionOperation extends Enumeration {
  type CompactionOperation = Value
  val SCHEDULE, RUN = Value
}
