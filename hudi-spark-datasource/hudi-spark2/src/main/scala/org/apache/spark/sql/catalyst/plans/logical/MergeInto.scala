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

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.{Expression, Unevaluable}
import org.apache.spark.sql.types.DataType

// This code is just copy from v2Commands.scala in spark 3.0

/**
  * The logical plan of the MERGE INTO command that works for v2 tables.
  */
case class MergeIntoTable(
   targetTable: LogicalPlan,
   sourceTable: LogicalPlan,
   mergeCondition: Expression,
   matchedActions: Seq[MergeAction],
   notMatchedActions: Seq[MergeAction]) extends Command  {
  override def children: Seq[LogicalPlan] = Seq(targetTable, sourceTable)
}


sealed abstract class MergeAction extends Expression with Unevaluable {
  def condition: Option[Expression]
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = throw new UnresolvedException(this, "nullable")
  override def children: Seq[Expression] = condition.toSeq
}

case class DeleteAction(condition: Option[Expression]) extends MergeAction

case class UpdateAction(
   condition: Option[Expression],
   assignments: Seq[Assignment]) extends MergeAction {
  override def children: Seq[Expression] = condition.toSeq ++ assignments
}

case class InsertAction(
   condition: Option[Expression],
   assignments: Seq[Assignment]) extends MergeAction {
  override def children: Seq[Expression] = condition.toSeq ++ assignments
}

case class Assignment(key: Expression, value: Expression) extends Expression with Unevaluable {
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = throw new UnresolvedException(this, "nullable")
  override def children: Seq[Expression] = key ::  value :: Nil
}
