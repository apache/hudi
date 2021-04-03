/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.analysis

import org.apache.hudi.execution.HudiSQLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.merge._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
  * inject plan here to convert Spark MergeIntoTable to HudiMergeIntoTable and analyze some new unresolveAttribute
  */
class ProcessHudiMerge(session: SparkSession, conf: SQLConf) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperators {
      case m @ MergeIntoTable(target, source, condition, matched, notMatched)
        if (m.childrenResolved && HudiSQLUtils.isHudiRelation(target)) =>
        val matchedActions = matched.map {
          case update: UpdateAction =>
            val isStarAction = {
              val last = update.assignments.lastOption
              if (last.isDefined && last.get.value == Literal.TrueLiteral) true else false
            }
            val actions = HudiMergeIntoUtils.convertToActions(update.assignments.dropRight(if (isStarAction) 1 else 0))
            HudiMergeUpdateClause(update.condition, actions, isStarAction)
          case delete: DeleteAction =>
            HudiMergeDeleteClause(delete.condition)
        }

        val noMatchedActions = notMatched.map {
          case insert: InsertAction =>
            val isStarAction = {
              val last = insert.assignments.lastOption
              if (last.isDefined && last.get.value == Literal.TrueLiteral) true else false
            }
            val actions = HudiMergeIntoUtils.convertToActions(insert.assignments.dropRight(if (isStarAction) 1 else 0))
            HudiMergeInsertClause(insert.condition, actions, isStarAction)
        }

        val hudiMergeIntoTable = new HudiMergeIntoTable(target, source, condition, matchedActions, noMatchedActions)
        // try resolve
        HudiMergeIntoUtils.resolveReferences(hudiMergeIntoTable, conf)(HudiMergeIntoUtils.tryResolveReferences(session) _)
    }
  }
}
