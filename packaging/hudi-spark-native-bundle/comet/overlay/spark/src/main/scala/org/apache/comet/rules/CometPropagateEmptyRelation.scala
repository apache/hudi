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

package org.apache.comet.rules

import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.CometBaseAggregateExec
import org.apache.spark.sql.execution.adaptive.{AQEOptimizer, AQEPropagateEmptyRelation, LogicalQueryStage, QueryStageExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Applies Spark's AQEPropagateEmptyRelation to a grouped Comet aggregate whose input stage
 * finished with no rows.
 *
 * Spark's rule reads the row count of the stage under a final aggregate only through
 * `LogicalQueryStage(_, agg: BaseAggregateExec)`, which a Comet aggregate does not match, so the
 * aggregate and everything above it stay in the plan, including a filter whose scalar subquery
 * then runs for a result that is already known to be empty.
 *
 * Injected runtime optimizer rules run after Spark's own batches, so once this rule has put an
 * empty relation in place of such an aggregate it runs Spark's adaptive optimizer again, which
 * carries the empty relation up the plan as it does for a Spark aggregate.
 */
object CometPropagateEmptyRelation extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (sparkRuleExcluded) {
      plan
    } else {
      val rewritten = plan.transformDown {
        case stage: LogicalQueryStage if producesNoRows(stage) =>
          LocalRelation(stage.output, data = Seq.empty, isStreaming = stage.isStreaming)
      }
      if (rewritten eq plan) plan else new AQEOptimizer(conf, Seq.empty).execute(rewritten)
    }
  }

  // An aggregate without grouping keys returns one row over an empty input, so only a grouped
  // aggregate is empty when its input is.
  private def producesNoRows(stage: LogicalQueryStage): Boolean = stage.physicalPlan match {
    case agg: CometBaseAggregateExec if agg.groupingExpressions.nonEmpty =>
      agg.child match {
        case input: QueryStageExec =>
          input.isMaterialized && input.getRuntimeStatistics.rowCount.contains(BigInt(0))
        case _ => false
      }
    case _ => false
  }

  private def sparkRuleExcluded: Boolean =
    conf
      .getConf(SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES)
      .exists(_.split(",").map(_.trim).contains(AQEPropagateEmptyRelation.ruleName))
}
