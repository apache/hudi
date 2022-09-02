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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.SparkAdapterSupport
import org.apache.spark.sql.catalyst.plans.logical.{HoodieLogicalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.hudi.analysis.HoodieSpark3Analysis.sparkAdapter
import org.apache.spark.sql.execution.datasources.LogicalRelation

// TODO elaborate
// TODO call out that can use Project in Spark 3.2+
case class HoodieSpark3ResolveLogicalRelations() extends Rule[LogicalPlan] {
  private val hudiLogicalRelationTag: TreeNodeTag[Boolean] = TreeNodeTag("__hudi_logical_relation")

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformDown {
      case lr @ LogicalRelation(_, _, Some(table), _)
        if sparkAdapter.isHoodieTable(table) && lr.getTagValue(hudiLogicalRelationTag).isEmpty =>
        // NOTE: Have to make a copy here, since by default Spark is caching resolved [[LogicalRelation]]s
        val logicalRelation = lr.newInstance()
        logicalRelation.setTagValue(hudiLogicalRelationTag, true)

        HoodieLogicalRelation(logicalRelation)
    }
}

// TODO elaborate
case class HoodieSpark3FoldLogicalRelations() extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformDown {
      // TODO elaborate
      case hlr @ HoodieLogicalRelation(lr: LogicalRelation) => Project(hlr.output, lr)
    }
}

object HoodieSpark3Analysis extends SparkAdapterSupport {}
