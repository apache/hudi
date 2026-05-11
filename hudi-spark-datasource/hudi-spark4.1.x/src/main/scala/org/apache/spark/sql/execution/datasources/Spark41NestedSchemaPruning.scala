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

package org.apache.spark.sql.execution.datasources

import org.apache.hudi.HoodieBaseRelation

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.types.StructType

class Spark41NestedSchemaPruning extends BaseHoodieNestedSchemaPruning {

  // Prune the given output to make it consistent with `requiredSchema`.
  protected def getPrunedOutput(output: Seq[AttributeReference],
                                requiredSchema: StructType): Seq[AttributeReference] = {
    // We need to replace the expression ids of the pruned relation output attributes
    // with the expression ids of the original relation output attributes so that
    // references to the original relation's output are not broken
    val outputIdMap = output.map(att => (att.name, att.exprId)).toMap
    DataTypeUtils.toAttributes(requiredSchema)
      .map {
        case att if outputIdMap.contains(att.name) =>
          att.withExprId(outputIdMap(att.name))
        case att => att
      }
  }

  override protected def apply0(plan: LogicalPlan): LogicalPlan =
    plan transformDown {
      case op@PhysicalOperation(projects, filters,
      // NOTE: This is modified to accommodate for Hudi's custom relations, given that original
      //       [[NestedSchemaPruning]] rule is tightly coupled w/ [[HadoopFsRelation]]
      // TODO generalize to any file-based relation
      l@LogicalRelation(relation: HoodieBaseRelation, _, _, _, _))
        if relation.canPruneRelationSchema =>

        prunePhysicalColumns(l.output, projects, filters, relation.dataSchema,
          prunedDataSchema => {
            val prunedRelation =
              relation.updatePrunedDataSchema(prunedSchema = prunedDataSchema)
            buildPrunedRelation(l, prunedRelation)
          }).getOrElse(op)
    }
}
