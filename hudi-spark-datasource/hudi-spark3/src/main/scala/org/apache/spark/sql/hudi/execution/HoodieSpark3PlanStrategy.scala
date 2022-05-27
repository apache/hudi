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

package org.apache.spark.sql.hudi.execution

import org.apache.hudi.SparkAdapterSupport
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.plans.logical.{CreateIndex, DropIndex, LogicalPlan, ShowIndexes}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hudi.catalog.HoodieInternalV2Table
import org.apache.spark.sql.hudi.execution.datasources.{CreateIndexExec, DropIndexExec, ShowIndexesExec}
import org.apache.spark.sql.{SparkSession, Strategy}

class HoodieSpark3PlanStrategy(session: SparkSession) extends Strategy with SparkAdapterSupport {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateIndex(indexName, t@ResolvedTable(_, _, table, _), colName, indexType, output)
      if t.resolved && table.isInstanceOf[HoodieInternalV2Table] =>
      val tableId = table.asInstanceOf[HoodieInternalV2Table].getTableIdentifier
      CreateIndexExec(indexName, tableId, colName, indexType, output) :: Nil

    case ShowIndexes(t@ResolvedTable(_, _, table, _), output)
      if t.resolved && table.isInstanceOf[HoodieInternalV2Table] =>
      val tableId = table.asInstanceOf[HoodieInternalV2Table].getTableIdentifier
      ShowIndexesExec(tableId, output) :: Nil

    case DropIndex(indexName, t@ResolvedTable(_, _, table, _), output)
      if t.resolved && table.isInstanceOf[HoodieInternalV2Table] =>
      val tableId = table.asInstanceOf[HoodieInternalV2Table].getTableIdentifier
      DropIndexExec(indexName, tableId, output) :: Nil

    case _ => Nil
  }
}
