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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.SparkAdapterSupport
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, MergeIntoTable, Project}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isMetaField
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.hudi.analysis.HoodieAnalysis.failAnalysis
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class MergeIntoHoodieTableCommand2(mergeInto: MergeIntoTable) extends HoodieLeafRunnableCommand
  with SparkAdapterSupport
  with ProvidesHoodieConfig
  with PredicateHelper {

  private var sparkSession: SparkSession = _

  private lazy val hoodieCatalogTable = sparkAdapter.resolveHoodieTable(mergeInto.targetTable) match {
    case Some(catalogTable) => HoodieCatalogTable(sparkSession, catalogTable)
    case _ =>
      failAnalysis(s"Failed to resolve MERGE INTO statement into the Hudi table. Got instead: ${mergeInto.targetTable}")
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    this.sparkSession = sparkSession
    val joinData =  Join(mergeInto.sourceTable, mergeInto.targetTable, LeftOuter, Some(mergeInto.mergeCondition), JoinHint.NONE)
    val tablemetacols = mergeInto.targetTable.output.filter(a => isMetaField(a.name))
    val datacols = joinData.output.filterNot(mergeInto.targetTable.outputSet.contains)
    val projectedData = Project(tablemetacols ++ datacols, joinData)
    Dataset.ofRows(sparkSession, projectedData).show(false)
    Seq.empty[Row]
  }
}
