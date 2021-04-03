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

package org.apache.spark.sql.hudi.execution

import org.apache.hudi.execution.HudiSQLUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.internal.StaticSQLConf

/**
 * update command, execute update operation for hudi
 */
case class UpdateFromHudiTable(target: LogicalPlan, updateExpressions: Seq[Expression], condition: Option[Expression])
  extends RunnableCommand with Logging {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val updateCondition = condition.getOrElse(Literal.TrueLiteral)

    val enableHive = "hive" == sparkSession.sessionState.conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION)
    val updateProps = HudiSQLUtils.getHoodiePropsFromRelation(target, sparkSession)
    val df = Dataset.ofRows(sparkSession, target).select(buildUpdateColumns(false): _*)
      .filter(new Column(updateCondition))
    HudiSQLUtils.update(df, HudiSQLUtils.buildDefaultParameter(updateProps, enableHive), sparkSession, enableHive = enableHive)
    sparkSession.catalog.refreshTable(updateProps.get("currentTable").get)
    Seq.empty[Row]
  }

  private def buildUpdateColumns(canTrimFields: Boolean = false): Seq[Column] = {
    updateExpressions.zip(target.output).map { case(update, original) =>
      new Column(Alias(update, original.name)())
    }
  }
}
