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

import org.apache.hudi.common.model.{HoodieRecord, WriteOperationType}
import org.apache.hudi.execution.HudiSQLUtils

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.internal.StaticSQLConf

/**
 * delete command, execute delete operation for hudi
 */
case class DeleteFromHudiTable(target: LogicalPlan, condition: Option[Expression]) extends RunnableCommand with Logging {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val enableHive = "hive" == sparkSession.sessionState.conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION)
    val df = Dataset.ofRows(sparkSession, Project(target.output, Filter(condition.get, target)))
      .drop(HoodieRecord.HOODIE_META_COLUMNS.asScala: _*)
    //execute delete
    val deleteProps = HudiSQLUtils.getHoodiePropsFromRelation(target, sparkSession)
    HudiSQLUtils.update(df, HudiSQLUtils.buildDefaultParameter(deleteProps, enableHive),
      sparkSession, WriteOperationType.DELETE, false, enableHive)
    sparkSession.catalog.refreshTable(deleteProps.get("currentTable").get)
    Seq.empty[Row]
  }
}
