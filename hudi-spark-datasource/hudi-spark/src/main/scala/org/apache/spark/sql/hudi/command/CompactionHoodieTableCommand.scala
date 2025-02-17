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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.CompactionOperation.CompactionOperation
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.getTableLocation
import org.apache.spark.sql.hudi.command.procedures.RunCompactionProcedure

@Deprecated
case class CompactionHoodieTableCommand(table: CatalogTable,
                                        operation: CompactionOperation,
                                        instantTimestamp: Option[Long])
  extends HoodieLeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val basePath = getTableLocation(table, sparkSession)
    CompactionHoodiePathCommand(basePath, operation, instantTimestamp).run(sparkSession)
  }

  override val output: Seq[Attribute] =
    SparkAdapterSupport.sparkAdapter.getSchemaUtils.toAttributes(
      RunCompactionProcedure.builder.get().build.outputType)
}
