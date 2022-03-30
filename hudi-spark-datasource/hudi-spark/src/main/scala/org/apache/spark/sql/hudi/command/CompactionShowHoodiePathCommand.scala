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

import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableMetaClient

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.hudi.command.procedures.{HoodieProcedureUtils, ShowCompactionProcedure}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

@Deprecated
case class CompactionShowHoodiePathCommand(path: String, limit: Int)
  extends HoodieLeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val metaClient = HoodieTableMetaClient.builder().setBasePath(path)
      .setConf(sparkSession.sessionState.newHadoopConf()).build()

    assert(metaClient.getTableType == HoodieTableType.MERGE_ON_READ,
      s"Cannot show compaction on a Non Merge On Read table.")

    val args = Map("path" -> UTF8String.fromString(path), "limit" -> limit)
    val procedureArgs = HoodieProcedureUtils.buildProcedureArgs(args)
    ShowCompactionProcedure.builder.get().build.call(procedureArgs)
  }

  override val output: Seq[Attribute] = {
    Seq(
      AttributeReference("instant", StringType, nullable = false)(),
      AttributeReference("action", StringType, nullable = false)(),
      AttributeReference("size", IntegerType, nullable = false)()
    )
  }
}
