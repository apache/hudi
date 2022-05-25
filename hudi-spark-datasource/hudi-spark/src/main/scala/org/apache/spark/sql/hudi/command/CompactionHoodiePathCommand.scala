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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.CompactionOperation.{CompactionOperation, RUN, SCHEDULE}
import org.apache.spark.sql.hudi.command.procedures.{HoodieProcedureUtils, RunCompactionProcedure}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

@Deprecated
case class CompactionHoodiePathCommand(path: String,
                                       operation: CompactionOperation,
                                       instantTimestamp: Option[Long] = None)
  extends HoodieLeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val metaClient = HoodieTableMetaClient.builder().setBasePath(path)
      .setConf(sparkSession.sessionState.newHadoopConf()).build()
    assert(metaClient.getTableType == HoodieTableType.MERGE_ON_READ, s"Must compaction on a Merge On Read table.")

    val op = operation match {
      case SCHEDULE => UTF8String.fromString("schedule")
      case RUN => UTF8String.fromString("run")
      case _ => throw new UnsupportedOperationException(s"Unsupported compaction operation: $operation")
    }

    var args: Map[String, Any] = Map("op" -> op, "path" -> UTF8String.fromString(path))
    instantTimestamp.foreach(timestamp => args += "timestamp" -> timestamp)
    val procedureArgs = HoodieProcedureUtils.buildProcedureArgs(args)
    RunCompactionProcedure.builder.get().build.call(procedureArgs)
  }

  override val output: Seq[Attribute] = RunCompactionProcedure.builder.get().build.outputType.toAttributes
}
