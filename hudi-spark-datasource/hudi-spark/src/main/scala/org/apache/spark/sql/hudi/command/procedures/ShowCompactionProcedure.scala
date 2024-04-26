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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.CompactionUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.util.function.Supplier
import scala.collection.JavaConverters._

class ShowCompactionProcedure extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport with Logging {
  /**
   * SHOW COMPACTION  ON tableIdentifier (LIMIT limit = INTEGER_VALUE)?
   * SHOW COMPACTION  ON path = STRING (LIMIT limit = INTEGER_VALUE)?
   */
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 20)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("timestamp", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("operation_size", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val limit = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]

    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    assert(metaClient.getTableType == HoodieTableType.MERGE_ON_READ,
      s"Cannot show compaction on a Non Merge On Read table.")
    val compactionInstants = metaClient.getActiveTimeline.getInstants.iterator().asScala
      .filter(p => p.getAction == HoodieTimeline.COMPACTION_ACTION || p.getAction == HoodieTimeline.COMMIT_ACTION)
      .toSeq
      .sortBy(f => f.getTimestamp)
      .reverse
      .take(limit)

    compactionInstants.map(instant =>
      (instant, CompactionUtils.getCompactionPlan(metaClient, instant.getTimestamp))
    ).map { case (instant, plan) =>
      Row(instant.getTimestamp, plan.getOperations.size(), instant.getState.name())
    }
  }

  override def build: Procedure = new ShowCompactionProcedure()
}

object ShowCompactionProcedure {
  val NAME = "show_compaction"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowCompactionProcedure
  }
}
