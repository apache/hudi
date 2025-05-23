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

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.config.HoodieTTLConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.{BiConsumer, Supplier}

import scala.collection.JavaConverters._

class RunTTLProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "ttl_policy", DataTypes.StringType),
    ProcedureParameter.optional(2, "retain_days", DataTypes.IntegerType),
    ProcedureParameter.optional(3, "options", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("deleted_partitions", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def build: Procedure = new RunTTLProcedure

  /**
   * Returns the input parameters of this procedure.
   */
  override def parameters: Array[ProcedureParameter] = PARAMETERS

  /**
   * Returns the type of rows produced by this procedure.
   */
  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    var confs: Map[String, String] = Map.empty
    if (getArgValueOrDefault(args, PARAMETERS(1)).isDefined) {
      confs += HoodieTTLConfig.PARTITION_TTL_STRATEGY_TYPE.key() -> getArgValueOrDefault(args, PARAMETERS(1)).get.toString
    }
    if (getArgValueOrDefault(args, PARAMETERS(2)).isDefined) {
      confs += HoodieTTLConfig.DAYS_RETAIN.key() -> getArgValueOrDefault(args, PARAMETERS(2)).get.toString
    }
    if (getArgValueOrDefault(args, PARAMETERS(3)).isDefined) {
      confs ++= HoodieCLIUtils.extractOptions(getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String])
    }

    val basePath = getBasePath(tableName, Option.empty)

    var client: SparkRDDWriteClient[_] = null
    try {
      client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, confs,
        tableName.asInstanceOf[Option[String]])
      val ttlInstantTime = client.createNewInstantTime()
      val hoodieTTLMeta = client.managePartitionTTL(ttlInstantTime)
      if (hoodieTTLMeta == null) {
        Seq.empty
      } else {
        client.commit(ttlInstantTime, hoodieTTLMeta.getWriteStatuses, org.apache.hudi.common.util.Option.empty[util.Map[String, String]], HoodieTimeline.REPLACE_COMMIT_ACTION,
          hoodieTTLMeta.getPartitionToReplaceFileIds, org.apache.hudi.common.util.Option.empty[BiConsumer[HoodieTableMetaClient, HoodieCommitMetadata]])
        hoodieTTLMeta.getPartitionToReplaceFileIds.keySet().asScala.map { p =>
          Row(p)
        }.toSeq
      }
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }
}

object RunTTLProcedure {
  val NAME = "run_ttl"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new RunTTLProcedure
  }
}
