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

import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.ClusteringUtils
import org.apache.hudi.{HoodieCLIUtils, SparkAdapterSupport}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.util.function.Supplier
import scala.collection.JavaConverters._

class ShowClusteringProcedure extends BaseProcedure with ProcedureBuilder with SparkAdapterSupport with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 20),
    ProcedureParameter.optional(3, "show_involved_partition", DataTypes.BooleanType, false)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("timestamp", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("input_group_size", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("involved_partitions", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val limit = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]
    val showInvolvedPartitions = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]

    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)
    val clusteringInstants = metaClient.getActiveTimeline.getInstants.iterator().asScala
      .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
      .toSeq
      .sortBy(f => f.getTimestamp)
      .reverse
      .take(limit)

    val clusteringPlans = clusteringInstants.map(instant =>
      ClusteringUtils.getClusteringPlan(metaClient, instant)
    )

    if (showInvolvedPartitions) {
      clusteringPlans.map { p =>
        Row(p.get().getLeft.getTimestamp, p.get().getRight.getInputGroups.size(),
          p.get().getLeft.getState.name(), HoodieCLIUtils.extractPartitions(p.get().getRight.getInputGroups.asScala.toSeq))
      }
    } else {
      clusteringPlans.map { p =>
        Row(p.get().getLeft.getTimestamp, p.get().getRight.getInputGroups.size(),
          p.get().getLeft.getState.name(), "*")
      }
    }
  }

  override def build: Procedure = new ShowClusteringProcedure()
}

object ShowClusteringProcedure {
  val NAME = "show_clustering"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowClusteringProcedure
  }
}
