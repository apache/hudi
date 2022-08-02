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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.BuildUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter}

class ShowBuildProcedure extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "path", DataTypes.StringType, None),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 20),
    ProcedureParameter.optional(3, "show_involved_partition", DataTypes.BooleanType, false)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("timestamp", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("task_num", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("involved_partitions", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  /**
   * Returns the input parameters of this procedure.
   */
  override def parameters: Array[ProcedureParameter] = PARAMETERS

  /**
   * Returns the type of rows produced by this procedure.
   */
  override def outputType: StructType = OUTPUT_TYPE

  /**
   * Executes this procedure.
   * <p>
   * Spark will align the provided arguments according to the input parameters
   * defined in {@link # parameters ( )} either by position or by name before execution.
   * <p>
   * Implementations may provide a summary of execution by returning one or many rows
   * as a result. The schema of output rows must match the defined output type
   * in {@link # outputType ( )}.
   *
   * @param args input arguments
   * @return the result of executing this procedure with the given arguments
   */
  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val limit = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]
    val showInvolvedPartitions = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]

    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build

    val buildInstants = metaClient.getActiveTimeline.getInstants.iterator().asScala
      .filter(p => p.getAction == HoodieTimeline.BUILD_ACTION)
      .toSeq
      .sortBy(f => f.getTimestamp)
      .reverse
      .take(limit)

    val buildPlans = buildInstants.map(instant => BuildUtils.getBuildPlan(metaClient, instant))
    if (showInvolvedPartitions) {
      buildPlans.map { p =>
        Row(p.get().getLeft.getTimestamp, p.get().getRight.getTasks.size(),
          p.get().getLeft.getState.name(),
          BuildUtils.extractPartitions(p.get().getRight.getTasks).asScala.mkString(","))
      }
    } else {
      buildPlans.map { p =>
        Row(p.get().getLeft.getTimestamp, p.get().getRight.getTasks.size(),
          p.get().getLeft.getState.name(), "*")
      }
    }
  }

  override def build: Procedure = new ShowBuildProcedure
}

object ShowBuildProcedure {
  val NAME = "show_build"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowBuildProcedure
  }
}
