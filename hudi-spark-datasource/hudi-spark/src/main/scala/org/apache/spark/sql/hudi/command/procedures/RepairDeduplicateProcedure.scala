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

import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.exception.HoodieException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import java.util.function.Supplier

import org.apache.spark.sql.hudi.{DeDupeType, DedupeSparkJob}

import scala.util.{Failure, Success, Try}

class RepairDeduplicateProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.required(1, "duplicated_partition_path", DataTypes.StringType, None),
    ProcedureParameter.required(2, "repaired_output_path", DataTypes.StringType, None),
    ProcedureParameter.optional(3, "dry_run", DataTypes.BooleanType, true),
    ProcedureParameter.optional(4, "dedupe_type", DataTypes.StringType, "insert_type")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.StringType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val duplicatedPartitionPath = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val repairedOutputPath = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val dryRun = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]
    val dedupeType = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]

    if (!DeDupeType.values.contains(DeDupeType.withName(dedupeType))) {
      throw new IllegalArgumentException("Please provide valid dedupe type!")
    }
    val basePath = getBasePath(tableName)

    Try {
      val job = new DedupeSparkJob(basePath, duplicatedPartitionPath, repairedOutputPath, spark.sqlContext,
        FSUtils.getFs(basePath, jsc.hadoopConfiguration), DeDupeType.withName(dedupeType))
      job.fixDuplicates(dryRun)
    } match {
      case Success(_) =>
        if (dryRun){
          Seq(Row(s"Deduplicated files placed in: $repairedOutputPath."))
        } else {
          Seq(Row(s"Deduplicated files placed in: $duplicatedPartitionPath."))
        }
      case Failure(e) =>
        throw new HoodieException(s"Deduplication failed!", e)
    }
  }
  override def build: Procedure = new RepairDeduplicateProcedure()
}

object RepairDeduplicateProcedure {
  val NAME = "repair_deduplicate"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new RepairDeduplicateProcedure()
  }
}
