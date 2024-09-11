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
import org.apache.hudi.cli.ArchiveExecutorUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.util.function.Supplier

class ArchiveCommitsProcedure extends BaseProcedure
  with ProcedureBuilder
  with SparkAdapterSupport
  with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "min_commits", DataTypes.IntegerType, 20),
    ProcedureParameter.optional(3, "max_commits", DataTypes.IntegerType, 30),
    ProcedureParameter.optional(4, "retain_commits", DataTypes.IntegerType, 10),
    ProcedureParameter.optional(5, "enable_metadata", DataTypes.BooleanType, true)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.IntegerType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))

    val minCommits = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]
    val maxCommits = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Int]
    val retainCommits = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[Int]
    val enableMetadata = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[Boolean]

    val basePath = getBasePath(tableName, tablePath)

    Seq(Row(ArchiveExecutorUtils.archive(jsc,
      minCommits,
      maxCommits,
      retainCommits,
      enableMetadata,
      basePath)))
  }

  override def build = new ArchiveCommitsProcedure()
}

object ArchiveCommitsProcedure {
  val NAME = "archive_commits"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ArchiveCommitsProcedure()
  }
}
