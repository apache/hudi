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

import org.apache.hudi.{HoodieCLIUtils, SparkAdapterSupport}
import org.apache.hudi.cli.ArchiveExecutorUtils
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.util.function.Supplier

import scala.collection.JavaConverters._

/**
 * Stored procedure that runs timeline archival on demand.
 *
 * Precedence (highest first):
 *   1. Named parameters explicitly supplied by the caller
 *      (min_commits / max_commits / retain_commits / enable_metadata)
 *   2. Entries in the `options` string
 *   3. Built-in defaults of `HoodieWriteConfig` / sub-configs
 *
 * Implementation note:
 *   `ArchiveExecutorUtils.archive` places `withProps(conf)` at the END of the
 *   builder chain so user options can beat sub-config defaults applied by
 *   `withArchivalConfig` / `withCleanConfig` / `withMetadataConfig`. As a side
 *   effect, options would also overwrite the named parameters' values pushed
 *   through the dedicated builders. To keep named-parameter precedence we
 *   strip from `options` any keys that the caller has explicitly overridden
 *   via a named parameter (detected via `isArgDefined`).
 */
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
    ProcedureParameter.optional(5, "enable_metadata", DataTypes.BooleanType, true),
    // free-form hoodie.* config overrides; format: 'k1=v1,k2=v2'
    ProcedureParameter.optional(6, "options", DataTypes.StringType)
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

    val rawOptionConfs: Map[String, String] = getArgValueOrDefault(args, PARAMETERS(6))
      .map(p => HoodieCLIUtils.extractOptions(p.toString))
      .getOrElse(Map.empty)

    // Drop options keys that the caller has explicitly overridden via a named
    // parameter, so the named parameter's value wins. (`withProps(conf)` runs
    // after `withArchivalConfig`/`withCleanConfig`/`withMetadataConfig` in
    // ArchiveExecutorUtils, otherwise options would silently overwrite them.)
    //
    // NOTE: `isArgDefined` checks `ProcedureArgs.map`, which is populated only
    // from arguments the caller actually wrote in the CALL statement
    // (see HoodieAnalysis#buildProcedureArgs). It is *not* affected by the
    // ProcedureParameter default values. So when the user calls with options
    // only — e.g. `options => 'hoodie.keep.min.commits=2'` without naming
    // `min_commits` — `isArgDefined(min_commits)` returns false and the
    // matching option key is preserved.
    val keysOverriddenByNamedArgs: Set[String] = Set(
      PARAMETERS(2) -> HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key,
      PARAMETERS(3) -> HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key,
      PARAMETERS(4) -> HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key,
      PARAMETERS(5) -> HoodieMetadataConfig.ENABLE.key
    ).collect { case (param, key) if isArgDefined(args, param) => key }.toSet

    val confs = rawOptionConfs -- keysOverriddenByNamedArgs

    val basePath = getBasePath(tableName, tablePath)
    Seq(Row(ArchiveExecutorUtils.archive(jsc,
      minCommits,
      maxCommits,
      retainCommits,
      enableMetadata,
      basePath,
      confs.asJava)))
  }

  override def build = new ArchiveCommitsProcedure()
}

object ArchiveCommitsProcedure {
  val NAME = "archive_commits"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ArchiveCommitsProcedure()
  }
}