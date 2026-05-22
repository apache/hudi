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

class ArchiveCommitsProcedure extends BaseProcedure
  with ProcedureBuilder
  with SparkAdapterSupport
  with Logging {
  // NOTE: min_commits / max_commits / retain_commits / enable_metadata are
  // intentionally declared WITHOUT default values. Whether a caller actually
  // passed them is determined by `isArgDefined`; their effective values fall
  // back to the corresponding ConfigProperty defaults (see `call`).
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "min_commits", DataTypes.IntegerType),
    ProcedureParameter.optional(3, "max_commits", DataTypes.IntegerType),
    ProcedureParameter.optional(4, "retain_commits", DataTypes.IntegerType),
    ProcedureParameter.optional(5, "enable_metadata", DataTypes.BooleanType),
    // free-form hoodie.* config overrides; format: 'k1=v1,k2=v2'
    ProcedureParameter.optional(6, "options", DataTypes.StringType)
  )

  // Mapping of (named parameter -> hoodie.* config key) used both to merge
  // named-parameter overrides on top of `options` and to back-fill scalar
  // values fed to ArchiveExecutorUtils. Listed once to keep the named-param
  // <-> ConfigProperty wiring in a single place.
  private val NAMED_PARAM_TO_CONFIG_KEY: Seq[(ProcedureParameter, String)] = Seq(
    PARAMETERS(2) -> HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(),
    PARAMETERS(3) -> HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(),
    PARAMETERS(4) -> HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(),
    PARAMETERS(5) -> HoodieMetadataConfig.ENABLE.key()
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
    val confs = getArchiveConfigs(args)

    val minCommits = parseInt(confs,
      HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(),
      HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.defaultValue())
    val maxCommits = parseInt(confs,
      HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(),
      HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.defaultValue())
    val retainCommits = parseInt(confs,
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(),
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.defaultValue())
    val enableMetadata = parseBoolean(confs,
      HoodieMetadataConfig.ENABLE.key(),
      HoodieMetadataConfig.ENABLE.defaultValue().toString)

    val basePath = getBasePath(tableName, tablePath)
    Seq(Row(ArchiveExecutorUtils.archive(jsc,
      minCommits,
      maxCommits,
      retainCommits,
      enableMetadata,
      basePath,
      confs.asJava)))
  }

  /**
   * Build the effective hoodie.* config map by overlaying named parameters
   * (only those the caller explicitly passed) on top of the user `options`
   * string. Whether a parameter was explicitly passed is decided by
   * `isArgDefined` rather than by checking the parameter's default, so the
   * precedence semantics stay correct even if future maintainers add defaults
   * back to the named parameters.
   */
  private def getArchiveConfigs(args: ProcedureArgs): Map[String, String] = {
    val optionConfs = getArgValueOrDefault(args, PARAMETERS(6))
      .map(p => HoodieCLIUtils.extractOptions(p.toString))
      .getOrElse(Map.empty[String, String])

    NAMED_PARAM_TO_CONFIG_KEY.foldLeft(optionConfs) {
      case (confs, (parameter, configKey)) =>
        if (isArgDefined(args, parameter)) {
          confs + (configKey -> getArgValueOrDefault(args, parameter).get.toString)
        } else {
          confs
        }
    }
  }

  private def parseInt(confs: Map[String, String], key: String, default: String): Int = {
    val raw = confs.getOrElse(key, default)
    try {
      raw.toInt
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(
          s"Invalid integer value for '$key': '$raw'. Expected a base-10 integer.")
    }
  }

  private def parseBoolean(confs: Map[String, String], key: String, default: String): Boolean = {
    val raw = confs.getOrElse(key, default).trim.toLowerCase
    raw match {
      case "true" => true
      case "false" => false
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid boolean value for '$key': '$raw'. Expected 'true' or 'false'.")
    }
  }

  override def build = new ArchiveCommitsProcedure()
}

object ArchiveCommitsProcedure {
  val NAME = "archive_commits"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ArchiveCommitsProcedure()
  }
}
