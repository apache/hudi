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
import org.apache.hudi.cli.ArchiveHistoryFileCleaner
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.util.function.Supplier

import scala.collection.JavaConverters._

/**
 * Trims archived timeline history. For layout V2 the cutoff applies to instant requested times:
 * records at the cutoff are retained. Old snapshots keep their Parquet files until archive GC,
 * so deleted_files is empty. Run with archiving stopped or with the same effective shared lock.
 * For V1 the file-based rules below apply.
 *
 * <p>Usage:
 * <pre>
 * CALL delete_archive_history_file(table => 'tbl', retain_days => 30)
 * CALL delete_archive_history_file(path => '/path/to/table', retain_days => 7,
 *                                  options => 'hoodie.write.lock.provider=...')
 * </pre>
 *
 * <p>A file is deleted only when its version is strictly lower than the highest
 * version present AND its last modification time is strictly earlier than the
 * cutoff time (requested time of the earliest commit/deltacommit in the active
 * timeline minus {@code retain_days} days).
 */
class DeleteArchiveHistoryFileProcedure extends BaseProcedure
  with ProcedureBuilder
  with SparkAdapterSupport
  with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.optional(2, "retain_days", DataTypes.IntegerType, 30),
    // free-form hoodie.* config overrides (incl. lock provider config); format: 'k1=v1,k2=v2'
    ProcedureParameter.optional(3, "options", DataTypes.StringType)
  )

  private val DELETED_FILE_TYPE = new StructType(Array[StructField](
    StructField("path", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("size_bytes", DataTypes.LongType, nullable = false, Metadata.empty),
    StructField("modification_time", DataTypes.LongType, nullable = false, Metadata.empty)
  ))

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("archive_path", DataTypes.StringType, nullable = false, Metadata.empty),
    // cutoff time in Unix millis; null when no commit/deltacommit instant exists
    StructField("cutoff_time", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("archive_size_bytes", DataTypes.LongType, nullable = false, Metadata.empty),
    StructField("deleted_files", DataTypes.createArrayType(DELETED_FILE_TYPE, false),
      nullable = false, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val retainDays = parseRetainDays(args)
    val confs = getConfigs(args)

    val basePath = getBasePath(tableName, tablePath)

    // Build a write config carrying the user-supplied hoodie.* options (lock
    // provider config etc.); `withProps` is placed last so user options win,
    // consistent with ArchiveExecutorUtils.
    val config = HoodieWriteConfig.newBuilder()
      .withPath(basePath)
      .withEmbeddedTimelineServerEnabled(false)
      .withProps(confs.asJava)
      .build()

    val metaClient = createMetaClient(jsc, basePath)
    val result = ArchiveHistoryFileCleaner.clean(metaClient, config, retainDays)

    val deletedRows = result.getDeletedFiles.asScala.map(f =>
      Row(f.getPath, f.getSizeBytes, f.getModificationTime)).toSeq

    Seq(Row(
      result.getArchivePath,
      result.getCutoffTime,
      result.getArchiveSizeBytes,
      deletedRows))
  }

  private def parseRetainDays(args: ProcedureArgs): Int = {
    val raw = getArgValueOrDefault(args, PARAMETERS(2))
      .map(_.toString.trim)
      .getOrElse("30")
    try {
      val value = raw.toInt
      if (value <= 0) {
        throw new IllegalArgumentException(
          s"Invalid value for 'retain_days': '$raw'. Expected a positive integer.")
      }
      value
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(
          s"Invalid value for 'retain_days': '$raw'. Expected a base-10 integer.")
    }
  }

  private def getConfigs(args: ProcedureArgs): Map[String, String] = {
    getArgValueOrDefault(args, PARAMETERS(3))
      .map(p => HoodieCLIUtils.extractOptions(p.toString))
      .getOrElse(Map.empty[String, String])
  }

  override def build = new DeleteArchiveHistoryFileProcedure()
}

object DeleteArchiveHistoryFileProcedure {
  val NAME = "delete_archive_history_file"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new DeleteArchiveHistoryFileProcedure()
  }
}
