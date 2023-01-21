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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieDefaultTimeline, HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.util
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.{Function, Supplier}
import java.util.stream.Collectors
import scala.collection.JavaConversions
import scala.collection.JavaConverters.{asJavaIterableConverter, asJavaIteratorConverter, asScalaIteratorConverter}

class ShowFileSystemViewProcedure(showLatest: Boolean) extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS_ALL: Array[ProcedureParameter] = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "max_instant", DataTypes.StringType, ""),
    ProcedureParameter.optional(2, "include_max", DataTypes.BooleanType, false),
    ProcedureParameter.optional(3, "include_in_flight", DataTypes.BooleanType, false),
    ProcedureParameter.optional(4, "exclude_compaction", DataTypes.BooleanType, false),
    ProcedureParameter.optional(5, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.optional(6, "path_regex", DataTypes.StringType, "*/*/*")
  )

  private val OUTPUT_TYPE_ALL: StructType = StructType(Array[StructField](
    StructField("partition", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_id", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("base_instant", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("data_file", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("data_file_size", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("num_delta_files", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_delta_file_size", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("delta_files", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  private val PARAMETERS_LATEST: Array[ProcedureParameter] = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "max_instant", DataTypes.StringType, ""),
    ProcedureParameter.optional(2, "include_max", DataTypes.BooleanType, false),
    ProcedureParameter.optional(3, "include_inflight", DataTypes.BooleanType, false),
    ProcedureParameter.optional(4, "exclude_compaction", DataTypes.BooleanType, false),
    ProcedureParameter.optional(5, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.required(6, "partition_path", DataTypes.StringType, None),
    ProcedureParameter.optional(7, "merge", DataTypes.BooleanType, true)

  )

  private val OUTPUT_TYPE_LATEST: StructType = StructType(Array[StructField](
    StructField("partition", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_id", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("base_instant", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("data_file", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("data_file_size", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("num_delta_files", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_delta_file_size", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("delta_size_compaction_scheduled", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("delta_size_compaction_unscheduled", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("delta_to_base_radio_compaction_scheduled", DataTypes.DoubleType, nullable = true, Metadata.empty),
    StructField("delta_to_base_radio_compaction_unscheduled", DataTypes.DoubleType, nullable = true, Metadata.empty),
    StructField("delta_files_compaction_scheduled", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("delta_files_compaction_unscheduled", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  private def buildFileSystemView(table: Option[Any],
                                  globRegex: String,
                                  maxInstant: String,
                                  includeMaxInstant: Boolean,
                                  includeInflight: Boolean,
                                  excludeCompaction: Boolean
                                 ): HoodieTableFileSystemView = {
    val basePath = getBasePath(table)
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    val fs = metaClient.getFs
    val globPath = String.format("%s/%s/*", basePath, globRegex)
    val statuses = FSUtils.getGlobStatusExcludingMetaFolder(fs, new Path(globPath))
    var timeline: HoodieTimeline = if (excludeCompaction) {
      metaClient.getActiveTimeline.getCommitsTimeline
    } else {
      metaClient.getActiveTimeline.getWriteTimeline
    }
    if (!includeInflight) {
      timeline = timeline.filterCompletedInstants()
    }
    var instants = timeline.getInstants.iterator().asScala
    if (maxInstant.nonEmpty) {
      val predicate = if (includeMaxInstant) {
        HoodieTimeline.GREATER_THAN_OR_EQUALS
      } else {
        HoodieTimeline.GREATER_THAN
      }
      instants = instants.filter(instant => predicate.test(maxInstant, instant.getTimestamp))
    }

    val details = new Function[HoodieInstant, org.apache.hudi.common.util.Option[Array[Byte]]]
      with java.io.Serializable {
      override def apply(instant: HoodieInstant): util.Option[Array[Byte]] = {
        metaClient.getActiveTimeline.getInstantDetails(instant)
      }
    }

    val filteredTimeline = new HoodieDefaultTimeline(
      new java.util.ArrayList[HoodieInstant](JavaConversions.asJavaCollection(instants.toList)).stream(), details)
    new HoodieTableFileSystemView(metaClient, filteredTimeline, statuses.toArray(new Array[FileStatus](0)))
  }

  private def showAllFileSlices(fsView: HoodieTableFileSystemView): java.util.List[Row] = {
    val rows: java.util.List[Row] = new java.util.ArrayList[Row]
    fsView.getAllFileGroups.iterator().asScala.foreach(fg => {
      fg.getAllFileSlices.iterator().asScala.foreach(fs => {
        val fileId = fg.getFileGroupId.getFileId
        var baseFilePath = ""
        var baseFileSize = 0L
        if (fs.getBaseFile.isPresent) {
          baseFilePath = fs.getBaseFile.get.getPath
          baseFileSize = fs.getBaseFile.get.getFileSize
        }
        val numLogFiles = fs.getLogFiles.count()
        val sumLogFileSize = fs.getLogFiles.iterator().asScala.map(_.getFileSize).sum
        val logFiles = fs.getLogFiles.collect(Collectors.toList[HoodieLogFile]).toString

        rows.add(Row(fg.getPartitionPath, fileId, fs.getBaseInstantTime, baseFilePath, baseFileSize, numLogFiles,
          sumLogFileSize, logFiles))
      })
    })
    rows
  }

  private def showLatestFileSlices(fsView: HoodieTableFileSystemView,
                                   table: Option[Any],
                                   partition: String,
                                   maxInstant: String,
                                   merge: Boolean): java.util.List[Row] = {
    var fileSliceStream: java.util.stream.Stream[FileSlice] = null
    if (!merge) {
      fileSliceStream = fsView.getLatestFileSlices(partition)
    } else {
      fileSliceStream = fsView.getLatestMergedFileSlicesBeforeOrOn(partition, if (maxInstant.isEmpty) {
        val basePath = getBasePath(table)
        val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
        metaClient.getActiveTimeline.filterCompletedAndCompactionInstants().lastInstant().get().getTimestamp
      } else {
        maxInstant
      })
    }
    val rows: java.util.List[Row] = new java.util.ArrayList[Row]
    fileSliceStream.iterator().asScala.foreach {
      fs => {
        val fileId = fs.getFileId
        val baseInstantTime = fs.getBaseInstantTime
        var baseFilePath = ""
        var baseFileSize = 0L
        if (fs.getBaseFile.isPresent) {
          baseFilePath = fs.getBaseFile.get.getPath
          baseFileSize = fs.getBaseFile.get.getFileSize
        }
        val numLogFiles = fs.getLogFiles.count()
        val sumLogFileSize = fs.getLogFiles.iterator().asScala.map(_.getFileSize).sum
        val logFilesScheduledForCompactionTotalSize = fs.getLogFiles.iterator().asScala
          .filter(logFile => logFile.getBaseCommitTime.equals(fs.getBaseInstantTime))
          .map(_.getFileSize).sum
        val logFilesUnscheduledTotalSize = fs.getLogFiles.iterator().asScala
          .filter(logFile => !logFile.getBaseCommitTime.equals(fs.getBaseInstantTime))
          .map(_.getFileSize).sum
        val logSelectedForCompactionToBaseRatio = if (baseFileSize > 0) {
          logFilesScheduledForCompactionTotalSize / (baseFileSize * 1.0)
        } else {
          -1
        }
        val logUnscheduledToBaseRatio = if (baseFileSize > 0) {
          logFilesUnscheduledTotalSize / (baseFileSize * 1.0)
        } else {
          -1
        }
        val logFilesCommitTimeEqualInstantTime = fs.getLogFiles.iterator().asScala
          .filter(logFile => logFile.getBaseCommitTime.equals(fs.getBaseInstantTime))
          .mkString("[", ",", "]")
        val logFilesCommitTimeNonEqualInstantTime = fs.getLogFiles.iterator().asScala
          .filter(logFile => !logFile.getBaseCommitTime.equals(fs.getBaseInstantTime))
          .mkString("[", ",", "]")
        rows.add(Row(partition, fileId, baseInstantTime, baseFilePath, baseFileSize, numLogFiles, sumLogFileSize,
          logFilesScheduledForCompactionTotalSize, logFilesUnscheduledTotalSize, logSelectedForCompactionToBaseRatio,
          logUnscheduledToBaseRatio, logFilesCommitTimeEqualInstantTime, logFilesCommitTimeNonEqualInstantTime
        ))
      }
    }
    rows
  }

  override def parameters: Array[ProcedureParameter] = if (showLatest) {
    PARAMETERS_LATEST
  } else {
    PARAMETERS_ALL
  }

  override def outputType: StructType = if (showLatest) {
    OUTPUT_TYPE_LATEST
  } else {
    OUTPUT_TYPE_ALL
  }

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(parameters, args)
    val table = getArgValueOrDefault(args, parameters(0))
    val maxInstant = getArgValueOrDefault(args, parameters(1)).get.asInstanceOf[String]
    val includeMax = getArgValueOrDefault(args, parameters(2)).get.asInstanceOf[Boolean]
    val includeInflight = getArgValueOrDefault(args, parameters(3)).get.asInstanceOf[Boolean]
    val excludeCompaction = getArgValueOrDefault(args, parameters(4)).get.asInstanceOf[Boolean]
    val limit = getArgValueOrDefault(args, parameters(5)).get.asInstanceOf[Int]
    val rows: java.util.List[Row] = if (!showLatest) {
      val globRegex = getArgValueOrDefault(args, parameters(6)).get.asInstanceOf[String]
      val fsView = buildFileSystemView(table, globRegex, maxInstant, includeMax, includeInflight, excludeCompaction)
      showAllFileSlices(fsView)
    } else {
      val partitionPath = getArgValueOrDefault(args, parameters(6)).get.asInstanceOf[String]
      val merge = getArgValueOrDefault(args, parameters(7)).get.asInstanceOf[Boolean]
      val fsView = buildFileSystemView(table, partitionPath, maxInstant, includeMax, includeInflight, excludeCompaction)
      showLatestFileSlices(fsView, table, partitionPath, maxInstant, merge)
    }
    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new ShowFileSystemViewProcedure(showLatest)
}

object ShowAllFileSystemViewProcedure {
  val NAME = "show_fsview_all"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowFileSystemViewProcedure(false)
  }
}

object ShowLatestFileSystemViewProcedure {
  val NAME = "show_fsview_latest"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new ShowFileSystemViewProcedure(true)
  }
}
