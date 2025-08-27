/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieArchivedTimeline, HoodieInstant, HoodieTimeline}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.table.HoodieSparkTable

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.hudi.command.procedures
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.collection.JavaConverters._

/**
 * Used to view the status of a specified file, such as whether it has been deleted, which action deleted it, etc
 */
class ShowFileStatusProcedure extends BaseProcedure
  with ProcedureBuilder
  with PredicateHelper
  with Logging {

  private val DEFAULT_VALUE = ""

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "partition", DataTypes.StringType),
    ProcedureParameter.required(2, "file", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("status", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("instant", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("timeline", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("full_path", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def build: Procedure = new ShowFileStatusProcedure

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val partition = getArgValueOrDefault(args, PARAMETERS(1))
    val fileName = getArgValueOrDefault(args, PARAMETERS(2))

    val basePath: String = getBasePath(tableName, Option.empty)
    val metaClient = createMetaClient(jsc, basePath)
    val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, tableName.asInstanceOf[Option[String]])
    val table: HoodieSparkTable[String] = HoodieSparkTable.create(client.getConfig, client.getEngineContext)

    if (partition.isEmpty && table.isPartitioned) {
      throw new HoodieException(s"table $tableName is a partitioned table. Please specify the partition name where the current file is located.")
    }

    // step1 lookup clean/rollback/restore metadata in active & archive timeline
    val fileStatus: Option[FileStatusInfo] = isDeleted(metaClient, partition.asInstanceOf[Option[String]], fileName.get.asInstanceOf[String])
    if (fileStatus.isDefined) {
      val res: FileStatusInfo = fileStatus.get
      Seq(Row(res.status, res.action, res.instant, res.timeline, res.fullPath))
    } else {
      // step2 lookup write commit metadata
      val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())
      var path: Path = null
      if (partition.nonEmpty) {
        path = new Path(basePath, partition.get.asInstanceOf[String])
      } else {
        path = new Path(basePath)
      }
      fs.listStatus(path).toList
        .find(f => f.getPath.getName.equals(fileName.get))
        .map(f => Seq(Row(FileStatus.EXIST.toString, DEFAULT_VALUE, DEFAULT_VALUE, TimelineType.ACTIVE.toString, f.getPath.toUri.getPath)))
        .getOrElse(Seq(Row(FileStatus.UNKNOWN.toString, DEFAULT_VALUE, DEFAULT_VALUE, DEFAULT_VALUE, DEFAULT_VALUE)))
    }
  }

  private def isDeleted(metaClient: HoodieTableMetaClient, partition: Option[String], fileName: String): Option[FileStatusInfo] = {
    // step1 check clean
    checkCleanMetadata(metaClient, partition, fileName)
      .orElse(
        // step2 check rollback
        checkRollbackMetadata(metaClient, partition, fileName)
          .orElse(
            // step3 check restore
            checkRestoreMetadata(metaClient, partition, fileName)
          )
      )
  }

  private def checkRestoreMetadata(metaClient: HoodieTableMetaClient, partition: Option[String], fileName: String): Option[FileStatusInfo] = {
    val restoreInstant = metaClient.reloadActiveTimeline
      .getRestoreTimeline
      .filterCompletedInstants
      .getReverseOrderedInstants
      .collect(java.util.stream.Collectors.toList[HoodieInstant])
      .asScala

    restoreInstant.find { instant =>
      val hoodieRestoreMetadata =
        metaClient.getActiveTimeline.readRestoreMetadata(instant)
      val restoreMetadata = hoodieRestoreMetadata.getHoodieRestoreMetadata.values().asScala

      restoreMetadata.exists { metadata =>
        metadata.asScala.exists { rollbackMetadata =>
          val partitionRollbackMetadata = rollbackMetadata.getPartitionMetadata
          partition.flatMap(
            p => Option.apply(partitionRollbackMetadata.get(p)).flatMap(
              _.getSuccessDeleteFiles.asScala.find(_.contains(fileName)))).isDefined ||
            partitionRollbackMetadata.values.iterator.asScala.exists(_.getSuccessDeleteFiles.asScala.exists(_.contains(fileName)))
        }
      }
    }.map(restoreInstant =>
      FileStatusInfo(
        FileStatus.DELETED.toString,
        HoodieTimeline.RESTORE_ACTION,
        restoreInstant.requestedTime,
        TimelineType.ACTIVE.toString,
        DEFAULT_VALUE
      )
    )
  }

  private def checkRollbackMetadata(metaClient: HoodieTableMetaClient, partition: Option[String], fileName: String): Option[FileStatusInfo] = {
    checkRollbackMetadataInternal(metaClient.getActiveTimeline, partition, fileName)
      .orElse(
        checkRollbackMetadataInternal(metaClient.getArchivedTimeline, partition, fileName)
      )
  }

  private def checkRollbackMetadataInternal(timeline: HoodieTimeline,
                                            partition: Option[String], fileName: String): Option[FileStatusInfo] = {
    val rollbackInstant = timeline.getRollbackTimeline
      .filterCompletedInstants()
      .getReverseOrderedInstants
      .collect(java.util.stream.Collectors.toList[HoodieInstant])
      .asScala

    reloadTimelineIfNecessary(timeline)

    rollbackInstant.find { instant =>
      val rollbackMetadata = timeline.readRollbackMetadata(instant)
      val partitionRollbackMetadata = rollbackMetadata.getPartitionMetadata
      partition.flatMap(
        p => Option.apply(partitionRollbackMetadata.get(p)).flatMap(
          _.getSuccessDeleteFiles.asScala.find(_.contains(fileName)))).isDefined ||
        partitionRollbackMetadata.values.iterator.asScala.exists(_.getSuccessDeleteFiles.asScala.exists(_.contains(fileName)))
    }.map(instant => getResult(timeline, HoodieTimeline.ROLLBACK_ACTION, instant.requestedTime).get)
  }


  private def checkCleanMetadata(metaClient: HoodieTableMetaClient, partition: Option[String], fileName: String): Option[FileStatusInfo] = {
    checkCleanMetadataInternal(metaClient.getActiveTimeline, partition, fileName)
      .orElse(checkCleanMetadataInternal(metaClient.getArchivedTimeline, partition, fileName))
  }

  private def checkCleanMetadataInternal(timeline: HoodieTimeline, partition: Option[String], fileName: String): Option[FileStatusInfo] = {
    val cleanedInstant = timeline.getCleanerTimeline
      .filterCompletedInstants()
      .getReverseOrderedInstants
      .collect(java.util.stream.Collectors.toList[HoodieInstant])
      .asScala
    reloadTimelineIfNecessary(timeline)
    cleanedInstant.find { instant =>
      val cleanMetadata = timeline.readCleanMetadata(instant)
      val partitionCleanMetadata = cleanMetadata.getPartitionMetadata
      partition.flatMap(p => Option.apply(partitionCleanMetadata.get(p)).flatMap(_.getSuccessDeleteFiles.asScala.find(_.contains(fileName)))).isDefined ||
        partitionCleanMetadata.values.iterator.asScala.exists(_.getSuccessDeleteFiles.asScala.exists(_.contains(fileName)))
    }.map(instant => getResult(timeline, HoodieTimeline.CLEAN_ACTION, instant.requestedTime).get)
  }

  private def getResult(timeline: HoodieTimeline, action: String, timestamp: String): Option[FileStatusInfo] = {
    timeline match {
      case _: HoodieActiveTimeline =>
        Option.apply(FileStatusInfo(FileStatus.DELETED.toString, action, timestamp, TimelineType.ACTIVE.toString, DEFAULT_VALUE))
      case _: HoodieArchivedTimeline =>
        Option.apply(FileStatusInfo(FileStatus.DELETED.toString, action, timestamp, TimelineType.ARCHIVED.toString, DEFAULT_VALUE))
      case _ => throw new HoodieException("Unsupported timeline type: " + timeline.getClass);
    }
  }

  private def reloadTimelineIfNecessary(timeline: HoodieTimeline): Unit = {
    timeline match {
      case _: HoodieArchivedTimeline =>
        val archivalTimeline: HoodieArchivedTimeline = timeline.asInstanceOf[HoodieArchivedTimeline]
        archivalTimeline.loadCompletedInstantDetailsInMemory()
      case _ =>
    }
  }
}

object ShowFileStatusProcedure {
  val NAME = "show_file_status"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowFileStatusProcedure()
  }
}

object FileStatus extends Enumeration {
  type FileStatus = Value

  val DELETED: procedures.FileStatus.Value = Value("deleted")
  val EXIST: procedures.FileStatus.Value = Value("exist")
  val UNKNOWN: procedures.FileStatus.Value = Value("unknown")
}

object TimelineType extends Enumeration {
  type TimelineType = Value

  val ACTIVE: procedures.TimelineType.Value = Value("active")
  val ARCHIVED: procedures.TimelineType.Value = Value("archived")
}

case class FileStatusInfo(status: String, action: String, instant: String, timeline: String, fullPath: String)