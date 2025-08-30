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

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.HoodieTableMetadata

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowMetadataTableHistoryProcedure extends BaseProcedure with ProcedureBuilder {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType, ""),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 20),
    ProcedureParameter.optional(3, "showArchived", DataTypes.BooleanType, false),
    ProcedureParameter.optional(4, "filter", DataTypes.StringType, ""),
    ProcedureParameter.optional(5, "startTime", DataTypes.StringType, ""),
    ProcedureParameter.optional(6, "endTime", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("instant_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("metadata_table_action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("metadata_table_state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("metadata_table_requested_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("metadata_table_inflight_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("metadata_table_completed_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("data_table_action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("data_table_state", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("data_table_requested_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("data_table_inflight_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("data_table_completed_time", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val path = getArgValueOrDefault(args, PARAMETERS(1))
    val limit = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]
    val showArchived = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]
    val filter = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]
    val startTime = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]
    val endTime = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[String]

    validateFilter(filter, outputType)

    val dataTableBasePath = getBasePath(tableName, path)
    val metaClient = createMetaClient(jsc, dataTableBasePath)

    val metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(dataTableBasePath)
    val metadataMetaClient = try {
      HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
        .setBasePath(metadataTableBasePath)
        .setLoadActiveTimelineOnLoad(false)
        .build()
    } catch {
      case _: Exception =>
        return Seq.empty[Row]
    }
    getTimelineEntries(metaClient, metadataMetaClient, limit, showArchived, filter, startTime, endTime)
  }

  override def build: Procedure = new ShowMetadataTableHistoryProcedure()

  private def getTimelineEntries(
                                  dataMetaClient: HoodieTableMetaClient,
                                  metadataMetaClient: HoodieTableMetaClient,
                                  limit: Int,
                                  showArchived: Boolean,
                                  filter: String,
                                  startTime: String,
                                  endTime: String): Seq[Row] = {

    val dataInstantInfoMap = buildInstantInfoFromTimeline(dataMetaClient)
    val metadataInstantInfoMap = buildInstantInfoFromTimeline(metadataMetaClient)

    val allInstantTimes = collection.mutable.Set[String]()

    val dataActiveTimeline = dataMetaClient.getActiveTimeline
    val dataArchivedTimeline = if (showArchived) Some(dataMetaClient.getArchivedTimeline) else None

    val metadataActiveTimeline = metadataMetaClient.getActiveTimeline
    val metadataArchivedTimeline = if (showArchived) Some(metadataMetaClient.getArchivedTimeline) else None

    allInstantTimes ++= dataActiveTimeline.getInstants.iterator().asScala.map(_.requestedTime())
    if (dataArchivedTimeline.isDefined) {
      allInstantTimes ++= dataArchivedTimeline.get.getInstants.iterator().asScala.map(_.requestedTime())
    }

    allInstantTimes ++= metadataActiveTimeline.getInstants.iterator().asScala.map(_.requestedTime())
    if (metadataArchivedTimeline.isDefined) {
      allInstantTimes ++= metadataArchivedTimeline.get.getInstants.iterator().asScala.map(_.requestedTime())
    }

    val filteredInstantTimes = if (startTime.nonEmpty && endTime.nonEmpty) {
      allInstantTimes.filter { instantTime =>
        val timeMatches = (instantTime >= startTime && instantTime <= endTime)
        timeMatches
      }
    } else {
      allInstantTimes
    }

    val sortedInstantTimes = filteredInstantTimes.toSeq.sorted(Ordering[String].reverse)

    val entries = sortedInstantTimes.map { instantTime =>
      createTimelineEntry(
        instantTime,
        dataActiveTimeline,
        dataArchivedTimeline,
        metadataActiveTimeline,
        metadataArchivedTimeline,
        dataInstantInfoMap,
        metadataInstantInfoMap
      )
    }

    val filteredEntries = applyFilter(entries, filter, outputType)

    val finalEntries = if (startTime.nonEmpty && endTime.nonEmpty) {
      filteredEntries
    } else {
      filteredEntries.take(limit)
    }

    finalEntries
  }

  private def createTimelineEntry(
                                   instantTime: String,
                                   dataActiveTimeline: HoodieTimeline,
                                   dataArchivedTimeline: Option[HoodieTimeline],
                                   metadataActiveTimeline: HoodieTimeline,
                                   metadataArchivedTimeline: Option[HoodieTimeline],
                                   dataInstantInfoMap: Map[String, Map[HoodieInstant.State, HoodieInstantWithModTime]],
                                   metadataInstantInfoMap: Map[String, Map[HoodieInstant.State, HoodieInstantWithModTime]]): Row = {

    val dataInstant = findInstant(dataActiveTimeline, dataArchivedTimeline, instantTime)
    val metadataInstant = findInstant(metadataActiveTimeline, metadataArchivedTimeline, instantTime)

    val action = dataInstant.map(_.getAction).orNull
    val state = dataInstant.map(_.getState.toString).orNull
    val requestedTime = getFormattedDateForState(instantTime, HoodieInstant.State.REQUESTED, dataInstantInfoMap)
    val inflightTime = getFormattedDateForState(instantTime, HoodieInstant.State.INFLIGHT, dataInstantInfoMap)
    val completedTime = getFormattedDateForState(instantTime, HoodieInstant.State.COMPLETED, dataInstantInfoMap)

    val mtAction = metadataInstant.map(_.getAction).orNull
    val mtState = metadataInstant.map(_.getState.toString).orNull
    val mtRequestedTime = getFormattedDateForState(instantTime, HoodieInstant.State.REQUESTED, metadataInstantInfoMap)
    val mtInflightTime = getFormattedDateForState(instantTime, HoodieInstant.State.INFLIGHT, metadataInstantInfoMap)
    val mtCompletedTime = getFormattedDateForState(instantTime, HoodieInstant.State.COMPLETED, metadataInstantInfoMap)

    Row(
      instantTime,
      mtAction,
      mtState,
      mtRequestedTime,
      mtInflightTime,
      mtCompletedTime,
      action,
      state,
      requestedTime,
      inflightTime,
      completedTime
    )
  }

  private def findInstant(
                           activeTimeline: HoodieTimeline,
                           archivedTimeline: Option[HoodieTimeline],
                           instantTime: String): Option[HoodieInstant] = {

    val activeInstant = activeTimeline.getInstants.iterator().asScala
      .find(_.requestedTime() == instantTime)

    if (activeInstant.isDefined) {
      return activeInstant
    }

    archivedTimeline.flatMap { archived =>
      archived.getInstants.iterator().asScala
        .find(_.requestedTime() == instantTime)
    }
  }

  private case class HoodieInstantWithModTime(state: HoodieInstant.State,
                                              action: String,
                                              requestedTime: String,
                                              completionTime: String,
                                              modificationTimeMs: Long
                                             ) {
    def getModificationTime: Long = modificationTimeMs
  }

  private def buildInstantInfoFromTimeline(metaClient: HoodieTableMetaClient): Map[String, Map[HoodieInstant.State, HoodieInstantWithModTime]] = {
    try {
      val storage = metaClient.getStorage
      val timelinePath = metaClient.getTimelinePath
      val instantFileNameParser = metaClient.getInstantFileNameParser
      val instantGenerator = metaClient.getInstantGenerator

      val instantMap = scala.collection.mutable.Map[String, scala.collection.mutable.Map[HoodieInstant.State, HoodieInstantWithModTime]]()

      val fileStream = HoodieTableMetaClient.scanFiles(storage, timelinePath, path => {
        val extension = instantFileNameParser.getTimelineFileExtension(path.getName)
        metaClient.getActiveTimeline.getValidExtensionsInActiveTimeline.contains(extension)
      })

      fileStream.forEach { storagePathInfo =>
        try {
          val instant = instantGenerator.createNewInstant(storagePathInfo)
          val instantWithModTime = HoodieInstantWithModTime(
            instant.getState,
            instant.getAction,
            instant.requestedTime(),
            instant.getCompletionTime,
            storagePathInfo.getModificationTime
          )

          instantMap.getOrElseUpdate(instant.requestedTime(), scala.collection.mutable.Map[HoodieInstant.State, HoodieInstantWithModTime]())
            .put(instant.getState, instantWithModTime)
        } catch {
          case _: Exception => // Skip invalid files
        }
      }
      instantMap.map { case (timestamp, stateMap) =>
        timestamp -> stateMap.toMap
      }.toMap
    } catch {
      case _: Exception => Map.empty[String, Map[HoodieInstant.State, HoodieInstantWithModTime]]
    }
  }

  private def getFormattedDateForState(instantTimestamp: String, state: HoodieInstant.State,
                                       instantInfoMap: Map[String, Map[HoodieInstant.State, HoodieInstantWithModTime]]): String = {
    val stateMap = instantInfoMap.get(instantTimestamp)
    if (stateMap.isDefined) {
      val stateInfo = stateMap.get.get(state)
      if (stateInfo.isDefined) {
        val modificationTime = stateInfo.get.getModificationTime
        if (modificationTime > 0) {
          val date = new java.util.Date(modificationTime)
          val formatter = new java.text.SimpleDateFormat("MM-dd HH:mm:ss")
          formatter.format(date)
        } else {
          instantTimestamp
        }
      } else {
        null
      }
    } else {
      null
    }
  }
}

object ShowMetadataTableHistoryProcedure {
  val NAME = "show_metadata_table_history"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new ShowMetadataTableHistoryProcedure()
  }
}
