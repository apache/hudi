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

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieReplaceCommitMetadata}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowCommitExtraMetadataProcedure() extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "limit", DataTypes.IntegerType, 100),
    ProcedureParameter.optional(2, "instant_time", DataTypes.StringType),
    ProcedureParameter.optional(3, "metadata_key", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("instant_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("metadata_key", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("metadata_value", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val limit = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Int]
    val instantTime = getArgValueOrDefault(args, PARAMETERS(2))
    val metadataKey = getArgValueOrDefault(args, PARAMETERS(3))

    val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, table)
    val basePath = hoodieCatalogTable.tableLocation
    val metaClient = createMetaClient(jsc, basePath)
    val activeTimeline = metaClient.getActiveTimeline
    val timeline = activeTimeline.getCommitsTimeline.filterCompletedInstants

    val hoodieInstantOption: Option[HoodieInstant] = if (instantTime.isEmpty) {
      getCommitForLastInstant(timeline)
    } else {
      getCommitForInstant(timeline, instantTime.get.asInstanceOf[String])
    }

    if (hoodieInstantOption.isEmpty) {
      throw new HoodieException(s"Commit $instantTime not found in Commits $timeline.")
    }

    val commitMetadataOptional = getHoodieCommitMetadata(timeline, hoodieInstantOption)

    if (commitMetadataOptional.isEmpty) {
      throw new HoodieException(s"Commit $instantTime not found commitMetadata in Commits $timeline.")
    }

    val meta = commitMetadataOptional.get
    val timestamp: String = hoodieInstantOption.get.getTimestamp
    val action: String = hoodieInstantOption.get.getAction
    val metadatas: util.Map[String, String] = if (metadataKey.isEmpty) {
      meta.getExtraMetadata
    } else {
      meta.getExtraMetadata.asScala.filter(r => r._1.equals(metadataKey.get.asInstanceOf[String].trim)).asJava
    }

    val rows = new util.ArrayList[Row]
    metadatas.asScala.foreach(r => rows.add(Row(timestamp, action, r._1, r._2)))
    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new ShowCommitExtraMetadataProcedure()

  private def getCommitForLastInstant(timeline: HoodieTimeline): Option[HoodieInstant] = {
    val instantOptional = timeline.getReverseOrderedInstants
      .findFirst
    if (instantOptional.isPresent) {
      Option.apply(instantOptional.get())
    } else {
      Option.empty
    }
  }

  private def getCommitForInstant(timeline: HoodieTimeline, instantTime: String): Option[HoodieInstant] = {
    val instants: util.List[HoodieInstant] = util.Arrays.asList(
      new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, instantTime),
      new HoodieInstant(false, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime),
      new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime))

    val hoodieInstant: Option[HoodieInstant] = instants.asScala.find((i: HoodieInstant) => timeline.containsInstant(i))
    hoodieInstant
  }

  private def getHoodieCommitMetadata(timeline: HoodieTimeline, hoodieInstant: Option[HoodieInstant]): Option[HoodieCommitMetadata] = {
    if (hoodieInstant.isDefined) {
      if (hoodieInstant.get.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION) {
        Option(HoodieReplaceCommitMetadata.fromBytes(timeline.getInstantDetails(hoodieInstant.get).get,
          classOf[HoodieReplaceCommitMetadata]))
      } else {
        Option(HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(hoodieInstant.get).get,
          classOf[HoodieCommitMetadata]))
      }
    } else {
      Option.empty
    }
  }
}

object ShowCommitExtraMetadataProcedure {
  val NAME = "show_commit_extra_metadata"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowCommitExtraMetadataProcedure()
  }
}
