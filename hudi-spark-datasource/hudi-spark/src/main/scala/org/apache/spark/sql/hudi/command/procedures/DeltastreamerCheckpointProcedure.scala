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
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{HoodieFailedWritesCleaningPolicy, HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.checkpoint.{Checkpoint, CheckpointUtils, StreamerCheckpointV1, StreamerCheckpointV2}
import org.apache.hudi.common.table.timeline.{HoodieTimeline, TimelineUtils}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig, HoodieClusteringConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class GetDeltastreamerCheckpointProcedure extends BaseProcedure with ProcedureBuilder {
  import DeltastreamerCheckpointProcedureUtils._

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType)
  )

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val metaClient = createMetaClient(jsc, getBasePath(tableName, tablePath))

    val checkpoint = getLatestCheckpoint(metaClient)
    if (checkpoint.isPresent) {
      Seq(Row(checkpoint.get.getCheckpointKey))
    } else {
      Seq.empty
    }
  }

  override def build: Procedure = new GetDeltastreamerCheckpointProcedure
}

class SetDeltastreamerCheckpointProcedure extends BaseProcedure with ProcedureBuilder {
  import DeltastreamerCheckpointProcedureUtils._

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "checkpoint", DataTypes.StringType),
    ProcedureParameter.optional(2, "path", DataTypes.StringType)
  )

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val checkpointValue = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val tablePath = getArgValueOrDefault(args, PARAMETERS(2))
    val basePath = getBasePath(tableName, tablePath)
    val metaClient = createMetaClient(jsc, basePath)

    val checkpoint = getLatestCheckpoint(metaClient)
      .orElse(new StreamerCheckpointV1(checkpointValue))
    checkpoint.setCheckpointKey(checkpointValue)
    val checkpointMetadata = checkpoint.getCheckpointCommitMetadata(
      checkpoint.getCheckpointResetKey, checkpoint.getCheckpointIgnoreKey)

    val writeOptions = Map(
      // This procedure only publishes checkpoint metadata. It must not run table services as a
      // side effect or clean up pending writes belonging to another writer.
      HoodieCleanConfig.AUTO_CLEAN.key -> "false",
      HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key -> HoodieFailedWritesCleaningPolicy.NEVER.name,
      HoodieArchivalConfig.AUTO_ARCHIVE.key -> "false",
      HoodieCompactionConfig.INLINE_COMPACT.key -> "false",
      HoodieClusteringConfig.INLINE_CLUSTERING.key -> "false",
      HoodieClusteringConfig.SCHEDULE_INLINE_CLUSTERING.key -> "false",
      HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key -> "true",
      // A minimally configured writer must never remove metadata partitions that are already
      // present on disk. Available partitions are still updated based on hoodie.properties.
      HoodieMetadataConfig.AUTO_DELETE_PARTITIONS.key -> "false"
    )

    var client: SparkRDDWriteClient[AnyRef] = null
    try {
      client = HoodieCLIUtils.createHoodieWriteClient(
        sparkSession,
        basePath,
        writeOptions,
        tableName.map(_.asInstanceOf[String]))
        .asInstanceOf[SparkRDDWriteClient[AnyRef]]

      val instantTime = client.startCommit(metaClient.getCommitActionType)
      val writeStatuses = client.upsert(jsc.emptyRDD[HoodieRecord[AnyRef]], instantTime)
      val committed = client.commit(instantTime, writeStatuses, HOption.of(checkpointMetadata))
      if (!committed) {
        throw new HoodieException(s"Failed to set deltastreamer checkpoint for table at $basePath")
      }
      Seq(Row(checkpointValue))
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  override def build: Procedure = new SetDeltastreamerCheckpointProcedure
}

private object DeltastreamerCheckpointProcedureUtils {
  private val CHECKPOINT_KEYS = Array(
    StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1,
    StreamerCheckpointV1.STREAMER_CHECKPOINT_RESET_KEY_V1,
    StreamerCheckpointV2.STREAMER_CHECKPOINT_KEY_V2,
    StreamerCheckpointV2.STREAMER_CHECKPOINT_RESET_KEY_V2
  )

  val OUTPUT_TYPE: StructType = new StructType(Array[StructField](
    StructField("checkpoint", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def getLatestCheckpoint(metaClient: HoodieTableMetaClient): HOption[Checkpoint] = {
    val commitsTimeline = getIngestionTimeline(metaClient)
    TimelineUtils.getLatestInstantAndCommitMetadataWithValidCheckpointInfo(
      commitsTimeline, CHECKPOINT_KEYS: _*)
      .map(pair => CheckpointUtils.getCheckpoint(pair.getRight))
  }

  private def getIngestionTimeline(metaClient: HoodieTableMetaClient): HoodieTimeline = {
    val commitsTimeline = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
    val deltaCommitTimeline = commitsTimeline.filter(
      instant => instant.getAction == HoodieTimeline.DELTA_COMMIT_ACTION)

    // Match Streamer's resume behavior: once a MOR table has delta commits, checkpoints from
    // older COW commits are no longer considered.
    if (metaClient.getTableType == HoodieTableType.MERGE_ON_READ && !deltaCommitTimeline.empty()) {
      deltaCommitTimeline
    } else {
      commitsTimeline
    }
  }
}

object GetDeltastreamerCheckpointProcedure {
  val NAME = "get_deltastreamer_checkpoint"

  def builder: Supplier[ProcedureBuilder] = () => new GetDeltastreamerCheckpointProcedure
}

object SetDeltastreamerCheckpointProcedure {
  val NAME = "set_deltastreamer_checkpoint"

  def builder: Supplier[ProcedureBuilder] = () => new SetDeltastreamerCheckpointProcedure
}
