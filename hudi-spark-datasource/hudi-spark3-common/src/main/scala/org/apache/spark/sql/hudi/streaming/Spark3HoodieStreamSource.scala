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

package org.apache.spark.sql.hudi.streaming

import org.apache.hudi.DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT
import org.apache.hudi.cdc.CDCRelation
import org.apache.hudi.{DataSourceReadOptions, IncrementalRelation, MergeOnReadIncrementalRelation}
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieDeltaWriteStat, HoodieTableType}
import org.apache.hudi.common.table.cdc.HoodieCDCUtils
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{ReadLimit, SupportsAdmissionControl}
import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.sql.hudi.streaming.HoodieSourceOffset.INIT_OFFSET
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.util.control.Breaks.{break, breakable}

class Spark3HoodieStreamSource(
    val sqlContext: SQLContext,
    val metadataPath: String,
    val schemaOption: Option[StructType],
    val parameters: Map[String, String],
    val offsetRangeLimit: HoodieOffsetRangeLimit)
  extends HoodieStreamSource(sqlContext, metadataPath, schemaOption, parameters, offsetRangeLimit)
    with SupportsAdmissionControl {


  private lazy val initialStreamOffset: HoodieStreamSourceOffset = {
    val metadataLog = new HoodieStreamMetadataLog(sqlContext.sparkSession, metadataPath)
    metadataLog.get(0).getOrElse {
      val offset = offsetRangeLimit match {
        case HoodieEarliestOffsetRangeLimit =>

          HoodieStreamSourceOffset(INIT_OFFSET.commitTime, "default", 0)
        case HoodieLatestOffsetRangeLimit =>
          HoodieStreamSourceOffset(getLatestOffset.getOrElse(INIT_OFFSET).commitTime, "default", 0)
        case HoodieSpecifiedOffsetRangeLimit(instantTime) =>
          HoodieStreamSourceOffset(instantTime, "default", 0)
      }
      metadataLog.add(0, offset)
      logInfo(s"The initial offset is $offset")
      offset
    }
  }

  private val maxFilesPerTrigger: Int =
    parameters.get(DataSourceReadOptions.STREAM_MAX_FILES_PER_TRIGGER.key())
      .map(_.toInt)
      .getOrElse(DataSourceReadOptions.STREAM_MAX_FILES_PER_TRIGGER.defaultValue())
  private val maxBytesPerTrigger: Int = parameters.get(DataSourceReadOptions.STREAM_MAX_BYTES_PER_TRIGGER.key())
    .map(_.toInt)
    .getOrElse(DataSourceReadOptions.STREAM_MAX_BYTES_PER_TRIGGER.defaultValue())

  private val maxRowsPerTrigger: Int = parameters.get(DataSourceReadOptions.STREAM_MAX_ROWS_PER_TRIGGER.key())
    .map(_.toInt)
    .getOrElse(DataSourceReadOptions.STREAM_MAX_ROWS_PER_TRIGGER.defaultValue())

  override def getDefaultReadLimit: ReadLimit = {
    if (maxFilesPerTrigger != DataSourceReadOptions.STREAM_MAX_FILES_PER_TRIGGER.defaultValue()) {
      ReadLimit.maxFiles(maxFilesPerTrigger)
    } else if (maxRowsPerTrigger != DataSourceReadOptions.STREAM_MAX_BYTES_PER_TRIGGER.defaultValue()) {
      ReadLimit.maxRows(maxRowsPerTrigger)
    } else {
      ReadLimit.allAvailable
    }
  }

  override def getOffset(): Option[org.apache.spark.sql.execution.streaming.Offset] = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    fetchMaxOffsets(startOffset, limit).asInstanceOf[streaming.Offset]
  }

  private def fetchMaxOffsets(start: streaming.Offset, limit: ReadLimit): HoodieStreamSourceOffset = {
    val startOffset: HoodieStreamSourceOffset = if (start != null) {
      HoodieStreamSourceOffset(start.asInstanceOf[Offset])
    } else {
      initialStreamOffset
    }
    val latestOffset: Option[HoodieSourceOffset] = super.getLatestOffset

    val endOffset: HoodieStreamSourceOffset = tableType match {
      case HoodieTableType.COPY_ON_WRITE =>
        // Get all commits
        val commitTimeline = metaClient.getCommitTimeline.filterCompletedInstants()
        val lastInstant = commitTimeline.lastInstant().get()
        val endCommit = latestOffset.getOrElse(HoodieSourceOffset("")).commitTime
        val commitsTimelineToReturn = {
          if (hollowCommitHandling == HollowCommitHandling.USE_STATE_TRANSITION_TIME) {
            commitTimeline.findInstantsInRangeByStateTransitionTime(
              startOffset.commitTime,
              if (endCommit.nonEmpty) endCommit else lastInstant.getStateTransitionTime)
          } else {
            commitTimeline.findInstantsInRange(
              startOffset.commitTime,
              if (endCommit.nonEmpty) endCommit else lastInstant.getTimestamp)
          }
        }

        // Get all instance that in range and sort by commit time
        val commitsToReturn:List[HoodieInstant] = commitsTimelineToReturn
          .getInstantsAsStream
          .iterator()
          .toList
          .sortBy(_.getTimestamp)

        var maxFiles = 0L
        var maxBytes = 0L
        var maxRows = 0L
        var endOffsetCommitTime = startOffset.commitTime
        var endPartition = startOffset.partition
        var endPartitionCursor = startOffset.cursor
        var flag = false

        breakable {
          // commit -> partitions -> files
          // maxFilesPerTrigger、maxBytesPerTrigger
          for (commit <- commitsToReturn) {
            val metadata: HoodieCommitMetadata = HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commit)
              .get, classOf[HoodieCommitMetadata])
            val partitions = metadata.getPartitionToWriteStats.keySet().iterator().toList.sortBy(p => p)
            // Traverse all partitions in one commit
            for(partition <- partitions) {
              endPartition = partition
              val writeStats = metadata.getPartitionToWriteStats.get(partition).iterator().toList
              endPartitionCursor = 0
              // Traverse all writeStats in one partition
              for (i <- 0 to writeStats.size -1) {
                maxFiles +=1
                maxBytes += writeStats(i).getTotalWriteBytes
                maxRows += writeStats(i).getNumWrites
                // Satisfy the maxFilesPerTrigger or maxBytesPerTrigger conditions
                if (maxFiles >= maxFilesPerTrigger || maxBytes >= maxBytesPerTrigger || maxRows >= maxRowsPerTrigger) {
                  endPartitionCursor = i
                  endPartition = partition
                  endOffsetCommitTime = commit.getTimestamp
                  flag = true
                  break()
                }
              }
              if (flag) {
                break()
              }
            }
            if (flag) {
              break()
            }
          }
        }
        HoodieStreamSourceOffset(commitTime = endOffsetCommitTime, partition = endPartition, cursor = endPartitionCursor)
      case HoodieTableType.MERGE_ON_READ =>
        val endCommit = latestOffset.getOrElse(HoodieSourceOffset("")).commitTime
        val lastInstant = metaClient.getCommitTimeline.filterCompletedInstants().lastInstant().get()
        // TODO support fullTableScan
        val fullTableScan: Boolean = false

        val morTimeline: HoodieTimeline = if (fullTableScan) {
            metaClient.getCommitsAndCompactionTimeline
          } else if (hollowCommitHandling == HollowCommitHandling.USE_STATE_TRANSITION_TIME) {
            metaClient.getCommitsAndCompactionTimeline.findInstantsInRangeByStateTransitionTime(
              startOffset.commitTime,
              if (endCommit.nonEmpty) endCommit else lastInstant.getStateTransitionTime)
          } else {
            metaClient.getCommitsAndCompactionTimeline.findInstantsInRange(
              startOffset.commitTime,
              if (endCommit.nonEmpty) endCommit else lastInstant.getTimestamp)
          }
        // Get all instance that in range and sort by commit time
        val includedCommits = morTimeline.getInstantsAsStream.iterator().toList.sortBy(_.getTimestamp)

        var maxFiles = 0L
        var maxBytes = 0L
        var maxRows = 0L
        var endOffsetCommitTime = startOffset.commitTime
        var endPartition = startOffset.partition
        var endPartitionCursor = startOffset.cursor
        var flag = false
        breakable {
          // commit -> partitions -> files
          // maxFilesPerTrigger、maxBytesPerTrigger
          for (commit <- includedCommits) {
            val metadata: HoodieCommitMetadata = HoodieCommitMetadata.fromBytes(morTimeline.getInstantDetails(commit)
              .get, classOf[HoodieCommitMetadata])
            val partitions = metadata.getPartitionToWriteStats.keySet().iterator().toList.sortBy(p => p)
            // Traverse all partitions in one commit
            for(partition <- partitions) {
              endPartition = partition
              val writeStats: List[HoodieDeltaWriteStat] = metadata.getPartitionToWriteStats
                .get(partition).asInstanceOf[List[HoodieDeltaWriteStat]]
              endPartitionCursor = 0
              // Traverse all writeStats in one partition
              for (i <- 0 to writeStats.size) {
                val deltaStat = writeStats(i)
                if (deltaStat.getBaseFile != null && deltaStat.getBaseFile.nonEmpty) {
                  maxFiles +=1
                }
                maxFiles += deltaStat.getLogFiles.size()
                maxBytes += writeStats(i).getTotalWriteBytes
                maxRows += writeStats(i).getNumWrites
                // Satisfy the maxFilesPerTrigger or maxBytesPerTrigger conditions
                if (maxFiles >= maxFilesPerTrigger || maxBytes >= maxBytesPerTrigger || maxRows >= maxRowsPerTrigger) {
                  endPartitionCursor = i
                  endPartition = partition
                  endOffsetCommitTime = commit.getTimestamp
                  flag = true
                  break()
                }
              }
              if (flag) {
                break()
              }
            }
            if (flag) {
              break()
            }
          }
        }

        HoodieStreamSourceOffset(endOffsetCommitTime, endPartition, endPartitionCursor)
      case _ => throw new IllegalArgumentException(s"UnSupport tableType: $tableType")
    }

    Option(endOffset).orNull
  }


  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startOffset = start.map(HoodieStreamSourceOffset(_))
      .getOrElse(initialStreamOffset)
    val endOffset = HoodieStreamSourceOffset(end)

    if (startOffset == endOffset) {
      sqlContext.internalCreateDataFrame(
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    } else {
      if (isCDCQuery) {
        val cdcOptions = Map(
          DataSourceReadOptions.BEGIN_INSTANTTIME.key()-> startOffset.commitTime,
          DataSourceReadOptions.END_INSTANTTIME.key() -> endOffset.commitTime
        )
        val rdd = CDCRelation.getCDCRelation(sqlContext, metaClient, cdcOptions)
          .buildScan0(HoodieCDCUtils.CDC_COLUMNS, Array.empty)

        sqlContext.sparkSession.internalCreateDataFrame(rdd, CDCRelation.FULL_CDC_SPARK_SCHEMA, isStreaming = true)
      } else {
        // Consume the data between (startCommitTime, endCommitTime]
        val incParams = parameters ++ Map(
          DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
          DataSourceReadOptions.BEGIN_INSTANTTIME.key -> startOffset.commitTime,
          DataSourceReadOptions.END_INSTANTTIME.key -> endOffset.commitTime,
          INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key -> hollowCommitHandling.name
        )

        val rdd = tableType match {
          case HoodieTableType.COPY_ON_WRITE =>
            val serDe = sparkAdapter.createSparkRowSerDe(schema)
            new IncrementalRelation(sqlContext, incParams, Some(schema), metaClient,
              Option(startOffset), Option(endOffset))
              .buildScan()
              .map(serDe.serializeRow)
          case HoodieTableType.MERGE_ON_READ =>
            val requiredColumns = schema.fields.map(_.name)
            MergeOnReadIncrementalRelation(sqlContext, incParams, metaClient, Some(schema), None,
              Option(startOffset), Option(endOffset))
              .buildScan(requiredColumns, Array.empty[Filter])
              .asInstanceOf[RDD[InternalRow]]
          case _ => throw new IllegalArgumentException(s"UnSupport tableType: $tableType")
        }
        sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
      }
    }
  }

  override def initialOffset(): Offset = {
    throw new IllegalStateException("should not be called.")
  }

  override def deserializeOffset(json: String): Offset = {
    throw new IllegalStateException("should not be called.")
  }

  override def commit(end: streaming.Offset): Unit = {}
}
