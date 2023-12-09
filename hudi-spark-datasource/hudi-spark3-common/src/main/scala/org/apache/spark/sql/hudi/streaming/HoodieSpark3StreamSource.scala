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

import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceReadOptions.{INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT, START_OFFSET}
import org.apache.hudi.cdc.CDCRelation
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.cdc.HoodieCDCUtils
import org.apache.hudi.common.table.timeline.{HoodieTimeline, InstantOffsetRange}
import org.apache.hudi.common.table.timeline.InstantOffsetRange.InstantOffset
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling._
import org.apache.hudi.common.table.timeline.TimelineUtils.{HollowCommitHandling, handleHollowCommitIfNeeded}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.TablePathUtils
import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, IncrementalRelation, MergeOnReadIncrementalRelation, SparkAdapterSupport}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{ReadAllAvailable, ReadLimit, SupportsAdmissionControl}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.hudi.streaming.HoodieSourceOffset.INIT_OFFSET
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * The Struct Stream Source for Hudi to consume the data by streaming job with AdmissionControl.
 *
 * @param sqlContext
 * @param metadataPath
 * @param schemaOption
 * @param parameters
 */
class HoodieSpark3StreamSource(sqlContext: SQLContext,
                               metadataPath: String,
                               schemaOption: Option[StructType],
                               parameters: Map[String, String])
  extends SupportsAdmissionControl with Source with Logging with Serializable with SparkAdapterSupport {

  @transient private val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()

  private val offsetRangeLimit: HoodieOffsetRangeLimit =
    parameters.getOrElse(START_OFFSET.key(), START_OFFSET.defaultValue()) match {
      case offset if offset.equalsIgnoreCase("earliest") =>
        HoodieEarliestOffsetRangeLimit
      case offset if offset.equalsIgnoreCase("latest") =>
        HoodieLatestOffsetRangeLimit
      case instantTime =>
        HoodieSpecifiedOffsetRangeLimit(instantTime)
    }

  private lazy val tablePath: Path = {
    val path = new Path(parameters.getOrElse("path", "Missing 'path' option"))
    val fs = path.getFileSystem(hadoopConf)
    TablePathUtils.getTablePath(fs, path).get()
  }

  private lazy val metaClient = HoodieTableMetaClient.builder()
    .setConf(hadoopConf).setBasePath(tablePath.toString).build()

  private lazy val tableType = metaClient.getTableType

  private val isCDCQuery = CDCRelation.isCDCEnabled(metaClient) &&
    parameters.get(DataSourceReadOptions.QUERY_TYPE.key).contains(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL) &&
    parameters.get(DataSourceReadOptions.INCREMENTAL_FORMAT.key).contains(DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL)

  private val maxFilesPerTrigger = parameters.get(DataSourceReadOptions.STREAM_MAX_FILES_PER_TRIGGER.key)
    .map(_.toInt)
    .getOrElse(DataSourceReadOptions.STREAM_MAX_FILES_PER_TRIGGER.defaultValue())
  private val maxRowsPerTrigger = parameters.get(DataSourceReadOptions.STREAM_MAX_ROWS_PER_TRIGGER.key)
    .map(_.toInt)
    .getOrElse(DataSourceReadOptions.STREAM_MAX_ROWS_PER_TRIGGER.defaultValue())

  /**
   * When hollow commits are found while doing streaming read , unlike batch incremental query,
   * we do not use [[HollowCommitHandling.FAIL]] by default, instead we use [[HollowCommitHandling.BLOCK]]
   * to block processing data from going beyond the hollow commits to avoid unintentional skip.
   *
   * Users can set [[DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT]] to
   * [[HollowCommitHandling.USE_TRANSITION_TIME]] to avoid the blocking behavior.
   */
  private val hollowCommitHandling: HollowCommitHandling =
    parameters.get(INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key)
      .map(HollowCommitHandling.valueOf)
      .getOrElse(HollowCommitHandling.BLOCK)

  /**
   * Get initial offset by respecting `offsetRangeLimit`
   */
  @transient private lazy val initialOffsets = {
    val metadataLog = new HoodieMetadataLog(sqlContext.sparkSession, metadataPath)
    metadataLog.get(0).getOrElse {
      val offset = offsetRangeLimit match {
        case HoodieEarliestOffsetRangeLimit =>
          INIT_OFFSET
        case HoodieLatestOffsetRangeLimit =>
          getLatestOffset
        case HoodieSpecifiedOffsetRangeLimit(instantTime) =>
          HoodieSourceOffset(instantTime)
      }
      metadataLog.add(0, offset)
      logInfo(s"The initial offset is $offset")
      offset
    }
  }

  override def schema: StructType = {
    if (isCDCQuery) {
      CDCRelation.FULL_CDC_SPARK_SCHEMA
    } else {
      schemaOption.getOrElse {
        val schemaUtil = new TableSchemaResolver(metaClient)
        AvroConversionUtils.convertAvroSchemaToStructType(schemaUtil.getTableAvroSchema)
      }
    }
  }

  private def getActiveTimeline: HoodieTimeline = {
    metaClient.reloadActiveTimeline()
    handleHollowCommitIfNeeded(
      metaClient.getActiveTimeline.filterCompletedInstants(), metaClient, hollowCommitHandling)
  }

  private def getLatestOffset: HoodieSourceOffset = getActiveTimeline match {
    case activeTimeline if !activeTimeline.empty() =>
      val timestamp = if (hollowCommitHandling == USE_TRANSITION_TIME) {
        activeTimeline.getInstantsOrderedByCompletionTime
          .skip(activeTimeline.countInstants() - 1)
          .findFirst()
          .get()
          .getCompletionTime
      } else {
        activeTimeline.lastInstant().get().getTimestamp
      }
      HoodieSourceOffset(timestamp)
    case _ =>
      INIT_OFFSET
  }

  override def getDefaultReadLimit: ReadLimit = {
    if (isCDCQuery) {
      ReadLimit.allAvailable()
      // TODO Add ReadLimit.compositeLimit once spark3.1 & spark3.0 are depreciated
    } else if (maxRowsPerTrigger != Integer.MAX_VALUE) {
      ReadLimit.maxRows(maxRowsPerTrigger)
    } else if (maxFilesPerTrigger != Integer.MAX_VALUE) {
      ReadLimit.maxFiles(maxFilesPerTrigger)
    } else {
      ReadLimit.allAvailable()
    }
  }

  override def latestOffset(startOffset: streaming.Offset, limit: streaming.ReadLimit): streaming.Offset = {
    limit match {
      case _: ReadAllAvailable =>
        getLatestOffset
      case _ =>
        getActiveTimeline match {
          case timeline: HoodieTimeline if !timeline.empty() =>
            HoodieSourceOffset(
              InstantOffsetRange.newBuilder()
                .withMaxRows(maxRowsPerTrigger)
                .withMaxFiles(maxFilesPerTrigger)
                .withCompletionTimeEnabled(hollowCommitHandling == USE_TRANSITION_TIME)
                .build(InstantOffset.fromString(
                  startOffset.asInstanceOf[HoodieSourceOffset].instantOffset), timeline)
                .getEndOffset.toString)
          case _ =>
            startOffset
        }
    }
  }

  /**
   * Get the latest offset from the hoodie table.
   */
  override def getOffset: Option[Offset] = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startOffset = start.map(HoodieSourceOffset(_))
      .getOrElse(initialOffsets)
    val endOffset = HoodieSourceOffset(end)

    if (startOffset == endOffset) {
      sqlContext.internalCreateDataFrame(
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    } else {
      if (isCDCQuery) {
        val cdcOptions = Map(
          DataSourceReadOptions.BEGIN_INSTANTTIME.key()-> startOffset.instantOffset,
          DataSourceReadOptions.END_INSTANTTIME.key() -> endOffset.instantOffset
        )
        val rdd = CDCRelation.getCDCRelation(sqlContext, metaClient, cdcOptions)
          .buildScan0(HoodieCDCUtils.CDC_COLUMNS, Array.empty)

        sqlContext.sparkSession.internalCreateDataFrame(rdd, CDCRelation.FULL_CDC_SPARK_SCHEMA, isStreaming = true)
      } else {
        // Consume the data between (startCommitTime, endCommitTime]
        val incParams = parameters ++ Map(
          DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
          DataSourceReadOptions.BEGIN_INSTANTTIME.key -> startOffset.instantOffset,
          DataSourceReadOptions.END_INSTANTTIME.key -> endOffset.instantOffset,
          INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key -> hollowCommitHandling.name
        )

        val rdd = tableType match {
          case HoodieTableType.COPY_ON_WRITE =>
            val serDe = sparkAdapter.createSparkRowSerDe(schema)
            new IncrementalRelation(sqlContext, incParams, Some(schema), metaClient)
              .buildScan()
              .map(serDe.serializeRow)
          case HoodieTableType.MERGE_ON_READ =>
            val requiredColumns = schema.fields.map(_.name)
            MergeOnReadIncrementalRelation(sqlContext, incParams, metaClient, Some(schema))
              .buildScan(requiredColumns, Array.empty[Filter])
              .asInstanceOf[RDD[InternalRow]]
          case _ => throw new IllegalArgumentException(s"UnSupport tableType: $tableType")
        }
        sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
      }
    }
  }

  override def stop(): Unit = {}
}
