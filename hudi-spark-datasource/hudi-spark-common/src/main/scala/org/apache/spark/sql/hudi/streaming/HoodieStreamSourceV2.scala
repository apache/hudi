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

import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, IncrementalRelationV2, MergeOnReadIncrementalRelationV2, SparkAdapterSupport}
import org.apache.hudi.cdc.CDCRelation
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.cdc.HoodieCDCUtils
import org.apache.hudi.common.table.checkpoint.{CheckpointUtils, StreamerCheckpointV2}
import org.apache.hudi.common.table.log.InstantRange.RangeType
import org.apache.hudi.common.table.{HoodieTableMetaClient, HoodieTableVersion, TableSchemaResolver}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.hudi.streaming.HoodieSourceOffset.INIT_OFFSET
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * The Struct Stream Source for Hudi to consume the data by streaming job.
  * @param sqlContext
  * @param metadataPath
  * @param schemaOption
  * @param parameters
  */
class HoodieStreamSourceV2(sqlContext: SQLContext,
                           metaClient: HoodieTableMetaClient,
                           metadataPath: String,
                           schemaOption: Option[StructType],
                           parameters: Map[String, String],
                           offsetRangeLimit: HoodieOffsetRangeLimit,
                           writeTableVersion: HoodieTableVersion)
  extends Source with Logging with Serializable with SparkAdapterSupport {

  private lazy val tableType = metaClient.getTableType

  private val isCDCQuery = CDCRelation.isCDCEnabled(metaClient) &&
    parameters.get(DataSourceReadOptions.QUERY_TYPE.key).contains(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL) &&
    parameters.get(DataSourceReadOptions.INCREMENTAL_FORMAT.key).contains(DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL)

  @transient private lazy val initialOffsets = {
    val metadataLog = new HoodieMetadataLog(sqlContext.sparkSession, metadataPath)
    metadataLog.get(0).getOrElse {
      val offset = offsetRangeLimit match {
        case HoodieEarliestOffsetRangeLimit =>
          INIT_OFFSET
        case HoodieLatestOffsetRangeLimit =>
          getLatestOffset.getOrElse(INIT_OFFSET)
        case HoodieSpecifiedOffsetRangeLimit(completionTime) =>
          HoodieSourceOffset(completionTime)
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

  private def getLatestOffset: Option[HoodieSourceOffset] = {
    metaClient.reloadActiveTimeline()
    val latestCompletionTime = metaClient.getActiveTimeline.filterCompletedInstants.getLatestCompletionTime
    if (latestCompletionTime.isPresent) {
      Some(HoodieSourceOffset(latestCompletionTime.get))
    } else {
      None
    }
  }

  /**
    * Get the latest offset from the hoodie table.
    * @return
    */
  override def getOffset: Option[Offset] = {
    getLatestOffset
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    var startOffset = start.map(HoodieSourceOffset(_))
      .getOrElse(initialOffsets)
    var endOffset = HoodieSourceOffset(end)

    // We update the offsets here since until this point the latest offsets have been
    // calculated no matter if it is in the expected version.
    // We translate them here, then the rest logic should be intact.
    startOffset = HoodieSourceOffset(translateCheckpoint(startOffset.offsetCommitTime))
    endOffset = HoodieSourceOffset(translateCheckpoint(endOffset.offsetCommitTime))

    if (startOffset == endOffset) {
      sqlContext.internalCreateDataFrame(
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    } else {
      val (startCompletionTime, rangeType) = getStartCompletionTimeAndRangeType(startOffset)
      if (isCDCQuery) {
        val cdcOptions = Map(
          DataSourceReadOptions.START_COMMIT.key() -> startCompletionTime,
          DataSourceReadOptions.END_COMMIT.key() -> endOffset.offsetCommitTime
        )
        val rdd = CDCRelation.getCDCRelation(sqlContext, metaClient, cdcOptions, rangeType)
          .buildScan0(HoodieCDCUtils.CDC_COLUMNS, Array.empty)

        sqlContext.sparkSession.internalCreateDataFrame(rdd, CDCRelation.FULL_CDC_SPARK_SCHEMA, isStreaming = true)
      } else {
        // Consume the data between (startCommitTime, endCommitTime]
        val incParams = parameters ++ Map(
          DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
          DataSourceReadOptions.START_COMMIT.key -> startCompletionTime,
          DataSourceReadOptions.END_COMMIT.key -> endOffset.offsetCommitTime
        )

        val rdd = tableType match {
          case HoodieTableType.COPY_ON_WRITE =>
            val serDe = sparkAdapter.createSparkRowSerDe(schema)
            new IncrementalRelationV2(sqlContext, incParams, Some(schema), metaClient, rangeType)
              .buildScan()
              .map(serDe.serializeRow)
          case HoodieTableType.MERGE_ON_READ =>
            val requiredColumns = schema.fields.map(_.name)
            new MergeOnReadIncrementalRelationV2(sqlContext, incParams, metaClient, Some(schema), rangeType = rangeType)
              .buildScan(requiredColumns, Array.empty[Filter])
              .asInstanceOf[RDD[InternalRow]]
          case _ => throw new IllegalArgumentException(s"UnSupport tableType: $tableType")
        }
        sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
      }
    }
  }

  private def getStartCompletionTimeAndRangeType(startOffset: HoodieSourceOffset): (String, RangeType) = {
    startOffset match {
      case INIT_OFFSET => (
        startOffset.offsetCommitTime, RangeType.CLOSED_CLOSED)
      case HoodieSourceOffset(completionTime) => (
        completionTime, RangeType.OPEN_CLOSED)
      case _=> throw new IllegalStateException("UnKnow offset type.")
    }
  }

  private def translateCheckpoint(commitTime: String): String = {
    if (writeTableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      commitTime
    } else {
      CheckpointUtils.convertToCheckpointV1ForCommitTime(
        new StreamerCheckpointV2(commitTime), metaClient).getCheckpointKey
    }
  }

  override def stop(): Unit = {

  }
}
