/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.streaming

import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, HoodieCopyOnWriteCDCHadoopFsRelationFactory, HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV1, HoodieMergeOnReadCDCHadoopFsRelationFactory, HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV1, IncrementalRelationV1, MergeOnReadIncrementalRelationV1, SparkAdapterSupport}
import org.apache.hudi.DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT
import org.apache.hudi.cdc.CDCRelation
import org.apache.hudi.common.config.HoodieReaderConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableMetaClient, HoodieTableVersion, TableSchemaResolver}
import org.apache.hudi.common.table.cdc.HoodieCDCUtils
import org.apache.hudi.common.table.checkpoint.{CheckpointUtils, StreamerCheckpointV1}
import org.apache.hudi.common.table.timeline.TimelineUtils.{handleHollowCommitIfNeeded, HollowCommitHandling}
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling._
import org.apache.hudi.util.SparkConfigUtils

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.hudi.streaming.HoodieSourceOffset.INIT_OFFSET
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * The Struct Stream Source for Hudi to consume the data by streaming job.
 * @param sqlContext
 * @param metadataPath
 * @param schemaOption
 * @param parameters
 */
class HoodieStreamSourceV1(sqlContext: SQLContext,
                           metaClient: HoodieTableMetaClient,
                           metadataPath: String,
                           schemaOption: Option[StructType],
                           parameters: Map[String, String],
                           offsetRangeLimit: HoodieOffsetRangeLimit,
                           writeTableVersion: HoodieTableVersion)
  extends Source with Logging with Serializable with SparkAdapterSupport {

  private lazy val enableFileGroupReader = SparkConfigUtils
    .getStringWithAltKeys(parameters, HoodieReaderConfig.FILE_GROUP_READER_ENABLED).toBoolean


  private lazy val tableType = metaClient.getTableType

  private lazy val isBootstrappedTable = metaClient.getTableConfig.getBootstrapBasePath.isPresent

  private val isCDCQuery = CDCRelation.isCDCEnabled(metaClient) &&
    parameters.get(DataSourceReadOptions.QUERY_TYPE.key).contains(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL) &&
    parameters.get(DataSourceReadOptions.INCREMENTAL_FORMAT.key).contains(DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL)

  /**
   * When hollow commits are found while doing streaming read , unlike batch incremental query,
   * we do not use [[HollowCommitHandling.FAIL]] by default, instead we use [[HollowCommitHandling.BLOCK]]
   * to block processing data from going beyond the hollow commits to avoid unintentional skip.
   *
   * Users can set [[DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT]] to
   * [[HollowCommitHandling.USE_TRANSITION_TIME]] to avoid the blocking behavior.
   */
  private val hollowCommitHandlingMode: HollowCommitHandling =
    parameters.get(INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key)
      .map(HollowCommitHandling.valueOf)
      .getOrElse(HollowCommitHandling.BLOCK)

  @transient private lazy val initialOffsets = {
    val metadataLog = new HoodieMetadataLog(sqlContext.sparkSession, metadataPath)
    metadataLog.get(0).getOrElse {
      val offset = offsetRangeLimit match {
        case HoodieEarliestOffsetRangeLimit =>
          INIT_OFFSET
        case HoodieLatestOffsetRangeLimit =>
          getLatestOffset.getOrElse(INIT_OFFSET)
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

  private def getLatestOffset: Option[HoodieSourceOffset] = {
    metaClient.reloadActiveTimeline()
    val filteredTimeline = handleHollowCommitIfNeeded(
      metaClient.getActiveTimeline.filterCompletedInstants(), metaClient, hollowCommitHandlingMode)
    filteredTimeline match {
      case activeInstants if !activeInstants.empty() =>
        val timestamp = if (hollowCommitHandlingMode == USE_TRANSITION_TIME) {
          activeInstants.getLatestCompletionTime.get()
        } else {
          activeInstants.lastInstant().get().requestedTime()
        }
        Some(HoodieSourceOffset(timestamp))
      case _ =>
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
      sparkAdapter.internalCreateDataFrame(sqlContext.sparkSession,
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    } else {
      if (isCDCQuery) {
        val cdcOptions = Map(
          DataSourceReadOptions.START_COMMIT.key()-> startCommitTime(startOffset),
          DataSourceReadOptions.END_COMMIT.key() -> endOffset.offsetCommitTime
        )
        if (enableFileGroupReader) {
          val relation = if (tableType == HoodieTableType.COPY_ON_WRITE) {
            new HoodieCopyOnWriteCDCHadoopFsRelationFactory(
              sqlContext, metaClient, parameters ++ cdcOptions, schemaOption, isBootstrappedTable).build()
          } else {
            new HoodieMergeOnReadCDCHadoopFsRelationFactory(
              sqlContext, metaClient, parameters ++ cdcOptions, schemaOption, isBootstrappedTable).build()
          }
          sparkAdapter.createStreamingDataFrame(sqlContext, relation, CDCRelation.FULL_CDC_SPARK_SCHEMA)
        } else {
          val rdd = CDCRelation.getCDCRelation(sqlContext, metaClient, cdcOptions)
            .buildScan0(HoodieCDCUtils.CDC_COLUMNS, Array.empty)

          sparkAdapter.internalCreateDataFrame(sqlContext.sparkSession, rdd, CDCRelation.FULL_CDC_SPARK_SCHEMA, isStreaming = true)}
      } else {
        // Consume the data between (startCommitTime, endCommitTime]
        val incParams = parameters ++ Map(
          DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
          DataSourceReadOptions.START_COMMIT.key -> startCommitTime(startOffset),
          DataSourceReadOptions.END_COMMIT.key -> endOffset.offsetCommitTime,
          INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key -> hollowCommitHandlingMode.name
        )
        if (enableFileGroupReader) {
          val relation = if (tableType == HoodieTableType.COPY_ON_WRITE) {
            new HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV1(sqlContext, metaClient, incParams, Option(schema), isBootstrappedTable)
              .build()
          } else {
            new HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV1(sqlContext, metaClient, incParams, Option(schema), isBootstrappedTable)
              .build()
          }
          sparkAdapter.createStreamingDataFrame(sqlContext, relation, schema)
        } else {
          val rdd = tableType match {
            case HoodieTableType.COPY_ON_WRITE =>
              val serDe = getCatalystRowSerDe(schema)
              new IncrementalRelationV1(sqlContext, incParams, Some(schema), metaClient)
                .buildScan()
                .map(serDe.serializeRow)
            case HoodieTableType.MERGE_ON_READ =>
              val requiredColumns = schema.fields.map(_.name)
              new MergeOnReadIncrementalRelationV1(sqlContext, incParams, metaClient, Some(schema))
                .buildScan(requiredColumns, Array.empty[Filter])
                .asInstanceOf[RDD[InternalRow]]
            case _ => throw new IllegalArgumentException(s"UnSupport tableType: $tableType")
          }
          sparkAdapter.internalCreateDataFrame(sqlContext.sparkSession,rdd, schema, isStreaming = true)
        }
      }
    }
  }

  private def startCommitTime(startOffset: HoodieSourceOffset): String = {
    startOffset match {
      case INIT_OFFSET => startOffset.offsetCommitTime
      case HoodieSourceOffset(commitTime) =>
        commitTime
      case _=> throw new IllegalStateException("UnKnow offset type.")
    }
  }

  private def translateCheckpoint(commitTime: String): String = {
    if (writeTableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      CheckpointUtils.convertToCheckpointV2ForCommitTime(
        new StreamerCheckpointV1(commitTime), metaClient, hollowCommitHandlingMode).getCheckpointKey
    } else {
      commitTime
    }
  }

  override def stop(): Unit = {

  }
}

