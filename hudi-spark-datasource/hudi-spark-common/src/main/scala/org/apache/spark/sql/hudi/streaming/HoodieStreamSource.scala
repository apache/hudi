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

import java.util.Date

import org.apache.hadoop.fs.Path

import org.apache.hudi.cdc.CDCRelation
import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, IncrementalRelation, MergeOnReadIncrementalRelation, SparkAdapterSupport}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.cdc.HoodieCDCUtils
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.TablePathUtils
import org.apache.spark.sql.hudi.streaming.HoodieSourceOffset.INIT_OFFSET
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
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
class HoodieStreamSource(
    sqlContext: SQLContext,
    metadataPath: String,
    schemaOption: Option[StructType],
    parameters: Map[String, String],
    offsetRangeLimit: HoodieOffsetRangeLimit)
  extends Source with Logging with Serializable with SparkAdapterSupport {

  @transient private val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()

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
    metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants() match {
      case activeInstants if !activeInstants.empty() =>
        Some(HoodieSourceOffset(activeInstants.lastInstant().get().getTimestamp))
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
    val startOffset = start.map(HoodieSourceOffset(_))
      .getOrElse(initialOffsets)
    val endOffset = HoodieSourceOffset(end)

    if (startOffset == endOffset) {
      sqlContext.internalCreateDataFrame(
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    } else {
      if (isCDCQuery) {
        val cdcOptions = Map(
          DataSourceReadOptions.BEGIN_INSTANTTIME.key()-> startCommitTime(startOffset),
          DataSourceReadOptions.END_INSTANTTIME.key() -> endOffset.commitTime
        )
        val rdd = CDCRelation.getCDCRelation(sqlContext, metaClient, cdcOptions)
          .buildScan0(HoodieCDCUtils.CDC_COLUMNS, Array.empty)

        sqlContext.sparkSession.internalCreateDataFrame(rdd, CDCRelation.FULL_CDC_SPARK_SCHEMA, isStreaming = true)
      } else {
        // Consume the data between (startCommitTime, endCommitTime]
        val incParams = parameters ++ Map(
          DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
          DataSourceReadOptions.BEGIN_INSTANTTIME.key -> startCommitTime(startOffset),
          DataSourceReadOptions.END_INSTANTTIME.key -> endOffset.commitTime
        )

        val rdd = tableType match {
          case HoodieTableType.COPY_ON_WRITE =>
            val serDe = sparkAdapter.createSparkRowSerDe(schema)
            new IncrementalRelation(sqlContext, incParams, Some(schema), metaClient)
              .buildScan()
              .map(serDe.serializeRow)
          case HoodieTableType.MERGE_ON_READ =>
            val requiredColumns = schema.fields.map(_.name)
            new MergeOnReadIncrementalRelation(sqlContext, incParams, metaClient, Some(schema))
              .buildScan(requiredColumns, Array.empty[Filter])
              .asInstanceOf[RDD[InternalRow]]
          case _ => throw new IllegalArgumentException(s"UnSupport tableType: $tableType")
        }
        sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
      }
    }
  }

  private def startCommitTime(startOffset: HoodieSourceOffset): String = {
    startOffset match {
      case INIT_OFFSET => startOffset.commitTime
      case HoodieSourceOffset(commitTime) =>
        val time = HoodieActiveTimeline.parseDateFromInstantTime(commitTime).getTime
        // As we consume the data between (start, end], start is not included,
        // so we +1s to the start commit time here.
        HoodieActiveTimeline.formatDate(new Date(time + 1000))
      case _=> throw new IllegalStateException("UnKnow offset type.")
    }
  }

  override def stop(): Unit = {

  }
}
