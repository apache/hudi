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

package org.apache.hudi

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, TimelineUtils}
import org.apache.hudi.common.util.CommitUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import java.util.function.Consumer

import scala.collection.JavaConverters._

/**
 * Relation to implement the Hoodie's timeline view for the table
 * valued function hoodie_query_timeline(...).
 *
 * The relation implements a simple buildScan() routine and does not support
 * any filtering primitives. Any column or predicate filtering needs to be done
 * explicitly by the execution layer.
 *
 * By default, it only loads instants from the active timeline.
 * If "ARCHIVED_TIMELINE" option is set, then it loads archived timeline.
 */
class TimelineRelation(val sqlContext: SQLContext,
                       val optParams: Map[String, String],
                       val metaClient: HoodieTableMetaClient) extends BaseRelation with TableScan {

  private val log = LoggerFactory.getLogger(classOf[TimelineRelation])

  // The schema for the Timeline view
  override def schema: StructType = StructType(Array(
    StructField("Timestamp", StringType, nullable = true),
    StructField("Action", StringType, nullable = true),
    StructField("State", StringType, nullable = true),
    StructField("Completion_Time", StringType, nullable = true),
    StructField("File_Name", StringType, nullable = true),
    StructField("Total_Bytes_Written", LongType, nullable = true),
    StructField("Total_Files_Updated", LongType, nullable = true),
    StructField("Total_Partitions_Written", LongType, nullable = true),
    StructField("Total_Records_Written", LongType, nullable = true),
    StructField("Total_Updated_Records_Written", LongType, nullable = true),
    StructField("Total_Write_Errors", LongType, nullable = true)
  ))

  // Whether to include archived timeline in the result?
  private val includeArchivedTimeline: Boolean = optParams.getOrElse(
    DataSourceReadOptions.TIMELINE_RELATION_ARG_ARCHIVED_TIMELINE.key(), "false").toBoolean

  // The buildScan(...) method implementation from TableScan
  // This builds the dataframe containing all the instants in the timeline
  // for the given table
  override def buildScan(): RDD[Row] = {
    val data = collection.mutable.ArrayBuffer[Row]()
    val timeline = TimelineUtils.getTimeline(metaClient, includeArchivedTimeline)

    val instants = timeline.getInstants
    instants.forEach(toJavaConsumer((instant: HoodieInstant) => {
      if (timeline.getInstantDetails(instant).isPresent) {
        var totalBytesWritten: Long = -1
        var totalFilesUpdated: Long = -1
        var totalPartitionsWritten: Long = -1
        var totalRecordsWritten: Long = -1
        var totalUpdatedRecordsWritten: Long = -1
        var totalWriteErrors: Long = -1
        val instantFileNameGenerator = metaClient.getTimelineLayout.getInstantFileNameGenerator;

        val commitMetadataOpt = CommitUtils.buildMetadataFromInstant(timeline, instant)
        if (commitMetadataOpt.isPresent) {
          val commitMetadata = commitMetadataOpt.get()
          totalBytesWritten = commitMetadata.fetchTotalBytesWritten
          totalFilesUpdated = commitMetadata.fetchTotalFilesUpdated
          totalPartitionsWritten = commitMetadata.fetchTotalPartitionsWritten
          totalRecordsWritten = commitMetadata.fetchTotalRecordsWritten
          totalUpdatedRecordsWritten = commitMetadata.fetchTotalUpdateRecordsWritten
          totalWriteErrors = commitMetadata.fetchTotalWriteErrors
        }

        val r = Row(instant.requestedTime,
          instant.getAction,
          instant.getState.toString,
          instant.getCompletionTime,
          instantFileNameGenerator.getFileName(instant),
          totalBytesWritten,
          totalFilesUpdated,
          totalPartitionsWritten,
          totalRecordsWritten,
          totalUpdatedRecordsWritten,
          totalWriteErrors)
        data += r
      }
    }))

    // Using deprecated `JavaConversions` to be compatible with scala versions < 2.12.
    // Can replace with JavaConverters.seqAsJavaList(...) once the support for scala versions < 2.12 is stopped
    sqlContext.createDataFrame(data.asJava, schema).rdd
  }

  private def toJavaConsumer[T](consumer: (T) => Unit): Consumer[T] = {
    new Consumer[T] {
      override def accept(t: T): Unit = {
        consumer(t)
      }
    }
  }
}

