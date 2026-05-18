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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, InstantComparison}
import org.apache.hudi.common.util.VisibleForTesting

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import scala.collection.JavaConverters._

object HoodieCommandMetrics {

  def updateCommitMetrics(metrics: Map[String, SQLMetric], metaClient: HoodieTableMetaClient, commitInstantTime: String): Unit = {
    val timeline = metaClient.getActiveTimeline.reload().getCommitsTimeline()
    val commitInstant = timeline.getInstants.asScala
      .filter(instant => InstantComparison.EQUALS.test(instant.requestedTime(), commitInstantTime))
    commitInstant.map { commit: HoodieInstant =>
      val metadata = timeline.readCommitMetadata(commit)
      updateCommitMetrics(metrics, metadata)
    }
  }

  def updateCommitMetrics(metrics: Map[String, SQLMetric], metadata: HoodieCommitMetadata): Unit = {
    updateCommitMetric(metrics, NUM_PARTITION_KEY, metadata.fetchTotalPartitionsWritten())
    updateCommitMetric(metrics, NUM_INSERT_FILE_KEY, metadata.fetchTotalFilesInsert())
    updateCommitMetric(metrics, NUM_UPDATE_FILE_KEY, metadata.fetchTotalFilesUpdated())
    updateCommitMetric(metrics, NUM_WRITE_ROWS_KEY, metadata.fetchTotalRecordsWritten())
    updateCommitMetric(metrics, NUM_UPDATE_ROWS_KEY, metadata.fetchTotalUpdateRecordsWritten())
    updateCommitMetric(metrics, NUM_INSERT_ROWS_KEY, metadata.fetchTotalInsertRecordsWritten())
    updateCommitMetric(metrics, NUM_DELETE_ROWS_KEY, metadata.getTotalRecordsDeleted())
    updateCommitMetric(metrics, NUM_OUTPUT_BYTES_KEY, metadata.fetchTotalBytesWritten())
    updateCommitMetric(metrics, INSERT_TIME, metadata.getTotalCreateTime())
    updateCommitMetric(metrics, UPSERT_TIME, metadata.getTotalUpsertTime())
  }

  private def updateCommitMetric(metrics: Map[String, SQLMetric], name: String, value: Long): Unit = {
    val metric = metrics.get(name)
    metric.foreach(_.set(value))
  }


  @VisibleForTesting
  val NUM_PARTITION_KEY = "number of written partitions"
  @VisibleForTesting
  val NUM_INSERT_FILE_KEY = "number of inserted files"
  @VisibleForTesting
  val NUM_UPDATE_FILE_KEY = "number of updated files"
  @VisibleForTesting
  val NUM_WRITE_ROWS_KEY = "number of written rows"
  @VisibleForTesting
  val NUM_UPDATE_ROWS_KEY = "number of updated rows"
  @VisibleForTesting
  val NUM_INSERT_ROWS_KEY = "number of inserted rows"
  @VisibleForTesting
  val NUM_DELETE_ROWS_KEY = "number of deleted rows"
  @VisibleForTesting
  val NUM_OUTPUT_BYTES_KEY = "output size in bytes"
  @VisibleForTesting
  val INSERT_TIME = "total insert time"
  @VisibleForTesting
  val UPSERT_TIME = "total upsert time"

  def metrics: Map[String, SQLMetric] = {
    val sparkContext = SparkContext.getActive.get
    Map(
      NUM_PARTITION_KEY -> SQLMetrics.createMetric(sparkContext, NUM_PARTITION_KEY),
      NUM_INSERT_FILE_KEY -> SQLMetrics.createMetric(sparkContext, NUM_INSERT_FILE_KEY),
      NUM_UPDATE_FILE_KEY -> SQLMetrics.createMetric(sparkContext, NUM_UPDATE_FILE_KEY),
      NUM_WRITE_ROWS_KEY -> SQLMetrics.createMetric(sparkContext, NUM_WRITE_ROWS_KEY),
      NUM_UPDATE_ROWS_KEY -> SQLMetrics.createMetric(sparkContext, NUM_UPDATE_ROWS_KEY),
      NUM_INSERT_ROWS_KEY -> SQLMetrics.createMetric(sparkContext, NUM_INSERT_ROWS_KEY),
      NUM_DELETE_ROWS_KEY -> SQLMetrics.createMetric(sparkContext, NUM_DELETE_ROWS_KEY),
      NUM_OUTPUT_BYTES_KEY -> SQLMetrics.createSizeMetric(sparkContext, NUM_OUTPUT_BYTES_KEY),
      INSERT_TIME -> SQLMetrics.createTimingMetric(sparkContext, INSERT_TIME),
      UPSERT_TIME -> SQLMetrics.createTimingMetric(sparkContext, UPSERT_TIME)
    )
  }
}
