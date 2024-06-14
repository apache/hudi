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

package org.apache.hudi.integ.testsuite.streaming

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig.FAIL_ON_TIMELINE_ARCHIVING_ENABLE
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener, Trigger}
import org.apache.log4j.LogManager

object StructuredStreamingSinkTestWriter {

  private val log = LogManager.getLogger(getClass)
  var validationComplete: Boolean = false;

  def waitUntilCondition(): Unit = {
    waitUntilCondition(1000 * 60 * 5, 500)
  }

  def waitUntilCondition(maxWaitTimeMs: Long, intervalTimeMs: Long): Unit = {
    var waitSoFar: Long = 0;
    while (waitSoFar < maxWaitTimeMs && !validationComplete) {
      log.info("Waiting for " + intervalTimeMs + ". Total wait time " + waitSoFar)
      Thread.sleep(intervalTimeMs)
      waitSoFar += intervalTimeMs
    }
  }

  def triggerStreaming(spark: SparkSession, tableType: String, inputPath: String, hudiPath: String, hudiCheckpointPath: String,
                       tableName: String, partitionPathField: String, recordKeyField: String,
                       preCombineField: String): Unit = {

    def validate(): Unit = {
      log.info("Validation starting")
      val inputDf = spark.read.format("parquet").load(inputPath)
      val hudiDf = spark.read.format("hudi").load(hudiPath)
      inputDf.registerTempTable("inputTbl")
      hudiDf.registerTempTable("hudiTbl")
      assert(spark.sql("select count(distinct " + partitionPathField + ", " + recordKeyField + ") from inputTbl").count ==
        spark.sql("select count(distinct " + partitionPathField + ", " + recordKeyField + ") from hudiTbl").count)
      validationComplete = true
      log.info("Validation complete")
    }

    def shutdownListener(spark: SparkSession) = new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        log.info("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        log.info("Query terminated! " + queryTerminated.id + ". Validating input and hudi")
        validate()
        log.info("Data Validation complete")
      }

      override def onQueryProgress(queryProgressEvent: QueryProgressEvent): Unit = {
        if (queryProgressEvent.progress.numInputRows == 0) {
          log.info("Stopping spark structured streaming as we have reached the end")
          spark.streams.active.foreach(_.stop())
        }
      }
    }

    spark.streams.addListener(shutdownListener(spark))
    log.info("Starting to consume from source and writing to hudi ")

    val inputDfSchema = spark.read.format("parquet").load(inputPath).schema
    val parquetdf = spark.readStream.option("spark.sql.streaming.schemaInference", "true").option("maxFilesPerTrigger", "1")
      .schema(inputDfSchema).parquet(inputPath)

    val writer = parquetdf.writeStream.format("org.apache.hudi").
      option(TABLE_TYPE.key, tableType).
      option(PRECOMBINE_FIELD.key, preCombineField).
      option(RECORDKEY_FIELD.key, recordKeyField).
      option(PARTITIONPATH_FIELD.key, partitionPathField).
      option(FAIL_ON_TIMELINE_ARCHIVING_ENABLE.key, false).
      option(STREAMING_IGNORE_FAILED_BATCH.key, false).
      option(STREAMING_RETRY_CNT.key, 0).
      option("hoodie.table.name", tableName).
      option("hoodie.compact.inline.max.delta.commits", "2").
      option("checkpointLocation", hudiCheckpointPath).
      outputMode(OutputMode.Append());

    writer.trigger(Trigger.ProcessingTime(30000)).start(hudiPath);
  }
}
