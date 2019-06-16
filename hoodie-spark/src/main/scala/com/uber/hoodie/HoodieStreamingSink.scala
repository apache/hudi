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
package com.uber.hoodie

import com.uber.hoodie.exception.HoodieCorruptedDataException
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.log4j.LogManager

import scala.util.{Failure, Success, Try}

class HoodieStreamingSink(sqlContext: SQLContext,
                          options: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode)
  extends Sink
    with Serializable {
  @volatile private var latestBatchId = -1L

  private val log = LogManager.getLogger(classOf[HoodieStreamingSink])

  private val retryCnt = options(DataSourceWriteOptions.STREAMING_RETRY_CNT_OPT_KEY).toInt
  private val retryIntervalMs = options(DataSourceWriteOptions.STREAMING_RETRY_INTERVAL_MS_OPT_KEY).toLong
  private val ignoreFailedBatch = options(DataSourceWriteOptions.STREAMING_IGNORE_FAILED_BATCH_OPT_KEY).toBoolean

  private val mode =
    if (outputMode == OutputMode.Append()) {
      SaveMode.Append
    } else {
      SaveMode.Overwrite
    }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    retry(retryCnt, retryIntervalMs)(
      Try(
        HoodieSparkSqlWriter.write(
          sqlContext,
          mode,
          options,
          data)
      ) match {
        case Success((true, commitOps)) =>
          log.info(s"Micro batch id=$batchId succeeded"
          + commitOps.map(commit => s" for commit=$commit").getOrElse(" with no new commits"))
          Success((true, commitOps))
        case Failure(e) =>
          // clean up persist rdds in the write process
          data.sparkSession.sparkContext.getPersistentRDDs
            .foreach {
              case (id, rdd) =>
                rdd.unpersist()
            }
          log.error(s"Micro batch id=$batchId threw following expection: ", e)
          if (ignoreFailedBatch) {
            log.info(s"Ignore the exception and move on streaming as per " +
              s"${DataSourceWriteOptions.STREAMING_IGNORE_FAILED_BATCH_OPT_KEY} configuration")
            Success((true, None))
          } else {
            if (retryCnt > 1) log.info(s"Retrying the failed micro batch id=$batchId ...")
            Failure(e)
          }
        case Success((false, commitOps)) =>
          log.error(s"Micro batch id=$batchId ended up with errors"
            + commitOps.map(commit => s" for commit=$commit").getOrElse(""))
          if (ignoreFailedBatch) {
            log.info(s"Ignore the errors and move on streaming as per " +
              s"${DataSourceWriteOptions.STREAMING_IGNORE_FAILED_BATCH_OPT_KEY} configuration")
            Success((true, None))
          } else {
            if (retryCnt > 1) log.info(s"Retrying the failed micro batch id=$batchId ...")
            Failure(new HoodieCorruptedDataException(s"Micro batch id=$batchId ended up with errors"))
          }
      }
    ) match {
      case Failure(e) =>
        if (!ignoreFailedBatch) {
          log.error(s"Micro batch id=$batchId threw following expections," +
            s"aborting streaming app to avoid data loss: ", e)
          // spark sometimes hangs upon exceptions and keep on hold of the executors
          // this is to force exit upon errors / exceptions and release all executors
          // will require redeployment / supervise mode to restart the streaming
          System.exit(1)
        }
      case Success(_) =>
        log.info(s"Micro batch id=$batchId succeeded")
    }
  }

  override def toString: String = s"HoodieStreamingSink[${options("path")}]"

  @annotation.tailrec
  private def retry[T](n: Int, waitInMillis: Long)(fn: => Try[T]): Try[T] = {
    fn match {
      case x: util.Success[T] => x
      case _ if n > 1 =>
        Thread.sleep(waitInMillis)
        retry(n - 1, waitInMillis * 2)(fn)
      case f => f
    }
  }
}