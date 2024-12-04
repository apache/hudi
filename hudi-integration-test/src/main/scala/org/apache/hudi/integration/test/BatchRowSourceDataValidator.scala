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

package org.apache.hudi.integration.test

import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.exception.HoodieIOException
import org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf
import org.apache.hudi.utilities.streamer.HoodieStreamer.CHECKPOINT_KEY

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.util.stream

class BatchRowSourceDataValidator(spark: SparkSession,
                                  sourcePathStr: String,
                                  hudiTablePathStr: String) extends Serializable {
  val LOG: Logger = LoggerFactory.getLogger(classOf[BatchRowSourceDataValidator]);

  def validate(): Unit = {
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(getStorageConf(spark.sparkContext.hadoopConfiguration))
      .setBasePath(hudiTablePathStr)
      .build()
    val timestampCheckpointOpt = getLatestCommitWithCheckpoint(metaClient.getCommitsTimeline)
    if (timestampCheckpointOpt.isDefined) {
      val latestCheckpoint = timestampCheckpointOpt.get.getRight
      val actualDf = spark.read.format("hudi").load(hudiTablePathStr)
      //actualDf.persist(StorageLevel.MEMORY_AND_DISK)
      LOG.info("Snapshot query result schema:")
      actualDf.printSchema()
      LOG.info("Count of rows: " + actualDf.count())
      LOG.info("Latest commit with checkpoint: " + timestampCheckpointOpt.get.getLeft)
      LOG.info("Latest checkpoint: " + latestCheckpoint)

      // actualDf.unpersist()
    }
  }

  @throws[IOException]
  protected def getLatestCommitWithCheckpoint(commitTimeline: HoodieTimeline): Option[Pair[String, String]] = {
    val timestampCheckpointStream: stream.Stream[Option[Pair[String, String]]] =
      commitTimeline.getReverseOrderedInstants.map((instant: HoodieInstant) => {
        try {
          val commitMetadata = HoodieCommitMetadata.fromBytes(
            commitTimeline.getInstantDetails(instant).get, classOf[HoodieCommitMetadata])
          val result: Option[Pair[String, String]] =
            if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_KEY))) {
              Option(Pair.of(instant.getTimestamp, commitMetadata.getMetadata(CHECKPOINT_KEY)))
            } else {
              Option.empty.asInstanceOf[Option[Pair[String, String]]]
            }
          result
        } catch {
          case e: IOException =>
            throw new HoodieIOException("Failed to parse HoodieCommitMetadata for " + instant.toString, e)
        }
      })
    timestampCheckpointStream.filter(e => e.isDefined).findFirst()
      .orElse(Option.empty.asInstanceOf[Option[Pair[String, String]]])
  }
}
