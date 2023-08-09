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

package org.apache.hudi.integ.testsuite

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.io.Serializable

class SparkDataSourceContinuousIngest(val spark: SparkSession, val conf: Configuration, val sourcePath: Path,
                                      val sourceFormat: String, val checkpointFile: Path, hudiBasePath: Path, hudiOptions: java.util.Map[String, String],
                                      minSyncIntervalSeconds: Long) extends Serializable {

  private val log = LoggerFactory.getLogger(getClass)

  def startIngestion(): Unit = {
    val fs = sourcePath.getFileSystem(conf)
    var checkPointFs = checkpointFile.getFileSystem(conf)
    var orderedBatch : Array[FileStatus] = null
    if (checkPointFs.exists(checkpointFile)) {
      log.info("Checkpoint file exists. ")
      val checkpoint = spark.sparkContext.textFile(checkpointFile.toString).collect()(0)
      log.warn("Checkpoint to resume from " + checkpoint)

      orderedBatch = fetchListOfFilesToConsume(fs, sourcePath, new PathFilter {
        override def accept(path: Path): Boolean = {
          path.getName.toLong > checkpoint.toLong
        }
      })
      if (log.isDebugEnabled) {
        log.debug("List of batches to consume in order ")
        orderedBatch.foreach(entry => log.warn(" " + entry.getPath.getName))
      }

    } else {
      log.warn("No checkpoint file exists. Starting from scratch ")
      orderedBatch = fetchListOfFilesToConsume(fs, sourcePath, new PathFilter {
        override def accept(path: Path): Boolean = {
          true
        }
      })
      if (log.isDebugEnabled) {
        log.debug("List of batches to consume in order ")
        orderedBatch.foreach(entry => log.warn(" " + entry.getPath.getName))
      }
    }

    if (orderedBatch.isEmpty) {
      log.info("All batches have been consumed. Exiting.")
    } else {
      orderedBatch.foreach(entry => {
        log.info("Consuming from batch " + entry)
        val pathToConsume = new Path(sourcePath.toString + "/" + entry.getPath.getName)
        val df = spark.read.format(sourceFormat).load(pathToConsume.toString)

        df.write.format("hudi").options(hudiOptions).mode(SaveMode.Append).save(hudiBasePath.toString)
        writeToFile(checkpointFile, entry.getPath.getName, checkPointFs)
        log.info("Completed batch " + entry + ". Moving to next batch. Sleeping for " + minSyncIntervalSeconds + " secs before next batch")
        Thread.sleep(minSyncIntervalSeconds * 1000)
      })
    }
  }

  def fetchListOfFilesToConsume(fs: FileSystem, basePath: Path, pathFilter: PathFilter): Array[FileStatus] = {
    val nextBatches = fs.listStatus(basePath, pathFilter)
    nextBatches.sortBy(fileStatus => fileStatus.getPath.getName.toLong)
  }

  def writeToFile(checkpointFilePath: Path, str: String, fs: FileSystem): Unit = {
    if (!fs.exists(checkpointFilePath)) {
      fs.create(checkpointFilePath)
    }
    val fsOutStream = fs.create(checkpointFilePath, true)
    fsOutStream.writeBytes(str)
    fsOutStream.flush()
    fsOutStream.close()
  }
}
