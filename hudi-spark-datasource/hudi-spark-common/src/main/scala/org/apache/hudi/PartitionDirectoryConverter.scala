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

import org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.model.FileSlice

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionDirectory

object PartitionDirectoryConverter extends SparkAdapterSupport {

  def convertFileSlicesToPartitionDirectory(partitionOpt: Option[PartitionPath],
                                            fileSlices: Seq[FileSlice],
                                            options: Map[String, String]): PartitionDirectory = {
    val logFileEstimationFraction = options.getOrElse(HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION.key(),
      HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION.defaultValue()).toDouble
    // 1. Generate a delegate file for each file slice, which spark uses to optimize rdd partition parallelism based on data such as file size
    // For file slice only has base file, we directly use the base file size as delegate file size
    // For file slice has log file, we estimate the delegate file size based on the log file size and option(base file) size
    val delegateFiles = fileSlices.map(slice => {
      val estimationFileSize = slice.getTotalFileSizeAsParquetFormat(logFileEstimationFraction)
      val fileInfo = if (slice.getBaseFile.isPresent) {
        slice.getBaseFile.get().getPathInfo
      } else if (slice.hasLogFiles) {
        slice.getLogFiles.findAny().get().getPathInfo
      } else {
        null
      }
      (fileInfo, estimationFileSize)
    }).filter(_._1 != null).map(f => {
      val (fileInfo, estimationFileSize) = f
      new FileStatus(estimationFileSize, fileInfo.isDirectory, 0, fileInfo.getBlockSize, fileInfo.getModificationTime, new Path(fileInfo.getPath.toUri))
    })
    // 2. Generate a mapping from fileId to file slice
    val c = fileSlices.filter(f => f.hasLogFiles || f.hasBootstrapBase).foldLeft(Map[String, FileSlice]()) { (m, f) => m + (f.getFileId -> f) }
    if (c.nonEmpty) {
      sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(
        new HoodiePartitionFileSliceMapping(InternalRow.fromSeq(partitionOpt.get.values), c), delegateFiles)
    } else {
      sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(
        InternalRow.fromSeq(partitionOpt.get.values), delegateFiles)
    }
  }

}
