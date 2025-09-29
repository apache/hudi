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

import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.model.FileSlice

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionDirectory

object PartitionDirectoryConverter extends SparkAdapterSupport {

  def convertFileSliceToPartitionDirectory(partitionValues: InternalRow,
                                           fileSlice: FileSlice,
                                           options: Map[String, String]): PartitionDirectory = {
    val logFileEstimationFraction = options.getOrElse(HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION.key(),
      HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION.defaultValue()).toDouble
    // 1. Generate a delegate file for file slice, which spark uses to optimize rdd partition parallelism based on data such as file size
    //    - For file slice only has base file, we directly use the base file size as delegate file size
    //    - For file slice has log file, we estimate the delegate file size based on the log file size and option(base file) size
    val estimationFileSize = fileSlice.getTotalFileSizeAsParquetFormat(logFileEstimationFraction)
    val fileInfo = if (fileSlice.getBaseFile.isPresent) {
      fileSlice.getBaseFile.get().getPathInfo
    } else {
      fileSlice.getLogFiles.findAny().get().getPathInfo
    }
    // create a delegate file status based on the file size estimation
    val delegateFile = new FileStatus(estimationFileSize, fileInfo.isDirectory, 0, fileInfo.getBlockSize, fileInfo.getModificationTime, new Path(fileInfo.getPath.toUri))

    // 2. Generate a partition directory based on the delegate file and partition values
    if (fileSlice.hasLogFiles || fileSlice.hasBootstrapBase) {
      // should read as file slice, so we need to create a mapping from fileId to file slice
      sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(
        sparkAdapter.createPartitionFileSliceMapping(partitionValues, Map(fileSlice.getFileId -> fileSlice)), Seq(delegateFile))
    } else {
      sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(partitionValues, Seq(delegateFile))
    }
  }

}
