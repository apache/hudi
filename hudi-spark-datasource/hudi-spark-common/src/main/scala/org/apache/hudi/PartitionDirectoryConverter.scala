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

import org.apache.hudi.common.config.HoodieConfig
import org.apache.hudi.common.model.FileSlice

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionDirectory

object PartitionDirectoryConverter extends SparkAdapterSupport {

  /**
   * Converts the file slices of one partition into partition directories.
   *
   * File slices that are read as a whole (with log files or a bootstrap base) each get their own
   * directory whose partition values carry only that slice, so that a task is shipped just the slice
   * it reads. Base-file-only slices need no such mapping and share a single directory per partition,
   * so that consumers of the file index see one entry per partition for the common copy-on-write case.
   */
  def convertFileSlicesToPartitionDirectories(partitionValues: InternalRow,
                                              fileSlices: Seq[FileSlice],
                                              config: HoodieConfig): Seq[PartitionDirectory] = {
    val (slicesReadAsFileSlice, baseFileOnlySlices) = fileSlices.partition(shouldReadAsFileSlice)
    val baseFileOnlyDirectory = if (baseFileOnlySlices.isEmpty) {
      Seq.empty
    } else {
      Seq(sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(
        partitionValues, baseFileOnlySlices.map(createDelegateFile(_, config))))
    }
    baseFileOnlyDirectory ++ slicesReadAsFileSlice.map(convertFileSliceToPartitionDirectory(partitionValues, _, config))
  }

  def convertFileSliceToPartitionDirectory(partitionValues: InternalRow,
                                           fileSlice: FileSlice,
                                           config: HoodieConfig): PartitionDirectory = {
    val delegateFile = createDelegateFile(fileSlice, config)
    if (shouldReadAsFileSlice(fileSlice)) {
      // should read as file slice, so we need to create a mapping from fileId to file slice
      sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(
        sparkAdapter.createPartitionFileSliceMapping(partitionValues, Map(fileSlice.getFileId -> fileSlice)), Seq(delegateFile))
    } else {
      sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(partitionValues, Seq(delegateFile))
    }
  }

  private def shouldReadAsFileSlice(fileSlice: FileSlice): Boolean = {
    fileSlice.hasLogFiles || fileSlice.hasBootstrapBase
  }

  /**
   * Generates a delegate file for the file slice, which spark uses to optimize rdd partition parallelism based on data such as file size
   *  - For file slice only has base file, we directly use the base file size as delegate file size
   *  - For file slice has log file, we estimate the delegate file size based on the log file size and option(base file) size
   */
  private def createDelegateFile(fileSlice: FileSlice, config: HoodieConfig): FileStatus = {
    val estimationFileSize = fileSlice.getTotalFileSizeAsParquetFormat(config)
    val fileInfo = if (fileSlice.getBaseFile.isPresent) {
      fileSlice.getBaseFile.get().getPathInfo
    } else {
      fileSlice.getLogFiles.findAny().get().getPathInfo
    }
    new FileStatus(estimationFileSize, fileInfo.isDirectory, 0, fileInfo.getBlockSize, fileInfo.getModificationTime, new Path(fileInfo.getPath.toUri))
  }
}
