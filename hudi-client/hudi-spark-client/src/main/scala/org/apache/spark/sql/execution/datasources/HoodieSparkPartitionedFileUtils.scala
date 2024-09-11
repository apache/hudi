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

package org.apache.spark.sql.execution.datasources

import org.apache.hudi.storage.StoragePath

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.InternalRow

/**
 * Utils on Spark [[PartitionedFile]] and [[PartitionDirectory]] to adapt to type changes.
 * Before Spark 3.4.0,
 * ```
 * case class PartitionedFile(
 *   partitionValues: InternalRow,
 *   filePath: String,
 *   start: Long,
 *   @transient locations: Array[String] = Array.empty)
 * ```,
 * Since Spark 3.4.0, the filePath is switch to [[SparkPath]] for type safety:
 * ```
 * case class PartitionedFile(
 *   partitionValues: InternalRow,
 *   filePath: SparkPath,
 *   start: Long,
 *   length: Long,
 *   @transient locations: Array[String] = Array.empty,
 *   modificationTime: Long = 0L,
 *   fileSize: Long = 0L)
 * ```
 */
trait HoodieSparkPartitionedFileUtils extends Serializable {
  /**
   * Gets the Hadoop [[Path]] instance from Spark [[PartitionedFile]] instance.
   *
   * @param partitionedFile Spark [[PartitionedFile]] instance.
   * @return Hadoop [[Path]] instance.
   */
  def getPathFromPartitionedFile(partitionedFile: PartitionedFile): StoragePath

  /**
   * Gets the [[String]] path from Spark [[PartitionedFile]] instance.
   *
   * @param partitionedFile Spark [[PartitionedFile]] instance.
   * @return path in [[String]].
   */
  def getStringPathFromPartitionedFile(partitionedFile: PartitionedFile): String

  /**
   * Creates a new [[PartitionedFile]] instance.
   *
   * @param partitionValues value of partition columns to be prepended to each row.
   * @param filePath        URI of the file to read.
   * @param start           the beginning offset (in bytes) of the block.
   * @param length          number of bytes to read.
   * @return a new [[PartitionedFile]] instance.
   */
  def createPartitionedFile(partitionValues: InternalRow,
                            filePath: StoragePath,
                            start: Long,
                            length: Long): PartitionedFile

  /**
   * SPARK-43039 FileIndex#PartitionDirectory refactored in Spark 3.5.0
   */
  def toFileStatuses(partitionDirs: Seq[PartitionDirectory]): Seq[FileStatus]

  /**
   * SPARK-43039 FileIndex#PartitionDirectory refactored in Spark 3.5.0
   */
  def newPartitionDirectory(internalRow: InternalRow, statuses: Seq[FileStatus]): PartitionDirectory
}
