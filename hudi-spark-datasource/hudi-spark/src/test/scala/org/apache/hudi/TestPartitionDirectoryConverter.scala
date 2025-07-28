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
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieBaseFile, HoodieFileGroupId, HoodieLogFile}
import org.apache.hudi.storage.{StoragePath, StoragePathInfo}
import org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest

import org.apache.commons.lang.math.RandomUtils.nextInt
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.FilePartition
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class TestPartitionDirectoryConverter extends SparkAdapterSupport {

  val blockSize = 1024
  val fixedSizePerRecordWithParquetFormat = 100L
  val partitionPath = "p_date=2025-01-01"
  val baseInstant = "20250101010101"

  @ParameterizedTest
  @ValueSource(doubles = Array(0.1, 0.2, 0.3, 0.5, 0.8, 1.0))
  def testConvertFileSlicesToPartitionDirectory(logFraction: Double): Unit = {
    val spark = SparkSession.builder
      .config(getSparkConfForTest("TestPartitionDirectoryConverter"))
      .config("spark.sql.files.openCostInBytes", "0")
      .getOrCreate

    val options = Map(
      s"${HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION.key()}" -> logFraction.toString
    )
    // There are 4 cases for file slices:
    // 1. base file only
    // 2. log files only
    // 3. base file + log files

    // iterate 100000 times to create file slices
    val slicesAndNum = (0 until 10000).map { i =>
      val fileId = s"f-$i"
      // random base record number
      val baseRecordNum = if (nextInt(2) == 0) {
        nextInt(1000)
      } else {
        0
      }
      val logRecordNums = if (baseRecordNum == 0 || nextInt(2) == 0) {
        Seq(nextInt(300), nextInt(300), nextInt(300))
      } else {
        Seq()
      }
      val tuple = (buildFileSlice(fileId, baseRecordNum, logRecordNums, logFraction), baseRecordNum + logRecordNums.sum)
      tuple
    }

    val totalRecordNum = slicesAndNum.map(_._2).sum
    val slices = slicesAndNum.map(_._1)

    val totalSize = totalRecordNum * fixedSizePerRecordWithParquetFormat
    val expectedTaskCount = 100
    val maxSplitSize = totalSize / expectedTaskCount
    val partitionValues = Seq("2025-01-01")

    val partitionedFiles = slices.flatMap(slice => {
      val dir = PartitionDirectoryConverter.convertFileSliceToPartitionDirectory(InternalRow.fromSeq(partitionValues), slice, options)
      sparkAdapter.splitFiles(spark, dir, false, maxSplitSize)
    })

    val tasks = sparkAdapter.getFilePartitions(spark, partitionedFiles, maxSplitSize)
    verifyBalanceByNum(tasks, totalRecordNum, logFraction)
    spark.stop()
  }

  private def verifyBalanceByNum(tasks: Seq[FilePartition], totalRecordNum: Int, logFraction: Double): Unit = {
    val expectedRecordNumPerTask = totalRecordNum / tasks.size
    val expectedToleranceMin = expectedRecordNumPerTask * 0.9
    val expectedToleranceMax = expectedRecordNumPerTask * 1.1
    var outOfRangeCount = 0;
    tasks.foreach(task => {
      val totalRecordNum = task.files.map(file => {
        file.partitionValues match {
          case fileSliceMapping: HoodiePartitionFileSliceMapping => {
            val fg = FSUtils.getFileIdFromFilePath(sparkAdapter.getSparkPartitionedFileUtils.getPathFromPartitionedFile(file))
            fileSliceMapping.getSlice(fg) match {
              case Some(slice) => calculateRecordNumForFileSlice(slice, logFraction)
              case None => file.fileSize / fixedSizePerRecordWithParquetFormat
            }
          }
          case _ => {
            file.fileSize / fixedSizePerRecordWithParquetFormat
          }
        }
      }).sum
      if (totalRecordNum < expectedToleranceMin || totalRecordNum > expectedToleranceMax) {
        outOfRangeCount += 1
      }
      assert(outOfRangeCount <= 1, s"Record num $totalRecordNum is not in the range [$expectedToleranceMin, $expectedToleranceMax]")
      totalRecordNum
    })
  }

  private def calculateRecordNumForFileSlice(fileSlice: FileSlice, logFraction: Double): Int = {
    val baseFile = fileSlice.getBaseFile
    val logFiles = fileSlice.getLogFiles
    val baseRecordNum = if (baseFile.isPresent) {
      (baseFile.get().getFileLen / fixedSizePerRecordWithParquetFormat).toInt
    } else {
      0
    }
    val logRecordNum = logFiles.mapToInt(logFile => {
      (logFile.getFileSize / (fixedSizePerRecordWithParquetFormat / logFraction)).toInt
    }).sum()

    baseRecordNum + logRecordNum
  }

  private def buildFileSlice(fileId: String, baseRecordNum: Int, logRecordNums: Seq[Int], logFraction: Double): FileSlice = {
    val slice = new FileSlice(new HoodieFileGroupId(partitionPath, fileId), baseInstant)
    if (baseRecordNum > 0) {
      slice.setBaseFile(buildHoodieBaseFile(fileId, baseRecordNum))
    }
    val sizePerLogRecord = fixedSizePerRecordWithParquetFormat / logFraction
    logRecordNums.zipWithIndex.foreach { case (logRecordNum, index) => {
      val logFile = buildHoodieLogFile(fileId, logRecordNum, sizePerLogRecord.toLong, index)
      slice.addLogFile(logFile)
    }
    }
    slice
  }

  private def buildHoodieBaseFile(fileId: String, recordsNum: Int): HoodieBaseFile = {
    val path = new StoragePath(s"${fileId}_-0-0-0_20250101010101.parquet")
    val fileLen = recordsNum * fixedSizePerRecordWithParquetFormat
    val info = new StoragePathInfo(path, fileLen, false, 1, blockSize, System.currentTimeMillis())
    new HoodieBaseFile(info)
  }

  private def buildHoodieLogFile(fileId: String, recordsNum: Int, sizePerLogRecord: Long, index: Int): HoodieLogFile = {
    val path = new StoragePath(s".${fileId}_20250101010101.log.${index}_0-0-0")
    val fileLen = recordsNum * sizePerLogRecord
    val info = new StoragePathInfo(path, fileLen, false, 1, blockSize, System.currentTimeMillis())
    new HoodieLogFile(info)
  }

}
