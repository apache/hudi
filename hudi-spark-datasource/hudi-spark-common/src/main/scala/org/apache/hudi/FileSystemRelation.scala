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

import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieFileGroup, HoodieLogFile}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.storage.StoragePath

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import java.util.function.{Consumer, Predicate, ToLongFunction}

import scala.collection.JavaConverters._

/**
 * Relation to implement the Hoodie's file-system view for the table
 * valued function hudi_filesystem_view(...).
 *
 * The relation implements a simple buildScan() routine and does not support
 * any filtering primitives. Any column or predicate filtering needs to be done
 * explicitly by the execution layer.
 *
 */
class FileSystemRelation(val sqlContext: SQLContext,
                         val optParams: Map[String, String],
                         val metaClient: HoodieTableMetaClient) extends BaseRelation with TableScan {

  private val log = LoggerFactory.getLogger(classOf[FileSystemRelation])

  // The schema for the FileSystemRelation view
  override def schema: StructType = StructType(Array(
    StructField("File_ID", StringType, nullable = true),
    StructField("Partition_Path", StringType, nullable = true),
    StructField("Base_Instant_Time", StringType, nullable = true),
    StructField("Base_File_Path", StringType, nullable = true),
    StructField("Base_File_Size", LongType, nullable = true),
    StructField("Log_File_Count", LongType, nullable = true),
    StructField("Log_File_Size", LongType, nullable = true),
    StructField("Log_File_Scheduled", LongType, nullable = true),
    StructField("Log_File_Unscheduled", LongType, nullable = true)
  ))

  // The buildScan(...) method implementation from TableScan
  // This builds the dataframe containing all the columns for the FileSystemView
  override def buildScan(): RDD[Row] = {
    val data = collection.mutable.ArrayBuffer[Row]()
    val subPath = optParams.getOrElse(DataSourceReadOptions.FILESYSTEM_RELATION_ARG_SUBPATH.key(), "")
    val path = String.format("%s/%s/*", metaClient.getBasePath, subPath)
    val fileStatusList = FSUtils.getGlobStatusExcludingMetaFolder(metaClient.getStorage, new StoragePath(path))


    val fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline.getWriteTimeline, fileStatusList)
    val fileGroups = fsView.getAllFileGroups

    fileGroups.forEach(toJavaConsumer((fg: HoodieFileGroup) => {
        fg.getAllFileSlices.forEach(toJavaConsumer((fs: FileSlice) => {
        val logFileSize = fs.getLogFiles.mapToLong(toJavaLongFunction((lf: HoodieLogFile) => {
          lf.getFileSize
        })).sum()

        val logFileCompactionSize = fs.getLogFiles.filter(toJavaPredicate((lf: HoodieLogFile) => {
          lf.getDeltaCommitTime == fs.getBaseInstantTime
        })).mapToLong(toJavaLongFunction((lf: HoodieLogFile) => {
          lf.getFileSize
        })).sum()

        val logFileNonCompactionSize = fs.getLogFiles.filter(toJavaPredicate((lf: HoodieLogFile) => {
          lf.getDeltaCommitTime != fs.getBaseInstantTime
        })).mapToLong(toJavaLongFunction((lf: HoodieLogFile) => {
          lf.getFileSize
        })).sum()


        val r = Row(
          fg.getFileGroupId.getFileId,
          fg.getPartitionPath,
          fs.getBaseInstantTime,
          if (fs.getBaseFile.isPresent) fs.getBaseFile.get.getPath else "",
          if (fs.getBaseFile.isPresent) fs.getBaseFile.get.getFileSize else -1,
          fs.getLogFiles.count,
          logFileSize,
          logFileCompactionSize,
          logFileNonCompactionSize
        )
        data += r
      }))
    }))

    sqlContext.createDataFrame(data.asJava, schema).rdd
  }

  private def toJavaConsumer[T](consumer: (T) => Unit): Consumer[T] = {
    new Consumer[T] {
      override def accept(t: T): Unit = {
        consumer(t)
      }
    }
  }

  private def toJavaLongFunction[T](apply: (T) => Long): ToLongFunction[T] = {
    new ToLongFunction[T] {
      override def applyAsLong(t: T): Long = {
        apply(t)
      }
    }
  }

  private def toJavaPredicate[T](tst: (T) => Boolean): Predicate[T] = {
    new Predicate[T] {
      override def test(t: T): Boolean = {
        tst(t)
      }
    }
  }
}
