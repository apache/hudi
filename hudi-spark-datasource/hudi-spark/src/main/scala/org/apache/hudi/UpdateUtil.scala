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
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordPayload, HoodieWriteStat}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieCommitException
import org.apache.hudi.io.storage.row.HoodieRowUpdateHandle
import org.apache.hudi.table.HoodieTable
import org.apache.log4j.LogManager
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow

import collection.JavaConverters._

object UpdateUtil {

  private val log = LogManager.getLogger(getClass)

  def updateFileInternal(
      partitionId: Int,
      rows: Iterator[InternalRow],
      instantTime: String,
      hoodieTable: HoodieTable[_, _, _, _],
      hoodieWriteConfig: HoodieWriteConfig,
      writeStructType: StructType,
      updateFieldsIndex: Set[Int]): Iterator[HoodieWriteStat] = {
    val metaColPos = HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS
    var lastKnownPartitionPath: String = null
    var lastKnownfileName: String = null
    var handle: HoodieRowUpdateHandle = null
    while (rows.hasNext) {
      val row = rows.next()
      val partitionPath = row.getString(metaColPos.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD))
      val prevFileName = row.getString(metaColPos.get(HoodieRecord.FILENAME_METADATA_FIELD))
      if (lastKnownPartitionPath == null && lastKnownfileName == null) {
        lastKnownPartitionPath = partitionPath
        lastKnownfileName = prevFileName
        handle = new HoodieRowUpdateHandle(
          hoodieTable.asInstanceOf[HoodieTable[_ <: HoodieRecordPayload[_ <: HoodieRecordPayload[_ <: AnyRef]], _, _, _]],
          hoodieWriteConfig,
          partitionPath,
          FSUtils.getFileId(prevFileName),
          instantTime,
          partitionId,
          TaskContext.get().taskAttemptId(),
          0,
          writeStructType,
          prevFileName,
          row.numFields - 1,
          updateFieldsIndex.map(new Integer(_)).asJava,
          Map.empty[Integer, org.apache.hudi.common.util.collection.Pair[Integer, DataType]].asJava
        )
      } else if (lastKnownPartitionPath != partitionPath && lastKnownfileName != prevFileName) {
        throw new HoodieCommitException("Unexpected update, each task should update only one file group")
      }
      handle.write(row)
    }
    if (handle != null) {
      val writeStatus = handle.close()
      if (handle.isFileUpdated && writeStatus.hasErrors) {
        writeStatus.getFailedRecordKeys.asScala.foreach(pair =>
          log.error("Failed Record Key: " + pair.getKey + " Exception: " + pair.getValue))
        throw new HoodieCommitException(s"Failed to update all records for file ${handle.getFileName}.")
      } else if (handle.isFileUpdated) {
        log.info(s"Successfully using file ${handle.getFileName} to update $lastKnownfileName")
        Seq(writeStatus.getStat).iterator
      } else {
        log.info(s"No record need to be updated for file $lastKnownfileName")
        Seq.empty[HoodieWriteStat].iterator
      }
    } else {
      Seq.empty[HoodieWriteStat].iterator
    }
  }
}
