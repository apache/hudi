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

package org.apache.hudi.cli

import java.util.stream.Collectors

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieBaseFile, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.exception.HoodieException
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.{Buffer, HashMap, HashSet, ListBuffer}

/**
  * Spark job to de-duplicate data present in a partition path
  */
class DedupeSparkJob(basePath: String,
                     duplicatedPartitionPath: String,
                     repairOutputPath: String,
                     sqlContext: SQLContext,
                     fs: FileSystem,
                     dedupeType: DeDupeType.Value) {

  val sparkHelper = new SparkHelper(sqlContext, fs)
  val LOG = Logger.getLogger(this.getClass)


  /**
    *
    * @param tblName
    * @return
    */
  def getDupeKeyDF(tblName: String): DataFrame = {
    val dupeSql =
      s"""
      select  `${HoodieRecord.RECORD_KEY_METADATA_FIELD}` as dupe_key,
      count(*) as dupe_cnt
      from ${tblName}
      group by `${HoodieRecord.RECORD_KEY_METADATA_FIELD}`
      having dupe_cnt > 1
      """
    sqlContext.sql(dupeSql)
  }


  /**
    *
    * Check a given partition for duplicates and suggest the deletions that need to be done in each file,
    * in order to set things right.
    *
    * @return
    */
  private def planDuplicateFix(): HashMap[String, HashSet[String]] = {

    val tmpTableName = s"htbl_${System.currentTimeMillis()}"
    val dedupeTblName = s"${tmpTableName}_dupeKeys"

    val metadata = HoodieTableMetaClient.builder().setConf(fs.getConf).setBasePath(basePath).build()

    val allFiles = fs.listStatus(new org.apache.hadoop.fs.Path(s"$basePath/$duplicatedPartitionPath"))
    val fsView = new HoodieTableFileSystemView(metadata, metadata.getActiveTimeline.getCommitTimeline.filterCompletedInstants(), allFiles)
    val latestFiles: java.util.List[HoodieBaseFile] = fsView.getLatestBaseFiles().collect(Collectors.toList[HoodieBaseFile]())
    val filteredStatuses = latestFiles.map(f => f.getPath)
    LOG.info(s" List of files under partition: ${} =>  ${filteredStatuses.mkString(" ")}")

    val df = sqlContext.parquetFile(filteredStatuses: _*)
    df.registerTempTable(tmpTableName)
    val dupeKeyDF = getDupeKeyDF(tmpTableName)
    dupeKeyDF.registerTempTable(dedupeTblName)

    // Obtain necessary satellite information for duplicate rows
    val dupeDataSql =
      s"""
        SELECT `_hoodie_record_key`, `_hoodie_partition_path`, `_hoodie_file_name`, `_hoodie_commit_time`
        FROM $tmpTableName h
        JOIN $dedupeTblName d
        ON h.`_hoodie_record_key` = d.dupe_key
                      """
    val dupeMap = sqlContext.sql(dupeDataSql).collectAsList().groupBy(r => r.getString(0))
    getDedupePlan(dupeMap)
  }

  private def getDedupePlan(dupeMap: Map[String, Buffer[Row]]): HashMap[String, HashSet[String]] = {
    val fileToDeleteKeyMap = new HashMap[String, HashSet[String]]()
    dupeMap.foreach(rt => {
      val (key, rows) = rt

      dedupeType match {
        case DeDupeType.UPDATE_TYPE =>
          /*
          This corresponds to the case where all duplicates have been updated at least once.
          Once updated, duplicates are bound to have same commit time unless forcefully modified.
          */
          rows.init.foreach(r => {
            val f = r(2).asInstanceOf[String].split("_")(0)
            if (!fileToDeleteKeyMap.contains(f)) {
              fileToDeleteKeyMap(f) = HashSet[String]()
            }
            fileToDeleteKeyMap(f).add(key)
          })

        case DeDupeType.INSERT_TYPE =>
          /*
          This corresponds to the case where duplicates got created due to INSERT and have never been updated.
          */
          var maxCommit = -1L

          rows.foreach(r => {
            val c = r(3).asInstanceOf[String].toLong
            if (c > maxCommit)
              maxCommit = c
          })

          rows.foreach(r => {
            val c = r(3).asInstanceOf[String].toLong
            if (c != maxCommit) {
              val f = r(2).asInstanceOf[String].split("_")(0)
              if (!fileToDeleteKeyMap.contains(f)) {
                fileToDeleteKeyMap(f) = HashSet[String]()
              }
              fileToDeleteKeyMap(f).add(key)
            }
          })

        case DeDupeType.UPSERT_TYPE =>
          /*
          This corresponds to the case where duplicates got created as a result of inserts as well as updates,
          i.e few duplicate records have been updated, while others were never updated.
           */
          var maxCommit = -1L

          rows.foreach(r => {
            val c = r(3).asInstanceOf[String].toLong
            if (c > maxCommit)
              maxCommit = c
          })
          val rowsWithMaxCommit = new ListBuffer[Row]()
          rows.foreach(r => {
            val c = r(3).asInstanceOf[String].toLong
            if (c != maxCommit) {
              val f = r(2).asInstanceOf[String].split("_")(0)
              if (!fileToDeleteKeyMap.contains(f)) {
                fileToDeleteKeyMap(f) = HashSet[String]()
              }
              fileToDeleteKeyMap(f).add(key)
            } else {
              rowsWithMaxCommit += r
            }
          })

          rowsWithMaxCommit.toList.init.foreach(r => {
            val f = r(2).asInstanceOf[String].split("_")(0)
            if (!fileToDeleteKeyMap.contains(f)) {
              fileToDeleteKeyMap(f) = HashSet[String]()
            }
            fileToDeleteKeyMap(f).add(key)
          })

        case _ => throw new IllegalArgumentException("Please provide valid type for deduping!")
      }
    })
    LOG.debug(s"fileToDeleteKeyMap size: ${fileToDeleteKeyMap.size}, map: $fileToDeleteKeyMap")
    fileToDeleteKeyMap
  }

  def fixDuplicates(dryRun: Boolean = true) = {
    val metadata = HoodieTableMetaClient.builder().setConf(fs.getConf).setBasePath(basePath).build()

    val allFiles = fs.listStatus(new Path(s"$basePath/$duplicatedPartitionPath"))
    val fsView = new HoodieTableFileSystemView(metadata, metadata.getActiveTimeline.getCommitTimeline.filterCompletedInstants(), allFiles)

    val latestFiles: java.util.List[HoodieBaseFile] = fsView.getLatestBaseFiles().collect(Collectors.toList[HoodieBaseFile]())

    val fileNameToPathMap = latestFiles.map(f => (f.getFileId, new Path(f.getPath))).toMap
    val dupeFixPlan = planDuplicateFix()

    // 1. Copy all latest files into the temp fix path
    fileNameToPathMap.foreach { case (fileName, filePath) =>
      val badSuffix = if (dupeFixPlan.contains(fileName)) ".bad" else ""
      val dstPath = new Path(s"$repairOutputPath/${filePath.getName}$badSuffix")
      LOG.info(s"Copying from $filePath to $dstPath")
      FileUtil.copy(fs, filePath, fs, dstPath, false, true, fs.getConf)
    }

    // 2. Remove duplicates from the bad files
    dupeFixPlan.foreach { case (fileName, keysToSkip) =>
      val instantTime = FSUtils.getCommitTime(fileNameToPathMap(fileName).getName)
      val badFilePath = new Path(s"$repairOutputPath/${fileNameToPathMap(fileName).getName}.bad")
      val newFilePath = new Path(s"$repairOutputPath/${fileNameToPathMap(fileName).getName}")
      LOG.info(" Skipping and writing new file for : " + fileName)
      SparkHelpers.skipKeysAndWriteNewFile(instantTime, fs, badFilePath, newFilePath, dupeFixPlan(fileName))
      fs.delete(badFilePath, true)
    }

    // 3. Check that there are no duplicates anymore.
    val df = sqlContext.read.parquet(s"$repairOutputPath/*.parquet")
    df.registerTempTable("fixedTbl")
    val dupeKeyDF = getDupeKeyDF("fixedTbl")
    val dupeCnt = dupeKeyDF.count()
    if (dupeCnt != 0) {
      dupeKeyDF.show()
      throw new HoodieException("Still found some duplicates!!.. Inspect output")
    }

    // 4. Additionally ensure no record keys are left behind.
    val sourceDF = sparkHelper.getDistinctKeyDF(fileNameToPathMap.map(t => t._2.toString).toList)
    val fixedDF = sparkHelper.getDistinctKeyDF(fileNameToPathMap.map(t => s"$repairOutputPath/${t._2.getName}").toList)
    val missedRecordKeysDF = sourceDF.except(fixedDF)
    val missedCnt = missedRecordKeysDF.count()
    if (missedCnt != 0) {
      missedRecordKeysDF.show()
      throw new HoodieException("Some records in source are not found in fixed files. Inspect output!!")
    }

    println("No duplicates found & counts are in check!!!! ")
    // 5. Prepare to copy the fixed files back.
    fileNameToPathMap.foreach { case (_, filePath) =>
      val srcPath = new Path(s"$repairOutputPath/${filePath.getName}")
      val dstPath = new Path(s"$basePath/$duplicatedPartitionPath/${filePath.getName}")
      if (dryRun) {
        LOG.info(s"[JUST KIDDING!!!] Copying from $srcPath to $dstPath")
      } else {
        // for real
        LOG.info(s"[FOR REAL!!!] Copying from $srcPath to $dstPath")
        FileUtil.copy(fs, srcPath, fs, dstPath, false, true, fs.getConf)
      }
    }
  }
}
