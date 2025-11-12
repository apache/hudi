/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.{DataSourceWriteOptions, HoodieCLIUtils, HoodieDataSourceHelpers}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.{Option => HOption}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hudi.command.procedures.{FileStatus, TimelineType}
import org.junit.jupiter.api.Assertions.assertTrue

import scala.collection.JavaConverters._

class TestShowFileStatusProcedure extends HoodieSparkProcedureTestBase {

  private val DEFAULT_VALUE = ""

  test("Test Call show_file_status Procedure By COW / MOR Partitioned Table") {
    withSQLConf(DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION.key() -> "upsert") {
      withTempDir { tmp =>
        Seq("mor", "cow").foreach { tableType =>

          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"

          // specify clean conf & archive conf & compaction conf
          spark.sql("set hoodie.clean.commits.retained = 2")
          spark.sql("set hoodie.keep.min.commits = 3")
          spark.sql("set hoodie.keep.max.commits = 4")
          spark.sql("set hoodie.compact.inline=false")
          spark.sql("set hoodie.compact.schedule.inline=false")

          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long,
               |  partition long
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
               | partitioned by(partition)
               | location '$basePath'
        """.stripMargin)

          val partition: String = "partition=1000"
          var before: List[String] = null
          var after: List[String] = null
          var cleanedDataFile: Option[String] = Option.empty

          val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())
          val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, Option(tableName))
          val metaClient: HoodieTableMetaClient = client.getInternalSchemaAndMetaClient.getRight

          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, 1000)")
          val firstCleanedDataFile = getAllDataFile(fs, basePath, Option.apply(partition)).toStream.filter(f => !f.startsWith(".")).head
          cleanedDataFile = Option.apply(firstCleanedDataFile)
          checkAnswer(s"call show_file_status(table => '$tableName', partition => '$partition', file => '${cleanedDataFile.get}')")(
            Seq(FileStatus.EXIST.toString, DEFAULT_VALUE, DEFAULT_VALUE, TimelineType.ACTIVE.toString, new Path(basePath, new Path(partition, cleanedDataFile.get)).toString))

          spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001, 1000)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1000)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1000)")

          checkAnswer(s"call show_file_status(table => '$tableName', partition => '$partition', file => '$firstCleanedDataFile')")(
            Seq(FileStatus.DELETED.toString, HoodieTimeline.CLEAN_ACTION, metaClient.reloadActiveTimeline().getCleanerTimeline.lastInstant().get.requestedTime, TimelineType.ACTIVE.toString, DEFAULT_VALUE)
          )

          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1001)")
          // clustering / compaction
          var newInstant: String = null
          if (tableType.equals("cow")) {
            newInstant = client.scheduleClustering(HOption.empty()).get()
            client.cluster(newInstant)
          } else {
            newInstant = client.scheduleCompaction(HOption.empty()).get()
            val result = client.compact(newInstant)
            client.commitCompaction(newInstant, result, HOption.empty())
            assertTrue(metaClient.reloadActiveTimeline.filterCompletedInstants.containsInstant(newInstant))
          }

          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1000)")
          spark.sql(s"insert into $tableName values(4, 'a4', 10, 1002, 1000)")

          // savepoint
          val savepointTime: String = getSpecifyActionLatestTime(fs, basePath, newInstant, 1).get
          spark.sql(s"call create_savepoint(table => '$tableName', commit_time => '$savepointTime')")
          spark.sql(s"insert into $tableName values(2, 'a2', 11, 1002, 1000)")
          spark.sql(s"insert into $tableName values(6, 'a6', 10, 1000, 1001)")

          // restore
          before = getAllDataFile(fs, basePath, Option.apply(partition))
          spark.sql(s"call rollback_to_savepoint(table => '$tableName', instant_time => '$savepointTime')")
          after = getAllDataFile(fs, basePath, Option.apply(partition))
          cleanedDataFile = getAnyOneDataFile(before, after)

          checkAnswer(s"call show_file_status(table => '$tableName', partition => '$partition', file => '${cleanedDataFile.get}')")(
            Seq(FileStatus.DELETED.toString, HoodieTimeline.RESTORE_ACTION, metaClient.reloadActiveTimeline().getRestoreTimeline.lastInstant().get.requestedTime, TimelineType.ACTIVE.toString, DEFAULT_VALUE))

          val latestTime = HoodieDataSourceHelpers.latestCommit(fs, basePath)
          spark.sql(s"insert into $tableName values(7, 'a7', 15, 1000, 1000)")
          spark.sql(s"insert into $tableName values(8, 'a8', 12, 1000, 1000)")

          before = getAllDataFile(fs, basePath, Option.apply(partition))
          // rollback
          val rollbackTime = getSpecifyActionLatestTime(fs, basePath, latestTime, 5).get
          spark.sql(s"call rollback_to_instant(table => '$tableName', instant_time => '$rollbackTime')")
          spark.sql(s"insert into $tableName values(9, 'a9', 16, 1000, 1000)")
          after = getAllDataFile(fs, basePath, Option.apply(partition))
          cleanedDataFile = getAnyOneDataFile(before, after)
          checkAnswer(s"call show_file_status(table => '$tableName', partition => '$partition', file => '${cleanedDataFile.get}')")(
            Seq(FileStatus.DELETED.toString, HoodieTimeline.ROLLBACK_ACTION, metaClient.reloadActiveTimeline().getRollbackTimeline.lastInstant().get.requestedTime, TimelineType.ACTIVE.toString, DEFAULT_VALUE))

          // unknown
          checkAnswer(s"call show_file_status(table => '$tableName', partition => '$partition', file => 'unknown')")(
            Seq(FileStatus.UNKNOWN.toString, DEFAULT_VALUE, DEFAULT_VALUE, DEFAULT_VALUE, DEFAULT_VALUE))
        }
      }
    }

  }

  test("Test Call show_file_status Procedure By COW / MOR Non_Partitioned Table") {
    withSQLConf(DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION.key() -> "upsert") {
      withTempDir { tmp =>
        Seq("mor", "cow").foreach { tableType =>

          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"

          // specify clean conf & archive conf & compaction conf
          spark.sql("set hoodie.clean.commits.retained = 2")
          spark.sql("set hoodie.keep.min.commits = 3")
          spark.sql("set hoodie.keep.max.commits = 4")
          spark.sql("set hoodie.compact.inline=false")
          spark.sql("set hoodie.compact.schedule.inline=false")

          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long,
               |  partition long
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
               | location '$basePath'
        """.stripMargin)

          val partition: Option[String] = Option.empty
          var before: List[String] = null
          var after: List[String] = null
          var cleanedDataFile: Option[String] = Option.empty

          val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())
          val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, Option(tableName))
          val metaClient: HoodieTableMetaClient = client.getInternalSchemaAndMetaClient.getRight

          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, 1000)")
          val firstCleanedDataFile = getAllDataFile(fs, basePath, partition).toStream.filter(f => !f.startsWith(".")).head
          cleanedDataFile = Option.apply(firstCleanedDataFile)
          checkAnswer(s"call show_file_status(table => '$tableName', file => '${cleanedDataFile.get}')")(
            Seq(FileStatus.EXIST.toString, DEFAULT_VALUE, DEFAULT_VALUE, TimelineType.ACTIVE.toString, new Path(basePath, new Path(cleanedDataFile.get)).toString))

          spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001, 1000)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1000)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1000)")

          checkAnswer(s"call show_file_status(table => '$tableName', file => '$firstCleanedDataFile')")(
            Seq(FileStatus.DELETED.toString, HoodieTimeline.CLEAN_ACTION, metaClient.reloadActiveTimeline().getCleanerTimeline.lastInstant().get.requestedTime, TimelineType.ACTIVE.toString, DEFAULT_VALUE)
          )

          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1000)")
          // clustering / compaction
          var newInstant: String = null
          if (tableType.equals("cow")) {
            newInstant = client.scheduleClustering(HOption.empty()).get()
            client.cluster(newInstant)
          } else {
            newInstant = client.scheduleCompaction(HOption.empty()).get()
            val result = client.compact(newInstant)
            client.commitCompaction(newInstant, result, HOption.empty())
            assertTrue(metaClient.reloadActiveTimeline.filterCompletedInstants.containsInstant(newInstant))
          }

          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1000)")
          spark.sql(s"insert into $tableName values(4, 'a4', 10, 1002, 1000)")

          // savepoint
          val savepointTime: String = getSpecifyActionLatestTime(fs, basePath, newInstant, 1).get
          spark.sql(s"call create_savepoint(table => '$tableName', commit_time => '$savepointTime')")
          spark.sql(s"insert into $tableName values(2, 'a2', 11, 1002, 1000)")
          spark.sql(s"insert into $tableName values(6, 'a6', 10, 1000, 1000)")

          // restore
          before = getAllDataFile(fs, basePath, partition)
          spark.sql(s"call rollback_to_savepoint(table => '$tableName', instant_time => '$savepointTime')")
          after = getAllDataFile(fs, basePath, partition)
          cleanedDataFile = getAnyOneDataFile(before, after)
          checkAnswer(s"call show_file_status(table => '$tableName', file => '${cleanedDataFile.get}')")(
            Seq(FileStatus.DELETED.toString, HoodieTimeline.RESTORE_ACTION,
              metaClient.reloadActiveTimeline().getRestoreTimeline.lastInstant().get.requestedTime, TimelineType.ACTIVE.toString, DEFAULT_VALUE))

          val latestTime = HoodieDataSourceHelpers.latestCommit(fs, basePath)
          spark.sql(s"insert into $tableName values(7, 'a7', 15, 1000, 1000)")
          spark.sql(s"insert into $tableName values(8, 'a8', 12, 1000, 1000)")

          before = getAllDataFile(fs, basePath, partition)
          // rollback
          val rollbackTime = getSpecifyActionLatestTime(fs, basePath, latestTime, 5).get
          spark.sql(s"call rollback_to_instant(table => '$tableName', instant_time => '$rollbackTime')")
          spark.sql(s"insert into $tableName values(9, 'a9', 16, 1000, 1000)")
          after = getAllDataFile(fs, basePath, partition)
          cleanedDataFile = getAnyOneDataFile(before, after)
          checkAnswer(s"call show_file_status(table => '$tableName', file => '${cleanedDataFile.get}')")(
            Seq(FileStatus.DELETED.toString, HoodieTimeline.ROLLBACK_ACTION, metaClient.reloadActiveTimeline().getRollbackTimeline.lastInstant().get.requestedTime, TimelineType.ACTIVE.toString, DEFAULT_VALUE))

          // unknown
          checkAnswer(s"call show_file_status(table => '$tableName', file => 'unknown')")(
            Seq(FileStatus.UNKNOWN.toString, DEFAULT_VALUE, DEFAULT_VALUE, DEFAULT_VALUE, DEFAULT_VALUE))
        }
      }
    }
  }


  test("Test Call show_file_status Procedure By COW / MOR For Archive") {
    withTempDir { tmp =>
      Seq("mor", "cow").foreach { tableType =>

        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"

        // specify clean conf & archive conf & compaction conf
        spark.sql("set hoodie.clean.commits.retained = 2")
        spark.sql("set hoodie.keep.min.commits = 3")
        spark.sql("set hoodie.keep.max.commits = 4")

        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  partition long
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  preCombineField = 'ts'
             | )
             | partitioned by(partition)
             | location '$basePath'
        """.stripMargin)

        val partition: String = "partition=1000"
        var cleanedDataFile: String = null

        val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, 1000)")
        cleanedDataFile = getAllDataFile(fs, basePath, Option.apply(partition)).toStream.filter(f => !f.startsWith(".")).head
        val firstCleanedDataFile = cleanedDataFile

        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001, 1000)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1000)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1000)")

        val firstCleanedDataTime =
          spark.sql(s"call show_file_status(table => '$tableName', partition => '$partition', file => '$firstCleanedDataFile')")
            .collect().toList.head.get(2)

        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1000)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1000)")
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1002, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 11, 1002, 1000)")
        spark.sql(s"insert into $tableName values(6, 'a6', 10, 1000, 1000)")
        spark.sql(s"insert into $tableName values(7, 'a7', 15, 1000, 1000)")
        spark.sql(s"insert into $tableName values(8, 'a8', 12, 1000, 1000)")
        spark.sql(s"insert into $tableName values(9, 'a9', 16, 1000, 1000)")

        checkAnswer(s"call show_file_status(table => '$tableName',  partition => '$partition', file => '$firstCleanedDataFile')")(
          Seq(FileStatus.DELETED.toString, HoodieTimeline.CLEAN_ACTION, firstCleanedDataTime, TimelineType.ARCHIVED.toString, DEFAULT_VALUE))
      }
    }
  }

  private def getAnyOneDataFile(before: List[String], after: List[String]): Option[String] = {
    val sortedBefore = before.sorted
    val sortedAfter = after.sorted
    sortedBefore.diff(sortedAfter).headOption
  }

  private def getAllDataFile(fs: FileSystem, basePath: String, partition: Option[String]): List[String] = {
    if (partition.isDefined) {
      fs.listStatus(new Path(basePath, partition.get)).filter(f => f.isFile && !f.getPath.getName.startsWith(".hoodie_partition_metadata")).map(f => f.getPath.getName).toList
    } else {
      fs.listStatus(new Path(basePath)).filter(f => f.isFile && !f.getPath.getName.startsWith(".hoodie_partition_metadata")).map(f => f.getPath.getName).toList
    }
  }


  private def getSpecifyActionLatestTime(fs: FileSystem, basePath: String, specifyInstant: String, after: Int): Option[String] = {
    val commitsSince = HoodieDataSourceHelpers.listCommitsSince(fs, basePath, specifyInstant).asScala.toList
    if (commitsSince.length > after) {
      Some(commitsSince(after))
    } else {
      commitsSince.lastOption
    }
  }
}

