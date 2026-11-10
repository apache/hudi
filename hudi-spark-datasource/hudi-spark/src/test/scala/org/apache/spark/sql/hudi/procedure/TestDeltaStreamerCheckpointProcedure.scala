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

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.DataSourceWriteOptions.{ORDERING_FIELDS, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.table.{HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.sql.SaveMode

class TestDeltaStreamerCheckpointProcedure extends HoodieSparkProcedureTestBase {

  Seq("cow", "mor").foreach { tableType =>
    test(s"get and set deltastreamer checkpoint for $tableType table") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  ts long
             |) using hudi
             | location '$tablePath'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             | )
             |""".stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 1000)")

        assertResult(0) {
          spark.sql(s"call get_deltastreamer_checkpoint(table => '$tableName')").count()
        }

        checkAnswer(
          s"call set_deltastreamer_checkpoint(table => '$tableName', checkpoint => 'checkpoint-1')")(
          Seq("checkpoint-1"))
        checkAnswer(s"call get_deltastreamer_checkpoint(table => '$tableName')")(
          Seq("checkpoint-1"))

        checkAnswer(s"select id, name, ts from $tableName")(Seq(1, "a1", 1000L))

        // Both procedures can address a table by path when it is not registered in the catalog.
        checkAnswer(
          s"call set_deltastreamer_checkpoint(path => '$tablePath', checkpoint => 'checkpoint-2')")(
          Seq("checkpoint-2"))
        checkAnswer(s"call get_deltastreamer_checkpoint(path => '$tablePath')")(
          Seq("checkpoint-2"))

        checkExceptionContain(
          s"call set_deltastreamer_checkpoint(table => '$tableName', checkpoint => '   ')")(
          "DeltaStreamer checkpoint must not be empty")
        checkAnswer(s"call get_deltastreamer_checkpoint(table => '$tableName')")(
          Seq("checkpoint-2"))
      }
    }
  }

  test("setting checkpoint does not remove metadata table partitions") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.index.column.stats.enable = 'true'
           | )
           |""".stripMargin)
      spark.sql(s"insert into $tableName values (1, 'a1', 1000)")

      val metadataPartitionsBefore = loadMetaClient(tablePath).getTableConfig.getMetadataPartitions
      assert(metadataPartitionsBefore.contains("files"))
      assert(metadataPartitionsBefore.contains("column_stats"))

      checkAnswer(
        s"call set_deltastreamer_checkpoint(table => '$tableName', checkpoint => 'checkpoint-1')")(
        Seq("checkpoint-1"))

      assertResult(metadataPartitionsBefore) {
        loadMetaClient(tablePath).getTableConfig.getMetadataPartitions
      }
    }
  }

  test("setting checkpoint does not schedule table services") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
           |""".stripMargin)
      spark.sql(s"insert into $tableName values (1, 'a1', 1000)")

      withSQLConf(
        HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT.key -> "true",
        HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1") {
        checkAnswer(
          s"call set_deltastreamer_checkpoint(table => '$tableName', checkpoint => 'checkpoint-1')")(
          Seq("checkpoint-1"))
      }

      assert(loadMetaClient(tablePath).getActiveTimeline.filterPendingCompactionTimeline.empty())
    }
  }

  test("setting checkpoint preserves table version and pending instants") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val hoodieTableName = tableName.split('.').last
      val tablePath = s"${tmp.getCanonicalPath}/$hoodieTableName"
      import spark.implicits._
      Seq((1, "a1", 1000L)).toDF("id", "name", "ts")
        .write.format("hudi")
        .option(HoodieWriteConfig.TBL_NAME.key, hoodieTableName)
        .option(TABLE_TYPE.key, "COPY_ON_WRITE")
        .option(RECORDKEY_FIELD.key, "id")
        .option(ORDERING_FIELDS.key, "ts")
        .option(HoodieWriteConfig.WRITE_TABLE_VERSION.key, HoodieTableVersion.EIGHT.versionCode().toString)
        .option(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key, "false")
        .option("hoodie.metadata.enable", "false")
        .mode(SaveMode.Overwrite)
        .save(tablePath)
      spark.sql(s"create table $tableName using hudi location '$tablePath'")

      val metaClient = loadMetaClient(tablePath)
      assertResult(HoodieTableVersion.EIGHT)(metaClient.getTableConfig.getTableVersion)
      val pendingInstantTime = metaClient.createNewInstantTime(false)
      val requested = metaClient.getTimelineLayout.getInstantGenerator.createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, pendingInstantTime)
      metaClient.getActiveTimeline.createNewInstant(requested)
      metaClient.getActiveTimeline.transitionRequestedToInflight(
        requested, HOption.empty[Array[Byte]]())

      checkAnswer(
        s"call set_deltastreamer_checkpoint(table => '$tableName', checkpoint => 'checkpoint-1')")(
        Seq("checkpoint-1"))

      val updatedMetaClient = loadMetaClient(tablePath)
      assertResult(HoodieTableVersion.EIGHT)(updatedMetaClient.getTableConfig.getTableVersion)
      val preservedPendingInstant = updatedMetaClient.getActiveTimeline
        .filterInflightsAndRequested()
        .getInstantsAsStream
        .filter(instant => instant.requestedTime == pendingInstantTime)
        .findFirst()
      assert(preservedPendingInstant.isPresent)
      assert(preservedPendingInstant.get.isInflight)
      checkAnswer(s"call get_deltastreamer_checkpoint(table => '$tableName')")(
        Seq("checkpoint-1"))
    }
  }

  private def loadMetaClient(tablePath: String): HoodieTableMetaClient = {
    HoodieTableMetaClient.builder()
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .setBasePath(tablePath)
      .build()
  }
}
