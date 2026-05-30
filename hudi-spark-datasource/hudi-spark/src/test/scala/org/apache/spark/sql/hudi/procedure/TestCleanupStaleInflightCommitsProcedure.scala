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

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.JavaConverters._

class TestCleanupStaleInflightCommitsProcedure extends HoodieSparkProcedureTestBase {

  /**
   * Creates a table DDL without inserting data. Tests that manipulate inflight instants must NOT
   * insert data before injecting the inflight, because BaseRollbackActionExecutor
   * .validateRollbackCommitSequence throws HoodieRollbackException when committed instants exist
   * after the injected (old) timestamp and no heartbeat exists for the injected instant.
   * With no prior inserts, commitTimeline.empty() = true and the guard is bypassed.
   */
  private def createEmptyTable(tableName: String, tablePath: String): Unit = {
    spark.sql(
      s"""
         |create table $tableName (
         | id int,
         | name string,
         | ts long
         | ) using hudi
         | location '$tablePath'
         | tblproperties (
         |   primaryKey = 'id',
         |   type = 'cow',
         |   preCombineField = 'ts',
         |   hoodie.metadata.enable = "false"
         | )
         |""".stripMargin)
  }

  private def createEmptyPartitionedTable(tableName: String, tablePath: String, tableType: String): Unit = {
    spark.sql(
      s"""
         |create table $tableName (
         | id int,
         | name string,
         | ts long,
         | part int
         | ) using hudi
         | partitioned by (part)
         | location '$tablePath'
         | tblproperties (
         |   primaryKey = 'id',
         |   type = '$tableType',
         |   preCombineField = 'ts',
         |   hoodie.metadata.enable = "false"
         | )
         |""".stripMargin)
  }

  test("Test cleanup_stale_inflight_commits returns empty when no stale inflights exist") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createEmptyTable(tableName, tmp.getCanonicalPath)
      spark.sql(s"insert into $tableName values(1, 'a1', 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 2000)")

      val result = spark.sql(s"call cleanup_stale_inflight_commits(table => '$tableName')").collect()
      assertResult(0)(result.length)
    }
  }

  test("Test cleanup_stale_inflight_commits rolls back stale REPLACE_COMMIT_ACTION inflight") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      // No inserts before injecting — see createEmptyTable docstring
      createEmptyTable(tableName, tablePath)

      val staleTs = "20200101120000"
      // Must use REPLACE_COMMIT_ACTION: inflightWriteCommitsOlderThan with
      // include_ingestion_commits=false (default) filters out COMMIT_ACTION and DELTA_COMMIT_ACTION.
      // REPLACE_COMMIT_ACTION is included in both getWriteTimeline() and getCommitsTimeline(),
      // so client.rollback() finds it and returns true.
      injectInflightInstant(tablePath, HoodieTimeline.REPLACE_COMMIT_ACTION, staleTs)

      val result = spark.sql(
        s"call cleanup_stale_inflight_commits(table => '$tableName', " +
          s"allowed_inflight_interval_minutes => 1)").collect()

      assertResult(1)(result.length)
      assertResult(staleTs)(result(0).getString(0))
      assertResult(HoodieTimeline.REPLACE_COMMIT_ACTION)(result(0).getString(1))
      assertResult(true)(result(0).getBoolean(2))

      // Verify the instant is gone from the active timeline
      val metaClient = HoodieTableMetaClient.builder
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .setBasePath(tablePath)
        .build
      val remaining = metaClient.reloadActiveTimeline().filterInflightsAndRequested()
        .getInstants.asScala
      assert(!remaining.exists(_.requestedTime == staleTs),
        s"Stale instant $staleTs should have been removed from the timeline after rollback")
    }
  }

  test("Test cleanup_stale_inflight_commits respects allowed_inflight_interval_minutes threshold") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      // No inserts before injecting — see createEmptyTable docstring
      createEmptyTable(tableName, tablePath)

      val staleTs = "20200101120000"
      val freshTs = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

      injectInflightInstant(tablePath, HoodieTimeline.REPLACE_COMMIT_ACTION, staleTs)
      injectInflightInstant(tablePath, HoodieTimeline.REPLACE_COMMIT_ACTION, freshTs)

      // 60-minute threshold: only the stale instant qualifies; fresh instant is too recent
      val result = spark.sql(
        s"call cleanup_stale_inflight_commits(table => '$tableName', " +
          s"allowed_inflight_interval_minutes => 60)").collect()

      assertResult(1)(result.length)
      assertResult(staleTs)(result(0).getString(0))
    }
  }

  test("Test cleanup_stale_inflight_commits cleans COMMIT_ACTION with include_ingestion_commits=true") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      // No inserts before injecting — see createEmptyTable docstring
      createEmptyTable(tableName, tablePath)

      val staleTs = "20200101120000"
      // COMMIT_ACTION is filtered out with the default include_ingestion_commits=false,
      // but included when include_ingestion_commits=true.
      // client.rollback returns true for COMMIT_ACTION since getCommitsTimeline() includes it.
      injectInflightInstant(tablePath, HoodieTimeline.COMMIT_ACTION, staleTs)

      // Default (include_ingestion_commits=false): should not find COMMIT_ACTION inflight
      val defaultResult = spark.sql(
        s"call cleanup_stale_inflight_commits(table => '$tableName', " +
          s"allowed_inflight_interval_minutes => 1)").collect()
      assertResult(0)(defaultResult.length)

      // With include_ingestion_commits=true: should find and process COMMIT_ACTION inflight
      val result = spark.sql(
        s"call cleanup_stale_inflight_commits(table => '$tableName', " +
          s"allowed_inflight_interval_minutes => 1, " +
          s"include_ingestion_commits => true)").collect()

      assertResult(1)(result.length)
      assertResult(staleTs)(result(0).getString(0))
      assertResult(HoodieTimeline.COMMIT_ACTION)(result(0).getString(1))
      assertResult(true)(result(0).getBoolean(2))
    }
  }

  test("Test cleanup_stale_inflight_commits dry_run lists matched instants without rolling back") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      createEmptyTable(tableName, tablePath)

      val staleTs = "20200101120000"
      injectInflightInstant(tablePath, HoodieTimeline.REPLACE_COMMIT_ACTION, staleTs)

      val result = spark.sql(
        s"call cleanup_stale_inflight_commits(table => '$tableName', " +
          s"allowed_inflight_interval_minutes => 1, " +
          s"dry_run => true)").collect()

      assertResult(1)(result.length)
      assertResult(staleTs)(result(0).getString(0))
      assertResult(HoodieTimeline.REPLACE_COMMIT_ACTION)(result(0).getString(1))
      // dry_run: rollback_status is NULL meaning "matched but not actioned"
      assert(result(0).isNullAt(2),
        "Expected rollback_status=NULL in dry_run mode, but got non-null value")

      // Verify the instant is STILL on the active timeline (dry_run did not act)
      val metaClient = HoodieTableMetaClient.builder
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .setBasePath(tablePath)
        .build
      val remaining = metaClient.reloadActiveTimeline().filterInflightsAndRequested()
        .getInstants.asScala
      assert(remaining.exists(_.requestedTime == staleTs),
        s"dry_run should not have rolled back $staleTs; expected to find it still on the timeline")
    }
  }

  test("Test cleanup_stale_inflight_commits rolls back stale COMPACTION_ACTION inflight via table.rollbackInflightCompaction") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // Compaction is MOR-only. Use the real schedule + run + delete-commit pattern from
      // TestRunRollbackInflightTableServiceProcedure to construct a valid compaction inflight.
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | tblproperties (
           |  primaryKey = 'id',
           |  type = 'mor',
           |  preCombineField = 'ts'
           | )
           | partitioned by(ts)
           | location '$tablePath'
       """.stripMargin)
      withSQLConf(
        "hoodie.parquet.max.file.size" -> "10000",
        "hoodie.compact.inline" -> "false",
        "hoodie.compact.schedule.inline" -> "false",
        // Prevent auto-clean from creating a clean instant after compaction completion,
        // which would shift getReverseOrderedInstants.findFirst() away from the compaction commit.
        "hoodie.clean.automatic" -> "false"
      ) {
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1000)")
        spark.sql(s"update $tableName set price = 11 where id = 1")

        spark.sql(s"call run_compaction(op => 'schedule', table => '$tableName')")
        spark.sql(s"call run_compaction(op => 'run', table => '$tableName')")

        // Delete the completed compaction commit file so the inflight remains
        val metaClient = HoodieTableMetaClient.builder
          .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
          .setBasePath(tablePath)
          .build
        val compactionInstant = metaClient.getActiveTimeline.getReverseOrderedInstants.findFirst().get()
        metaClient.getActiveTimeline.deleteInstantFileIfExists(compactionInstant)
        val compactionInstantTime = compactionInstant.requestedTime

        // Confirm the compaction inflight is actually present before we call cleanup.
        // If this assertion fires, the test setup (schedule+run+delete) didn't produce the expected
        // state and the rest of the test is moot — fail with a clear diagnostic instead of an empty result.
        val reloadedTimeline = metaClient.reloadActiveTimeline()
        val compactionInflightPresent = reloadedTimeline.getWriteTimeline.filterInflightsAndRequested.getInstants.asScala
          .exists(i => i.getAction == HoodieTimeline.COMPACTION_ACTION && i.requestedTime == compactionInstantTime)
        assert(compactionInflightPresent,
          s"Setup failure: compaction inflight at $compactionInstantTime not present after deleting completed commit. " +
            s"Active timeline: ${reloadedTimeline.getInstants.asScala.map(i => s"${i.requestedTime}/${i.getAction}/${i.getState}").mkString(", ")}")

        // Sleep so the second-precision cutoff timestamp is strictly newer than the inflight's timestamp
        Thread.sleep(2000)

        val result = spark.sql(
          s"call cleanup_stale_inflight_commits(table => '$tableName', " +
            s"allowed_inflight_interval_minutes => 0)").collect()

        val compactionRow = result.find(r => r.getString(0) == compactionInstantTime)
        assert(compactionRow.isDefined,
          s"Expected compaction inflight $compactionInstantTime in result; got ${result.map(r => s"${r.getString(0)}/${r.getString(1)}").mkString(",")}")
        assertResult(HoodieTimeline.COMPACTION_ACTION)(compactionRow.get.getString(1))
        assertResult(true)(compactionRow.get.getBoolean(2))

        // Inflight should be removed by table.rollbackInflightCompaction
        val instantGenerator = metaClient.getTimelineLayout.getInstantGenerator
        val expectedInflight = instantGenerator.createNewInstant(
          HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionInstantTime)
        assert(!metaClient.reloadActiveTimeline().getInstants.contains(expectedInflight),
          s"Compaction inflight $compactionInstantTime should be gone after rollback")
      }
    }
  }

  test("Test cleanup_stale_inflight_commits rolls back stale CLUSTERING_ACTION inflight via table.rollbackInflightClustering") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // Use the real schedule + execute + delete-commit pattern from
      // TestRunRollbackInflightTableServiceProcedure so the clustering inflight is valid.
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | tblproperties (
           |  primaryKey = 'id',
           |  type = 'cow',
           |  preCombineField = 'ts'
           | )
           | partitioned by(ts)
           | location '$tablePath'
       """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

      spark.sql(s"call run_clustering(table => '$tableName', op => 'schedule')")
      spark.sql(s"call run_clustering(table => '$tableName', op => 'execute')")

      val metaClient = HoodieTableMetaClient.builder
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .setBasePath(tablePath)
        .build
      val clusteringInstant = metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.get(0)
      metaClient.getActiveTimeline.deleteInstantFileIfExists(clusteringInstant)
      val clusteringInstantTime = clusteringInstant.requestedTime

      Thread.sleep(2000)

      val result = spark.sql(
        s"call cleanup_stale_inflight_commits(table => '$tableName', " +
          s"allowed_inflight_interval_minutes => 0)").collect()

      val clusteringRow = result.find(r => r.getString(0) == clusteringInstantTime)
      assert(clusteringRow.isDefined,
        s"Expected clustering inflight $clusteringInstantTime in result; got ${result.map(_.getString(0)).mkString(",")}")
      assertResult(HoodieTimeline.CLUSTERING_ACTION)(clusteringRow.get.getString(1))
      assertResult(true)(clusteringRow.get.getBoolean(2))

      val instantGenerator = metaClient.getTimelineLayout.getInstantGenerator
      val expectedInflight = instantGenerator.createNewInstant(
        HoodieInstant.State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, clusteringInstantTime)
      assert(!metaClient.reloadActiveTimeline().getInstants.contains(expectedInflight),
        s"Clustering inflight $clusteringInstantTime should be gone after rollback")
    }
  }

  test("Test cleanup_stale_inflight_commits handles partitioned COW table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      createEmptyPartitionedTable(tableName, tablePath, "cow")

      val staleTs = "20200101120000"
      injectInflightInstant(tablePath, HoodieTimeline.REPLACE_COMMIT_ACTION, staleTs)

      val result = spark.sql(
        s"call cleanup_stale_inflight_commits(table => '$tableName', " +
          s"allowed_inflight_interval_minutes => 1)").collect()

      assertResult(1)(result.length)
      assertResult(staleTs)(result(0).getString(0))
      assertResult(HoodieTimeline.REPLACE_COMMIT_ACTION)(result(0).getString(1))
      assertResult(true)(result(0).getBoolean(2))
    }
  }

  test("Test cleanup_stale_inflight_commits handles MOR table DELTA_COMMIT_ACTION with include_ingestion_commits") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      createEmptyPartitionedTable(tableName, tablePath, "mor")

      val staleTs = "20200101120000"
      injectInflightInstant(tablePath, HoodieTimeline.DELTA_COMMIT_ACTION, staleTs)

      // Default (include_ingestion_commits=false): DELTA_COMMIT_ACTION is filtered out
      val defaultResult = spark.sql(
        s"call cleanup_stale_inflight_commits(table => '$tableName', " +
          s"allowed_inflight_interval_minutes => 1)").collect()
      assertResult(0)(defaultResult.length)

      // With include_ingestion_commits=true: DELTA_COMMIT_ACTION is processed
      val result = spark.sql(
        s"call cleanup_stale_inflight_commits(table => '$tableName', " +
          s"allowed_inflight_interval_minutes => 1, " +
          s"include_ingestion_commits => true)").collect()

      assertResult(1)(result.length)
      assertResult(staleTs)(result(0).getString(0))
      assertResult(HoodieTimeline.DELTA_COMMIT_ACTION)(result(0).getString(1))
      assertResult(true)(result(0).getBoolean(2))
    }
  }

  /**
   * Injects a REQUESTED→INFLIGHT instant into the active timeline without completing it.
   * Used to simulate stale inflight operations for testing.
   */
  private def injectInflightInstant(tablePath: String, action: String, instantTime: String): Unit = {
    val metaClient = HoodieTableMetaClient.builder
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .setBasePath(tablePath)
      .build
    val timeline = metaClient.getActiveTimeline
    val instantGenerator = metaClient.getTimelineLayout.getInstantGenerator
    val requested = instantGenerator.createNewInstant(HoodieInstant.State.REQUESTED, action, instantTime)
    timeline.createNewInstant(requested)
    timeline.transitionRequestedToInflight(requested, HOption.empty[Array[Byte]]())
  }
}
