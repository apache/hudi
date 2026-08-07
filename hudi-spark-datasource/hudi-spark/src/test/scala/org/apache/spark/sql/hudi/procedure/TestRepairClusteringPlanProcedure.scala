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

import org.apache.hudi.avro.model.HoodieClusteringPlan
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.ClusteringUtils
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.replaceWithEmptyFile
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertTrue}

import scala.collection.JavaConverters._

class TestRepairClusteringPlanProcedure extends HoodieSparkProcedureTestBase {

  test("Test repair_clustering_plan dry run, delete and validate delete") {
    withTempDir { tmp =>
      withSQLConf("hoodie.parquet.small.file.limit" -> "0") {
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"

        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  dt string
             |) using hudi
             | tblproperties (
             |  primaryKey = 'id',
             |  type = 'cow',
             |  orderingFields = 'ts',
             |  hoodie.metadata.enable = 'false'
             | )
             | partitioned by (dt)
             | location '$basePath'
             |""".stripMargin)

        insertRows(tableName, "2026-01-27", 1)
        insertRows(tableName, "2026-01-27", 4)
        insertRows(tableName, "2026-01-27", 7)

        runClustering(tableName, "schedule")
        var metadata = getLatestRequestedClusteringPlan(basePath)
        val instant1 = metadata.getLeft
        val plannedFiles = getPlanDataFiles(metadata.getRight)
        assertTrue(plannedFiles.size > 1, s"Expected more than one planned file, but got $plannedFiles")

        val fileToRemove = plannedFiles.head
        val dryRunRows = spark.sql(
          s"call repair_clustering_plan(table => '$tableName', instant => '${instant1.requestedTime}', " +
            s"op => 'delete', invalid_parquet_files => '$fileToRemove')").collect()
        assertRepairResult(dryRunRows, instant1.requestedTime, fileToRemove, "WOULD_REMOVE_FROM_PLAN", deleted = false, "USER_REQUESTED")
        assertTrue(getPlanDataFiles(getLatestRequestedClusteringPlan(basePath).getRight).contains(fileToRemove))

        val deleteRows = spark.sql(
          s"call repair_clustering_plan(table => '$tableName', instant => '${instant1.requestedTime}', " +
            s"op => 'delete', invalid_parquet_files => '$fileToRemove', dry_run => false)").collect()
        assertRepairResult(deleteRows, instant1.requestedTime, fileToRemove, "REMOVED_FROM_PLAN", deleted = false, "USER_REQUESTED")

        metadata = getLatestRequestedClusteringPlan(basePath)
        assertFalse(getPlanDataFiles(metadata.getRight).contains(fileToRemove))

        var metaClient = createMetaClient(spark, basePath)
        assertTrue(metaClient.getStorage.exists(new StoragePath(fileToRemove)))
        assertTrue(metaClient.getStorage.exists(new StoragePath(metaClient.getMetaPath, ".repair")))

        runClustering(tableName, "execute", Some(instant1.requestedTime))
        assertClusteringCompleted(basePath, instant1.requestedTime)
        assertEquals(9, spark.sql(s"select id from $tableName where dt = '2026-01-27'").collect().length)

        checkExceptionContain(
          s"call repair_clustering_plan(table => '$tableName', instant => '${instant1.requestedTime}', " +
            s"op => 'delete', invalid_parquet_files => '$fileToRemove', dry_run => false)")(
          "does not exist")

        insertRows(tableName, "2026-01-28", 10)
        insertRows(tableName, "2026-01-28", 13)
        insertRows(tableName, "2026-01-28", 16)

        runClustering(tableName, "schedule")
        metadata = getLatestRequestedClusteringPlan(basePath)
        val instant2 = metadata.getLeft
        val corruptFile = getPlanDataFiles(metadata.getRight).head
        metaClient = createMetaClient(spark, basePath)
        replaceWithEmptyFile(metaClient.getStorage, new StoragePath(corruptFile))

        val validateRows = spark.sql(
          s"call repair_clustering_plan(table => '$tableName', instant => '${instant2.requestedTime}', " +
            s"op => 'validate_delete', need_delete => true, dry_run => false)").collect()
        assertRepairResult(validateRows, instant2.requestedTime, corruptFile, "REMOVED_FROM_PLAN", deleted = true, "NOT_PARQUET_FILE")

        metadata = getLatestRequestedClusteringPlan(basePath)
        assertFalse(getPlanDataFiles(metadata.getRight).contains(corruptFile))
        metaClient = createMetaClient(spark, basePath)
        assertFalse(metaClient.getStorage.exists(new StoragePath(corruptFile)))
      }
    }
  }

  test("Test repair_clustering_plan is registered") {
    assertNotNull(org.apache.spark.sql.hudi.command.procedures.HoodieProcedures.newBuilder("repair_clustering_plan"))
  }

  test("Test repair_clustering_plan validate_delete, unsupported op and allow_empty_plan guard") {
    withTempDir { tmp =>
      withSQLConf("hoodie.parquet.small.file.limit" -> "0") {
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"

        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  dt string
             |) using hudi
             | tblproperties (
             |  primaryKey = 'id',
             |  type = 'cow',
             |  orderingFields = 'ts',
             |  hoodie.metadata.enable = 'false'
             | )
             | partitioned by (dt)
             | location '$basePath'
             |""".stripMargin)

        insertRows(tableName, "2026-01-27", 1)
        insertRows(tableName, "2026-01-27", 4)
        insertRows(tableName, "2026-01-27", 7)

        runClustering(tableName, "schedule")
        val metadata = getLatestRequestedClusteringPlan(basePath)
        val instant = metadata.getLeft
        val plannedFiles = getPlanDataFiles(metadata.getRight)
        assertTrue(plannedFiles.size > 1, s"Expected more than one planned file, but got $plannedFiles")

        // validate_delete over a healthy plan finds no corrupt files, so no candidates are returned.
        val healthyRows = spark.sql(
          s"call repair_clustering_plan(table => '$tableName', instant => '${instant.requestedTime}', " +
            s"op => 'validate_delete')").collect()
        assertEquals(0, healthyRows.length)

        // An unknown operation is rejected.
        checkExceptionContain(
          s"call repair_clustering_plan(table => '$tableName', instant => '${instant.requestedTime}', op => 'bogus')")(
          "Unsupported operation")

        // Removing every input group without allow_empty_plan is rejected.
        val allFilesArg = plannedFiles.distinct.mkString(",")
        checkExceptionContain(
          s"call repair_clustering_plan(table => '$tableName', instant => '${instant.requestedTime}', " +
            s"op => 'delete', invalid_parquet_files => '$allFilesArg')")(
          "would remove all input groups")

        // With allow_empty_plan the (dry-run) removal of every planned file is reported.
        val emptyPlanRows = spark.sql(
          s"call repair_clustering_plan(table => '$tableName', instant => '${instant.requestedTime}', " +
            s"op => 'delete', invalid_parquet_files => '$allFilesArg', allow_empty_plan => true)").collect()
        assertEquals(plannedFiles.distinct.size, emptyPlanRows.length)
        assertTrue(emptyPlanRows.forall(_.getString(2) == "WOULD_REMOVE_FROM_PLAN"),
          s"Expected all rows to be WOULD_REMOVE_FROM_PLAN, but got ${emptyPlanRows.mkString("[", ", ", "]")}")
      }
    }
  }

  private def runClustering(tableName: String,
                            operation: String,
                            instant: Option[String] = None): Unit = {
    val instantClause = instant.map(value => s", instants => '$value'").getOrElse("")
    spark.sql(s"call run_clustering(table => '$tableName', op => '$operation'$instantClause)").collect()
  }

  private def insertRows(tableName: String, dt: String, startId: Int): Unit = {
    spark.sql(
      s"""
         |insert into $tableName values
         | (${startId}, 'a${startId}', ${startId}.0, ${startId * 1000L}, '$dt'),
         | (${startId + 1}, 'a${startId + 1}', ${startId + 1}.0, ${(startId + 1) * 1000L}, '$dt'),
         | (${startId + 2}, 'a${startId + 2}', ${startId + 2}.0, ${(startId + 2) * 1000L}, '$dt')
         |""".stripMargin)
  }

  private def getLatestRequestedClusteringPlan(basePath: String) = {
    val metaClient = createMetaClient(spark, basePath)
    val plans = ClusteringUtils.getAllPendingClusteringPlans(metaClient).iterator().asScala
      .filter(_.getLeft.isRequested)
      .toSeq
    assertTrue(plans.nonEmpty)
    plans.maxBy(_.getLeft.requestedTime)
  }

  private def getPlanDataFiles(plan: HoodieClusteringPlan): Seq[String] = {
    plan.getInputGroups.asScala
      .flatMap(_.getSlices.asScala)
      .map(_.getDataFilePath)
      .filter(path => path != null && path.nonEmpty)
      .toSeq
  }

  private def assertRepairResult(rows: Array[Row],
                                 instant: String,
                                 file: String,
                                 action: String,
                                 deleted: Boolean,
                                 reason: String): Unit = {
    assertTrue(rows.exists { row =>
      row.getString(0) == instant &&
        row.getString(1) == file &&
        row.getString(2) == action &&
        row.getBoolean(3) == deleted &&
        row.getString(4) == reason
    }, s"Expected repair result to contain ($instant, $file, $action, $deleted, $reason), but got ${rows.mkString("[", ", ", "]")}")
  }

  private def assertClusteringCompleted(basePath: String, instantTime: String): Unit = {
    val metaClient = createMetaClient(spark, basePath)
    val instants = metaClient.reloadActiveTimeline().getInstants.iterator().asScala
      .filter(instant => instant.requestedTime == instantTime
        && (instant.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION || instant.getAction == HoodieTimeline.CLUSTERING_ACTION))
      .toSeq

    assertTrue(instants.exists(_.isCompleted), s"Expected clustering instant $instantTime to be completed")
    assertFalse(instants.exists(instant => instant.isRequested || instant.isInflight),
      s"Expected clustering instant $instantTime to have no pending state, but got ${instants.mkString("[", ", ", "]")}")
  }
}
