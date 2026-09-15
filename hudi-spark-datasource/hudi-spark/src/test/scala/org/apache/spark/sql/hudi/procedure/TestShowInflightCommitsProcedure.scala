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

class TestShowInflightCommitsProcedure extends HoodieSparkProcedureTestBase {

  test("Test show_inflight_commits returns empty for a fully committed table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | ts long
           | ) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |   primaryKey = 'id',
           |   type = 'cow',
           |   preCombineField = 'ts',
           |   hoodie.metadata.enable = "false"
           | )
           |""".stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 2000)")

      val result = spark.sql(s"call show_inflight_commits(table => '$tableName')").collect()
      assertResult(0)(result.length)
    }
  }

  test("Test show_inflight_commits returns injected inflight instant") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
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

      val injectedTs = "20200101120000"
      injectInflightInstant(tablePath, HoodieTimeline.COMMIT_ACTION, injectedTs)

      val result = spark.sql(s"call show_inflight_commits(table => '$tableName')").collect()
      assert(result.length >= 1)
      val row = result.find(r => r.getString(0) == injectedTs)
      assert(row.isDefined, s"Expected inflight instant $injectedTs not found in results")
      assertResult(HoodieTimeline.COMMIT_ACTION)(row.get.getString(1))
      assertResult("INFLIGHT")(row.get.getString(2))
    }
  }

  test("Test show_inflight_commits min_age_minutes filter includes old and excludes recent") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
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

      val oldTs   = "20200101120000"
      val freshTs = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

      injectInflightInstant(tablePath, HoodieTimeline.COMMIT_ACTION, oldTs)
      injectInflightInstant(tablePath, HoodieTimeline.DELTA_COMMIT_ACTION, freshTs)

      // No filter: both should appear
      val allResults = spark.sql(s"call show_inflight_commits(table => '$tableName', min_age_minutes => 0)").collect()
      assert(allResults.length >= 2)
      assert(allResults.exists(r => r.getString(0) == oldTs))
      assert(allResults.exists(r => r.getString(0) == freshTs))

      // 60-minute filter: only the old instant should appear
      val filteredResults = spark.sql(
        s"call show_inflight_commits(table => '$tableName', min_age_minutes => 60)").collect()
      assert(filteredResults.exists(r => r.getString(0) == oldTs),
        s"Expected old instant $oldTs to appear with min_age_minutes=60")
      assert(!filteredResults.exists(r => r.getString(0) == freshTs),
        s"Fresh instant $freshTs should not appear with min_age_minutes=60")
    }
  }

  test("Test show_inflight_commits requires table parameter") {
    checkExceptionContain(
      "call show_inflight_commits()")(
      "Argument: table is required")
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
