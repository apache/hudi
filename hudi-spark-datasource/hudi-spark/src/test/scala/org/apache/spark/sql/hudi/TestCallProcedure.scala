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

package org.apache.spark.sql.hudi

import org.apache.hadoop.fs.Path
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieTimeline}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.{HoodieCommonUtils, HoodieDataSourceHelpers}

import scala.collection.JavaConverters.asScalaIteratorConverter

class TestCallProcedure extends TestHoodieSqlBase {

  test("Test Call show_commits Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // Check required fields
      checkExceptionContain(s"""call show_commits(limit => 10)""")(
        s"Argument: table is required")

      // collect commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(2) {
        commits.length
      }
    }
  }

  test("Test Call show_commits_metadata Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      // Check required fields
      checkExceptionContain(s"""call show_commits_metadata(limit => 10)""")(
        s"Argument: table is required")

      // collect commits for table
      val commits = spark.sql(s"""call show_commits_metadata(table => '$tableName', limit => 10)""").collect()
      assertResult(1) {
        commits.length
      }
    }
  }

  test("Test Call rollback_to_instant Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")

      // Check required fields
      checkExceptionContain(s"""call rollback_to_instant(table => '$tableName')""")(
        s"Argument: instant_time is required")

      // 3 commits are left before rollback
      var commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(3){commits.length}

      // Call rollback_to_instant Procedure with Named Arguments
      var instant_time = commits(0).get(0).toString
      checkAnswer(s"""call rollback_to_instant(table => '$tableName', instant_time => '$instant_time')""")(Seq(true))
      // Call rollback_to_instant Procedure with Positional Arguments
      instant_time = commits(1).get(0).toString
      checkAnswer(s"""call rollback_to_instant('$tableName', '$instant_time')""")(Seq(true))

      // 1 commits are left after rollback
      commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(1){commits.length}
    }
  }

  test("Test Call run_clustering Procedure By Table") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  preCombineField = 'ts'
             | )
             | partitioned by(ts)
             | location '$basePath'
       """.stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")
        val client = HoodieCommonUtils.createHoodieClientFromPath(spark, basePath, Map.empty)
        // Generate the first clustering plan
        val firstScheduleInstant = HoodieActiveTimeline.createNewInstantTime
        client.scheduleClusteringAtInstant(firstScheduleInstant, HOption.empty())

        // Generate the second clustering plan
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1003)")
        val secondScheduleInstant = HoodieActiveTimeline.createNewInstantTime
        client.scheduleClusteringAtInstant(secondScheduleInstant, HOption.empty())
        checkAnswer(s"call show_clustering('$tableName')")(
          Seq(firstScheduleInstant, 3),
          Seq(secondScheduleInstant, 1)
        )

        // Do clustering for all clustering plan generated above, and no new clustering
        // instant will be generated because of there is no commit after the second
        // clustering plan generated
        spark.sql(s"call run_clustering(table => '$tableName', order => 'ts')")

        // No new commits
        val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())
        assertResult(false)(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, secondScheduleInstant))

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 10.0, 1001),
          Seq(3, "a3", 10.0, 1002),
          Seq(4, "a4", 10.0, 1003)
        )
        // After clustering there should be no pending clustering.
        checkAnswer(s"call show_clustering(table => '$tableName')")()

        // Check the number of finished clustering instants
        val finishedClustering = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
          .getInstants
          .iterator().asScala
          .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
          .toSeq
        assertResult(2)(finishedClustering.size)

        // Do clustering without manual schedule(which will do the schedule if no pending clustering exists)
        spark.sql(s"insert into $tableName values(5, 'a5', 10, 1004)")
        spark.sql(s"insert into $tableName values(6, 'a6', 10, 1005)")
        spark.sql(s"call run_clustering(table => '$tableName', order => 'ts')")

        val thirdClusteringInstant = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
          .findInstantsAfter(secondScheduleInstant)
          .getInstants
          .iterator().asScala
          .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
          .toSeq
        // Should have a new replace commit after the second clustering command.
        assertResult(1)(thirdClusteringInstant.size)

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 10.0, 1001),
          Seq(3, "a3", 10.0, 1002),
          Seq(4, "a4", 10.0, 1003),
          Seq(5, "a5", 10.0, 1004),
          Seq(6, "a6", 10.0, 1005)
        )
      }
    }
  }

  test("Test Call run_clustering Procedure By Path") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  preCombineField = 'ts'
             | )
             | partitioned by(ts)
             | location '$basePath'
       """.stripMargin)

        spark.sql(s"call run_clustering(path => '$basePath')")
        checkAnswer(s"call show_clustering(path => '$basePath')")()

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")
        val client = HoodieCommonUtils.createHoodieClientFromPath(spark, basePath, Map.empty)
        // Generate the first clustering plan
        val firstScheduleInstant = HoodieActiveTimeline.createNewInstantTime
        client.scheduleClusteringAtInstant(firstScheduleInstant, HOption.empty())
        checkAnswer(s"call show_clustering(path => '$basePath')")(
          Seq(firstScheduleInstant, 3)
        )
        // Do clustering for all the clustering plan
        spark.sql(s"call run_clustering(path => '$basePath', order => 'ts')")
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 10.0, 1001),
          Seq(3, "a3", 10.0, 1002)
        )
        val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())
        HoodieDataSourceHelpers.hasNewCommits(fs, basePath, firstScheduleInstant)

        // Check the number of finished clustering instants
        var finishedClustering = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
          .getInstants
          .iterator().asScala
          .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
          .toSeq
        assertResult(1)(finishedClustering.size)

        // Do clustering without manual schedule(which will do the schedule if no pending clustering exists)
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1003)")
        spark.sql(s"insert into $tableName values(5, 'a5', 10, 1004)")
        spark.sql(s"call run_clustering(table => '$tableName', predicate => 'ts >= 1003L')")
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 10.0, 1001),
          Seq(3, "a3", 10.0, 1002),
          Seq(4, "a4", 10.0, 1003),
          Seq(5, "a5", 10.0, 1004)
        )

        finishedClustering = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
          .getInstants
          .iterator().asScala
          .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
          .toSeq
        assertResult(2)(finishedClustering.size)
      }
    }
  }

  test("Test Call run_clustering Procedure With Partition Pruning") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  preCombineField = 'ts'
             | )
             | partitioned by(ts)
             | location '$basePath'
       """.stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

        // Do clustering table with partition predicate
        spark.sql(s"call run_clustering(table => '$tableName', predicate => 'ts <= 1001L', order => 'ts')")

        // Check the num of completed clustering instant
        val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())
        val clusteringInstants = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
          .getInstants
          .iterator().asScala
          .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
          .toSeq
        assertResult(1)(clusteringInstants.size)

        val clusteringInstant = clusteringInstants.head
        val clusteringPlan = HoodieDataSourceHelpers.getClusteringPlan(fs, basePath, clusteringInstant.getTimestamp)
        assertResult(true)(clusteringPlan.isPresent)
        assertResult(2)(clusteringPlan.get().getInputGroups.size())

        checkAnswer(s"call show_clustering(table => '$tableName')")()

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 10.0, 1001),
          Seq(3, "a3", 10.0, 1002)
        )
      }
    }
  }
}
