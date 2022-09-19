/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.procedure

import org.apache.hadoop.fs.Path
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.{HoodieCLIUtils, HoodieDataSourceHelpers}

import scala.collection.JavaConverters.asScalaIteratorConverter

class TestClusteringProcedure extends HoodieSparkProcedureTestBase {

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
        val client = HoodieCLIUtils.createHoodieClientFromPath(spark, basePath, Map.empty)
        // Generate the first clustering plan
        val firstScheduleInstant = HoodieActiveTimeline.createNewInstantTime
        client.scheduleClusteringAtInstant(firstScheduleInstant, HOption.empty())

        // Generate the second clustering plan
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1003)")
        val secondScheduleInstant = HoodieActiveTimeline.createNewInstantTime
        client.scheduleClusteringAtInstant(secondScheduleInstant, HOption.empty())
        checkAnswer(s"call show_clustering('$tableName')")(
          Seq(secondScheduleInstant, 1, HoodieInstant.State.REQUESTED.name(), "*"),
          Seq(firstScheduleInstant, 3, HoodieInstant.State.REQUESTED.name(), "*")
        )

        // Do clustering for all clustering plan generated above, and no new clustering
        // instant will be generated because of there is no commit after the second
        // clustering plan generated
        checkAnswer(s"call run_clustering(table => '$tableName', order => 'ts', show_involved_partition => true)")(
          Seq(secondScheduleInstant, 1, HoodieInstant.State.COMPLETED.name(), "ts=1003"),
          Seq(firstScheduleInstant, 3, HoodieInstant.State.COMPLETED.name(), "ts=1000,ts=1001,ts=1002")
        )

        // No new commits
        val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())
        assertResult(false)(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, secondScheduleInstant))

        // Check the number of finished clustering instants
        val finishedClustering = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
          .getInstants
          .iterator().asScala
          .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
          .toSeq
        assertResult(2)(finishedClustering.size)

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 10.0, 1001),
          Seq(3, "a3", 10.0, 1002),
          Seq(4, "a4", 10.0, 1003)
        )

        // After clustering there should be no pending clustering and all clustering instants should be completed
        checkAnswer(s"call show_clustering(table => '$tableName')")(
          Seq(secondScheduleInstant, 1, HoodieInstant.State.COMPLETED.name(), "*"),
          Seq(firstScheduleInstant, 3, HoodieInstant.State.COMPLETED.name(), "*")
        )

        // Do clustering without manual schedule(which will do the schedule if no pending clustering exists)
        spark.sql(s"insert into $tableName values(5, 'a5', 10, 1004)")
        spark.sql(s"insert into $tableName values(6, 'a6', 10, 1005)")
        spark.sql(s"call run_clustering(table => '$tableName', order => 'ts', show_involved_partition => true)").show()

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

        spark.sql(s"call run_clustering(path => '$basePath')").show()
        checkAnswer(s"call show_clustering(path => '$basePath')")()

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")
        val client = HoodieCLIUtils.createHoodieClientFromPath(spark, basePath, Map.empty)
        // Generate the first clustering plan
        val firstScheduleInstant = HoodieActiveTimeline.createNewInstantTime
        client.scheduleClusteringAtInstant(firstScheduleInstant, HOption.empty())
        checkAnswer(s"call show_clustering(path => '$basePath', show_involved_partition => true)")(
          Seq(firstScheduleInstant, 3, HoodieInstant.State.REQUESTED.name(), "ts=1000,ts=1001,ts=1002")
        )
        // Do clustering for all the clustering plan
        checkAnswer(s"call run_clustering(path => '$basePath', order => 'ts')")(
          Seq(firstScheduleInstant, 3, HoodieInstant.State.COMPLETED.name(), "*")
        )

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 10.0, 1001),
          Seq(3, "a3", 10.0, 1002)
        )

        val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())
        assertResult(false)(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, firstScheduleInstant))

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
        val resultA = spark.sql(s"call run_clustering(table => '$tableName', predicate => 'ts >= 1003L', show_involved_partition => true)")
          .collect()
          .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2), row.getString(3)))
        assertResult(1)(resultA.length)
        assertResult("ts=1003,ts=1004")(resultA(0)(3))

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

        val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())

        // Test partition pruning with single predicate
        var resultA: Array[Seq[Any]] = Array.empty

        {
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

          checkException(
            s"call run_clustering(table => '$tableName', predicate => 'ts <= 1001L and id = 10', order => 'ts')"
          )("Only partition predicates are allowed")

          // Do clustering table with partition predicate
          resultA = spark.sql(s"call run_clustering(table => '$tableName', predicate => 'ts <= 1001L', order => 'ts', show_involved_partition => true)")
            .collect()
            .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2), row.getString(3)))
          assertResult(1)(resultA.length)
          assertResult("ts=1000,ts=1001")(resultA(0)(3))

          // There is 1 completed clustering instant
          val clusteringInstants = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
            .getInstants
            .iterator().asScala
            .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
            .toSeq
          assertResult(1)(clusteringInstants.size)

          // The latest clustering should contain 2 file groups
          val clusteringInstant = clusteringInstants.last
          val clusteringPlan = HoodieDataSourceHelpers.getClusteringPlan(fs, basePath, clusteringInstant.getTimestamp)
          assertResult(true)(clusteringPlan.isPresent)
          assertResult(2)(clusteringPlan.get().getInputGroups.size())
          assertResult(resultA(0)(1))(clusteringPlan.get().getInputGroups.size())

          // All clustering instants are completed
          checkAnswer(s"call show_clustering(table => '$tableName', show_involved_partition => true)")(
            Seq(resultA(0).head, resultA(0)(1), HoodieInstant.State.COMPLETED.name(), "ts=1000,ts=1001")
          )

          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(1, "a1", 10.0, 1000),
            Seq(2, "a2", 10.0, 1001),
            Seq(3, "a3", 10.0, 1002)
          )
        }

        // Test partition pruning with {@code And} predicates
        var resultB: Array[Seq[Any]] = Array.empty

        {
          spark.sql(s"insert into $tableName values(4, 'a4', 10, 1003)")
          spark.sql(s"insert into $tableName values(5, 'a5', 10, 1004)")
          spark.sql(s"insert into $tableName values(6, 'a6', 10, 1005)")

          checkException(
            s"call run_clustering(table => '$tableName', predicate => 'ts > 1001L and ts <= 1005L and id = 10', order => 'ts')"
          )("Only partition predicates are allowed")

          // Do clustering table with partition predicate
          resultB = spark.sql(s"call run_clustering(table => '$tableName', predicate => 'ts > 1001L and ts <= 1005L', order => 'ts', show_involved_partition => true)")
            .collect()
            .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2), row.getString(3)))
          assertResult(1)(resultB.length)
          assertResult("ts=1002,ts=1003,ts=1004,ts=1005")(resultB(0)(3))

          // There are 2 completed clustering instants
          val clusteringInstants = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
            .getInstants
            .iterator().asScala
            .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
            .toSeq
          assertResult(2)(clusteringInstants.size)

          // The latest clustering should contain 4 file groups(1002,1003,1004,1005)
          val clusteringInstant = clusteringInstants.last
          val clusteringPlan = HoodieDataSourceHelpers.getClusteringPlan(fs, basePath, clusteringInstant.getTimestamp)
          assertResult(true)(clusteringPlan.isPresent)
          assertResult(4)(clusteringPlan.get().getInputGroups.size())

          // All clustering instants are completed
          checkAnswer(s"call show_clustering(table => '$tableName', show_involved_partition => true)")(
            Seq(resultA(0).head, resultA(0)(1), HoodieInstant.State.COMPLETED.name(), "ts=1000,ts=1001"),
            Seq(resultB(0).head, resultB(0)(1), HoodieInstant.State.COMPLETED.name(), "ts=1002,ts=1003,ts=1004,ts=1005")
          )

          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(1, "a1", 10.0, 1000),
            Seq(2, "a2", 10.0, 1001),
            Seq(3, "a3", 10.0, 1002),
            Seq(4, "a4", 10.0, 1003),
            Seq(5, "a5", 10.0, 1004),
            Seq(6, "a6", 10.0, 1005)
          )
        }

        // Test partition pruning with {@code And}-{@code Or} predicates
        var resultC: Array[Seq[Any]] = Array.empty

        {
          spark.sql(s"insert into $tableName values(7, 'a7', 10, 1006)")
          spark.sql(s"insert into $tableName values(8, 'a8', 10, 1007)")
          spark.sql(s"insert into $tableName values(9, 'a9', 10, 1008)")
          spark.sql(s"insert into $tableName values(10, 'a10', 10, 1009)")

          checkException(
            s"call run_clustering(table => '$tableName', predicate => 'ts < 1007L or ts >= 1008L or id = 10', order => 'ts')"
          )("Only partition predicates are allowed")

          // Do clustering table with partition predicate
          resultC = spark.sql(s"call run_clustering(table => '$tableName', predicate => '(ts >= 1006L and ts < 1008L) or ts >= 1009L', order => 'ts', show_involved_partition => true)")
            .collect()
            .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2), row.getString(3)))
          assertResult(1)(resultC.length)
          assertResult("ts=1006,ts=1007,ts=1009")(resultC(0)(3))

          // There are 3 completed clustering instants
          val clusteringInstants = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
            .getInstants
            .iterator().asScala
            .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
            .toSeq
          assertResult(3)(clusteringInstants.size)

          // The latest clustering should contain 3 file groups(1006,1007,1009)
          val clusteringInstant = clusteringInstants.last
          val clusteringPlan = HoodieDataSourceHelpers.getClusteringPlan(fs, basePath, clusteringInstant.getTimestamp)
          assertResult(true)(clusteringPlan.isPresent)
          assertResult(3)(clusteringPlan.get().getInputGroups.size())

          // All clustering instants are completed
          checkAnswer(s"call show_clustering(table => '$tableName', show_involved_partition => true)")(
            Seq(resultA(0).head, resultA(0)(1), HoodieInstant.State.COMPLETED.name(), "ts=1000,ts=1001"),
            Seq(resultB(0).head, resultB(0)(1), HoodieInstant.State.COMPLETED.name(), "ts=1002,ts=1003,ts=1004,ts=1005"),
            Seq(resultC(0).head, resultC(0)(1), HoodieInstant.State.COMPLETED.name(), "ts=1006,ts=1007,ts=1009")
          )

          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(1, "a1", 10.0, 1000),
            Seq(2, "a2", 10.0, 1001),
            Seq(3, "a3", 10.0, 1002),
            Seq(4, "a4", 10.0, 1003),
            Seq(5, "a5", 10.0, 1004),
            Seq(6, "a6", 10.0, 1005),
            Seq(7, "a7", 10.0, 1006),
            Seq(8, "a8", 10.0, 1007),
            Seq(9, "a9", 10.0, 1008),
            Seq(10, "a10", 10.0, 1009)
          )
        }
      }
    }
  }
}
