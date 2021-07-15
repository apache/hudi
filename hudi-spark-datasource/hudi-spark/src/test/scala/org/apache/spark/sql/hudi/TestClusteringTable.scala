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
import org.apache.hudi.HoodieDataSourceHelpers
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieTimeline}

import scala.collection.JavaConverters.asScalaIteratorConverter

class TestClusteringTable extends TestHoodieSqlBase {

  test("Test clustering table") {
    withTempDir{tmp =>
      Seq("cow", "mor").foreach {tableType =>
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
             | location '$basePath'
             | options (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        val client = HoodieSqlUtils.createHoodieClientFromPath(spark, basePath, Map.empty)
        // First schedule a clustering plan
        val firstScheduleInstant = HoodieActiveTimeline.createNewInstantTime
        client.scheduleClusteringAtInstant(firstScheduleInstant, HOption.empty())

        // Second schedule a clustering plan
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")
        val secondScheduleInstant = HoodieActiveTimeline.createNewInstantTime
        client.scheduleClusteringAtInstant(secondScheduleInstant, HOption.empty())
        checkAnswer(s"show clustering on $tableName")(
          Seq(firstScheduleInstant, 1),
          Seq(secondScheduleInstant, 1)
        )

        // Do clustering for all the clustering plan
        spark.sql(s"run clustering on $tableName order by ts")
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 10.0, 1001),
          Seq(3, "a3", 10.0, 1002)
        )
        // After clustering there should be no pending clustering.
        checkAnswer(s"show clustering on $tableName")()

        // Do clustering without manual schedule(which will do the schedule if no pending clustering exists)
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1003)")
        spark.sql(s"insert into $tableName values(5, 'a5', 10, 1004)")
        spark.sql(s"run clustering on $tableName")
        // Get the second replace commit
        val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())
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
          Seq(5, "a5", 10.0, 1004)
        )
      }
    }
  }


  test("Test clustring table path") {
    withTempDir{tmp =>
      Seq("cow", "mor").foreach {tableType =>
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
             | location '$basePath'
             | options (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)

        spark.sql(s"run clustering on '$basePath'")
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")
        val client = HoodieSqlUtils.createHoodieClientFromPath(spark, basePath, Map.empty)
        // Schedule a clustering plan
        val scheduleInstant = HoodieActiveTimeline.createNewInstantTime
        client.scheduleClusteringAtInstant(scheduleInstant, HOption.empty())
        checkAnswer(s"show clustering on '$basePath'")(
          Seq(scheduleInstant, 1)
        )
        // Do clustering for all the clustering plan
        spark.sql(s"run clustering on '$basePath' order by ts at $scheduleInstant")
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 10.0, 1001),
          Seq(3, "a3", 10.0, 1002)
        )
        val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())
        HoodieDataSourceHelpers.hasNewCommits(fs, basePath, scheduleInstant)
      }
    }
  }

}
