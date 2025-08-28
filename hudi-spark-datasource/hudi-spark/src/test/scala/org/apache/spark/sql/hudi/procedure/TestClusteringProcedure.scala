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

import org.apache.hudi.{DataSourceReadOptions, HoodieCLIUtils, HoodieDataSourceHelpers, HoodieFileIndex}
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, RECORDKEY_FIELD}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{HoodieCommitMetadata, WriteOperationType}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline, TimelineLayout}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.types.{DataTypes, IntegerType, Metadata, StringType, StructField, StructType}

import java.util

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
        // disable automatic inline compaction so that HoodieDataSourceHelpers.allCompletedCommitsCompactions
        // does not count compaction instants
        spark.sql("set hoodie.compact.inline=false")
        spark.sql("set hoodie.compact.schedule.inline=false")

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1002)")
        val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, Option(tableName))
        // Generate the first clustering plan
        val firstScheduleInstant = client.scheduleClustering(HOption.empty()).get()

        // Generate the second clustering plan
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1003, 1003)")
        val secondScheduleInstant = client.scheduleClustering(HOption.empty()).get()
        checkAnswer(s"call show_clustering('$tableName')")(
          Seq(secondScheduleInstant, 1, HoodieInstant.State.REQUESTED.name(), "*"),
          Seq(firstScheduleInstant, 3, HoodieInstant.State.REQUESTED.name(), "*")
        )

        // Do clustering for all clustering plan generated above, and no new clustering
        // instant will be generated because of there is no commit after the second
        // clustering plan generated
        checkAnswer(s"call run_clustering(table => '$tableName', order => 'partition', show_involved_partition => true)")(
          Seq(secondScheduleInstant, 1, HoodieInstant.State.COMPLETED.name(), "partition=1003"),
          Seq(firstScheduleInstant, 3, HoodieInstant.State.COMPLETED.name(), "partition=1000,partition=1001,partition=1002")
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

        checkAnswer(s"select id, name, price, ts, partition from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000, 1000),
          Seq(2, "a2", 10.0, 1001, 1001),
          Seq(3, "a3", 10.0, 1002, 1002),
          Seq(4, "a4", 10.0, 1003, 1003)
        )

        // After clustering there should be no pending clustering and all clustering instants should be completed
        checkAnswer(s"call show_clustering(table => '$tableName')")(
          Seq(secondScheduleInstant, 1, HoodieInstant.State.COMPLETED.name(), "*"),
          Seq(firstScheduleInstant, 3, HoodieInstant.State.COMPLETED.name(), "*")
        )

        // Do clustering without manual schedule(which will do the schedule if no pending clustering exists)
        spark.sql(s"insert into $tableName values(5, 'a5', 10, 1004, 1004)")
        spark.sql(s"insert into $tableName values(6, 'a6', 10, 1005, 1005)")
        spark.sql(s"call run_clustering(table => '$tableName', order => 'partition', show_involved_partition => true)").show()

        val thirdClusteringInstant = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
          .findInstantsAfter(secondScheduleInstant)
          .getInstants
          .iterator().asScala
          .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
          .toSeq
        // Should have a new replace commit after the second clustering command.
        assertResult(1)(thirdClusteringInstant.size)

        checkAnswer(s"select id, name, price, ts, partition from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000, 1000),
          Seq(2, "a2", 10.0, 1001, 1001),
          Seq(3, "a3", 10.0, 1002, 1002),
          Seq(4, "a4", 10.0, 1003, 1003),
          Seq(5, "a5", 10.0, 1004, 1004),
          Seq(6, "a6", 10.0, 1005, 1005)
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

        spark.sql(s"call run_clustering(path => '$basePath')").show()
        checkAnswer(s"call show_clustering(path => '$basePath')")()

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1002)")
        val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, Option(tableName))
        // Generate the first clustering plan
        val firstScheduleInstant = client.scheduleClustering(HOption.empty()).get()
        checkAnswer(s"call show_clustering(path => '$basePath', show_involved_partition => true)")(
          Seq(firstScheduleInstant, 3, HoodieInstant.State.REQUESTED.name(), "partition=1000,partition=1001,partition=1002")
        )
        // Do clustering for all the clustering plan
        checkAnswer(s"call run_clustering(path => '$basePath', order => 'partition')")(
          Seq(firstScheduleInstant, 3, HoodieInstant.State.COMPLETED.name(), "*")
        )

        checkAnswer(s"select id, name, price, ts, partition from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000, 1000),
          Seq(2, "a2", 10.0, 1001, 1001),
          Seq(3, "a3", 10.0, 1002, 1002)
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
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1003, 1003)")
        spark.sql(s"insert into $tableName values(5, 'a5', 10, 1004, 1004)")
        val resultA = spark.sql(s"call run_clustering(table => '$tableName', predicate => 'partition >= 1003L', show_involved_partition => true)")
          .collect()
          .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2), row.getString(3)))
        assertResult(1)(resultA.length)
        assertResult("partition=1003,partition=1004")(resultA(0)(3))

        checkAnswer(s"select id, name, price, ts, partition from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000, 1000),
          Seq(2, "a2", 10.0, 1001, 1001),
          Seq(3, "a3", 10.0, 1002, 1002),
          Seq(4, "a4", 10.0, 1003, 1003),
          Seq(5, "a5", 10.0, 1004, 1004)
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

        val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())

        // Test partition pruning with single predicate
        var resultA: Array[Seq[Any]] = Array.empty

        {
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, 1000)")
          spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001, 1001)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1002)")

          checkException(
            s"call run_clustering(table => '$tableName', predicate => 'partition <= 1001L and id = 10', order => 'partition')"
          )("Only partition predicates are allowed")

          // Do clustering table with partition predicate
          resultA = spark.sql(s"call run_clustering(table => '$tableName', predicate => 'partition <= 1001L', order => 'partition', show_involved_partition => true)")
            .collect()
            .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2), row.getString(3)))
          assertResult(1)(resultA.length)
          assertResult("partition=1000,partition=1001")(resultA(0)(3))

          // There is 1 completed clustering instant
          val clusteringInstants = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
            .getInstants
            .iterator().asScala
            .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
            .toSeq
          assertResult(1)(clusteringInstants.size)

          // The latest clustering should contain 2 file groups
          val clusteringInstant = clusteringInstants.last
          val clusteringPlan = HoodieDataSourceHelpers.getClusteringPlan(fs, basePath, clusteringInstant.requestedTime)
          assertResult(true)(clusteringPlan.isPresent)
          assertResult(2)(clusteringPlan.get().getInputGroups.size())
          assertResult(resultA(0)(1))(clusteringPlan.get().getInputGroups.size())

          // All clustering instants are completed
          checkAnswer(s"call show_clustering(table => '$tableName', show_involved_partition => true)")(
            Seq(resultA(0).head, resultA(0)(1), HoodieInstant.State.COMPLETED.name(), "partition=1000,partition=1001")
          )

          checkAnswer(s"select id, name, price, ts, partition from $tableName order by id")(
            Seq(1, "a1", 10.0, 1000, 1000),
            Seq(2, "a2", 10.0, 1001, 1001),
            Seq(3, "a3", 10.0, 1002, 1002)
          )
        }

        // Test partition pruning with {@code And} predicates
        var resultB: Array[Seq[Any]] = Array.empty

        {
          spark.sql(s"insert into $tableName values(4, 'a4', 10, 1003, 1003)")
          spark.sql(s"insert into $tableName values(5, 'a5', 10, 1004, 1004)")
          spark.sql(s"insert into $tableName values(6, 'a6', 10, 1005, 1005)")

          checkException(
            s"call run_clustering(table => '$tableName', predicate => 'partition > 1001L and partition <= 1005L and id = 10', order => 'partition')"
          )("Only partition predicates are allowed")

          // Do clustering table with partition predicate
          resultB = spark.sql(s"call run_clustering(table => '$tableName', predicate => 'partition > 1001L and partition <= 1005L', order => 'partition', show_involved_partition => true)")
            .collect()
            .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2), row.getString(3)))
          assertResult(1)(resultB.length)
          assertResult("partition=1002,partition=1003,partition=1004,partition=1005")(resultB(0)(3))

          // There are 2 completed clustering instants
          val clusteringInstants = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
            .getInstants
            .iterator().asScala
            .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
            .toSeq
          assertResult(2)(clusteringInstants.size)

          // The latest clustering should contain 4 file groups(1002,1003,1004,1005)
          val clusteringInstant = clusteringInstants.last
          val clusteringPlan = HoodieDataSourceHelpers.getClusteringPlan(fs, basePath, clusteringInstant.requestedTime)
          assertResult(true)(clusteringPlan.isPresent)
          assertResult(4)(clusteringPlan.get().getInputGroups.size())

          // All clustering instants are completed
          checkAnswer(s"call show_clustering(table => '$tableName', show_involved_partition => true)")(
            Seq(resultA(0).head, resultA(0)(1), HoodieInstant.State.COMPLETED.name(), "partition=1000,partition=1001"),
            Seq(resultB(0).head, resultB(0)(1), HoodieInstant.State.COMPLETED.name(), "partition=1002,partition=1003,partition=1004,partition=1005")
          )

          checkAnswer(s"select id, name, price, ts, partition from $tableName order by id")(
            Seq(1, "a1", 10.0, 1000, 1000),
            Seq(2, "a2", 10.0, 1001, 1001),
            Seq(3, "a3", 10.0, 1002, 1002),
            Seq(4, "a4", 10.0, 1003, 1003),
            Seq(5, "a5", 10.0, 1004, 1004),
            Seq(6, "a6", 10.0, 1005, 1005)
          )
        }

        // Test partition pruning with {@code And}-{@code Or} predicates
        var resultC: Array[Seq[Any]] = Array.empty

        {
          spark.sql(s"insert into $tableName values(7, 'a7', 10, 1006, 1006)")
          spark.sql(s"insert into $tableName values(8, 'a8', 10, 1007, 1007)")
          spark.sql(s"insert into $tableName values(9, 'a9', 10, 1008, 1008)")
          spark.sql(s"insert into $tableName values(10, 'a10', 10, 1009, 1009)")

          checkException(
            s"call run_clustering(table => '$tableName', predicate => 'partition < 1007L or partition >= 1008L or id = 10', order => 'partition')"
          )("Only partition predicates are allowed")

          // Do clustering table with partition predicate
          resultC = spark.sql(s"call run_clustering(table => '$tableName', predicate => '(partition >= 1006L and partition < 1008L) or partition >= 1009L', order => 'partition', show_involved_partition => true)")
            .collect()
            .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2), row.getString(3)))
          assertResult(1)(resultC.length)
          assertResult("partition=1006,partition=1007,partition=1009")(resultC(0)(3))

          // There are 3 completed clustering instants
          val clusteringInstants = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, basePath)
            .getInstants
            .iterator().asScala
            .filter(p => p.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION)
            .toSeq
          assertResult(3)(clusteringInstants.size)

          // The latest clustering should contain 3 file groups(1006,1007,1009)
          val clusteringInstant = clusteringInstants.last
          val clusteringPlan = HoodieDataSourceHelpers.getClusteringPlan(fs, basePath, clusteringInstant.requestedTime)
          assertResult(true)(clusteringPlan.isPresent)
          assertResult(3)(clusteringPlan.get().getInputGroups.size())

          // All clustering instants are completed
          checkAnswer(s"call show_clustering(table => '$tableName', show_involved_partition => true)")(
            Seq(resultA(0).head, resultA(0)(1), HoodieInstant.State.COMPLETED.name(), "partition=1000,partition=1001"),
            Seq(resultB(0).head, resultB(0)(1), HoodieInstant.State.COMPLETED.name(), "partition=1002,partition=1003,partition=1004,partition=1005"),
            Seq(resultC(0).head, resultC(0)(1), HoodieInstant.State.COMPLETED.name(), "partition=1006,partition=1007,partition=1009")
          )

          checkAnswer(s"select id, name, price, ts, partition from $tableName order by id")(
            Seq(1, "a1", 10.0, 1000, 1000),
            Seq(2, "a2", 10.0, 1001, 1001),
            Seq(3, "a3", 10.0, 1002, 1002),
            Seq(4, "a4", 10.0, 1003, 1003),
            Seq(5, "a5", 10.0, 1004, 1004),
            Seq(6, "a6", 10.0, 1005, 1005),
            Seq(7, "a7", 10.0, 1006, 1006),
            Seq(8, "a8", 10.0, 1007, 1007),
            Seq(9, "a9", 10.0, 1008, 1008),
            Seq(10, "a10", 10.0, 1009, 1009)
          )
        }

        // Test partition pruning with invalid predicates
        {
          val resultD = spark.sql(s"call run_clustering(table => '$tableName', predicate => 'partition > 1111L', order => 'partition', show_involved_partition => true)")
            .collect()
            .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2), row.getString(3)))
          assertResult(0)(resultD.length)
        }
      }
    }
  }

  test("Test Call run_clustering Procedure With Partition Pruning Regex") {
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
             |  ts bigint,
             |  sex string,
             |  addr string
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  preCombineField = 'ts'
             | )
             | partitioned by(sex, addr)
             | location '$basePath'
       """.stripMargin)
        val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())
        // Test partition pruning with single predicate
        var resultA: Array[Seq[Any]] = Array.empty
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, 's1', 'addr1')")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001, 's2', 'addr1')")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 's1', 'addr2')")
        // clustering table with partition predicate
        resultA = spark.sql(s"call run_clustering(table => '$tableName', partition_regex_pattern => 'sex=s1/.*', order => 'ts', show_involved_partition => true)")
          .collect()
          .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2), row.getString(3)))
        assertResult(1)(resultA.length)
        assertResult("sex=s1/addr=addr1,sex=s1/addr=addr2")(resultA(0)(3))
      }
    }
  }

  test("Test Call run_clustering Procedure with specific instants") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"

      spark.sql(
        s"""
           |create table $tableName (
           |  c1 int,
           |  c2 string,
           |  c3 double
           |) using hudi
           | options (
           |  primaryKey = 'c1',
           |  type = 'cow',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.index.column.stats.enable = 'true',
           |  hoodie.enable.data.skipping = 'true',
           |  hoodie.datasource.write.operation = 'insert'
           | )
           | location '$basePath'
     """.stripMargin)

      writeRecords(2, 4, 0, basePath, Map("hoodie.avro.schema.validate" -> "false"))
      spark.sql(s"call run_clustering(table => '$tableName', op => 'schedule')")

      writeRecords(2, 4, 0, basePath, Map("hoodie.avro.schema.validate" -> "false"))
      spark.sql(s"call run_clustering(table => '$tableName', op => 'schedule')")

      val conf = new Configuration
      val metaClient = HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(conf), basePath)
      val instants = metaClient.getActiveTimeline.filterPendingClusteringTimeline().getInstants.iterator().asScala.map(_.requestedTime).toSeq
      assert(2 == instants.size)

      checkExceptionContain(
        s"call run_clustering(table => '$tableName', instants => '000000, ${instants.head}')"
      )("specific 000000 instants is not exist")
      metaClient.reloadActiveTimeline()
      assert(0 == metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.size())
      assert(2 == metaClient.getActiveTimeline.filterPendingClusteringTimeline().getInstants.size())

      writeRecords(2, 4, 0, basePath, Map("hoodie.avro.schema.validate" -> "false"))
      // specific instants will not schedule new cluster plan
      spark.sql(s"call run_clustering(table => '$tableName', instants => '${instants.mkString(",")}')")
      metaClient.reloadActiveTimeline()
      assert(2 == metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.size())
      assert(0 == metaClient.getActiveTimeline.filterPendingClusteringTimeline().getInstants.size())

      // test with operator schedule
      checkExceptionContain(
        s"call run_clustering(table => '$tableName', instants => '000000', op => 'schedule')"
      )("specific instants only can be used in 'execute' op or not specific op")

      // test with operator scheduleAndExecute
      checkExceptionContain(
        s"call run_clustering(table => '$tableName', instants => '000000', op => 'scheduleAndExecute')"
      )("specific instants only can be used in 'execute' op or not specific op")

      // test with operator execute
      spark.sql(s"call run_clustering(table => '$tableName', op => 'schedule')")
      metaClient.reloadActiveTimeline()
      val instants2 = metaClient.getActiveTimeline.filterPendingClusteringTimeline().getInstants.iterator().asScala.map(_.requestedTime).toSeq
      spark.sql(s"call run_clustering(table => '$tableName', instants => '${instants2.mkString(",")}', op => 'execute')")
      metaClient.reloadActiveTimeline()
      assert(3 == metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.size())
      assert(0 == metaClient.getActiveTimeline.filterPendingClusteringTimeline().getInstants.size())
    }
  }

  test("Test Call run_clustering Procedure op") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      val config = Map("hoodie.avro.schema.validate"-> "false")

      spark.sql(
        s"""
           |create table $tableName (
           |  c1 int,
           |  c2 string,
           |  c3 double
           |) using hudi
           | options (
           |  primaryKey = 'c1',
           |  type = 'cow',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.index.column.stats.enable = 'true',
           |  hoodie.enable.data.skipping = 'true',
           |  hoodie.datasource.write.operation = 'insert'
           | )
           | location '$basePath'
     """.stripMargin)

      writeRecords(2, 4, 0, basePath, config)
      val conf = new Configuration
      val metaClient = HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(conf), basePath)
      assert(0 == metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.size())
      assert(metaClient.getActiveTimeline.filterPendingClusteringTimeline().empty())

      spark.sql(s"call run_clustering(table => '$tableName', op => 'schedule')")
      metaClient.reloadActiveTimeline()
      assert(0 == metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.size())
      assert(1 == metaClient.getActiveTimeline.filterPendingClusteringTimeline().getInstants.size())

      writeRecords(2, 4, 0, basePath, config)
      spark.sql(s"call run_clustering(table => '$tableName', op => 'execute')")
      metaClient.reloadActiveTimeline()
      assert(1 == metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.size())
      assert(0 == metaClient.getActiveTimeline.filterPendingClusteringTimeline().getInstants.size())

      writeRecords(2, 4, 0, basePath, config)
      spark.sql(s"call run_clustering(table => '$tableName')")
      metaClient.reloadActiveTimeline()
      assert(2 == metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.size())
      assert(0 == metaClient.getActiveTimeline.filterPendingClusteringTimeline().getInstants.size())

      writeRecords(2, 4, 0, basePath, config)
      spark.sql(s"call run_clustering(table => '$tableName')")
      metaClient.reloadActiveTimeline()
      assert(3 == metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.size())
      assert(0 == metaClient.getActiveTimeline.filterPendingClusteringTimeline().getInstants.size())

      checkExceptionContain(s"call run_clustering(table => '$tableName', op => 'null')")("Invalid value")
    }
  }

  test("Test Call run_clustering Procedure Order Strategy") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"

      val metadataOpts = Map(
        HoodieMetadataConfig.ENABLE.key -> "true",
        HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true" ,
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key() -> "true"
      )

      val queryOpts = metadataOpts ++ Map(
        "path" -> basePath,
        DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL
      )

      val dataFilterC2 = EqualTo(AttributeReference("c2", StringType, nullable = false)(), Literal("foo23"))
      val dataFilterC3 = EqualTo(AttributeReference("c3", StringType, nullable = false)(), Literal("bar23"))

      spark.sql(
        s"""
           |create table $tableName (
           |  c1 int,
           |  c2 string,
           |  c3 double
           |) using hudi
           | options (
           |  primaryKey = 'c1',
           |  type = 'cow',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.index.column.stats.enable = 'true',
           |  hoodie.enable.data.skipping = 'true',
           |  hoodie.datasource.write.operation = 'insert'
           | )
           | location '$basePath'
     """.stripMargin)

      val fileNum = 20
      val numRecords = 400000
      val config = metadataOpts ++ Map("hoodie.avro.schema.validate" -> "false")

      // insert records
      val records = writeRecords(fileNum, numRecords, 0, basePath, config)
      val conf = new Configuration
      val metaClient = HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(conf), basePath)
      val avgSize = avgRecord(metaClient.getActiveTimeline)
      val avgCount = Math.ceil(1.0 * numRecords / fileNum).toLong

      spark.sql(
        s"""call run_clustering(table => '$tableName', order => 'c2,c3', order_strategy => 'linear', options => "
           | hoodie.copyonwrite.record.size.estimate=$avgSize,
           | hoodie.parquet.max.file.size=${avgSize * avgCount},
           | hoodie.parquet.small.file.limit=0,
           | hoodie.clustering.plan.strategy.target.file.max.bytes=${avgSize * avgCount},
           | hoodie.clustering.plan.strategy.max.bytes.per.group=${4 * avgSize * avgCount},
           | hoodie.metadata.enable=true,
           | hoodie.metadata.index.column.stats.enable=true
           |")""".stripMargin)

      metaClient.reloadActiveTimeline()
      val fileIndex1 = HoodieFileIndex(spark, metaClient, None, queryOpts)
      val orderAllFiles = fileIndex1.allBaseFiles.size
      val c2OrderFilterCount = fileIndex1.listFiles(Seq(), Seq(dataFilterC2)).head.files.size
      val c3OrderFilterCount = fileIndex1.listFiles(Seq(), Seq(dataFilterC3)).head.files.size
      val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())

      // re-create and ingestion + clustering
      fs.delete(new Path(basePath), true)
      writeRecordsArray(fileNum, records, basePath, config ++ Map("hoodie.table.name" -> tableName))
      spark.sql(
        s"""call run_clustering(table => '$tableName', order => 'c2,c3', order_strategy => 'z-order', options => "
           | hoodie.copyonwrite.record.size.estimate=$avgSize,
           | hoodie.parquet.max.file.size=${avgSize * avgCount},
           | hoodie.parquet.small.file.limit=0,
           | hoodie.clustering.plan.strategy.target.file.max.bytes=${avgSize * avgCount},
           | hoodie.clustering.plan.strategy.max.bytes.per.group=${4 * avgSize * avgCount},
           | hoodie.metadata.enable=true,
           | hoodie.metadata.index.column.stats.enable=true
           |")""".stripMargin)

      metaClient.reloadActiveTimeline()
      val fileIndex2 = HoodieFileIndex(spark, metaClient, None, queryOpts)
      val ZOrderAllFiles = fileIndex2.allBaseFiles.size
      val c2ZOrderFilterCount = fileIndex2.listFiles(Seq(), Seq(dataFilterC2)).head.files.size
      val c3ZOrderFilterCount = fileIndex2.listFiles(Seq(), Seq(dataFilterC3)).head.files.size

      assert((1.0 * c2OrderFilterCount / orderAllFiles) < (1.0 * c2ZOrderFilterCount / ZOrderAllFiles))
      assert((1.0 * c3OrderFilterCount / orderAllFiles) > (1.0 * c3ZOrderFilterCount / ZOrderAllFiles))
    }
  }

  test("Test Call run_clustering with partition selected") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
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
           |  type = 'cow',
           |  preCombineField = 'ts'
           | )
           | partitioned by(partition)
           | location '$basePath'
     """.stripMargin)

      // Test clustering with PARTITION_SELECTED config set, choose only a part of all partitions to schedule
      {
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1010, 1010)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1010, 1010)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1011, 1011)")
        // Do
        val result = spark.sql(s"call run_clustering(table => '$tableName', " +
          s"selected_partitions => 'partition=1010', show_involved_partition => true)")
          .collect()
          .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2), row.getString(3)))
        assertResult(1)(result.length)
        assertResult("partition=1010")(result(0)(3))

        checkAnswer(s"select id, name, price, ts, partition from $tableName order by id")(
          Seq(1, "a1", 10.0, 1010, 1010),
          Seq(2, "a2", 10.0, 1010, 1010),
          Seq(3, "a3", 10.0, 1011, 1011)
        )
      }

      // Test clustering with PARTITION_SELECTED, choose all partitions to schedule
      {
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1010, 1010)")
        spark.sql(s"insert into $tableName values(5, 'a5', 10, 1011, 1011)")
        spark.sql(s"insert into $tableName values(6, 'a6', 10, 1012, 1012)")
        val result = spark.sql(s"call run_clustering(table => '$tableName', " +
          s"selected_partitions => 'partition=1010,partition=1011,partition=1012', show_involved_partition => true)")
          .collect()
          .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2), row.getString(3)))
        assertResult(1)(result.length)
        assertResult("partition=1010,partition=1011,partition=1012")(result(0)(3))

        checkAnswer(s"select id, name, price, ts, partition from $tableName order by id")(
          Seq(1, "a1", 10.0, 1010, 1010),
          Seq(2, "a2", 10.0, 1010, 1010),
          Seq(3, "a3", 10.0, 1011, 1011),
          Seq(4, "a4", 10.0, 1010, 1010),
          Seq(5, "a5", 10.0, 1011, 1011),
          Seq(6, "a6", 10.0, 1012, 1012)
        )
      }
    }
  }

  test("Test Call run_clustering with unsupported bucket index") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  partition long
           |) using hudi
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  preCombineField = 'ts',
           |  hoodie.index.type = 'BUCKET',
           |  hoodie.bucket.index.hash.field = 'id'
           | )
           | partitioned by (partition)
           | location '$basePath'
     """.stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1010, 1010)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1010, 1010)")

      checkExceptionContain(s"call run_clustering(table => '$tableName')")(
        "Executor SparkExecuteClusteringCommitActionExecutor is not compatible with table layout HoodieSimpleBucketLayout")
    }
  }

  test("Test Call run_clustering with limit parameter") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""
           |create table $tableName (
           |  c1 int,
           |  c2 string,
           |  c3 double
           |) using hudi
           | options (
           |  primaryKey = 'c1',
           |  type = 'cow',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.index.column.stats.enable = 'true',
           |  hoodie.enable.data.skipping = 'true',
           |  hoodie.datasource.write.operation = 'insert'
           | )
           | location '$basePath'
     """.stripMargin)

      val conf = new Configuration
      val metaClient = HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(conf), basePath)
      assert(0 == metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.size())
      assert(metaClient.getActiveTimeline.filterPendingClusteringTimeline().empty())

      writeRecords(2, 4, 0, basePath, Map("hoodie.avro.schema.validate" -> "false"))
      spark.sql(s"call run_clustering(table => '$tableName', op => 'schedule')")

      writeRecords(2, 4, 0, basePath, Map("hoodie.avro.schema.validate" -> "false"))
      spark.sql(s"call run_clustering(table => '$tableName', op => 'schedule')")

      metaClient.reloadActiveTimeline()
      assert(0 == metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.size())
      assert(2 == metaClient.getActiveTimeline.filterPendingClusteringTimeline().getInstants.size())

      spark.sql(s"call run_clustering(table => '$tableName', op => 'execute')")
      metaClient.reloadActiveTimeline()
      assert(2 == metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.size())
      assert(0 == metaClient.getActiveTimeline.filterPendingClusteringTimeline().getInstants.size())

      writeRecords(2, 4, 0, basePath, Map("hoodie.avro.schema.validate" -> "false"))
      spark.sql(s"call run_clustering(table => '$tableName', op => 'schedule')")

      writeRecords(2, 4, 0, basePath, Map("hoodie.avro.schema.validate" -> "false"))
      spark.sql(s"call run_clustering(table => '$tableName', op => 'schedule')")

      spark.sql(s"call run_clustering(table => '$tableName', op => 'execute', limit => 1)")
      metaClient.reloadActiveTimeline()
      assert(3 == metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.size())
      assert(1 == metaClient.getActiveTimeline.filterPendingClusteringTimeline().getInstants.size())
    }
  }

  test("Test Call show_clustering with limit") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      val hudiOptions = Map(
        "hoodie.table.name" -> tableName,
        "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
        "hoodie.datasource.write.recordkey.field" -> "a",
        "hoodie.datasource.write.partitionpath.field" -> "a,b,c",
        "hoodie.clean.automatic" -> "true",
        "hoodie.metadata.enable" -> "true",
        "hoodie.clustering.inline" -> "true",
        "hoodie.clustering.inline.max.commits" -> "1",
        "hoodie.cleaner.commits.retained" -> "2",
        "hoodie.datasource.write.operation" -> "insert_overwrite"
      )

      val data = Seq(
        (1, 2, 4),
        (1, 2, 4),
        (1, 2, 3)
      )
      val schema = StructType(Array(
        StructField("a", IntegerType, true),
        StructField("b", IntegerType, true),
        StructField("c", IntegerType, true)
      ))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data).map {
        case (a, b, c) => Row(a, b, c)
      }, schema)

      df.write
        .options(hudiOptions)
        .format("hudi")
        .mode("append")
        .save(s"$basePath")

      assertResult(1)(spark.sql(s"call show_clustering(path => '$basePath')").count())
      assertResult(1)(spark.sql(s"call show_clustering(path => '$basePath', limit => 1)").count())
      assertResult(1)(spark.sql(s"call show_clustering(path => '$basePath', limit => 2)").count())
    }
  }

  def avgRecord(commitTimeline: HoodieTimeline): Long = {
    var totalByteSize = 0L
    var totalRecordsCount = 0L
    val layout = TimelineLayout.fromVersion(commitTimeline.getTimelineLayoutVersion)
    commitTimeline.getReverseOrderedInstants.toArray.foreach(instant => {
      val commitMetadata = commitTimeline.readCommitMetadata(instant.asInstanceOf[HoodieInstant])
      totalByteSize = totalByteSize + commitMetadata.fetchTotalBytesWritten()
      totalRecordsCount = totalRecordsCount + commitMetadata.fetchTotalRecordsWritten()
    })

    Math.ceil((1.0 * totalByteSize) / totalRecordsCount).toLong
  }

  def writeRecords(files: Int, numRecords: Int, partitions: Int, location: String, options: Map[String, String]): util.ArrayList[Row] = {
    val records = new util.ArrayList[Row](numRecords)
    val rowDimension = Math.ceil(Math.sqrt(numRecords)).toInt

    val data = Stream.range(0, rowDimension, 1)
      .flatMap(x => Stream.range(0, rowDimension, 1).map(y => Pair.of(x, y)))

    if (partitions > 0) {
      data.foreach { i =>
        records.add(Row(i.getLeft % partitions, "foo" + i.getLeft, "bar" + i.getRight))
      }
    } else {
      data.foreach { i =>
        records.add(Row(i.getLeft, "foo" + i.getLeft, "bar" + i.getRight))
      }
    }

    val struct = StructType(Array[StructField](
      StructField("c1", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("c2", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("c3", DataTypes.StringType, nullable = true, Metadata.empty)
    ))

    // files can not effect for hudi
    val df = spark.createDataFrame(records, struct).repartition(files)
    writeDF(df, location, options)
    records
  }

  def writeRecordsArray(files: Int, records: util.ArrayList[Row], location: String, options: Map[String, String]): Unit = {
    val struct = StructType(Array[StructField](
      StructField("c1", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("c2", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("c3", DataTypes.StringType, nullable = true, Metadata.empty)
    ))

    // files can not effect for hudi
    val df = spark.createDataFrame(records, struct).repartition(files)
    writeDF(df, location, options)
  }

  def writeDF(df: Dataset[Row], location: String, options: Map[String, String]): Unit = {
    df.select("c1", "c2", "c3")
      .sortWithinPartitions("c1", "c2")
      .write
      .format("hudi")
      .option(OPERATION.key(), WriteOperationType.INSERT.value())
      .option(RECORDKEY_FIELD.key(), "c1")
      .options(options)
      .mode("append").save(location)
  }
}
