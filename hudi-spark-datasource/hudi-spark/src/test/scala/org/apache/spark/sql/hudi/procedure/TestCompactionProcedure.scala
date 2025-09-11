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

import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.testutils.HoodieTestUtils.createMetaClient
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.hadoop.conf.Configuration

class TestCompactionProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call run_compaction Procedure by Table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'mor',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      spark.sql("set hoodie.parquet.max.file.size = 10000")
      // disable automatic inline compaction
      spark.sql("set hoodie.compact.inline=false")
      spark.sql("set hoodie.compact.schedule.inline=false")

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
      spark.sql(s"insert into $tableName values(4, 'a4', 10, 1000)")
      spark.sql(s"update $tableName set price = 11 where id = 1")

      // Schedule the first compaction
      val resultA = spark.sql(s"call run_compaction(op => 'schedule', table => '$tableName')")
        .collect()
        .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2)))

      spark.sql(s"update $tableName set price = 12 where id = 2")

      // Schedule the second compaction
      val resultB = spark.sql(s"call run_compaction('schedule', '$tableName')")
        .collect()
        .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2)))

      assertResult(1)(resultA.length)
      assertResult(1)(resultB.length)
      val showCompactionSql: String = s"call show_compaction(table => '$tableName', limit => 10)"
      val showCompactionResultsDf = spark.sql(showCompactionSql)
      showCompactionResultsDf.show(false)
      val showCompactionResults = showCompactionResultsDf.collect()
      assertResult(2)(showCompactionResults.length)

      val firstResult = showCompactionResults.find(_.getString(0) == resultA(0).head).get
      val secondResult = showCompactionResults.find(_.getString(0) == resultB(0).head).get

      assertResult(resultA(0).head)(firstResult.getString(0))
      assertResult(resultA(0)(1))(firstResult.getInt(5))
      assertResult(resultA(0)(2))(firstResult.getString(2))
      assertResult("compaction")(firstResult.getString(3))

      assertResult(resultB(0).head)(secondResult.getString(0))
      assertResult(resultB(0)(1))(secondResult.getInt(5))
      assertResult(resultB(0)(2))(secondResult.getString(2))
      assertResult("compaction")(secondResult.getString(3))

      val compactionRows = spark.sql(showCompactionSql).collect()
      val timestamps = compactionRows.map(_.getString(0)).sorted
      assertResult(2)(timestamps.length)

      // Execute the second scheduled compaction instant actually
      checkAnswer(s"call run_compaction(op => 'run', table => '$tableName', timestamp => ${timestamps(1)})")(
        Seq(resultB(0).head, resultB(0)(1), HoodieInstant.State.COMPLETED.name())
      )
      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 11.0, 1000),
        Seq(2, "a2", 12.0, 1000),
        Seq(3, "a3", 10.0, 1000),
        Seq(4, "a4", 10.0, 1000)
      )

      // A compaction action eventually becomes commit when completed, so show_compaction
      // can only see the first scheduled compaction instant
      val resultC = spark.sql(s"call show_compaction('$tableName')")
        .collect()
        .map(row => Seq(row.getString(0), row.getInt(5), row.getString(2)))
      assertResult(2)(resultC.length)

      checkAnswer(s"call run_compaction(op => 'run', table => '$tableName', timestamp => ${timestamps(0)})")(
        Seq(resultA(0).head, resultA(0)(1), HoodieInstant.State.COMPLETED.name())
      )
      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 11.0, 1000),
        Seq(2, "a2", 12.0, 1000),
        Seq(3, "a3", 10.0, 1000),
        Seq(4, "a4", 10.0, 1000)
      )
      assertResult(2)(spark.sql(s"call show_compaction(table => '$tableName')").collect().length)
    }
  }

  test("Test Call run_compaction Procedure by Path") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'mor',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      spark.sql("set hoodie.parquet.max.file.size = 10000")
      // disable automatic inline compaction
      spark.sql("set hoodie.compact.inline=false")
      spark.sql("set hoodie.compact.schedule.inline=false")

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
      spark.sql(s"update $tableName set price = 11 where id = 1")

      checkAnswer(s"call run_compaction(op => 'run', path => '${tmp.getCanonicalPath}')")()
      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 11.0, 1000),
        Seq(2, "a2", 10.0, 1000),
        Seq(3, "a3", 10.0, 1000)
      )
      assertResult(0)(spark.sql(s"call show_compaction(path => '${tmp.getCanonicalPath}')").collect().length)

      spark.sql(s"update $tableName set price = 12 where id = 1")

      // Schedule the first compaction
      val resultA = spark.sql(s"call run_compaction(op=> 'schedule', path => '${tmp.getCanonicalPath}')")
        .collect()
        .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2)))

      spark.sql(s"update $tableName set price = 12 where id = 2")

      // Schedule the second compaction
      val resultB = spark.sql(s"call run_compaction(op => 'schedule', path => '${tmp.getCanonicalPath}')")
        .collect()
        .map(row => Seq(row.getString(0), row.getInt(1), row.getString(2)))

      assertResult(1)(resultA.length)
      assertResult(1)(resultB.length)
      val showCompactionResults = spark.sql(s"call show_compaction(path => '${tmp.getCanonicalPath}')").collect()
      assertResult(2)(showCompactionResults.length)

      val firstResult = showCompactionResults.find(_.getString(0) == resultA(0).head).get
      val secondResult = showCompactionResults.find(_.getString(0) == resultB(0).head).get

      assertResult(resultA(0).head)(firstResult.getString(0))
      assertResult(resultA(0)(1))(firstResult.getInt(5))
      assertResult(resultA(0)(2))(firstResult.getString(2))
      assertResult("compaction")(firstResult.getString(3))

      assertResult(resultB(0).head)(secondResult.getString(0))
      assertResult(resultB(0)(1))(secondResult.getInt(5))
      assertResult(resultB(0)(2))(secondResult.getString(2))
      assertResult("compaction")(secondResult.getString(3))

      // Run compaction for all the scheduled compaction
      checkAnswer(s"call run_compaction(op => 'run', path => '${tmp.getCanonicalPath}')")(
        Seq(resultA(0).head, resultA(0)(1), HoodieInstant.State.COMPLETED.name()),
        Seq(resultB(0).head, resultB(0)(1), HoodieInstant.State.COMPLETED.name())
      )

      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 12.0, 1000),
        Seq(2, "a2", 12.0, 1000),
        Seq(3, "a3", 10.0, 1000)
      )
      assertResult(2)(spark.sql(s"call show_compaction(path => '${tmp.getCanonicalPath}')").collect().length)

      checkException(s"call run_compaction(op => 'run', path => '${tmp.getCanonicalPath}', timestamp => 12345L)")(
        s"specific 12345 instants is not exist"
      )
    }
  }

  test("Test show_compaction Procedure by Path") {
    withTempDir { tmp =>
      val tableName1 = generateTableName
      spark.sql(
        s"""
           |create table $tableName1 (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
           | location '${tmp.getCanonicalPath}/$tableName1'
       """.stripMargin)
      // set inline compaction
      spark.sql("set hoodie.compact.inline=true")
      spark.sql("set hoodie.compact.inline.max.delta.commits=2")

      spark.sql(s"insert into $tableName1 values(1, 'a1', 10, 1000)")
      spark.sql(s"update $tableName1 set name = 'a2' where id = 1")
      spark.sql(s"update $tableName1 set name = 'a3' where id = 1")
      spark.sql(s"update $tableName1 set name = 'a4' where id = 1")
      assertResult(2)(spark.sql(s"call show_compaction(path => '${tmp.getCanonicalPath}/$tableName1')").collect().length)
    }
  }

  test("Test run_compaction Procedure with options") {
    withSQLConf("hoodie.compact.inline" -> "false", "hoodie.compact.inline.max.delta.commits" -> "1") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"update $tableName set name = 'a2' where id = 1")
        spark.sql(s"update $tableName set name = 'a3' where id = 1")

        val result1 = spark.sql(
          s"""call run_compaction(table => '$tableName', op => 'run', options => "
             | hoodie.compaction.strategy=org.apache.hudi.table.action.compact.strategy.LogFileNumBasedCompactionStrategy,
             | hoodie.compaction.logfile.num.threshold=3
             |")""".stripMargin)
          .collect()
        assertResult(0)(result1.length)

        spark.sql(s"update $tableName set name = 'a4' where id = 1")
        val result2 = spark.sql(
          s"""call run_compaction(table => '$tableName', op => 'run', options => "
             | hoodie.compaction.strategy=org.apache.hudi.table.action.compact.strategy.LogFileNumBasedCompactionStrategy,
             | hoodie.compaction.logfile.num.threshold=3
             |")""".stripMargin)
          .collect()
        assertResult(1)(result2.length)
      }
    }
  }

  test("Test Call run_compaction Procedure with specific instants") {
    withSQLConf("hoodie.compact.inline" -> "false", "hoodie.compact.inline.max.delta.commits" -> "1") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"update $tableName set name = 'a2' where id = 1")

        spark.sql(s"call run_compaction(table => '$tableName', op => 'schedule')")

        val metaClient = createMetaClient(tmp.getCanonicalPath)
        val instants = metaClient.getActiveTimeline.filterPendingCompactionTimeline().getInstants
        assertResult(1)(instants.size())
        val ts = instants.get(0).requestedTime
        assertResult(1)(spark.sql(s"call run_compaction(table => '$tableName', op => 'execute', instants => '$ts')").collect().length)

        checkExceptionContain(
          s"call run_compaction(table => '$tableName', op => 'execute', instants => '000000')"
        )("specific 000000 instants is not exist")
      }
    }
  }

  test("Test Call run_compaction Procedure with operation") {
    withSQLConf("hoodie.compact.inline" -> "false", "hoodie.compact.inline.max.delta.commits" -> "1") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"update $tableName set name = 'a2' where id = 1")

        assertResult(0)(spark.sql(s"call run_compaction(table => '$tableName', op => 'execute')").collect().length)

        assertResult(1)(spark.sql(s"call run_compaction(table => '$tableName', op => 'schedule')").collect().length)

        assertResult(1)(spark.sql(s"call run_compaction(table => '$tableName', op => 'execute')").collect().length)

        spark.sql(s"update $tableName set name = 'a3' where id = 1")

        assertResult(1)(spark.sql(s"call run_compaction(table => '$tableName', op => 'scheduleAndExecute')").collect().length)
      }
    }
  }

  test("Test Call run_clustering with limit parameter") {
    withSQLConf("hoodie.compact.inline" -> "false", "hoodie.compact.inline.max.delta.commits" -> "1") {
      withTempDir { tmp =>
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
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | location '${basePath}'
       """.stripMargin)

        val conf = new Configuration
        val metaClient = HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(conf), basePath)

        assert(0 == metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.size())
        assert(metaClient.getActiveTimeline.filterPendingClusteringTimeline().empty())

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"update $tableName set name = 'a2' where id = 1")

        spark.sql(s"call run_compaction(table => '$tableName', op => 'schedule')")

        spark.sql(s"insert into $tableName values(2, 'b1', 20, 3000)")
        spark.sql(s"update $tableName set name = 'b3' where id = 2")

        spark.sql(s"call run_compaction(table => '$tableName', op => 'schedule')")
        metaClient.reloadActiveTimeline();
        assert(2 == metaClient.getActiveTimeline.filterPendingCompactionTimeline().getInstants.size())

        spark.sql(s"call run_compaction(table => '$tableName', op => 'execute', limit => 1)");

        metaClient.reloadActiveTimeline();
        assert(1 == metaClient.getActiveTimeline.filterPendingCompactionTimeline().getInstants.size())
      }
    }
  }

  test("Test show_compaction procedure") {
    withSQLConf("hoodie.compact.inline" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | ts long
             | ) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |   primaryKey = 'id',
             |   type = 'mor',
             |   preCombineField = 'ts'
             | )
             |""".stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"update $tableName set price = 11 where id = 1")
        spark.sql(s"update $tableName set price = 12 where id = 1")

        spark.sql(s"call run_compaction(table => '$tableName', op => 'schedule')")

        val allCompactions = spark.sql(s"call show_compaction(table => '$tableName')")
        allCompactions.show(false)
        val allCompactionData = allCompactions.collect()

        assert(allCompactionData.length >= 1, "Should have at least one compaction operation")
        assert(allCompactions.schema.fields.length == 11, "show_compaction should have 11 fields")

        val schema = allCompactions.schema
        assert(schema.fieldNames.contains("compaction_time"))
        assert(schema.fieldNames.contains("state_transition_time"))
        assert(schema.fieldNames.contains("state"))
        assert(schema.fieldNames.contains("action"))
        assert(schema.fieldNames.contains("operation_size"))
        assert(schema.fieldNames.contains("partition_path"))
        assert(schema.fieldNames.contains("total_log_files_per_partition"))
        assert(schema.fieldNames.contains("total_updated_records_compacted_per_partition"))
        assert(schema.fieldNames.contains("total_log_size_compacted_per_partition"))
        assert(schema.fieldNames.contains("total_write_bytes_per_partition"))

        val pendingCompactions = allCompactionData.filter(row =>
          row.getString(2) == "REQUESTED" || row.getString(2) == "INFLIGHT")
        assert(pendingCompactions.length >= 1, "Should have at least one pending compaction")

        pendingCompactions.foreach { pendingCompaction =>
          assert(pendingCompaction.getString(0) != null)
          assert(pendingCompaction.getString(2) != null)
          assert(pendingCompaction.getString(3) == "compaction")
          assert(pendingCompaction.getInt(5) >= 0)
        }
      }
    }
  }
}
