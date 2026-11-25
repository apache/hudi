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

package org.apache.spark.sql.hudi.dml.others

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.getMetaClientAndFileSystemView
import org.junit.jupiter.api.Assertions.assertFalse

class TestDeleteTable extends HoodieSparkSqlTestBase {

  test("Test Delete Table") {
    withTempDir { tmp =>
      Seq(true, false).foreach { sparkSqlOptimizedWrites =>
        Seq("cow", "mor").foreach { tableType =>
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
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               | )
       """.stripMargin)

          // test with optimized sql writes enabled / disabled.
          withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> sparkSqlOptimizedWrites.toString) {
            // insert data to table
            spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
            checkAnswer(s"select id, name, price, ts from $tableName")(
              Seq(1, "a1", 10.0, 1000)
            )

            // delete data from table
            spark.sql(s"delete from $tableName where id = 1")
            checkAnswer(s"select count(1) from $tableName")(
              Seq(0)
            )

            spark.sql(s"insert into $tableName select 2, 'a2', 10, 1000")
            spark.sql(s"delete from $tableName where id = 1")
            checkAnswer(s"select id, name, price, ts from $tableName")(
              Seq(2, "a2", 10.0, 1000)
            )

            spark.sql(s"delete from $tableName")
            checkAnswer(s"select count(1) from $tableName")(
              Seq(0)
            )
          }
        }
      }
    }
  }

  test("Test Delete Table With A Condition That Is Always False") {
    withTempDir { tmp =>
      Seq(true, false).foreach { sparkSqlOptimizedWrites =>
        Seq("cow", "mor").foreach { tableType =>
          val tableName = generateTableName
          val tablePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | location '$tablePath'
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               | )
       """.stripMargin)

          withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> sparkSqlOptimizedWrites.toString) {
            spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000), (2, null, 20, 1000)")
            def completedCommits(): Int =
              createMetaClient(spark, tablePath).getActiveTimeline.getCommitsTimeline.filterCompletedInstants().countInstants()
            val commitsAfterInsert = completedCommits()

            // Conditions that fold to false or null delete nothing and write no commit.
            Seq("false", "50 <> 50", "1 > null", "name = null").foreach { condition =>
              spark.sql(s"delete from $tableName where $condition")
              assertResult(commitsAfterInsert, condition)(completedCommits())
            }
            checkAnswer(s"select id, name, price, ts from $tableName order by id")(
              Seq(1, "a1", 10.0, 1000),
              Seq(2, null, 20.0, 1000)
            )

            // A condition evaluated against the rows still commits when it matches none.
            spark.sql(s"delete from $tableName where id = 3")
            assertResult(commitsAfterInsert + 1)(completedCommits())

            spark.sql(s"delete from $tableName where 21 = 21")
            assertResult(commitsAfterInsert + 2)(completedCommits())
            checkAnswer(s"select count(1) from $tableName")(Seq(0))
          }
        }
      }
    }
  }

  test("Test Delete Table Without Primary Key") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        Seq(true, false).foreach { isPartitioned =>
          val tableName = generateTableName
          val partitionedClause = if (isPartitioned) {
            "PARTITIONED BY (name)"
          } else {
            ""
          }
          // create table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  price double,
               |  ts long,
               |  name string
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
               | $partitionedClause
   """.stripMargin)

          // test with optimized sql writes enabled.
          withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> "true") {
            // insert data to table
            spark.sql(s"insert into $tableName select 1, 10, 1000, 'a1'")
            checkAnswer(s"select id, name, price, ts from $tableName")(
              Seq(1, "a1", 10.0, 1000)
            )

            // delete data from table
            spark.sql(s"delete from $tableName where id = 1")
            checkAnswer(s"select count(1) from $tableName")(
              Seq(0)
            )

            spark.sql(s"insert into $tableName select 2, 10, 1000, 'a2'")
            spark.sql(s"delete from $tableName where id = 1")
            checkAnswer(s"select id, name, price, ts from $tableName")(
              Seq(2, "a2", 10.0, 1000)
            )

            spark.sql(s"delete from $tableName")
            checkAnswer(s"select count(1) from $tableName")(
              Seq(0)
            )
          }
        }
      }
    }
  }

  test("Test Delete From Partitioned Table Without Primary Key") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
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
             |  type = '$tableType',
             |  preCombineField = 'price'
             |)
             |partitioned by (ts)
             |""".stripMargin)

        // test with optimized sql writes enabled.
        withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> "true") {
          // insert data to table
          spark.sql(
            s"""
               |insert into $tableName
               |values
               |  (1, 'a1', cast(10.0 as double), 1000),
               |  (2, 'a2', cast(20.0 as double), 1000),
               |  (3, 'a2', cast(30.0 as double), 1000)
               |""".stripMargin)
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 10.0, 1000),
            Seq(2, "a2", 20.0, 1000),
            Seq(3, "a2", 30.0, 1000)
          )

          // delete data from table
          spark.sql(s"delete from $tableName where id = 1")

          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(2, "a2", 20.0, 1000),
            Seq(3, "a2", 30.0, 1000)
          )
        }
      }
    }
  }

  test("Test Delete Table On Non-PK Condition") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>

        /** non-partitioned table */
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
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
          """.stripMargin)

        // insert data to table
        spark.sql(
          s"""
             |insert into $tableName
             |values (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 1000), (3, 'a2', 30.0, 1000)
             |""".stripMargin)

        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 20.0, 1000),
          Seq(3, "a2", 30.0, 1000)
        )

        // delete data on non-pk condition
        spark.sql(s"delete from $tableName where name = 'a2'")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        /** partitioned table */
        val ptTableName = generateTableName + "_pt"
        // create table
        spark.sql(
          s"""
             |create table $ptTableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  pt string
             |) using hudi
             | location '${tmp.getCanonicalPath}/$ptTableName'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | partitioned by (pt)
          """.stripMargin)

        // insert data to table
        spark.sql(
          s"""
             |insert into $ptTableName
             |values (1, 'a1', 10.0, 1000, "2021"), (2, 'a2', 20.0, 1000, "2021"), (3, 'a2', 30.0, 1000, "2022")
             |""".stripMargin)

        checkAnswer(s"select id, name, price, ts, pt from $ptTableName")(
          Seq(1, "a1", 10.0, 1000, "2021"),
          Seq(2, "a2", 20.0, 1000, "2021"),
          Seq(3, "a2", 30.0, 1000, "2022")
        )

        // delete data on non-pk condition
        spark.sql(s"delete from $ptTableName where name = 'a2'")
        checkAnswer(s"select id, name, price, ts, pt from $ptTableName")(
          Seq(1, "a1", 10.0, 1000, "2021")
        )

        spark.sql(s"delete from $ptTableName where pt = '2021'")
        checkAnswer(s"select id, name, price, ts, pt from $ptTableName")(
          Seq.empty: _*
        )
      }
    }
  }

  test("Test Delete Table removes every row of a duplicate key") {
    // An insert with the none duplicate policy keeps rows that share a record key, in the same base file and across file groups
    Seq(("cow", false), ("mor", false), ("mor", true)).foreach { case (tableType, updatesAsDeletesAndInserts) =>
      withTempDir { tmp =>
        val tableName = generateTableName
        withSQLConf(HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() -> updatesAsDeletesAndInserts.toString,
          "hoodie.datasource.insert.dup.policy" -> "none") {
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  dt string
               |) using hudi
               | partitioned by (dt)
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  'hoodie.record.merge.mode' = 'COMMIT_TIME_ORDERING'
               | )
             """.stripMargin)
          spark.sql(s"insert into $tableName values (0, 'a0', 'p1'), (1, 'a1', 'p1'), (1, 'b1', 'p1'), (1, 'c1', 'p2')")
          spark.sql(s"insert into $tableName values (1, 'd1', 'p1'), (1, 'e1', 'p2'), (2, 'a2', 'p2')")
          checkAnswer(s"select count(1) from $tableName where id = 1")(Seq(5))

          spark.sql(s"delete from $tableName where id in (1, 100)")
          checkAnswer(s"select id, name, dt from $tableName order by id")(
            Seq(0, "a0", "p1"), Seq(2, "a2", "p2"))
        }
      }
    }
  }

  test("Test Delete Table with op upsert") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
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
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts',
             |  hoodie.datasource.write.operation = 'upsert'
             | )
       """.stripMargin)
        // insert data to table
        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        // delete data from table
        spark.sql(s"delete from $tableName where id = 1")
        checkAnswer(s"select count(1) from $tableName")(
          Seq(0)
        )

        spark.sql(s"insert into $tableName select 2, 'a2', 10, 1000")
        spark.sql(s"delete from $tableName where id = 1")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(2, "a2", 10.0, 1000)
        )

        spark.sql(s"delete from $tableName")
        checkAnswer(s"select count(1) from $tableName")(
          Seq(0)
        )
      }
    }
  }

  test("Test a cached view of a table reads the rows that a later DML statement writes") {
    // On Spark 4 the refresh of the table after a write resolves a relation whose options differ from those of
    // the relation in the cached plan, so it does not find the cached plan and the view is not re-executed
    assume(HoodieSparkUtils.isSpark3, "A cached plan is not re-executed after a write on Spark 4")
    withTempDir { tmp =>
      val statements = Seq(
        ("delete", (t: String) => s"delete from $t where id = 1", Seq.empty[Seq[Any]]),
        ("update", (t: String) => s"update $t set name = 'a1u', ts = 1001 where id = 1", Seq(Seq(1, "a1u"))),
        ("merge", (t: String) =>
          s"""merge into $t t using (select 1 as id, 'a1m' as name, 10.0 as price, 1001L as ts) s
             |on t.id = s.id when matched then update set *""".stripMargin, Seq(Seq(1, "a1m"))))
      Seq("cow", "mor").foreach { tableType =>
        statements.foreach { case (operation, statement, expectedRows) =>
          val tableName = generateTableName
          val tablePath = s"${tmp.getCanonicalPath}/$tableName"
          val viewName = s"${tableName}_view"
          spark.sql(
            s"""
               |create table $tableName (id int, name string, price double, ts long) using hudi
               | location '$tablePath'
               | tblproperties (type = '$tableType', primaryKey = 'id', preCombineField = 'ts')
             """.stripMargin)
          spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000), (2, 'a2', 10.0, 1000)")
          spark.sql(s"select id, name from $tableName where id = 1").createOrReplaceTempView(viewName)
          spark.sql(s"cache table $viewName")
          checkAnswer(s"select id, name from $viewName")(Seq(1, "a1"))

          spark.sql(statement(tableName))
          if (tableType == "mor") {
            val (_, fsView) = getMetaClientAndFileSystemView(tablePath)
            assertFalse(fsView.getLatestFileSlices("").findFirst().get().getLogFiles.findAny().isEmpty,
              s"the $operation must write a log file")
          }
          withClue(s"$tableType table, $operation: ") {
            checkAnswer(s"select id, name from $viewName")(expectedRows: _*)
          }
          spark.sql(s"uncache table $viewName")
        }
      }
    }
  }

  Seq(false, true).foreach { urlencode =>
    test(s"Test Delete single-partition table' partitions, urlencode: $urlencode") {
      Seq(true, false).foreach { sparkSqlOptimizedWrites =>
        withTempDir { tmp =>
          val tableName = generateTableName
          val tablePath = s"${tmp.getCanonicalPath}/$tableName"

          import spark.implicits._
          val df = Seq((1, "z3", "v1", "2021/10/01"), (2, "l4", "v1", "2021/10/02"))
            .toDF("id", "name", "ts", "dt")

          df.write.format("hudi")
            .option(HoodieWriteConfig.TBL_NAME.key, tableName)
            .option(TABLE_TYPE.key, MOR_TABLE_TYPE_OPT_VAL)
            .option(RECORDKEY_FIELD.key, "id")
            .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
            .option(PARTITIONPATH_FIELD.key, "dt")
            .option(URL_ENCODE_PARTITIONING.key(), urlencode)
            .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
            .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
            .mode(SaveMode.Overwrite)
            .save(tablePath)

          // register meta to spark catalog by creating table
          spark.sql(
            s"""
               |create table $tableName using hudi
               |location '$tablePath'
               |""".stripMargin)

          // test with optimized sql writes enabled / disabled.
          withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> sparkSqlOptimizedWrites.toString) {
            // delete 2021-10-01 partition
            if (urlencode) {
              spark.sql(s"""delete from $tableName where dt="2021/10/01"""")
            } else {
              spark.sql(s"delete from $tableName where dt='2021/10/01'")
            }

            checkAnswer(s"select dt from $tableName")(Seq(s"2021/10/02"))
          }
        }
      }
    }
  }
}
