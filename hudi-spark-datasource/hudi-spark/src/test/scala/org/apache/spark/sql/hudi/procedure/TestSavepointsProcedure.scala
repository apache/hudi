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

class TestSavepointsProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call create_savepoint Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      // create table
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
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      var commits = spark.sql(s"""call show_commits(table => '$tableName')""").limit(2).collect()
      assertResult(2) {
        commits.length
      }

      // Create savepoint using table name and commit time
      val commitTime1 = commits.apply(0).getString(0)
      checkAnswer(s"""call create_savepoint('$tableName', '$commitTime1', 'admin', '1')""")(Seq(true))

      // Create savepoint using table base path and commit time
      val commitTime2 = commits.apply(1).getString(0)
      checkAnswer(
        s"""call create_savepoint(path => '$tablePath', commit_time => '$commitTime2',
           | user => 'admin', comment => '2')""".stripMargin)(Seq(true))

      // insert data to table
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")

      commits = spark.sql(s"""call show_commits(table => '$tableName')""").limit(3).collect()
      assertResult(3) {
        commits.length
      }

      // Create savepoint using table name and latest commit
      val commitTime3 = commits.apply(2).getString(0)
      checkAnswer(s"""call create_savepoint('$tableName', '', 'admin', '3')""")(Seq(true))
      var savepoints = spark.sql(s"""call show_savepoints(table => '$tableName')""").collect()
      assertResult(3) {
        savepoints.length
      }
      assertResult(commitTime3)(savepoints(2).getString(0))

      // insert data to table
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")

      commits = spark.sql(s"""call show_commits(table => '$tableName')""").limit(4).collect()
      assertResult(4) {
        commits.length
      }

      // Create savepoint using table base path and latest commit
      val commitTime4 = commits.apply(3).getString(0)
      checkAnswer(
        s"""call create_savepoint(path => '$tablePath', user => 'admin', comment => '4')""".stripMargin)(Seq(true))
      savepoints = spark.sql(s"""call show_savepoints(table => '$tableName')""").collect()
      assertResult(4) {
        savepoints.length
      }
      assertResult(commitTime4)(savepoints(3).getString(0))
    }
  }

  test("Test Call show_savepoints Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      // create table
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
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")

      val commits = spark.sql(s"""call show_commits(table => '$tableName')""").collect()
      assertResult(3) {
        commits.length
      }

      val commitTime = commits.apply(1).getString(0)
      checkAnswer(s"""call create_savepoint('$tableName', '$commitTime')""")(Seq(true))

      // Show savepoints using table name
      val savepoints = spark.sql(s"""call show_savepoints(table => '$tableName')""").collect()
      assertResult(1) {
        savepoints.length
      }
      // Show savepoints using table base path
      assertResult(1) {
        spark.sql(s"""call show_savepoints(path => '$tablePath')""").collect().length
      }
    }
  }

  test("Test Call delete_savepoint Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      // create table
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
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")

      val commits = spark.sql(s"""call show_commits(table => '$tableName')""").collect()
      assertResult(4) {
        commits.length
      }

      // create 4 savepoints
      commits.foreach(r => {
        checkAnswer(s"""call create_savepoint('$tableName', '${r.getString(0)}')""")(Seq(true))
      })

      // Delete a savepoint with table name and instant time
      checkAnswer(s"""call delete_savepoint('$tableName', '${commits.apply(1).getString(0)}')""")(Seq(true))
      // Delete a savepoint with table base path and instant time
      checkAnswer(
        s"""call delete_savepoint(path => '$tablePath',
           | instant_time => '${commits.apply(0).getString(0)}')""".stripMargin)(Seq(true))

      // show_savepoints should return two savepoint
      var savepoints = spark.sql(s"""call show_savepoints(table => '$tableName')""").collect()
      assertResult(2) {
        savepoints.length
      }

      assertResult(commits(2).getString(0))(savepoints(0).getString(0))
      assertResult(commits(3).getString(0))(savepoints(1).getString(0))

      // Delete a savepoint with table name and latest savepoint time
      checkAnswer(s"""call delete_savepoint('$tableName', '')""")(Seq(true))

      // show_savepoints should return one savepoint
      savepoints = spark.sql(s"""call show_savepoints(table => '$tableName')""").collect()
      assertResult(1) {
        savepoints.length
      }

      assertResult(commits(3).getString(0))(savepoints(0).getString(0))

      // Delete a savepoint with table base path and latest savepoint time
      checkAnswer(s"""call delete_savepoint(path => '$tablePath')""".stripMargin)(Seq(true))

      // show_savepoints should return zero savepoint
      savepoints = spark.sql(s"""call show_savepoints(table => '$tableName')""").collect()
      assertResult(0) {
        savepoints.length
      }
    }
  }

  test("Test Call delete_savepoint Procedure with batch mode") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      // create table
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
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")

      val commits = spark.sql(s"""call show_commits(table => '$tableName')""").collect()
      assertResult(4) {
        commits.length
      }

      // create 4 savepoints
      commits.foreach(r => {
        checkAnswer(s"""call create_savepoint('$tableName', '${r.getString(0)}')""")(Seq(true))
      })

      // Delete 2 savepoint with table name and instant time
      val toDeleteInstant = s"${commits.apply(1).getString(0)},${commits.apply(0).getString(0)}"
      checkAnswer(s"""call delete_savepoint('$tableName', '${toDeleteInstant}')""")(Seq(true))

      // show_savepoints should return two savepoint
      var savepoints = spark.sql(s"""call show_savepoints(table => '$tableName')""").collect()
      assertResult(2) {
        savepoints.length
      }

      assertResult(commits(2).getString(0))(savepoints(0).getString(0))
      assertResult(commits(3).getString(0))(savepoints(1).getString(0))

      // Delete a savepoint with table name and latest savepoint time
      checkAnswer(s"""call delete_savepoint('$tableName', '')""")(Seq(true))

      // show_savepoints should return one savepoint
      savepoints = spark.sql(s"""call show_savepoints(table => '$tableName')""").collect()
      assertResult(1) {
        savepoints.length
      }

      assertResult(commits(3).getString(0))(savepoints(0).getString(0))

      // Delete a savepoint with table base path and latest savepoint time
      checkAnswer(s"""call delete_savepoint(path => '$tablePath')""".stripMargin)(Seq(true))

      // show_savepoints should return zero savepoint
      savepoints = spark.sql(s"""call show_savepoints(table => '$tableName')""").collect()
      assertResult(0) {
        savepoints.length
      }
    }
  }

  test("Test Call rollback_to_savepoint Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      // create table
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
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")

      val commits = spark.sql(s"""call show_commits(table => '$tableName')""").collect()
        .map(c => c.getString(0)).sorted
      assertResult(4) {
        commits.length
      }

      // create 4 savepoints
      commits.foreach(commit => {
        checkAnswer(s"""call create_savepoint('$tableName', '$commit')""")(Seq(true))
      })

      // rollback to the fourth savepoint with the table name and instant time
      checkAnswer(s"""call rollback_to_savepoint('$tableName', '${commits(3)}')""")(Seq(true))
      checkAnswer(
        s"""call delete_savepoint(path => '$tablePath',
           | instant_time => '${commits(3)}')""".stripMargin)(Seq(true))

      // rollback to the third savepoint with the table base path and instant time
      checkAnswer(
        s"""call rollback_to_savepoint(path => '$tablePath',
           | instant_time => '${commits(2)}')""".stripMargin)(Seq(true))

      // rollback to the second savepoint with the table name and latest savepoint time
      checkAnswer(s"""call rollback_to_savepoint('$tableName', '')""")(Seq(true))

      // rollback to the first savepoint with the table base path and latest savepoint time
      checkAnswer(s"""call rollback_to_savepoint(path => '$tablePath')""".stripMargin)(Seq(true))
    }
  }

  test("Test Call rollback_to_savepoint Procedure with refreshTable") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      // create table
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
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")

      val commits = spark.sql(s"""call show_commits(table => '$tableName')""").collect()
        .map(c => c.getString(0)).sorted
      assertResult(3) {
        commits.length
      }

      // create 3 savepoints
      checkAnswer(s"""call create_savepoint('$tableName', '${commits(0)}')""")(Seq(true))
      checkAnswer(s"""call create_savepoint('$tableName', '${commits(1)}')""")(Seq(true))

      spark.table(s"$tableName").select("id").cache()
      assertCached(spark.table(s"$tableName").select("id"), 1)

      // rollback to the second savepoint with the table name
      checkAnswer(s"""call rollback_to_savepoint('$tableName', '${commits(1)}')""")(Seq(true))
      assertCached(spark.table(s"$tableName").select("id"), 0)
      checkAnswer(
        s"""call delete_savepoint(path => '$tablePath',
           | instant_time => '${commits(1)}')""".stripMargin)(Seq(true))

      // rollback to the first savepoint with the table base path
      checkAnswer(
        s"""call rollback_to_savepoint(path => '$tablePath',
           | instant_time => '${commits(0)}')""".stripMargin)(Seq(true))
      // Check cache whether invalidate
      assertCached(spark.table(s"$tableName").select("id"), 0)
    }
  }

  test("Test Savepoint with Log Only MOR Table") {
    withRecordType()(withTempDir { tmp =>
      // Create table with INMEMORY index to generate log only mor table.
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
           |  preCombineField = 'ts',
           |  hoodie.index.type = 'INMEMORY',
           |  hoodie.compact.inline = 'false',
           |  hoodie.compact.inline.max.delta.commits = '1',
           |  hoodie.clean.automatic = 'false'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000L)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001L)")

      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 10, 1000L),
        Seq(2, "a2", 10, 1001L)
      )

      // Create savepoint
      val savePointTime = spark.sql(s"call show_commits(table => '$tableName')").sort("commit_time").collect().last.getString(0)
      spark.sql(s"call create_savepoint('$tableName', '$savePointTime')")

      // Run compaction
      spark.sql(s"call run_compaction(table => '$tableName', op=> 'run')")

      // Run clean
      spark.sql(s"call run_clean(table => '$tableName', clean_policy => 'KEEP_LATEST_FILE_VERSIONS', file_versions_retained => 1)")

      // Rollback to savepoint
      spark.sql(s"call rollback_to_savepoint('$tableName', '$savePointTime')")

      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 10, 1000L),
        Seq(2, "a2", 10, 1001L)
      )
    })
  }
}
