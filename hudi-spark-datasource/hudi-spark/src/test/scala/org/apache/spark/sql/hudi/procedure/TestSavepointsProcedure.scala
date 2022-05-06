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

import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase

class TestSavepointsProcedure extends HoodieSparkSqlTestBase {

  test("Test Call create_savepoints Procedure") {
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

      val commits = spark.sql(s"""call show_commits(table => '$tableName')""").limit(1).collect()
      assertResult(1) {
        commits.length
      }

      val commitTime = commits.apply(0).getString(0)
      checkAnswer(s"""call create_savepoints('$tableName', '$commitTime', 'admin', '1')""")(Seq(true))
    }
  }

  test("Test Call show_savepoints Procedure") {
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

      val commits = spark.sql(s"""call show_commits(table => '$tableName')""").collect()
      assertResult(3) {
        commits.length
      }

      val commitTime = commits.apply(1).getString(0)
      checkAnswer(s"""call create_savepoints('$tableName', '$commitTime')""")(Seq(true))

      // show savepoints
      val savepoints = spark.sql(s"""call show_savepoints(table => '$tableName')""").collect()
      assertResult(1) {
        savepoints.length
      }
    }
  }

  test("Test Call delete_savepoints Procedure") {
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

      val commits = spark.sql(s"""call show_commits(table => '$tableName')""").collect()
      assertResult(3) {
        commits.length
      }

      // create 3 savepoints
      commits.foreach(r => {
        checkAnswer(s"""call create_savepoints('$tableName', '${r.getString(0)}')""")(Seq(true))
      })

      // delete savepoints
      checkAnswer(s"""call delete_savepoints('$tableName', '${commits.apply(1).getString(0)}')""")(Seq(true))

      // show savepoints with only 2
      val savepoints = spark.sql(s"""call show_savepoints(table => '$tableName')""").collect()
      assertResult(2) {
        savepoints.length
      }
    }
  }

  test("Test Call rollback_savepoints Procedure") {
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

      val commits = spark.sql(s"""call show_commits(table => '$tableName')""").collect()
      assertResult(2) {
        commits.length
      }

      // create 2 savepoints
      commits.foreach(r => {
        checkAnswer(s"""call create_savepoints('$tableName', '${r.getString(0)}')""")(Seq(true))
      })

      // rollback savepoints
      checkAnswer(s"""call rollback_savepoints('$tableName', '${commits.apply(0).getString(0)}')""")(Seq(true))
    }
  }
}
