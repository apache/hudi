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

class TestCleanProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call run_clean Procedure by Table") {
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.parquet.max.file.size" -> "10000") {
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
             |   type = 'cow',
             |   orderingFields = 'ts'
             | )
             |""".stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"update $tableName set price = 11 where id = 1")
        spark.sql(s"update $tableName set price = 12 where id = 1")
        spark.sql(s"update $tableName set price = 13 where id = 1")

        // KEEP_LATEST_COMMITS
        val result1 = spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")
          .collect()
          .map(row => Seq(row.getString(0), row.getLong(1), row.getInt(2), row.getString(3), row.getString(4), row.getInt(5)))

        assertResult(1)(result1.length)
        assertResult(2)(result1(0)(2))

        val result11 = spark.sql(
          s"call show_fsview_all(table => '$tableName', path_regex => '', limit => 10)").collect()
        assertResult(2)(result11.length)

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 13, 1000)
        )

        val result2 = spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")
          .collect()
        assertResult(0)(result2.length)

        // KEEP_LATEST_FILE_VERSIONS
        spark.sql(s"update $tableName set price = 14 where id = 1")

        val result3 = spark.sql(
          s"call run_clean(table => '$tableName', clean_policy => 'KEEP_LATEST_FILE_VERSIONS', file_versions_retained => 3)").collect()
        assertResult(0)(result3.length)

        val result4 = spark.sql(
          s"call show_fsview_all(table => '$tableName', path_regex => '', limit => 10)").collect()
        assertResult(3)(result4.length)

        val result5 = spark.sql(
          s"call run_clean(table => '$tableName', clean_policy => 'KEEP_LATEST_FILE_VERSIONS', file_versions_retained => 1)").collect()
        assertResult(1)(result5.length)
        assertResult(2)(result5(0)(2))

        val result6 = spark.sql(
          s"call show_fsview_all(table => '$tableName', path_regex => '', limit => 10)").collect()
        assertResult(1)(result6.length)

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 14, 1000)
        )

        // trigger time
        spark.sql(s"update $tableName set price = 15 where id = 1")

        // no trigger, only has 1 commit
        val result7 = spark.sql(
          s"call run_clean(table => '$tableName', trigger_max_commits => 2, clean_policy => 'KEEP_LATEST_FILE_VERSIONS', file_versions_retained => 1)").collect()
        assertResult(0)(result7.length)

        val result8 = spark.sql(
          s"call show_fsview_all(table => '$tableName', path_regex => '', limit => 10)").collect()
        assertResult(2)(result8.length)

        // trigger
        val result9 = spark.sql(
          s"call run_clean(table => '$tableName', trigger_max_commits => 1, clean_policy => 'KEEP_LATEST_FILE_VERSIONS', file_versions_retained => 1)").collect()
        assertResult(1)(result9.length)
        assertResult(1)(result9(0)(2))

        val result10 = spark.sql(
          s"call show_fsview_all(table => '$tableName', path_regex => '', limit => 10)").collect()
        assertResult(1)(result10.length)

        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 15, 1000)
        )
      }
    }
  }

  test("Test Call run_clean Procedure with table props") {
    withSQLConf("hoodie.clean.automatic" -> "false") {
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
             |   type = 'cow',
             |   orderingFields = 'ts',
             |   hoodie.clean.policy = 'KEEP_LATEST_COMMITS',
             |   hoodie.clean.commits.retained = '2'
             | )
             |""".stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1001)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1002)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1003)")
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1004)")

        val result1 = spark.sql(s"call run_clean(table => '$tableName')")
          .collect()
          .map(row => Seq(row.getString(0), row.getLong(1), row.getInt(2), row.getString(3), row.getString(4), row.getInt(5)))
        assertResult(1)(result1.length)
        assertResult(1)(result1(0)(2))

        val result2 = spark.sql(
          s"call show_fsview_all(table => '$tableName', path_regex => '', limit => 10)").collect()
        assertResult(3)(result2.length)
      }
    }
  }

  test("Test Call run_clean Procedure with options") {
    withSQLConf("hoodie.clean.automatic" -> "false") {
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
             |   type = 'cow',
             |   orderingFields = 'ts'
             | )
             |""".stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1001)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1002)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1003)")
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1004)")

        val result1 = spark.sql(
          s"""call run_clean(table => '$tableName', options => "
             | hoodie.clean.policy=KEEP_LATEST_COMMITS,
             | hoodie.clean.commits.retained=2
             |")""".stripMargin)
          .collect()
          .map(row => Seq(row.getString(0), row.getLong(1), row.getInt(2), row.getString(3), row.getString(4), row.getInt(5)))
        assertResult(1)(result1.length)
        assertResult(1)(result1(0)(2))

        val result2 = spark.sql(
          s"call show_fsview_all(table => '$tableName', path_regex => '', limit => 10)").collect()
        assertResult(3)(result2.length)
      }
    }
  }
}
