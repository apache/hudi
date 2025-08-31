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

class TestFsViewProcedure extends HoodieSparkProcedureTestBase {
  test("Test Call show_fsview_all Procedure") {
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
           | partitioned by (ts)
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // Check required fields
      checkExceptionContain(s"""call show_fsview_all(limit => 10)""")(
        s"Table name or table path must be given one")

      // collect result for table
      val result = spark.sql(
        s"""call show_fsview_all(table => '$tableName', path_regex => '*/', limit => 10)""".stripMargin).collect()
      assertResult(2) {
        result.length
      }

      // not specify partition
      val result1 = spark.sql(
        s"""call show_fsview_all(table => '$tableName')""".stripMargin).collect()
      assertResult(2){
        result1.length
      }
    }
  }

  test("Test Call show_fsview_all Procedure For NonPartition") {
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
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // Check required fields
      checkExceptionContain(s"""call show_fsview_all(limit => 10)""")(
        s"Table name or table path must be given one")

      // collect result for table
      val result = spark.sql(
        s"""call show_fsview_all(table => '$tableName', limit => 10)""".stripMargin).collect()
      assertResult(2) {
        result.length
      }
    }
  }

  test("Test Call show_fsview_all Procedure For Three-Level Partition") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  f1 string,
           |  f2 string,
           |  ts long
           |) using hudi
           | partitioned by(f1, f2, ts)
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 'f11', 'f21', 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 'f12', 'f22', 1500")

      // Check required fields
      checkExceptionContain(s"""call show_fsview_all(limit => 10)""")(
        s"Table name or table path must be given one")

      // not specify partition
      val result = spark.sql(
        s"""call show_fsview_all(table => '$tableName', limit => 10)""".stripMargin).collect()
      assertResult(2) {
        result.length
      }

      val result1 = spark.sql(
        s"""call show_fsview_all(table => '$tableName', path_regex => '*/*/*/')""".stripMargin).collect()
      assertResult(2){
        result1.length
      }

      val result2 = spark.sql(
        s"""call show_fsview_all(table => '$tableName', path_regex => 'f1=f11/*/*/')""".stripMargin).collect()
      assertResult(1) {
        result2.length
      }
    }
  }

  test("Test Call show_fsview_latest Procedure") {
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
           | partitioned by (ts)
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // Check required fields
      checkExceptionContain(s"""call show_fsview_latest(limit => 10)""")(
        s"Table name or table path must be given one")

      // collect result for table
      val result = spark.sql(
        s"""call show_fsview_latest(table => '$tableName', partition_path => 'ts=1000', limit => 10)""".stripMargin).collect()
      assertResult(1) {
        result.length
      }
    }
  }

  test("Test Call show_fsview_latest Procedure with NonPartition") {
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
           |  orderingFields = 'ts',
           |  'hoodie.parquet.small.file.limit' = '0'
           | )
       """.stripMargin)
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 2, 'a3', 20, 1500")

      val result = spark.sql(
        s"""call show_fsview_latest(table => '$tableName', limit => 10)""".stripMargin).collect()
      assertResult(2) {
        result.length
      }
    }
  }

  test("Test Call show_fsview_latest Procedure with path_regex") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts long,
           |  day string,
           |  hh string
           |) using hudi
           | partitioned by(day, hh)
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts',
           |  'hoodie.parquet.small.file.limit' = '0'
           | )
       """.stripMargin)

      val result1 = spark.sql(s"call show_fsview_all(table => '$tableName')").collect()
      assertResult(0) {
        result1.length
      }

      spark.sql(s"insert into $tableName select 1, 'a1', 1001, 'd1', 'h1'")
      spark.sql(s"insert into $tableName select 1, 'a2', 1002, 'd1', 'h1'")
      spark.sql(s"insert into $tableName select 2, 'a3', 1003, 'd1', 'h2'")
      spark.sql(s"insert into $tableName select 3, 'a4', 1004, 'd1', 'h2'")
      spark.sql(s"insert into $tableName select 4, 'a5', 1005, 'd2', 'h1'")

      val result2 = spark.sql(
        s"call show_fsview_latest(table => '$tableName')").collect()
      assertResult(4) {
        result2.length
      }

      val result3 = spark.sql(
        s"call show_fsview_latest(table => '$tableName', path_regex => 'day=d1/*/')").collect()
      assertResult(3) {
        result3.length
      }

      val result4 = spark.sql(
        s"call show_fsview_latest(table => '$tableName', path_regex => 'day=d1/hh=h2/')").collect()
      assertResult(2) {
        result4.length
      }
    }
  }
}
