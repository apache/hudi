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

class TestFsViewProcedure extends HoodieSparkSqlTestBase {
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
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // Check required fields
      checkExceptionContain(s"""call show_fsview_all(limit => 10)""")(
        s"Argument: table is required")

      // collect result for table
      val result = spark.sql(
        s"""call show_fsview_all(table => '$tableName', path_regex => '*/', limit => 10)""".stripMargin).collect()
      assertResult(2) {
        result.length
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
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // Check required fields
      checkExceptionContain(s"""call show_fsview_latest(limit => 10)""")(
        s"Argument: table is required")

      // collect result for table
      val result = spark.sql(
        s"""call show_fsview_latest(table => '$tableName', partition_path => 'ts=1000', limit => 10)""".stripMargin).collect()
      assertResult(1) {
        result.length
      }
    }
  }
}
