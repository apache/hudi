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

class TestStatsProcedure extends HoodieSparkProcedureTestBase {
  test("Test Call stats_wa Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
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
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"update $tableName set name = 'b1', price = 100 where id = 1")

      // Check required fields
      checkExceptionContain(s"""call stats_wa(limit => 10)""")(
        s"Argument: table is required")

      // collect result for table
      val result = spark.sql(
        s"""call stats_wa(table => '$tableName')""".stripMargin).collect()
      assertResult(4) {
        result.length
      }
    }
  }

  test("Test Call stats_file_sizes Procedure for partitioned tables") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  price double,
           |  name string,
           |  ts long
           |) using hudi
           | partitioned by (name, ts)
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 10, 'a1', 1000")
      spark.sql(s"insert into $tableName select 2, 20, 'a2', 1500")

      // Check required fields
      checkExceptionContain(s"""call stats_file_sizes(limit => 10)""")(
        s"Argument: table is required")

      // collect result for partitioned table without specifying partition_path (resolve dual layer partitioning)
      // will show an extra row with commit_time == ALL
      val noPartitionRegexResult = spark.sql(
        s"""call stats_file_sizes(table => '$tableName')""".stripMargin).collect()
      assertResult(3) {
        noPartitionRegexResult.length
      }

      // collect result for partitioned table with partition_path regex
      val validPartitionRegexResult = spark.sql(
        s"""call stats_file_sizes(table => '$tableName', partition_path => 'a1/*')""".stripMargin).collect()
      assertResult(1) {
        validPartitionRegexResult.length
      }

      // collect result for partitioned table with partition_path regex that has a leading '/' character
      val leadingDirectoryDelimiterResult = spark.sql(
        s"""call stats_file_sizes(table => '$tableName', partition_path => '/a1/*')""".stripMargin).collect()
      assertResult(1) {
        leadingDirectoryDelimiterResult.length
      }

      // collect result for partitioned table with invalid partition_path regex
      checkExceptionContain(s"""call stats_file_sizes(table => '$tableName', partition_path => '/*')""".stripMargin)(
        "Provided partition_path file depth of 1 does not match table's maximum partition depth of 2"
      )
    }
  }

  test("Test Call stats_file_sizes Procedure for un-partitioned tables") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  price double,
           |  name string,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 10, 'a1', 1000")
      spark.sql(s"insert into $tableName select 2, 20, 'a2', 1500")

      // Check required fields
      checkExceptionContain(s"""call stats_file_sizes(limit => 10)""")(
        s"Argument: table is required")

      // collect result for  un-partitioned table without specifying partition_path regex
      // will show an extra row with commit_time == ALL
      val noPartitionRegexResult = spark.sql(
        s"""call stats_file_sizes(table => '$tableName')""".stripMargin).collect()
      assertResult(3) {
        noPartitionRegexResult.length
      }

      // collect result for un-partitioned table by specifying partition_path regex
      val partitionRegexResult = spark.sql(
        s"""call stats_file_sizes(table => '$tableName', partition_path => '*')""".stripMargin).collect()
      assertResult(3) {
        partitionRegexResult.length
      }

      // collect result for un-partitioned table by specifying partition_path regex
      val leadingDirectoryDelimiterResult = spark.sql(
        s"""call stats_file_sizes(table => '$tableName', partition_path => '/*')""".stripMargin).collect()
      assertResult(3) {
        leadingDirectoryDelimiterResult.length
      }

      // collect result for un-partitioned table by specifying WRONG partition_path regex
      // globRegex is ignored for un-partitioned tables
      val wrongPartitionRegexResult = spark.sql(
        s"""call stats_file_sizes(table => '$tableName', partition_path => '/*')""".stripMargin).collect()
      assertResult(3) {
        wrongPartitionRegexResult.length
      }
    }
  }
}
