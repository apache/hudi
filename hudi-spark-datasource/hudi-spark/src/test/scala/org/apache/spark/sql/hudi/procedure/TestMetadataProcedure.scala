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

class TestMetadataProcedure extends HoodieSparkSqlTestBase {

  test("Test Call delete_metadata_table Procedure") {
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

      // delete the metadata
      val deleteResult = spark.sql(s"""call delete_metadata_table(table => '$tableName')""").collect()
      assertResult(1) {
        deleteResult.length
      }
    }
  }

  test("Test Call create_metadata_table Procedure") {
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

      // The first step is delete the metadata
      val deleteResult = spark.sql(s"""call delete_metadata_table(table => '$tableName')""").collect()
      assertResult(1) {
        deleteResult.length
      }

      // The second step is create the metadata
      val createResult = spark.sql(s"""call create_metadata_table(table => '$tableName')""").collect()
      assertResult(1) {
        createResult.length
      }
    }
  }

  test("Test Call init_metadata_table Procedure") {
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

      // read only, no initialize
      val readResult = spark.sql(s"""call init_metadata_table(table => '$tableName', read_only => true)""").collect()
      assertResult(1) {
        readResult.length
      }

      // initialize metadata
      val initResult = spark.sql(s"""call init_metadata_table(table => '$tableName')""").collect()
      assertResult(1) {
        initResult.length
      }
    }
  }

  test("Test Call show_metadata_table_stats Procedure") {
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
           |  preCombineField = 'ts',
           |  hoodie.metadata.metrics.enable = 'true'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // collect metadata stats for table
      val metadataStats = spark.sql(s"""call show_metadata_table_stats(table => '$tableName')""").collect()
      assertResult(0) {
        metadataStats.length
      }
    }
  }

  test("Test Call show_metadata_table_partitions Procedure") {
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
           | partitioned by (ts)
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // collect metadata partitions for table
      val partitions = spark.sql(s"""call show_metadata_table_partitions(table => '$tableName')""").collect()
      assertResult(2) {
        partitions.length
      }
    }
  }

  test("Test Call show_metadata_table_files Procedure") {
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
           | partitioned by (ts)
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // collect metadata partitions for table
      val partitions = spark.sql(s"""call show_metadata_table_partitions(table => '$tableName')""").collect()
      assertResult(2) {
        partitions.length
      }

      // collect metadata files for a partition of a table
      val partition = partitions(0).get(0).toString
      val filesResult = spark.sql(s"""call show_metadata_table_files(table => '$tableName', partition => '$partition')""").collect()
      assertResult(1) {
        filesResult.length
      }
    }
  }

  test("Test Call validate_metadata_table_files Procedure") {
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
           | partitioned by (ts)
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // collect validate metadata files result
      val validateFilesResult = spark.sql(s"""call validate_metadata_table_files(table => '$tableName')""").collect()
      assertResult(0) {
        validateFilesResult.length
      }

      // collect validate metadata files result with verbose
      val validateFilesVerboseResult = spark.sql(s"""call validate_metadata_table_files(table => '$tableName', verbose => true)""").collect()
      assertResult(2) {
        validateFilesVerboseResult.length
      }
    }
  }
}
