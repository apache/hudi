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

class TestCommitsProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call show_archived_commits Procedure") {
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
           |  hoodie.keep.max.commits = 5,
           |  hoodie.keep.min.commits = 4,
           |  hoodie.clean.commits.retained = 1
           | )
       """.stripMargin)

      // insert data to table, will generate 5 active commits and 2 archived commits
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")
      spark.sql(s"insert into $tableName select 5, 'a5', 50, 3000")
      spark.sql(s"insert into $tableName select 6, 'a6', 60, 3500")
      spark.sql(s"insert into $tableName select 7, 'a7', 70, 4000")

      // Check required fields
      checkExceptionContain(s"""call show_archived_commits(limit => 10)""")(
        s"Argument: table is required")

      // collect active commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(5) {
        commits.length
      }

      // collect archived commits for table
      val endTs = commits(0).get(0).toString
      val archivedCommits = spark.sql(s"""call show_archived_commits(table => '$tableName', end_ts => '$endTs')""").collect()
      assertResult(2) {
        archivedCommits.length
      }
    }
  }

  test("Test Call show_archived_commits_metadata Procedure") {
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
           |  hoodie.keep.max.commits = 5,
           |  hoodie.keep.min.commits = 4,
           |  hoodie.clean.commits.retained = 1
           | )
       """.stripMargin)

      // insert data to table, will generate 5 active commits and 2 archived commits
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName select 4, 'a4', 40, 2500")
      spark.sql(s"insert into $tableName select 5, 'a5', 50, 3000")
      spark.sql(s"insert into $tableName select 6, 'a6', 60, 3500")
      spark.sql(s"insert into $tableName select 7, 'a7', 70, 4000")

      // Check required fields
      checkExceptionContain(s"""call show_archived_commits_metadata(limit => 10)""")(
        s"Argument: table is required")

      // collect active commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(5) {
        commits.length
      }

      // collect archived commits for table
      val endTs = commits(0).get(0).toString
      val archivedCommits = spark.sql(s"""call show_archived_commits_metadata(table => '$tableName', end_ts => '$endTs')""").collect()
      assertResult(2) {
        archivedCommits.length
      }
    }
  }

  test("Test Call show_commit_files Procedure") {
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
      checkExceptionContain(s"""call show_commit_files(table => '$tableName')""")(
        s"Argument: instant_time is required")

      // collect commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(2){commits.length}

      // collect commit files for table
      val instant_time = commits(0).get(0).toString
      val commitFiles = spark.sql(s"""call show_commit_files(table => '$tableName', instant_time => '$instant_time')""").collect()
      assertResult(1){commitFiles.length}
    }
  }

  test("Test Call show_commit_partitions Procedure") {
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
      checkExceptionContain(s"""call show_commit_partitions(table => '$tableName')""")(
        s"Argument: instant_time is required")

      // collect commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(2){commits.length}

      // collect commit partitions files for table
      val instant_time = commits(0).get(0).toString
      val commitPartitions = spark.sql(s"""call show_commit_partitions(table => '$tableName', instant_time => '$instant_time')""").collect()
      assertResult(1){commitPartitions.length}
    }
  }

  test("Test Call show_commit_write_stats Procedure") {
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
      checkExceptionContain(s"""call show_commit_write_stats(table => '$tableName')""")(
        s"Argument: instant_time is required")

      // collect commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(2){commits.length}

      // collect commit write stats for table
      val instant_time = commits(0).get(0).toString
      val commitPartitions = spark.sql(s"""call show_commit_write_stats(table => '$tableName', instant_time => '$instant_time')""").collect()
      assertResult(1){commitPartitions.length}
    }
  }

  test("Test Call commits_compare Procedure") {
    withTempDir { tmp =>
      val tableName1 = generateTableName
      val tableName2 = generateTableName
      // create table1
      spark.sql(
        s"""
           |create table $tableName1 (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName1'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table1
      spark.sql(s"insert into $tableName1 select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName1 select 2, 'a2', 20, 1500")

      // create table2
      spark.sql(
        s"""
           |create table $tableName2 (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName2'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table2
      spark.sql(s"insert into $tableName2 select 3, 'a3', 30, 2000")
      spark.sql(s"insert into $tableName2 select 4, 'a4', 40, 2500")

      // Check required fields
      checkExceptionContain(s"""call commits_compare(table => '$tableName1')""")(
        s"Argument: path is required")

      // collect commits for table1
      var commits1 = spark.sql(s"""call show_commits(table => '$tableName1', limit => 10)""").collect()
      assertResult(2){commits1.length}

      // collect commits for table2
      var commits2 = spark.sql(s"""call show_commits(table => '$tableName2', limit => 10)""").collect()
      assertResult(2){commits2.length}

      // collect commits compare for table1 and table2
      val result = spark.sql(s"""call commits_compare(table => '$tableName1', path => '${tmp.getCanonicalPath}/$tableName2')""").collect()
      assertResult(1){result.length}
    }
  }

  test("Test Call show_commit_extra_metadata Procedure") {
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
      checkExceptionContain(s"""call show_commit_extra_metadata()""")(
        s"Argument: table is required")

      // collect commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(2){commits.length}

      val instant_time = commits(0).get(0).toString
      // get specify instantTime's extraMetadatas
      val metadatas1 = spark.sql(s"""call show_commit_extra_metadata(table => '$tableName', instant_time => '$instant_time')""").collect()
      assertResult(true){metadatas1.length > 0}

      // get last instantTime's extraMetadatas
      val metadatas2 = spark.sql(s"""call show_commit_extra_metadata(table => '$tableName')""").collect()
      assertResult(true){metadatas2.length > 0}

      // get last instantTime's extraMetadatas and filter extraMetadatas with metadata_key
      val metadatas3 = spark.sql(s"""call show_commit_extra_metadata(table => '$tableName', metadata_key => 'schema')""").collect()
      assertResult(1){metadatas3.length}
    }
  }
}
