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

  test("Test Call show_commits Procedure with showArchived=true") {
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
      checkExceptionContain(s"""call show_commits(limit => 10, showArchived => true)""")(
        s"Table name or table path must be given one")

      val activeCommits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(5) {
        activeCommits.length
      }

      val endTs = activeCommits(0).get(0).toString
      val allCommits = spark.sql(
        s"""call show_commits(
           |  table => '$tableName',
           |  showArchived => true,
           |  endTime => '$endTs'
           |)""".stripMargin).collect()

      assert(allCommits.length > activeCommits.length,
        s"Should have more commits with showArchived=true (${allCommits.length}) than without (${activeCommits.length})")

      val archivedCommitsCount = allCommits.length - activeCommits.length
      assertResult(2) {
        archivedCommitsCount
      }
    }
  }

  test("Test Call show_commits Procedure with showArchived=true and file metadata") {
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
      checkExceptionContain(s"""call show_commits(limit => 10, showArchived => true, showFiles => true)""")(
        s"Table name or table path must be given one")

      // collect active commits for table
      val activeCommits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(5) {
        activeCommits.length
      }

      val endTs = activeCommits(0).get(0).toString
      val archivedCommitsWithMetadata = spark.sql(
        s"""call show_commits(
           |  table => '$tableName',
           |  showArchived => true,
           |  showFiles => true,
           |  endTime => '$endTs'
           |)""".stripMargin).collect()

      assert(archivedCommitsWithMetadata.length > 0, "Should have archived commits with file metadata")

      val fileSpecificRows = archivedCommitsWithMetadata.filter(row =>
        row.getString(5) != null && !row.getString(5).equals("*"))
      assert(fileSpecificRows.length > 0, "Should have rows with file specific details")
      fileSpecificRows.foreach { row =>
        assert(row.getString(5) != null && row.getString(5).nonEmpty, "File ID should be populated")

        val hasMetrics = row.getLong(7) > 0 || // num_writes
          row.getLong(20) > 0 || // total_bytes_written
          row.getLong(22) > 0 // file_size
        assert(hasMetrics, "Should have some metrics populated for archived files")
      }
    }
  }

  test("Test Call show_commits Procedure") {
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

      // collect commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(2) {
        commits.length
      }

      val instant_time = commits(0).getString(0)
      val commitFiles = spark.sql(
        s"""call show_commits(
           |  table => '$tableName',
           |  filter => "commit_time = '$instant_time'"
           |)""".stripMargin).collect()
      assert(commitFiles.length == 1, "Should have one file for the commit")
    }
  }

  test("Test Call show_commits Procedure for file specific details") {
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

      // collect commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(2) {
        commits.length
      }

      val instant_time = commits(0).getString(0)

      val commitFiles = spark.sql(
        s"""call show_commits(
           |  table => '$tableName',
           |  showFiles => true,
           |  filter => "commit_time = '$instant_time'"
           |)""".stripMargin).collect()

      assert(commitFiles.length >= 1, s"Expected at least one file, got ${commitFiles.length}")

      commitFiles.foreach { row =>
        val fileId = row.getString(5)
        assert(!fileId.equals("*"), "File ID should not be '*' for file-level details")
        val numWrites = row.getLong(7)
        val fileSize = row.getLong(22)
        val avgRecordSize = row.getLong(23)
        assert(numWrites > 0, s"Number of writes should be > 0, got $numWrites")
        assert(fileSize > 0, s"File size should be > 0, got $fileSize")
        val totalBytesWritten = row.getLong(20)
        val calculatedAvgSize = if (numWrites > 0) totalBytesWritten / numWrites else 0
        assert(avgRecordSize == calculatedAvgSize,
          s"avg_record_size ($avgRecordSize) should equal totalBytesWritten/numWrites ($calculatedAvgSize)")
      }
    }
  }

  test("Test Call show_commits Procedure to test for some write stats") {
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

      // collect commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(2) {
        commits.length
      }
      val instant_time = commits(0).getString(0)

      val commitStats = spark.sql(
        s"""call show_commits(
           |  table => '$tableName',
           |  filter => "commit_time = '$instant_time'"
           |)""".stripMargin).collect()
      commitStats.foreach { row =>
        assert(row.get(16) != null, "total_files_added should be populated")
      }
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
      assertResult(2) {
        commits1.length
      }

      // collect commits for table2
      var commits2 = spark.sql(s"""call show_commits(table => '$tableName2', limit => 10)""").collect()
      assertResult(2) {
        commits2.length
      }

      // collect commits compare for table1 and table2
      val result = spark.sql(s"""call commits_compare(table => '$tableName1', path => '${tmp.getCanonicalPath}/$tableName2')""").collect()
      assertResult(1) {
        result.length
      }
    }
  }

  test("Test Call show_commits Procedure for metadata") {
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

      // collect commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(2) {
        commits.length
      }

      val instant_time = commits(0).getString(0)

      val metadatas = spark.sql(
        s"""call show_commits(
           |  table => '$tableName',
           |  filter => "commit_time = '$instant_time'"
           |)""".stripMargin).collect()

      assert(metadatas.length == 1, "Should have at least one row with metadata for the commit")

      metadatas.foreach { row =>
        val extraMetadata = row.getString(24)
        assert(extraMetadata != null, "extra_metadata should be populated")
        assert(extraMetadata.contains("schema"), "extra_metadata should contain schema information")
      }
    }
  }

  test("Test Call show_commits Procedure - with bytes written filter") {
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
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
      spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")

      val allCommits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(3) {
        allCommits.length
      }

      val filteredCommitsDf = spark.sql(
        s"""call show_commits(
           |  table => '$tableName',
           |  limit => 10,
           |  filter => "total_bytes_written > 0"
           |)""".stripMargin)
      filteredCommitsDf.show(false)
      val filteredCommits = filteredCommitsDf.collect()

      assert(filteredCommits.length >= 1, s"Should find at least 1 commit with bytes written > 0, got: ${filteredCommits.length}")
      filteredCommits.foreach { row =>
        assert(row.getLong(20) > 0, s"total_bytes_written should be > 0, got: ${row.getLong(20)}")
      }

      val complexFilterCommits = spark.sql(
        s"""call show_commits(
           |  table => '$tableName',
           |  limit => 10,
           |  filter => "total_bytes_written > 0 AND total_files_added >= 1"
           |)""".stripMargin).collect()

      assert(complexFilterCommits.length >= 1, s"Should find commits matching complex filter, got: ${complexFilterCommits.length}")
      complexFilterCommits.foreach { row =>
        assert(row.getLong(20) > 0, s"total_bytes_written should be > 0, got: ${row.getLong(20)}")
        assert(row.getLong(16) >= 1, s"total_files_added should be >= 1, got: ${row.getLong(16)}")
      }
    }
  }
}
