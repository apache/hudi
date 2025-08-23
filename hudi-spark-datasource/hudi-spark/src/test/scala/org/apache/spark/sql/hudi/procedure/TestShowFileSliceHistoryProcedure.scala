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

import org.apache.hudi.HoodieSparkUtils

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestShowFileSliceHistoryProcedure extends HoodieSparkSqlTestBase {

  test("Test show_file_slice_history - basic functionality test") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           | primaryKey = 'id',
           | type = 'cow',
           | preCombineField = 'ts'
           |)
           |""".stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000)")

      val fileListResult = spark.sql(s"call show_commits(table => '$tableName', limit => 10)").collect()
      assert(fileListResult.length >= 2, "Should have at least 2 commits")

      val showFiles = spark.sql(s"select _hoodie_file_name from $tableName limit 1").collect()
      assert(showFiles.length > 0, "Should have at least one file")

      val fileName = showFiles.head.getString(0)

      val historyResultDf = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName'
           |)""".stripMargin)
      historyResultDf.show(false)
      val historyResult = historyResultDf.collect()

      assert(historyResult.length >= 1, "Should show at least 1 history entry for file slice")

      val headRow = historyResult.head
      assert(headRow.length == 25, "Should have 25 columns in result")
      assert(headRow.getString(2).equals("commit"), "Action should be commit here")
      assert(headRow.getString(7).contains(fileName) || headRow.getString(7).contains(fileName.split("_")(0)),
        "File name should match or contain the queried file name")
      assert(headRow.getString(8) == "INSERT" || headRow.getString(8) == "UPDATE", "Operation type should be INSERT or UPDATE")
    }
  }

  test("Test show_file_slice_history - with partition filter test") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | category string,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           | primaryKey = 'id',
           | type = 'cow',
           | preCombineField = 'ts'
           |)
           | partitioned by (category)
           |""".stripMargin)

      spark.sql(s"insert into $tableName (id, name, price, category, ts) values(1, 'a1', 10, 'electronics', 1000)")
      spark.sql(s"insert into $tableName (id, name, price, category, ts) values(2, 'a2', 20, 'books', 2000)")
      spark.sql(s"insert into $tableName (id, name, price, category, ts) values(3, 'a3', 30, 'electronics', 3000)")

      val electronicsFiles = spark.sql(
        s"select _hoodie_file_name from $tableName where category = 'electronics' limit 1"
      ).collect()

      val fileName = electronicsFiles.head.getString(0)

      val historyWithPartition = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName',
           |  partition => 'category=electronics'
           |)""".stripMargin).collect()

      assert(historyWithPartition.length >= 1, "Should find history for electronics partition")

      historyWithPartition.foreach { row =>
        val partitionPath = row.getString(5)
        assert(partitionPath.contains("electronics") || partitionPath.contains("UNKNOWN"),
          s"Partition path should contain 'electronics' or be UNKNOWN, got: $partitionPath")
      }

      val historyWithoutPartition = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName'
           |)""".stripMargin).collect()

      assert(historyWithoutPartition.length >= historyWithPartition.length,
        "History without partition filter should have >= results than with filter")
    }
  }

  test("Test show_file_slice_history - complex case with updates and limit") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           | primaryKey = 'id',
           | type = 'cow',
           | preCombineField = 'ts'
           |)
           |""".stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000)")
      spark.sql(s"update $tableName set price = 15 where id = 1")
      spark.sql(s"insert into $tableName values(3, 'a3', 30, 3000)")
      spark.sql(s"update $tableName set price = 25 where id = 2")
      spark.sql(s"update $tableName set price = 18 where id = 1")

      val fileInfo = spark.sql(s"select _hoodie_file_name from $tableName where id = 1 limit 1").collect()
      val fileName = fileInfo.head.getString(0)

      val limitedHistoryDf = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName',
           |  limit => 3
           |)""".stripMargin)
      limitedHistoryDf.show(false)
      val limitedHistory = limitedHistoryDf.collect()

      assert(limitedHistory.length <= 3, "Should respect limit parameter")

      val unlimitedHistoryDf = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName',
           |  limit => 20
           |)""".stripMargin)
      unlimitedHistoryDf.show(false)
      val unlimitedHistory = unlimitedHistoryDf.collect()

      assert(unlimitedHistory.length >= limitedHistory.length,
        "Higher limit should return >= results")

      val operationTypes = unlimitedHistory.map(_.getString(8)).distinct.filter(_ != null)

      val hasInsertOrUpdate = operationTypes.exists(op =>
        op != null && (op.contains("INSERT") || op.contains("UPDATE")))
      assert(hasInsertOrUpdate, s"Should have INSERT/UPDATE operations, got: ${operationTypes.mkString(", ")}")
    }
  }

  test("Test show_file_slice_history - with cleaning operations") {
    withSQLConf("hoodie.clean.automatic" -> "false") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tableLocation = tmp.getCanonicalPath
        if (HoodieSparkUtils.isSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled = false")
        }

        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | ts long
             |) using hudi
             | location '$tableLocation'
             | tblproperties (
             | primaryKey = 'id',
             | type = 'cow',
             | preCombineField = 'ts',
             | 'hoodie.parquet.max.file.size' = '1024'
             |)
             |""".stripMargin)

        spark.sql(s"insert into $tableName values(1, 'name1', 10.0, 1001)")
        spark.sql(s"insert into $tableName values(2, 'name2', 20.0, 1002)")
        spark.sql(s"insert into $tableName values(3, 'name3', 30.0, 1003)")

        spark.sql(s"update $tableName set price = 15.0, ts = 2001 where id = 1")
        spark.sql(s"update $tableName set price = 25.0, ts = 2002 where id = 2")
        spark.sql(s"update $tableName set price = 18.0, ts = 3001 where id = 1")
        spark.sql(s"update $tableName set price = 28.0, ts = 3002 where id = 2")

        val fileInfo = spark.sql(s"select _hoodie_file_name from $tableName order by _hoodie_commit_time asc").collect()

        val fileName = if (fileInfo.length >= 2) fileInfo(0).getString(0) else fileInfo.head.getString(0)

        val historyBeforeCleanDf = spark.sql(
          s"""call show_file_slice_history(
             |  table => '$tableName',
             |  file_name => '$fileName',
             |  limit => 25
             |)""".stripMargin)

        historyBeforeCleanDf.show(false)

        val historyBeforeClean = historyBeforeCleanDf.collect()

        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)").collect()

        val historyAfterCleanDf = spark.sql(
          s"""call show_file_slice_history(
             |  table => '$tableName',
             |  file_name => '$fileName',
             |  limit => 25
             |)""".stripMargin)

        historyAfterCleanDf.show(false)
        val historyAfterClean = historyAfterCleanDf.collect()

        assert(historyAfterClean.length >= historyBeforeClean.length,
          s"Should have at least the same number of history entries, got ${historyAfterClean.length}")

        val hasCleanInHistory = historyAfterClean.exists(_.getString(2).contains("clean"))
        if (hasCleanInHistory) {
          assert(historyAfterClean.length == historyBeforeClean.length + 1,
            s"Should have one additional entry for clean operation, got ${historyAfterClean.length}")
        }

        val deletionInfo = historyAfterClean.filter(_.getBoolean(16))
        if (deletionInfo.nonEmpty) {
          deletionInfo.foreach { row =>
            val deleteAction = row.getString(17)
            assert(deleteAction != null && deleteAction.equals("clean"),
              s"Delete action should be 'clean' for this test, got: $deleteAction")
          }
        }
      }
    }
  }

  test("Test show_file_slice_history - partial file name matching") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           | primaryKey = 'id',
           | type = 'cow',
           | preCombineField = 'ts'
           |)
           |""".stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"update $tableName set price = 15 where id = 1")

      val fileInfo = spark.sql(s"select _hoodie_file_name from $tableName limit 1").collect()
      val fullFileName = fileInfo.head.getString(0)

      val fileNameParts = fullFileName.split("_")
      val partialFileName = fileNameParts(0)

      val historyWithPartialName = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$partialFileName'
           |)""".stripMargin).collect()

      assert(historyWithPartialName.length >= 1, "Should find history with partial file name")

      val historyWithFullName = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fullFileName'
           |)""".stripMargin).collect()

      assert(historyWithFullName.length >= 1, "Should find history with full file name")

      assert(historyWithPartialName.length >= historyWithFullName.length,
        "Partial name matching should return >= results than full name")
    }
  }

  test("Test show_file_slice_history - non-existent file name") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           | primaryKey = 'id',
           | type = 'cow',
           | preCombineField = 'ts'
           |)
           |""".stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 1000)")

      val noHistory = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => 'non-existent-file-name.parquet'
           |)""".stripMargin).collect()

      assert(noHistory.length == 0, "Should return empty results for non-existent file name")
    }
  }

  test("Test show_file_slice_history - error handling") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           | primaryKey = 'id',
           | type = 'cow',
           | preCombineField = 'ts'
           |)
           |""".stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 1000)")

      val emptyResult = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '',
           |  limit => 0
           |)""".stripMargin).collect()

      assert(emptyResult.length == 0, "Should handle empty file name gracefully")
    }
  }

  test("Test show_file_slice_history - with archived timeline") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           | primaryKey = 'id',
           | type = 'cow',
           | preCombineField = 'ts'
           |)
           |""".stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"update $tableName set price = 15 where id = 1")

      val fileInfo = spark.sql(s"select _hoodie_file_name from $tableName limit 1").collect()
      val fileName = fileInfo.head.getString(0)

      val historyWithoutArchived = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName',
           |  show_archived => false
           |)""".stripMargin).collect()

      val historyWithArchived = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName',
           |  show_archived => true
           |)""".stripMargin).collect()

      assert(historyWithArchived.length >= historyWithoutArchived.length,
        "History with archived should have >= results than without archived")

      val timelineTypes = historyWithArchived.map(_.getString(3)).distinct.filter(_ != null)
      assert(timelineTypes.contains("ACTIVE"), "Should have ACTIVE timeline entries")
    }
  }

  test("Test show_file_slice_history - with operation type filter") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           | primaryKey = 'id',
           | type = 'cow',
           | preCombineField = 'ts'
           |)
           |""".stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"update $tableName set price = 15 where id = 1")
      spark.sql(s"update $tableName set price = 20 where id = 1")

      val fileInfo = spark.sql(s"select _hoodie_file_name from $tableName where id = 1 limit 1").collect()
      val fileName = fileInfo.head.getString(0)

      val updateOnlyHistoryDf = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName',
           |  filter => "operation_type = 'UPDATE'"
           |)""".stripMargin)
      updateOnlyHistoryDf.show(false)
      val updateOnlyHistory = updateOnlyHistoryDf.collect()

      assert(updateOnlyHistory.length == 1, "Should find at least 1 UPDATE operation")
      updateOnlyHistory.foreach { row =>
        assert(row.getString(8) == "UPDATE", s"All entries should be UPDATE operations, got: ${row.getString(8)}")
      }

      val activeTimelineHistoryDf = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName',
           |  filter => "timeline_type = 'ACTIVE'"
           |)""".stripMargin)
      activeTimelineHistoryDf.show(false)
      val activeTimelineHistory = activeTimelineHistoryDf.collect()

      assert(activeTimelineHistory.length == 1, "Should find at least 2 ACTIVE timeline entries")
      activeTimelineHistory.foreach { row =>
        assert(row.getString(3) == "ACTIVE", s"All entries should be ACTIVE timeline, got: ${row.getString(3)}")
      }

      val allHistory = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName'
           |)""".stripMargin).collect()

      assert(allHistory.length >= updateOnlyHistory.length, "Unfiltered results should have >= filtered results")
      assert(allHistory.length >= activeTimelineHistory.length, "Unfiltered results should have >= filtered results")
    }
  }

  test("Test show_file_slice_history - with file name pattern and numeric filters") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tableLocation = tmp.getCanonicalPath
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           |) using hudi
           | location '$tableLocation'
           | tblproperties (
           | primaryKey = 'id',
           | type = 'cow',
           | preCombineField = 'ts'
           |)
           |""".stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000)")
      spark.sql(s"update $tableName set price = 15 where id = 1")
      spark.sql(s"update $tableName set price = 25 where id = 2")

      val fileInfo = spark.sql(s"select _hoodie_file_name from $tableName where id = 1 limit 1").collect()
      val fileName = fileInfo.head.getString(0)

      val fileNameFilterHistory = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName',
           |  filter => "file_name LIKE '%.parquet'"
           |)""".stripMargin).collect()

      assert(fileNameFilterHistory.length == 1, "Should find entries with .parquet extension")
      fileNameFilterHistory.foreach { row =>
        val actualFileName = row.getString(7)
        assert(actualFileName != null && actualFileName.endsWith(".parquet"),
          s"File name should end with .parquet, got: $actualFileName")
      }

      val updatesFilterHistory = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName',
           |  filter => "num_updates > 0"
           |)""".stripMargin).collect()

      assert(updatesFilterHistory.length == 1, "Should find entries with updates > 0")
      updatesFilterHistory.foreach { row =>
        assert(row.getLong(11) > 0, s"All entries should have num_updates > 0, got: ${row.getLong(11)}")
      }

      val complexFilterHistory = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName',
           |  filter => "action = 'commit' AND state = 'COMPLETED' AND num_writes > 0"
           |)""".stripMargin).collect()

      assert(complexFilterHistory.length == 1, "Should find entries matching complex filter")
      complexFilterHistory.foreach { row =>
        assert(row.getString(2) == "commit", s"Action should be commit, got: ${row.getString(2)}")
        assert(row.getString(4) == "COMPLETED", s"State should be COMPLETED, got: ${row.getString(4)}")
        assert(row.getLong(9) > 0, s"num_writes should be > 0, got: ${row.getLong(9)}")
      }

      val sizeFilterHistory = spark.sql(
        s"""call show_file_slice_history(
           |  table => '$tableName',
           |  file_name => '$fileName',
           |  filter => "file_size_bytes > 1000 AND total_write_bytes > 1000"
           |)""".stripMargin).collect()

      assert(sizeFilterHistory.length == 1, "Should find entries with file size > 1000 bytes")
      sizeFilterHistory.foreach { row =>
        assert(row.getLong(13) > 1000, s"file_size_bytes should be > 1000, got: ${row.getLong(13)}")
        assert(row.getLong(14) > 1000, s"total_write_bytes should be > 1000, got: ${row.getLong(14)}")
      }
    }
  }
}
