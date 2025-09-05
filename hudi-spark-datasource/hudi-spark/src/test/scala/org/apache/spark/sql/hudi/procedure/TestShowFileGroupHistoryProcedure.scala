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

class TestShowFileGroupHistoryProcedure extends HoodieSparkSqlTestBase {

  test("Test show_file_group_history - basic functionality test") {
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
      val fileGroupId = fileName.split("_")(0)

      val historyResultDf = spark.sql(
        s"""call show_file_group_history(
           |  table => '$tableName',
           |  fileGroupId => '$fileGroupId'
           |)""".stripMargin)
      historyResultDf.show(false)
      val historyResult = historyResultDf.collect()

      assert(historyResult.length == 2, "Should show 2 history entries for file group")

      val headRow = historyResult.head
      assert(headRow.length == 27, "Should have 27 columns in result")
      assert(headRow.getString(2).equals("commit"), "Action should be commit here")
      assert(headRow.getString(7) == "INSERT", "Operation type should be INSERT here")
      assert(headRow.getLong(8) == 2, "Small file handling logic coming into play, should have 2 files here")
    }
  }

  test("Test show_file_group_history - with partition filter test") {
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
      val fileGroupId = fileName.split("_")(0)

      val historyWithPartition = spark.sql(
        s"""call show_file_group_history(
           |  table => '$tableName',
           |  fileGroupId => '$fileGroupId',
           |  partition => 'category=electronics'
           |)""".stripMargin).collect()

      assert(historyWithPartition.length == 2, "Should find history for electronics partition")

      historyWithPartition.foreach { row =>
        val partitionPath = row.getString(5)
        assert(partitionPath.contains("electronics") || partitionPath.contains("UNKNOWN"),
          s"Partition path should contain 'electronics' or be UNKNOWN, got: $partitionPath")
      }

      val historyWithoutPartition = spark.sql(
        s"""call show_file_group_history(
           |  table => '$tableName',
           |  fileGroupId => '$fileGroupId'
           |)""".stripMargin).collect()

      assert(historyWithoutPartition.length >= historyWithPartition.length,
        "History without partition filter should have >= results than with filter")
    }
  }

  test("Test show_file_group_history - little complex case with updates and limit") {
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
      val fileGroupId = fileName.split("_")(0)

      val limitedHistoryDf = spark.sql(
        s"""call show_file_group_history(
           |  table => '$tableName',
           |  fileGroupId => '$fileGroupId',
           |  limit => 3
           |)""".stripMargin)
      limitedHistoryDf.show(false)
      val limitedHistory = limitedHistoryDf.collect()

      assert(limitedHistory.length == 3, "Should respect limit parameter")

      val higherLimitedHistoryDf = spark.sql(
        s"""call show_file_group_history(
           |  table => '$tableName',
           |  fileGroupId => '$fileGroupId',
           |  limit => 20
           |)""".stripMargin)
      higherLimitedHistoryDf.show(false)
      val higherLimitedHistory = higherLimitedHistoryDf.collect()

      assert(higherLimitedHistory.length >= limitedHistory.length,
        "Higher limit should return >= results")

      val operationTypes = higherLimitedHistory.map(_.getString(7)).distinct
      assert(operationTypes.length == 2, "Should have INSERT and UPDATE types")

      val hasInsertOrUpdate = operationTypes.exists(op =>
        op != null && (op.contains("INSERT") || op.contains("UPDATE")))
      assert(hasInsertOrUpdate, s"Should have INSERT/UPDATE operations, got: ${operationTypes.mkString(", ")}")
    }
  }

  test("Test show_file_group_history - with cleaning operations") {
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

        val fileInfo = spark.sql(s"select _hoodie_file_name from $tableName where id = 1 limit 1").collect()
        val fileName = fileInfo.head.getString(0)
        val fileGroupId = fileName.split("_")(0)

        val historyBeforeCleanDf = spark.sql(
          s"""call show_file_group_history(
             |  table => '$tableName',
             |  fileGroupId => '$fileGroupId',
             |  limit => 25
             |)""".stripMargin)

        historyBeforeCleanDf.show(false)

        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 2)").collect()

        val historyAfterCleanDf = spark.sql(
          s"""call show_file_group_history(
             |  table => '$tableName',
             |  fileGroupId => '$fileGroupId',
             |  limit => 25
             |)""".stripMargin)

        historyAfterCleanDf.show(false)
        val historyAfterClean = historyAfterCleanDf.collect()

        val actionTypes = historyAfterClean.map(_.getString(2)).distinct.filter(_ != null)
        val hasCommitActions = actionTypes.exists(_.contains("commit"))

        assert(hasCommitActions, s"Should have commit actions, got: ${actionTypes.mkString(", ")}")

        val deletedEntries = historyAfterClean.filter(_.getBoolean(15))
        val nonDeletedEntries = historyAfterClean.filter(!_.getBoolean(15))

        assert(nonDeletedEntries.length == 2, s"Should have at least 2 non-deleted entries, got ${nonDeletedEntries.length}")

        deletedEntries.foreach { row =>
          val deleteAction = row.getString(16)
          assert(deleteAction == "clean", s"Delete action should be 'clean', got: $deleteAction")
        }
      }
    }
  }

  test("Test show_file_group_history - non-existent file group") {
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
        s"""call show_file_group_history(
           |  table => '$tableName',
           |  fileGroupId => 'non-existent-file-group-id'
           |)""".stripMargin).collect()

      assert(noHistory.length == 0, "Should return empty results for non-existent file group")
    }
  }

  test("Test show_file_group_history - error handling") {
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
        s"""call show_file_group_history(
           |  table => '$tableName',
           |  fileGroupId => '',
           |  limit => 0
           |)""".stripMargin).collect()

      assert(emptyResult.length == 0, "Should handle empty file group ID gracefully")
    }
  }

  test("Test show_file_group_history - with action filter") {
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
      val fileGroupId = fileName.split("_")(0)

      val commitOnlyHistory = spark.sql(
        s"""call show_file_group_history(
           |  table => '$tableName',
           |  fileGroupId => '$fileGroupId',
           |  filter => "action = 'commit'"
           |)""".stripMargin).collect()

      assert(commitOnlyHistory.length >= 2, "Should find at least 2 commit entries")
      commitOnlyHistory.foreach { row =>
        assert(row.getString(2) == "commit", s"All entries should be commit actions, got: ${row.getString(2)}")
      }

      val insertOnlyHistory = spark.sql(
        s"""call show_file_group_history(
           |  table => '$tableName',
           |  fileGroupId => '$fileGroupId',
           |  filter => "operation_type = 'INSERT'"
           |)""".stripMargin).collect()

      assert(insertOnlyHistory.length == 1, "Should find exactly 1 INSERT operation")
      insertOnlyHistory.foreach { row =>
        assert(row.getString(7) == "INSERT", s"All entries should be INSERT operations, got: ${row.getString(7)}")
      }

      val allHistory = spark.sql(
        s"""call show_file_group_history(
           |  table => '$tableName',
           |  fileGroupId => '$fileGroupId'
           |)""".stripMargin).collect()

      assert(allHistory.length >= commitOnlyHistory.length, "Unfiltered results should have >= filtered results")
      assert(allHistory.length >= insertOnlyHistory.length, "Unfiltered results should have >= filtered results")
    }
  }

  test("Test show_file_group_history - MOR table compaction operations") {
    withSQLConf("hoodie.compact.inline" -> "false", "hoodie.compact.schedule.inline" -> "false") {
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
             | type = 'mor',
             | preCombineField = 'ts',
             | 'hoodie.parquet.max.file.size' = '1024'
             |)
             |""".stripMargin)

        spark.sql(s"insert into $tableName values(1, 'name1', 10.0, 1001)")
        spark.sql(s"insert into $tableName values(2, 'name2', 20.0, 1002)")

        val fileInfo = spark.sql(s"select _hoodie_file_name from $tableName where id = 1 limit 1").collect()
        val fileName = fileInfo.head.getString(0)
        val fileGroupId = fileName.split("_")(0)

        spark.sql(s"update $tableName set price = 15.0, ts = 2001 where id = 1")
        spark.sql(s"update $tableName set price = 25.0, ts = 2002 where id = 2")
        spark.sql(s"update $tableName set price = 18.0, ts = 3001 where id = 1")

        val historyBeforeCompactionDf = spark.sql(
          s"""call show_file_group_history(
             | table => '$tableName',
             | fileGroupId => '$fileGroupId',
             | limit => 25
             |)""".stripMargin)

        historyBeforeCompactionDf.show(false)

        val historyBeforeCompaction = historyBeforeCompactionDf.collect()

        assert(historyBeforeCompaction.length == 3, s"Should have one base file and two updates on record key for id = 1, got ${historyBeforeCompaction.length}")

        spark.sql(s"call run_compaction(op => 'schedule', table => '$tableName')").collect()
        val compactionResult = spark.sql(s"call show_compaction(table => '$tableName')").collect()
        val compactionInstant = compactionResult.head.getString(0)

        spark.sql(s"call run_compaction(op => 'run', table => '$tableName', timestamp => $compactionInstant)").collect()

        val historyAfterCompactionDf = spark.sql(
          s"""call show_file_group_history(
             | table => '$tableName',
             | fileGroupId => '$fileGroupId',
             | limit => 25
             |)""".stripMargin)

        historyAfterCompactionDf.show(false)

        val historyAfterCompaction = historyAfterCompactionDf.collect()

        assert(historyAfterCompaction.length >= historyBeforeCompaction.length,
          s"Should have at least same number of entries after compaction")

        val actionTypes = historyAfterCompaction.map(_.getString(2)).distinct.filter(_ != null)
        val hasCommitActions = actionTypes.exists(_.contains("commit"))
        val hasDeltaCommitActions = actionTypes.exists(_.contains("deltacommit"))

        assert(hasCommitActions && hasDeltaCommitActions, s"Should have commit/deltacommit actions, got: ${actionTypes.mkString(", ")}")

        val oldBaseFileEntries = historyAfterCompaction.filter { row =>
          val instant = row.getString(0)
          instant != compactionInstant
        }

        assert(oldBaseFileEntries.length > 0, "Should have old file entries in history")
        assert(oldBaseFileEntries.last.getString(19).equals("compaction"), s"replace_action should be compaction, got: ${oldBaseFileEntries.last.getString(19)}")
      }
    }
  }

  test("Test show_file_group_history - with numeric and complex filters") {
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
      val fileGroupId = fileName.split("_")(0)

      val writesFilterHistory = spark.sql(
        s"""call show_file_group_history(
           |  table => '$tableName',
           |  fileGroupId => '$fileGroupId',
           |  filter => "num_writes > 0"
           |)""".stripMargin).collect()

      assert(writesFilterHistory.length >= 1, "Should find entries with writes > 0")
      writesFilterHistory.foreach { row =>
        assert(row.getLong(8) > 0, s"All entries should have num_writes > 0, got: ${row.getLong(9)}")
      }

      val complexFilterHistory = spark.sql(
        s"""call show_file_group_history(
           |  table => '$tableName',
           |  fileGroupId => '$fileGroupId',
           |  filter => "action = 'commit' AND num_writes > 0 AND state = 'COMPLETED'"
           |)""".stripMargin).collect()

      assert(complexFilterHistory.length >= 1, "Should find entries matching complex filter")
      complexFilterHistory.foreach { row =>
        assert(row.getString(2) == "commit", s"Action should be commit, got: ${row.getString(2)}")
        assert(row.getLong(8) > 0, s"num_writes should be > 0, got: ${row.getLong(9)}")
        assert(row.getString(4) == "COMPLETED", s"State should be COMPLETED, got: ${row.getString(4)}")
      }

      val fileSizeFilterHistory = spark.sql(
        s"""call show_file_group_history(
           |  table => '$tableName',
           |  fileGroupId => '$fileGroupId',
           |  filter => "file_size_bytes > 0 AND total_write_bytes > 0"
           |)""".stripMargin).collect()

      assert(fileSizeFilterHistory.length >= 1, "Should find entries with file size > 0")
      fileSizeFilterHistory.foreach { row =>
        assert(row.getLong(12) > 0, s"file_size_bytes should be > 0, got: ${row.getLong(12)}")
        assert(row.getLong(13) > 0, s"total_write_bytes should be > 0, got: ${row.getLong(13)}")
      }
    }
  }

  test("Test show_file_group_history - clustering operations") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }

      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long,
           | partition long
           |) using hudi
           | options (
           |  primaryKey = 'id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
           | partitioned by(partition)
           | location '$basePath'
           | tblproperties (
           |  'hoodie.parquet.max.file.size' = '1024',
           |  'hoodie.parquet.small.file.limit' = '512'
           | )
           |""".stripMargin)

      spark.sql("set hoodie.compact.inline=false")
      spark.sql("set hoodie.compact.schedule.inline=false")

      spark.sql(s"insert into $tableName values(1, 'a1', 10.0, 1000, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10.0, 1001, 1001)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10.0, 1002, 1002)")

      val fileInfo = spark.sql(s"select _hoodie_file_name from $tableName where partition = 1000 limit 1").collect()
      val fileName = fileInfo.head.getString(0)
      val fileGroupId = fileName.split("_")(0)

      val historyBeforeClusteringDf = spark.sql(
        s"""call show_file_group_history(
           | table => '$tableName',
           | fileGroupId => '$fileGroupId',
           | limit => 25
           |)""".stripMargin)

      historyBeforeClusteringDf.show(false)

      val client = org.apache.hudi.HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, Option(tableName))
      client.scheduleClustering(org.apache.hudi.common.util.Option.empty()).get()

      spark.sql(s"insert into $tableName values(4, 'a4', 10.0, 1003, 1003)")
      client.scheduleClustering(org.apache.hudi.common.util.Option.empty()).get()

      spark.sql(s"call run_clustering(table => '$tableName', order => 'partition', show_involved_partition => true)")

      val originalHistoryAfterClusteringDf = spark.sql(
        s"""call show_file_group_history(
           | table => '$tableName',
           | fileGroupId => '$fileGroupId',
           | limit => 25
           |)""".stripMargin)
      originalHistoryAfterClusteringDf.show(false)
      val originalHistoryAfterClustering = originalHistoryAfterClusteringDf.collect()

      val currentFileGroups = spark.sql(s"select distinct _hoodie_file_name from $tableName").collect()

      currentFileGroups.foreach { row =>
        val currentFileName = row.getString(0)
        val currentFileGroupId = currentFileName.split("_")(0)

        val currentHistoryDf = spark.sql(
          s"""call show_file_group_history(
             | table => '$tableName',
             | fileGroupId => '$currentFileGroupId',
             | limit => 25
             |)""".stripMargin)

        currentHistoryDf.show(false)
        assert(currentHistoryDf.collect().head.getString(2).equals("replacecommit"), s"Should have replacecommit action for file group $currentFileGroupId")
      }
      assert(originalHistoryAfterClustering.head.getString(19).equals("clustering"), s"replace_action should be clustering, got: ${originalHistoryAfterClustering.head.getString(19)}")
    }
  }
}
