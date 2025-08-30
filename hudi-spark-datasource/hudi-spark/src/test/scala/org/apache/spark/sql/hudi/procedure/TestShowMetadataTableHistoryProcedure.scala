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

class TestShowMetadataTableHistoryProcedure extends HoodieSparkProcedureTestBase {

  test("Test show_metadata_table_history procedure") {
    withSQLConf("hoodie.metadata.enable" -> "true") {
      withTempDir { tmp =>
        val tableName = generateTableName
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
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             | primaryKey = 'id',
             | orderingFields = 'ts',
             | 'hoodie.metadata.enable' = 'true'
             | )
             |""".stripMargin)

        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
        spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
        spark.sql(s"update $tableName set price = 15 where id = 1")

        val historyDf = spark.sql(s"""call show_metadata_table_history(table => '$tableName', limit => 10)""")
        historyDf.show(false)
        val history = historyDf.collect()

        assert(history.length >= 3, s"Expected at least 3 history entry, got ${history.length}")

        val dataTableEntries = history.filter(row => row.getString(6) != null && row.getString(6).nonEmpty)
        val metadataTableEntries = history.filter(row => row.getString(1) != null && row.getString(1).nonEmpty)

        assert(dataTableEntries.length > 0, "Should have data table entries")
        assert(metadataTableEntries.length > 0, "Should have metadata table entries")
      }
    }
  }

  test("Test show_metadata_table_history procedure - without metadata table") {
    withTempDir { tmp =>
      val tableName = generateTableName
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
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           | primaryKey = 'id',
           | orderingFields = 'ts',
           | 'hoodie.metadata.enable' = 'false'
           | )
           |""".stripMargin)

      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      val history = spark.sql(s"""call show_metadata_table_history(table => '$tableName', limit => 10)""").collect()

      assertResult(0)(history.length)
    }
  }

  test("Test show_metadata_table_history procedure - with filters and showArchived") {
    withSQLConf("hoodie.metadata.enable" -> "true") {
      withTempDir { tmp =>
        val tableName = generateTableName
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
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             | primaryKey = 'id',
             | orderingFields = 'ts',
             | 'hoodie.metadata.enable' = 'true'
             | )
             |""".stripMargin)

        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
        spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")
        spark.sql(s"insert into $tableName select 3, 'a3', 30, 2000")
        spark.sql(s"insert into $tableName select 4, 'a4', 20, 1500")
        spark.sql(s"insert into $tableName select 5, 'a5', 10, 1000")
        spark.sql(s"update $tableName set price = 15 where id = 1")
        spark.sql(s"update $tableName set price = 20 where id = 2")

        val allHistoryDf = spark.sql(s"""call show_metadata_table_history(table => '$tableName', limit => 20)""")
        allHistoryDf.show(false)
        val allHistory = allHistoryDf.collect()
        assert(allHistory.length > 0, "Should have history entries")

        val archivedHistoryDf = spark.sql(
          s"""call show_metadata_table_history(
             | table => '$tableName',
             | limit => 20,
             | showArchived => true
             |)""".stripMargin)
        archivedHistoryDf.show(false)
        val archivedHistory = archivedHistoryDf.collect()

        assert(archivedHistory.length >= allHistory.length, "Archived timeline should include at least active entries")

        val commitHistory = spark.sql(
          s"""call show_metadata_table_history(
             | table => '$tableName',
             | limit => 10,
             | filter => "metadata_table_action = 'commit'"
             |)""".stripMargin).collect()

        assertResult(0)(commitHistory.length)

        val completedMetadataHistory = spark.sql(
          s"""call show_metadata_table_history(
             | table => '$tableName',
             | limit => 10,
             | filter => "metadata_table_state = 'COMPLETED'"
             |)""".stripMargin).collect()

        assertResult(completedMetadataHistory.length)(allHistory.length)
      }
    }
  }
}
