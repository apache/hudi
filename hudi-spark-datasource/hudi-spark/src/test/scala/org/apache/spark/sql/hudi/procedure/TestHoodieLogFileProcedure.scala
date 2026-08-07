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

class TestHoodieLogFileProcedure extends HoodieSparkProcedureTestBase {
  test("Test Call show_logfile_metadata Procedure") {
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
           |  ts long,
           |  partition int
           |) using hudi
           | partitioned by (partition)
           | location '$tablePath'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500, 1500")
      spark.sql(s"update $tableName set name = 'b1', price = 100 where id = 1")

      // Check required fields
      checkExceptionContain(s"""call show_logfile_metadata(limit => 10)""")(
        s"Table name or table path must be given one")

      // collect result for table
      val result = spark.sql(
        s"""call show_logfile_metadata(table => '$tableName', log_file_path_pattern => '$tablePath/partition=1000/*.log.*')""".stripMargin).collect()
      assertResult(1) {
        result.length
      }
    }
  }

  test("Test Call show_logfile_records Procedure") {
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
           |  ts long,
           |  partition long
           |) using hudi
           | partitioned by (partition)
           | location '$tablePath'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500, 1500")
      spark.sql(s"update $tableName set name = 'b1' where id = 1")
      spark.sql(s"update $tableName set name = 'b2' where id = 2")

      // Check required fields
      checkExceptionContain(s"""call show_logfile_records(limit => 10)""")(
        s"Table name or table path must be given one")

      // collect result for table
      val result = spark.sql(
        s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$tablePath/*/*.log.*', limit => 1)""".stripMargin).collect()
      assertResult(1) {
        result.length
      }
    }
  }

  test("Test Call show_logfile_records Procedure with merge and filter") {
    // Keep automatic cleaning off so the pre-compaction log files survive for the merged scan, and
    // lower the compaction trigger so the explicit run_compaction below actually schedules and
    // executes (producing the commit instant that the merged scan needs to find the latest instant).
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.compact.inline.max.delta.commits" -> "1") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  partition long
             |) using hudi
             | partitioned by (partition)
             | location '$tablePath'
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             | )
       """.stripMargin)
        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000, 1000")
        spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500, 1000")
        spark.sql(s"update $tableName set name = 'b1' where id = 1")
        spark.sql(s"update $tableName set name = 'b2' where id = 2")

        // Compaction produces a commit instant on the timeline, which the merged scan needs
        // to determine the latest instant time; the delta log files remain on disk.
        spark.sql(s"call run_compaction(op => 'run', table => '$tableName')").collect()

        val pattern = s"$tablePath/*/*.log.*"

        // Merged scan returns records from the log files.
        val merged = spark.sql(
          s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$pattern', merge => true, limit => 10)""".stripMargin).collect()
        assert(merged.nonEmpty, "Merged log-file scan should return records")

        // Unmerged scan, used as the baseline for the filter assertions.
        val unfiltered = spark.sql(
          s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$pattern', limit => 10)""".stripMargin).collect()
        assert(unfiltered.nonEmpty)

        // A filter that always holds keeps every row.
        val keepAll = spark.sql(
          s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$pattern', limit => 10, filter => "records IS NOT NULL")""".stripMargin).collect()
        assertResult(unfiltered.length)(keepAll.length)

        // A filter that never holds drops every row.
        val dropAll = spark.sql(
          s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$pattern', limit => 10, filter => "records LIKE '%__no_such_token__%'")""".stripMargin).collect()
        assertResult(0)(dropAll.length)
      }
    }
  }
}
