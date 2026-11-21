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

import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.Row

class TestHoodieLogFileProcedure extends HoodieSparkProcedureTestBase {

  /** The name field of each record string, sorted. */
  private def names(rows: Array[Row]): Seq[String] =
    rows.toSeq.map(row => "\"name\": \"([^\"]*)\"".r.findFirstMatchIn(row.getString(0)).map(_.group(1)).orNull).sorted

  private def createMorTable(tableName: String, tablePath: String, tableVersion: Option[Int] = None): Unit = {
    val versionProperty = tableVersion.map(version => s", hoodie.write.table.version = '$version'").getOrElse("")
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
         |  orderingFields = 'ts'$versionProperty
         | )
       """.stripMargin)
  }

  test("Test Call show_logfile_metadata Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      createMorTable(tableName, tablePath)
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
      // The single log block holds the one updated record, and the block type is version dependent:
      // v10 native logs give PARQUET_DATA_BLOCK, v9 avro logs give AVRO_DATA_BLOCK.
      assertResult(1)(result.head.getInt(1))
      assert(result.head.getString(2).endsWith("DATA_BLOCK"),
        s"unexpected block type ${result.head.getString(2)}")
    }
  }

  test("Test Call show_logfile_records Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      createMorTable(tableName, tablePath)
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
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      createMorTable(tableName, tablePath)
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500, 1000")
      spark.sql(s"update $tableName set name = 'b1' where id = 1")
      spark.sql(s"update $tableName set name = 'b2' where id = 2")

      val pattern = s"$tablePath/*/*.log.*"

      // Merged scan returns one row per updated key.
      val merged = spark.sql(
        s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$pattern', merge => true, limit => 10)""".stripMargin).collect()
      assertResult(Seq("b1", "b2"))(names(merged))

      // Every key is updated once, so the unmerged scan returns the same number of rows.
      val unfiltered = spark.sql(
        s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$pattern', limit => 10)""".stripMargin).collect()
      assertResult(merged.length)(unfiltered.length)

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

  test("Test Call show_logfile_records Procedure merging a key updated twice on an uncompacted table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      createMorTable(tableName, tablePath)
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500, 1000")
      spark.sql(s"update $tableName set name = 'b1', ts = 2000 where id = 1")
      spark.sql(s"update $tableName set name = 'c1', ts = 3000 where id = 1")
      assert(createMetaClient(spark, tablePath).getActiveTimeline.getCommitAndReplaceTimeline.empty(),
        "the table must have no commit or replacecommit, only deltacommits")

      val pattern = s"$tablePath/*/*.log.*"
      val unmerged = spark.sql(
        s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$pattern', limit => 10)""".stripMargin).collect()
      assertResult(2)(unmerged.length)

      // The two updates of key 1 merge into its latest version.
      val merged = spark.sql(
        s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$pattern', merge => true, limit => 10)""".stripMargin).collect()
      assertResult(1)(merged.length)
      val record = merged.head.getString(0)
      assert(record.contains("\"name\": \"c1\"") && record.contains("\"ts\": 3000"), record)
    }
  }

  test("Test Call show_logfile_records Procedure with merge when the last log file has no data block") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // Table version 9 keeps deletes inline in the *.log.* files.
      createMorTable(tableName, tablePath, Some(9))
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500, 1000")
      spark.sql(s"update $tableName set name = 'b1', ts = 2000 where id = 1")
      spark.sql(s"delete from $tableName where id = 2")

      // The reader schema comes from an earlier log file with a data block; deleted key 2 is not shown.
      val merged = spark.sql(
        s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$tablePath/*/*.log.*', merge => true, limit => 10)""".stripMargin).collect()
      assertResult(Seq("b1"))(names(merged))
    }
  }

  test("Test Call show_logfile_records Procedure with merge reads log blocks written after a compaction") {
    withSQLConf("hoodie.compact.inline" -> "false", "hoodie.compact.inline.max.delta.commits" -> "1") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        createMorTable(tableName, tablePath)
        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000, 1000")
        spark.sql(s"update $tableName set name = 'b1', ts = 2000 where id = 1")
        spark.sql(s"call run_compaction(op => 'schedule', table => '$tableName')").collect()
        spark.sql(s"call run_compaction(op => 'run', table => '$tableName')").collect()
        assert(createMetaClient(spark, tablePath).getActiveTimeline.getCommitsTimeline.filterCompletedInstants.getInstantsAsStream
          .anyMatch(instant => instant.getAction == "commit"), "the table must have a completed compaction")
        spark.sql(s"update $tableName set name = 'c1', ts = 3000 where id = 1")

        // The update after the compaction is the latest version of key 1.
        val merged = spark.sql(
          s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$tablePath/*/*.log.*', merge => true, limit => 10)""".stripMargin).collect()
        assertResult(Seq("c1"))(names(merged))
      }
    }
  }

  test("Test Call show_logfile_records Procedure over a non-data log block") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // Table version 9 keeps deletes inline in the *.log.* files; under the v10 native log default
      // they land in separate *.deletes.* files that the log glob below would not match.
      createMorTable(tableName, tablePath, Some(9))
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500, 1000")
      // One statement that both updates and deletes, so the delta commit appends a data block and a
      // delete block to the same log file.
      spark.sql(
        s"""
           |merge into $tableName as target
           |using (
           |  select cast(1 as int) as id, 'x1' as name, cast(10 as double) as price,
           |    cast(2000 as long) as ts, cast(1000 as long) as partition
           |  union all
           |  select cast(2 as int) as id, 'b2' as name, cast(20 as double) as price,
           |    cast(2000 as long) as ts, cast(1000 as long) as partition
           |) src
           |on target.id = src.id
           |when matched and src.id = 1 then delete
           |when matched and src.id = 2 then update set target.name = src.name, target.ts = src.ts
           |""".stripMargin)

      val pattern = s"$tablePath/*/*.log.*"

      // Both keys must share one file group, so the data block and the delete block share one log file.
      val globbedLogFiles = FSUtils.getGlobStatusExcludingMetaFolder(
        createMetaClient(spark, tablePath).getStorage, new StoragePath(pattern))
      assertResult(1, "the delete block must share one log file with the data block")(globbedLogFiles.size())

      // The unmerged scan skips the delete block and returns the updated record of key 2.
      val unmerged = spark.sql(
        s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$pattern', limit => 10)""".stripMargin).collect()
      assertResult(1)(unmerged.length)
      assert(unmerged.head.getString(0).contains("\"name\": \"b2\""), unmerged.head.getString(0))

      // The merged scan drops deleted key 1 and keeps key 2.
      val merged = spark.sql(
        s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$pattern', merge => true, limit => 10)""".stripMargin).collect()
      assertResult(1)(merged.length)
      assert(merged.head.getString(0).contains("\"name\": \"b2\""), merged.head.getString(0))

      // The metadata lists the delete block with no records next to the data block.
      val metadata = spark.sql(
        s"""call show_logfile_metadata(table => '$tableName', log_file_path_pattern => '$pattern', limit => 10)""".stripMargin).collect()
      assertResult(Seq(("AVRO_DATA_BLOCK", 1), ("DELETE_BLOCK", 0)))(
        metadata.map(row => (row.getString(2), row.getInt(1))).toSeq.sorted)
    }
  }

  test("Test Call show_logfile_records Procedure over a delete-only log file") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // Table version 9 keeps deletes inline in the *.log.* files; under the v10 native log default
      // they land in separate *.deletes.* files that the log glob below would not match.
      createMorTable(tableName, tablePath, Some(9))
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500, 1000")
      spark.sql(s"delete from $tableName where id = 1")

      val pattern = s"$tablePath/*/*.log.*"
      // The inserts go to a base file, so the only log file the glob matches holds just the delete block.
      val globbedLogFiles = FSUtils.getGlobStatusExcludingMetaFolder(
        createMetaClient(spark, tablePath).getStorage, new StoragePath(pattern))
      assertResult(1, "the delete block must be alone in the only log file")(globbedLogFiles.size())

      // A delete-only log file has no records to show, merged or not.
      Seq(false, true).foreach { merge =>
        val result = spark.sql(
          s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$pattern', merge => $merge, limit => 10)""".stripMargin).collect()
        assertResult(0, s"merge = $merge")(result.length)
      }
    }
  }
}
