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

class TestHoodieLogFileProcedure extends HoodieSparkProcedureTestBase {

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
    // Keep automatic cleaning off so the pre-compaction log files survive for the merged scan. The
    // lowered compaction trigger + explicit run_compaction below are a workaround: the merged scan
    // resolves the latest instant via getCommitAndReplaceTimeline.lastInstant.get, which throws on
    // a deltacommit-only (never compacted) MOR table; see #19634.
    withSQLConf("hoodie.clean.automatic" -> "false", "hoodie.compact.inline.max.delta.commits" -> "1") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        createMorTable(tableName, tablePath)
        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000, 1000")
        spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500, 1000")
        // Each update touches a distinct key on purpose: a key updated in more than one log block
        // makes the merged scan cast the merged record to HoodieRecordPayload and fail with a
        // ClassCastException, so the merge-is-a-no-op shape is the only one that works; see #19634.
        spark.sql(s"update $tableName set name = 'b1' where id = 1")
        spark.sql(s"update $tableName set name = 'b2' where id = 2")

        // Compaction produces a commit instant on the timeline, which the merged scan needs to
        // determine the latest instant time; the delta log files remain on disk. The lowered
        // delta-commit threshold above already compacts inline, so this call usually has nothing
        // left to schedule and returns no rows -- it is the fallback for the day inline compaction
        // stops firing. Assert the precondition the merged scan actually depends on, otherwise it
        // dies later with an opaque NoSuchElementException; see #19634.
        spark.sql(s"call run_compaction(op => 'run', table => '$tableName')").collect()
        assert(createMetaClient(spark, tablePath).getActiveTimeline.getCommitAndReplaceTimeline.lastInstant.isPresent,
          "compaction must produce a commit instant for the merged scan")

        val pattern = s"$tablePath/*/*.log.*"

        // Merged scan returns one row per updated key.
        val merged = spark.sql(
          s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$pattern', merge => true, limit => 10)""".stripMargin).collect()
        assertResult(2)(merged.length)

        // Unmerged scan, used as the baseline for the filter assertions. Because every key is
        // updated exactly once, merging is a no-op here and both scans return the same rows, so
        // this asserts no merge-vs-no-merge output distinction: a shape that would show one needs a
        // key updated in more than one log block, which the merged scan cannot read; see #19634.
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
      // delete block to the same log file. Standalone deletes are not the only way to trip the NPE:
      // show_logfile_records resolves its reader schema from the LAST globbed log file
      // (Objects.requireNonNull(readSchemaFromLogFile(...logFilePaths.last))), so any glob whose last
      // file carries no data block dies the same way, merged or unmerged. The CLI twin fixed this in
      // HUDI-6694 (#9445) with per-file null tolerance and a backward scan for the schema; see #19634.
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

      // Known limitation: both show_logfile procedures match log blocks with a single
      // `case dataBlock: HoodieDataBlock` arm and no default arm, so a non-data block -- here the
      // delete block -- raises a MatchError instead of being skipped; see #19634. A fix flips these
      // assertions to assert the delete block is skipped, or surfaced, gracefully.
      val pattern = s"$tablePath/*/*.log.*"

      // The MatchError pins below depend on small-file packing putting both keys in ONE file group,
      // so the data block and the delete block share a single log file. If they ever split into two
      // file groups, the last globbed file would be delete only and the procedures would die with a
      // bare NPE on the null log-file schema instead, turning this test red for the wrong reason.
      val globbedLogFiles = FSUtils.getGlobStatusExcludingMetaFolder(
        createMetaClient(spark, tablePath).getStorage, new StoragePath(pattern))
      assertResult(1, "the delete block must share one log file with the data block")(globbedLogFiles.size())

      val deleteBlockMatchError = "of class org.apache.hudi.common.table.log.block.HoodieDeleteBlock"
      checkExceptionContain(
        s"""call show_logfile_records(table => '$tableName', log_file_path_pattern => '$pattern', limit => 10)""")(
        deleteBlockMatchError)
      checkExceptionContain(
        s"""call show_logfile_metadata(table => '$tableName', log_file_path_pattern => '$pattern', limit => 10)""")(
        deleteBlockMatchError)
    }
  }
}
