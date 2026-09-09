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

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.FileCreateUtils
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.hadoop.fs.Path

import java.util.UUID

class TestRepairOrphanFilesProcedure extends HoodieSparkProcedureTestBase {

  private val ORPHAN_INSTANT = "20000101000000000"  // Year 2000; never in any test timeline

  private def metaClientFor(tablePath: String): HoodieTableMetaClient = {
    HoodieTableMetaClient.builder
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .setBasePath(tablePath)
      .build
  }

  // Test 1 — dry run detects a base-file orphan in a non-partitioned COW table
  test("Test Call repair_orphan_files dry run finds base file orphan") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // Create a non-partitioned COW table and write one real commit
      spark.sql(
        s"""create table $tableName (id int, name string, price double, ts long)
           |using hudi
           |location '$tablePath'
           |tblproperties (primaryKey = 'id', preCombineField = 'ts')
           |""".stripMargin)
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      // Inject orphan base file directly onto the filesystem with a stale instant timestamp
      val orphanFileId = UUID.randomUUID().toString
      FileCreateUtils.createBaseFile(metaClientFor(tablePath), "", ORPHAN_INSTANT, orphanFileId)

      // dry_run=true (default): should return exactly 1 row for the orphan, touch nothing
      val result = spark.sql(s"call repair_orphan_files(table => '$tableName')").collect()
      assertResult(1)(result.length)

      val row = result(0)
      assertResult("")(row.getString(0))             // partition (root = "" for non-partitioned)
      assert(row.getString(1).contains(ORPHAN_INSTANT), s"file_name should contain orphan instant: ${row.getString(1)}")
      assertResult(ORPHAN_INSTANT)(row.getString(2)) // instant_time
      assertResult("")(row.getString(3))             // backup_path is empty in dry run
      assertResult("IDENTIFIED")(row.getString(4))   // status
    }
  }

  // Test 2 — cleanup mode backs up the orphan file and removes it from the table path
  test("Test Call repair_orphan_files cleanup backs up base file orphan") {
    withTempDir { tmp =>
      val tableName  = generateTableName
      val tablePath  = s"${tmp.getCanonicalPath}/$tableName"
      val backupDir  = s"${tmp.getCanonicalPath}/backup"

      spark.sql(
        s"""create table $tableName (id int, name string, price double, ts long)
           |using hudi
           |location '$tablePath'
           |tblproperties (primaryKey = 'id', preCombineField = 'ts')
           |""".stripMargin)
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      val orphanFileId   = UUID.randomUUID().toString
      // FileCreateUtils.createBaseFile returns the absolute path of the created file
      val orphanAbsPath  = FileCreateUtils.createBaseFile(metaClientFor(tablePath), "", ORPHAN_INSTANT, orphanFileId)
      val orphanFilePath = new Path(orphanAbsPath)

      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = HadoopFSUtils.getFs(tablePath, hadoopConf)
      assert(fs.exists(orphanFilePath), "Orphan file should exist before cleanup")

      // dry_run=false: orphan should be moved to backup
      val result = spark.sql(
        s"""call repair_orphan_files(
           |  table => '$tableName',
           |  dry_run => false,
           |  backup_path => '$backupDir'
           |)""".stripMargin).collect()

      assertResult(1)(result.length)
      assertResult("BACKED_UP")(result(0).getString(4))

      // Orphan file must be gone from the table path
      assert(!fs.exists(orphanFilePath), "Orphan file should no longer exist at original path")

      // Orphan file must exist at the backup path
      val backedUpPath = new Path(result(0).getString(3))
      assert(fs.exists(backedUpPath), "Orphan file should exist at backup path")

      // Real data must still be readable
      assertResult(1)(spark.sql(s"select id from $tableName").collect().length)
    }
  }

  // Test 3 — inflight commit files are skipped (RepairUtils skips non-completed instants)
  test("Test Call repair_orphan_files skips inflight commit files") {
    withTempDir { tmp =>
      val tableName    = generateTableName
      val tablePath    = s"${tmp.getCanonicalPath}/$tableName"
      val inflightTs   = "20010101000000000"

      spark.sql(
        s"""create table $tableName (id int, name string, price double, ts long)
           |using hudi
           |location '$tablePath'
           |tblproperties (primaryKey = 'id', preCombineField = 'ts')
           |""".stripMargin)
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      val metaClient = metaClientFor(tablePath)
      // Create the inflight marker in .hoodie so the active timeline sees it as non-completed
      FileCreateUtils.createInflightCommit(metaClient, inflightTs)

      // Place a data file with the inflight instant timestamp on disk
      FileCreateUtils.createBaseFile(metaClient, "", inflightTs, UUID.randomUUID().toString)

      // Procedure must return 0 rows — inflight instant is excluded by RepairUtils
      val result = spark.sql(s"call repair_orphan_files(table => '$tableName')").collect()
      assertResult(0)(result.length)
    }
  }

  // Test 4 — backup_path validation: error when dry_run=false and no backup_path given
  test("Test Call repair_orphan_files requires backup_path when not dry run") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      spark.sql(
        s"""create table $tableName (id int, name string, price double, ts long)
           |using hudi
           |location '$tablePath'
           |tblproperties (primaryKey = 'id', preCombineField = 'ts')
           |""".stripMargin)

      checkExceptionContain(
        s"call repair_orphan_files(table => '$tableName', dry_run => false)"
      )("backup_path is required")
    }
  }

  // Test 5 — partition filter scopes the scan to a specific partition
  test("Test Call repair_orphan_files partition filter scopes scan") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      spark.sql(
        s"""create table $tableName (id int, name string, price double, ts long)
           |using hudi
           |location '$tablePath'
           |tblproperties (primaryKey = 'id', preCombineField = 'ts')
           |""".stripMargin)
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      // Inject orphan at table root (partition = "")
      FileCreateUtils.createBaseFile(metaClientFor(tablePath), "", ORPHAN_INSTANT, UUID.randomUUID().toString)

      // Filtering to a non-existent partition should find nothing
      val filtered = spark.sql(
        s"call repair_orphan_files(table => '$tableName', partition => 'nonexistent')").collect()
      assertResult(0)(filtered.length)

      // Filtering to the root partition ("") should find the orphan
      val root = spark.sql(
        s"call repair_orphan_files(table => '$tableName', partition => '')").collect()
      assertResult(1)(root.length)
      assertResult("IDENTIFIED")(root(0).getString(4))
    }
  }

  // Test 6 — log file orphan detected on a MOR table
  test("Test Call repair_orphan_files detects log file orphan on MOR table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      spark.sql(
        s"""create table $tableName (id int, name string, price double, ts long)
           |using hudi
           |location '$tablePath'
           |tblproperties (primaryKey = 'id', preCombineField = 'ts', type = 'mor')
           |""".stripMargin)
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      // Inject an orphan log file with a stale instant timestamp
      val orphanFileId = UUID.randomUUID().toString
      FileCreateUtils.createLogFile(metaClientFor(tablePath), "", ORPHAN_INSTANT, orphanFileId, 1)

      val result = spark.sql(s"call repair_orphan_files(table => '$tableName')").collect()

      // At least the orphan log file must appear in the result
      val orphanRows = result.filter(_.getString(2) == ORPHAN_INSTANT)
      assert(orphanRows.length >= 1,
        s"Expected at least 1 orphan row with instant $ORPHAN_INSTANT, got: ${result.mkString(", ")}")
      assert(orphanRows.forall(_.getString(4) == "IDENTIFIED"))
    }
  }

  // Note: the SKIPPED_PRESENT_IN_MDT branch (surfaces MDT-visible orphan candidates instead of
  // silently dropping them) is verified by inspection rather than an end-to-end test.
  // HoodieBackedTableMetadata.getAllFilesInPartition dynamically filters by the data table's
  // timeline state — deleting a timeline file immediately removes the corresponding data file
  // from the MDT's view as well. As a result, the (orphan ∧ in-MDT) state cannot be constructed
  // by manipulating the timeline alone; only direct MDT writes could produce it, and the setup
  // cost outweighs the value for what is a defense-in-depth path.

  // Test 7 — max_orphans cap prevents driver OOM: error when detected count exceeds the cap
  test("Test Call repair_orphan_files max_orphans cap triggers error when exceeded") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      spark.sql(
        s"""create table $tableName (id int, name string, price double, ts long)
           |using hudi
           |location '$tablePath'
           |tblproperties (primaryKey = 'id', preCombineField = 'ts')
           |""".stripMargin)
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      // Inject 3 orphan files, then cap at 2 — should throw
      val metaClient = metaClientFor(tablePath)
      FileCreateUtils.createBaseFile(metaClient, "", ORPHAN_INSTANT, UUID.randomUUID().toString)
      FileCreateUtils.createBaseFile(metaClient, "", ORPHAN_INSTANT, UUID.randomUUID().toString)
      FileCreateUtils.createBaseFile(metaClient, "", ORPHAN_INSTANT, UUID.randomUUID().toString)

      checkExceptionContain(
        s"call repair_orphan_files(table => '$tableName', max_orphans => 2)"
      )("exceeds max_orphans=2")
    }
  }
}
