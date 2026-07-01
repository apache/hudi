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

package org.apache.spark.sql.hudi.dml.others

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig, RecordMergeMode}
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile}
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaUtils}
import org.apache.hudi.common.table.{HoodieTableMetaClient, HoodieTableVersion, TableSchemaResolver}
import org.apache.hudi.common.table.log.HoodieLogFileReader
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.CompactionUtils
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieNotSupportedException
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.DataSourceTestUtils

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.getMetaClientAndFileSystemView
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import java.util.{Collections, List, Optional}

import scala.collection.JavaConverters._

class TestPartialUpdateForMergeInto extends HoodieSparkSqlTestBase {

  test("Test partial update with COW and Avro log format") {
    testPartialUpdate("cow", "avro")
  }

  test("Test partial update with COW and Avro log format and commit time ordering") {
    testPartialUpdate("cow", "avro", commitTimeOrdering = true)
  }

  test("Test partial update with MOR and Avro log format") {
    testPartialUpdate("mor", "avro")
  }

  test("Test partial update with MOR and avro log format and commit time ordering") {
    testPartialUpdate("mor", "avro", commitTimeOrdering = true)
  }

  test("Test partial update with MOR and Parquet log format") {
    testPartialUpdate("mor", "parquet")
  }

  test("Test partial update with MOR and Parquet log format and commit time ordering") {
    testPartialUpdate("mor", "parquet", commitTimeOrdering = true)
  }

  test("Test partial update incremental query returns full row with MOR and Avro log format") {
    testPartialUpdateIncrementalQuery("avro", commitTimeOrdering = true)
  }

  test("Test partial update incremental query returns full row with MOR and Parquet log format") {
    testPartialUpdateIncrementalQuery("parquet", commitTimeOrdering = true)
  }

  test("Test partial update incremental query returns full row with MOR and event time ordering") {
    testPartialUpdateIncrementalQuery("avro", commitTimeOrdering = false)
  }

  test("Test partial update incremental query returns full row with MOR on a partitioned table") {
    testPartialUpdateIncrementalQueryPartitioned("avro")
  }

  test("Test incremental query reflects event time ordering on a MOR table (no partial updates)") {
    testIncrementalEventTimeOrdering("avro")
  }

  test("Test partial update incremental query bounds the window and excludes untouched records") {
    testPartialUpdateIncrementalWindowBounds("avro")
  }

  test("Test partial update incremental query returns inserts and updates in the same window") {
    testPartialUpdateIncrementalInsertAndUpdate("avro")
  }

  test("Test partial update incremental query merges multiple partial updates to the same key") {
    testPartialUpdateIncrementalMultipleUpdatesSameKey("avro")
  }

  test("Test partial update incremental query excludes log files committed after the window end") {
    testPartialUpdateIncrementalExcludesLaterCommits("avro")
  }

  test("Test partial update incremental query merges a log file on a compacted base file") {
    testPartialUpdateIncrementalAfterCompaction("avro")
  }

  test("Test incremental query on a COW table is unaffected by the MOR incremental change") {
    testIncrementalCowUnaffected("avro")
  }

  /**
   * HUDI #18943: for a MOR table with partial updates, an incremental query must return ALL columns
   * of a changed record (unchanged columns filled in from the base file), not only the columns
   * altered by the partial update.
   */
  def testPartialUpdateIncrementalQuery(logDataBlockFormat: String, commitTimeOrdering: Boolean): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> logDataBlockFormat,
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {
        val mergeMode = if (commitTimeOrdering) {
          RecordMergeMode.COMMIT_TIME_ORDERING.name()
        } else {
          RecordMergeMode.EVENT_TIME_ORDERING.name()
        }
        val preCombineString = if (commitTimeOrdering) "" else "preCombineField = '_ts',"
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts int,
             | description string
             |) using hudi
             |tblproperties(
             | type ='mor',
             | primaryKey = 'id',
             | $preCombineString
             | recordMergeMode = '$mergeMode'
             |)
             |location '$basePath'
        """.stripMargin)

        // Commit 1: full rows land in the base file.
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2'), (3, 'a3', 30, 1250, 'a3: desc3')")
        val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))
        val firstCompletionTime = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

        // Commit 2: partial update changing only "price" and "_ts" -> written as a partial log block.
        // The new "_ts" values are larger than the existing ones so the update wins under event time
        // ordering as well.
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 12.0 as price, 1001 as ts
             |union select 3 as id, 25.0 as price, 1260 as ts) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price, _ts = s0.ts
             |""".stripMargin)

        // Snapshot query returns the merged full rows (unchanged columns preserved).
        checkAnswer(s"select id, name, price, _ts, description from $tableName order by id")(
          Seq(1, "a1", 12.0, 1001, "a1: desc1"),
          Seq(2, "a2", 20.0, 1200, "a2: desc2"),
          Seq(3, "a3", 25.0, 1260, "a3: desc3")
        )

        // Incremental query over just commit 2 must return the FULL updated rows, including the
        // columns ("name", "description") not part of the partial update. Before the fix these come
        // back null/garbled because the base-file row is dropped before the runtime merge.
        checkAnswer(readIncremental(basePath, firstCompletionTime, "id", "name", "price", "_ts", "description"))(
          Seq(1, "a1", 12.0, 1001, "a1: desc1"),
          Seq(3, "a3", 25.0, 1260, "a3: desc3")
        )
      }
    }
  }

  /**
   * Same as [[testPartialUpdateIncrementalQuery]] but on a partitioned table where only some file
   * groups in the modified partitions are touched. Exercises the file-group scoping in the
   * incremental file listing.
   */
  def testPartialUpdateIncrementalQueryPartitioned(logDataBlockFormat: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> logDataBlockFormat,
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts int,
             | description string,
             | part string
             |) using hudi
             |partitioned by (part)
             |tblproperties(
             | type ='mor',
             | primaryKey = 'id',
             | recordMergeMode = '${RecordMergeMode.COMMIT_TIME_ORDERING.name()}'
             |)
             |location '$basePath'
        """.stripMargin)

        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1', 'p1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2', 'p1'), (3, 'a3', 30, 1250, 'a3: desc3', 'p2')")
        val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))
        val firstCompletionTime = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 12.0 as price, 1001 as ts, 'p1' as part
             |union select 3 as id, 25.0 as price, 1260 as ts, 'p2' as part) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price, _ts = s0.ts
             |""".stripMargin)

        checkAnswer(s"select id, name, price, _ts, description, part from $tableName order by id")(
          Seq(1, "a1", 12.0, 1001, "a1: desc1", "p1"),
          Seq(2, "a2", 20.0, 1200, "a2: desc2", "p1"),
          Seq(3, "a3", 25.0, 1260, "a3: desc3", "p2")
        )

        checkAnswer(readIncremental(basePath, firstCompletionTime, "id", "name", "price", "_ts", "description", "part"))(
          Seq(1, "a1", 12.0, 1001, "a1: desc1", "p1"),
          Seq(3, "a3", 25.0, 1260, "a3: desc3", "p2")
        )
      }
    }
  }

  /**
   * HUDI #18943: even without partial updates, an incremental query on an EVENT_TIME_ORDERING MOR
   * table must resolve the winning record version via a runtime merge against the base/earlier log,
   * not just surface the value written within the window. A window write with a lower ordering value
   * loses, so it must not appear in the incremental result; a window write with a higher ordering
   * value wins and is returned.
   */
  def testIncrementalEventTimeOrdering(logDataBlockFormat: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> logDataBlockFormat,
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts int
             |) using hudi
             |tblproperties(
             | type ='mor',
             | primaryKey = 'id',
             | preCombineField = '_ts',
             | recordMergeMode = '${RecordMergeMode.EVENT_TIME_ORDERING.name()}'
             |)
             |location '$basePath'
        """.stripMargin)

        // Commit 1: base file with high ordering values.
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000), (2, 'a2', 20, 1000)")
        val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))
        val firstCompletionTime = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

        // Commit 2: id 1 updated with a HIGHER _ts (wins); id 2 updated with a LOWER _ts (loses).
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 99.0 as price, 2000 as ts
             |union select 2 as id, 'a2' as name, 88.0 as price, 500 as ts) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price, _ts = s0.ts
             |""".stripMargin)

        // Snapshot: id 1 took the higher-ts update; id 2 kept the base value (lower-ts update lost).
        checkAnswer(s"select id, name, price, _ts from $tableName order by id")(
          Seq(1, "a1", 99.0, 2000),
          Seq(2, "a2", 20.0, 1000)
        )

        // Incremental over commit 2: only id 1 actually changed its winning version; id 2's losing
        // write must not surface (its merged commit time stays outside the window).
        checkAnswer(readIncremental(basePath, firstCompletionTime, "id", "name", "price", "_ts"))(
          Seq(1, "a1", 99.0, 2000)
        )
      }
    }
  }

  /**
   * Verifies the incremental window is honored across multiple commits: a sub-window returns only
   * the records changed within it (fully merged), and a record never touched in the window does not
   * appear. Commit-time ordering keeps the latest write winning.
   */
  def testPartialUpdateIncrementalWindowBounds(logDataBlockFormat: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> logDataBlockFormat,
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {
        spark.sql(
          s"""
             |create table $tableName (id int, name string, price double, _ts int, description string)
             |using hudi
             |tblproperties(type ='mor', primaryKey = 'id', recordMergeMode = '${RecordMergeMode.COMMIT_TIME_ORDERING.name()}')
             |location '$basePath'
        """.stripMargin)
        val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))

        // Commit 1: base file with three records.
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2'), (3, 'a3', 30, 1250, 'a3: desc3')")
        val t1 = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

        // Commit 2: partial update of id 1.
        spark.sql(s"merge into $tableName t0 using (select 1 as id, 11.0 as price, 1001 as ts) s0 " +
          "on t0.id = s0.id when matched then update set price = s0.price, _ts = s0.ts")
        val t2 = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

        // Commit 3: partial update of id 2.
        spark.sql(s"merge into $tableName t0 using (select 2 as id, 22.0 as price, 1201 as ts) s0 " +
          "on t0.id = s0.id when matched then update set price = s0.price, _ts = s0.ts")

        // Window (t1, t2] = commit 2 only -> just id 1 (full row); id 2 and id 3 excluded.
        checkAnswer(readIncrementalBounded(basePath, t1, t2, "id", "name", "price", "_ts", "description"))(
          Seq(1, "a1", 11.0, 1001, "a1: desc1")
        )

        // Window (t2, latest] = commit 3 only -> just id 2 (full row).
        checkAnswer(readIncremental(basePath, t2, "id", "name", "price", "_ts", "description"))(
          Seq(2, "a2", 22.0, 1201, "a2: desc2")
        )

        // Window (t1, latest] = commits 2 and 3 -> id 1 and id 2 (full rows); id 3 never touched.
        checkAnswer(readIncremental(basePath, t1, "id", "name", "price", "_ts", "description"))(
          Seq(1, "a1", 11.0, 1001, "a1: desc1"),
          Seq(2, "a2", 22.0, 1201, "a2: desc2")
        )
      }
    }
  }

  /**
   * Verifies a single incremental window that contains both a partial update (existing key) and an
   * insert (new key) returns both with all columns.
   */
  def testPartialUpdateIncrementalInsertAndUpdate(logDataBlockFormat: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> logDataBlockFormat,
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {
        spark.sql(
          s"""
             |create table $tableName (id int, name string, price double, _ts int, description string)
             |using hudi
             |tblproperties(type ='mor', primaryKey = 'id', recordMergeMode = '${RecordMergeMode.COMMIT_TIME_ORDERING.name()}')
             |location '$basePath'
        """.stripMargin)
        val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))

        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1'), (2, 'a2', 20, 1200, 'a2: desc2')")
        val t1 = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

        // Commit 2: partial update of id 1 + insert of new id 3.
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 12.0 as price, 1001 as _ts, 'a1: desc1' as description
             |union select 3 as id, 'a3' as name, 30.0 as price, 1300 as _ts, 'a3: desc3' as description) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price, _ts = s0._ts
             |when not matched then insert *
             |""".stripMargin)

        // Window over commit 2 -> id 1 (partial update, full row) and id 3 (insert, full row); id 2 excluded.
        checkAnswer(readIncremental(basePath, t1, "id", "name", "price", "_ts", "description"))(
          Seq(1, "a1", 12.0, 1001, "a1: desc1"),
          Seq(3, "a3", 30.0, 1300, "a3: desc3")
        )
      }
    }
  }

  /**
   * Verifies multiple partial updates to the same key across commits merge into a single full row
   * in the incremental result (later partial columns win, earlier untouched columns retained).
   */
  def testPartialUpdateIncrementalMultipleUpdatesSameKey(logDataBlockFormat: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> logDataBlockFormat,
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {
        spark.sql(
          s"""
             |create table $tableName (id int, name string, price double, _ts int, description string)
             |using hudi
             |tblproperties(type ='mor', primaryKey = 'id', recordMergeMode = '${RecordMergeMode.COMMIT_TIME_ORDERING.name()}')
             |location '$basePath'
        """.stripMargin)
        val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))

        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')")
        val t1 = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

        // Commit 2: update "price". Commit 3: update "description". Both partial, same key.
        spark.sql(s"merge into $tableName t0 using (select 1 as id, 12.0 as price, 1001 as ts) s0 " +
          "on t0.id = s0.id when matched then update set price = s0.price, _ts = s0.ts")
        spark.sql(s"merge into $tableName t0 using (select 1 as id, 'a1: desc1-v2' as descr, 1002 as ts) s0 " +
          "on t0.id = s0.id when matched then update set description = s0.descr, _ts = s0.ts")

        // Window (t1, latest] -> a single fully-merged row: name from base, price from commit 2,
        // description from commit 3, _ts the latest.
        checkAnswer(readIncremental(basePath, t1, "id", "name", "price", "_ts", "description"))(
          Seq(1, "a1", 12.0, 1002, "a1: desc1-v2")
        )
      }
    }
  }

  /**
   * Verifies that a bounded incremental query (with an END_COMMIT) does not merge in log files
   * committed after the window end. A record updated inside the window and again afterwards must be
   * returned with its value as of the window end, not its latest value (and must not be dropped).
   */
  def testPartialUpdateIncrementalExcludesLaterCommits(logDataBlockFormat: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> logDataBlockFormat,
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {
        spark.sql(
          s"""
             |create table $tableName (id int, name string, price double, _ts int, description string)
             |using hudi
             |tblproperties(type ='mor', primaryKey = 'id', recordMergeMode = '${RecordMergeMode.COMMIT_TIME_ORDERING.name()}')
             |location '$basePath'
        """.stripMargin)
        val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))

        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')")
        val t1 = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

        // Commit 2 (inside the window): price -> 11.
        spark.sql(s"merge into $tableName t0 using (select 1 as id, 11.0 as price, 1001 as ts) s0 " +
          "on t0.id = s0.id when matched then update set price = s0.price, _ts = s0.ts")
        val t2 = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

        // Commit 3 (after the window): price -> 99.
        spark.sql(s"merge into $tableName t0 using (select 1 as id, 99.0 as price, 9999 as ts) s0 " +
          "on t0.id = s0.id when matched then update set price = s0.price, _ts = s0.ts")

        // Snapshot reflects the latest value.
        checkAnswer(s"select id, name, price, _ts, description from $tableName")(
          Seq(1, "a1", 99.0, 9999, "a1: desc1")
        )

        // Incremental over (t1, t2] must return id 1 as of commit 2 (price 11), not the later value
        // and not an empty result.
        checkAnswer(readIncrementalBounded(basePath, t1, t2, "id", "name", "price", "_ts", "description"))(
          Seq(1, "a1", 11.0, 1001, "a1: desc1")
        )
      }
    }
  }

  /**
   * Verifies a partial update written as a log file on top of a compacted base file is merged
   * correctly in an incremental query: the value carried into the compacted base file is retained
   * and the later partial update is applied on top.
   */
  def testPartialUpdateIncrementalAfterCompaction(logDataBlockFormat: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> logDataBlockFormat,
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true",
        HoodieCompactionConfig.INLINE_COMPACT.key() -> "true",
        HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "2") {
        spark.sql(
          s"""
             |create table $tableName (id int, name string, price double, _ts int, description string)
             |using hudi
             |tblproperties(type ='mor', primaryKey = 'id', recordMergeMode = '${RecordMergeMode.COMMIT_TIME_ORDERING.name()}')
             |location '$basePath'
        """.stripMargin)
        val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))

        // Delta commit 1 (base) + delta commit 2 (partial update) -> triggers inline compaction, so
        // the compacted base file now carries price = 11.
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'd1')")
        spark.sql(s"merge into $tableName t0 using (select 1 as id, 11.0 as price, 1001 as ts) s0 " +
          "on t0.id = s0.id when matched then update set price = s0.price, _ts = s0.ts")
        val afterCompaction = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

        // A further partial update lands as a log file on top of the compacted base file.
        spark.sql(s"merge into $tableName t0 using (select 1 as id, 'd1v2' as descr, 1002 as ts) s0 " +
          "on t0.id = s0.id when matched then update set description = s0.descr, _ts = s0.ts")

        // Incremental over the last commit merges the log file with the compacted base: price 11
        // (from the compacted base), description d1v2 (from the log), name retained.
        checkAnswer(readIncremental(basePath, afterCompaction, "id", "name", "price", "_ts", "description"))(
          Seq(1, "a1", 11.0, 1002, "d1v2")
        )
      }
    }
  }

  /**
   * Confirms the MOR incremental change does not regress COW incremental queries (COW file groups
   * have no log files, so they keep the original read path).
   */
  def testIncrementalCowUnaffected(logDataBlockFormat: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {
        spark.sql(
          s"""
             |create table $tableName (id int, name string, price double, _ts int, description string)
             |using hudi
             |tblproperties(type ='cow', primaryKey = 'id', recordMergeMode = '${RecordMergeMode.COMMIT_TIME_ORDERING.name()}')
             |location '$basePath'
        """.stripMargin)
        val storage = HoodieTestUtils.getStorage(new StoragePath(basePath))

        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'd1'), (2, 'a2', 20, 1200, 'd2')")
        val t1 = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

        spark.sql(s"merge into $tableName t0 using (select 1 as id, 12.0 as price, 1001 as ts) s0 " +
          "on t0.id = s0.id when matched then update set price = s0.price, _ts = s0.ts")

        // Incremental over the update returns only id 1 with its full updated row; id 2 untouched.
        checkAnswer(readIncremental(basePath, t1, "id", "name", "price", "_ts", "description"))(
          Seq(1, "a1", 12.0, 1001, "d1")
        )
      }
    }
  }

  private def readIncrementalBounded(basePath: String, startCompletionTime: String, endCompletionTime: String,
                                     cols: String*): Array[org.apache.spark.sql.Row] = {
    spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, startCompletionTime)
      .option(DataSourceReadOptions.END_COMMIT.key, endCompletionTime)
      .load(basePath)
      .select(cols.head, cols.tail: _*)
      .orderBy("id")
      .collect()
  }

  private def readIncremental(basePath: String, startCompletionTime: String, cols: String*): Array[org.apache.spark.sql.Row] = {
    spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, startCompletionTime)
      .load(basePath)
      .select(cols.head, cols.tail: _*)
      .orderBy("id")
      .collect()
  }

  test("Test partial update and insert with COW and Avro log format") {
    testPartialUpdateWithInserts("cow", "avro")
  }

  test("Test partial update and insert with MOR and Avro log format") {
    testPartialUpdateWithInserts("mor", "avro")
  }

  test("Test partial update and insert with MOR and Avro log format and commit time ordering") {
    testPartialUpdateWithInserts("mor", "avro", commitTimeOrdering = true)
  }

  test("Test partial update and insert with MOR and Parquet log format") {
    testPartialUpdateWithInserts("mor", "parquet")
  }

  test("Test partial update and insert with MOR and Parquet log format and commit time ordering") {
    testPartialUpdateWithInserts("mor", "parquet", commitTimeOrdering = true)
  }

  test("Test partial update with schema on read enabled") {
    withSQLConf(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key() -> "true") {
      try {
        testPartialUpdate("mor", "parquet")
        fail("Expected exception to be thrown")
      } catch {
        case t: Throwable => assertTrue(t.isInstanceOf[HoodieNotSupportedException])
      }
    }
  }

  test("Test fallback to full update with MOR even if partial updates are enabled") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {

        // Create a table with five data fields
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts int,
             | description string
             |) using hudi
             |tblproperties(
             | type ='mor',
             | primaryKey = 'id',
             | preCombineField = '_ts'
             |)
             |location '$basePath'
        """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2'), (3, 'a3', 30, 1250, 'a3: desc3')")

        // Update all fields
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 12 as price, 1001 as _ts, 'a1: updated' as description
             |union select 3 as id, 'a3' as name, 25 as price, 1260 as _ts, 'a3: updated' as description) s0
             |on t0.id = s0.id
             |when matched then update set *
             |""".stripMargin)

        checkAnswer(s"select id, name, price, _ts, description from $tableName")(
          Seq(1, "a1", 12.0, 1001, "a1: updated"),
          Seq(2, "a2", 20.0, 1200, "a2: desc2"),
          Seq(3, "a3", 25.0, 1260, "a3: updated")
        )

        validateLogBlock(basePath, 1, Seq(Seq("id", "name", "price", "_ts", "description")), false)
      }
    }
  }

  test("Test MERGE INTO with inserts only on MOR table when partial updates are enabled") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true",
        // Write inserts to log block
        HoodieIndexConfig.INDEX_TYPE.key() -> "INMEMORY"
      ) {
        // Create a table with five data fields
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts int,
             | description string
             |) using hudi
             |tblproperties(
             | type ='mor',
             | primaryKey = 'id',
             | preCombineField = '_ts'
             |)
             |location '$basePath'
        """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2'), (3, 'a3', 30, 1250, 'a3: desc3')")

        // Inserts only
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 12 as price, 1001 as _ts, 'a1: updated' as description
             |union select 3 as id, 'a3' as name, 25 as price, 1260 as _ts, 'a3: updated' as description
             |union select 4 as id, 'a4' as name, 60 as price, 1270 as _ts, 'a4: desc4' as description) s0
             |on t0.id = s0.id
             |when not matched then insert *
             |""".stripMargin)

        checkAnswer(s"select id, name, price, _ts, description from $tableName")(
          Seq(1, "a1", 10.0, 1000, "a1: desc1"),
          Seq(2, "a2", 20.0, 1200, "a2: desc2"),
          Seq(3, "a3", 30.0, 1250, "a3: desc3"),
          Seq(4, "a4", 60.0, 1270, "a4: desc4")
        )

        validateLogBlock(
          basePath,
          2,
          Seq(Seq("id", "name", "price", "_ts", "description"), Seq("id", "name", "price", "_ts", "description")),
          false)
      }
    }
  }

  test("Test MERGE INTO with partial updates containing non-existent columns on COW table") {
    testPartialUpdateWithNonExistentColumns("cow")
  }

  test("Test MERGE INTO with partial updates containing non-existent columns on MOR table") {
    testPartialUpdateWithNonExistentColumns("mor")
  }

  def testPartialUpdateWithNonExistentColumns(tableType: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true") {

        // Create a table with five data fields
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts int,
             | description string
             |) using hudi
             |tblproperties(
             | type ='$tableType',
             | primaryKey = 'id',
             | preCombineField = '_ts'
             |)
             |location '$basePath'
        """.stripMargin)
        val structFields = scala.collection.immutable.List(
          StructField("id", IntegerType, nullable = true),
          StructField("name", StringType, nullable = true),
          StructField("price", DoubleType, nullable = true),
          StructField("_ts", IntegerType, nullable = true),
          StructField("description", StringType, nullable = true))

        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2'), (3, 'a3', 30, 1250, 'a3: desc3')")
        validateTableSchema(tableName, structFields)

        // Partial updates using MERGE INTO statement with changed fields: "price", "_ts"
        // This is OK since the "UPDATE SET" clause does not contain the new column
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 12.0 as price, 1001 as ts, 'x' as new_col
             |union select 3 as id, 'a3' as name, 25.0 as price, 1260 as ts, 'y' as new_col) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price, _ts = s0.ts
             |""".stripMargin)

        validateTableSchema(tableName, structFields)
        checkAnswer(s"select id, name, price, _ts, description from $tableName")(
          Seq(1, "a1", 12.0, 1001, "a1: desc1"),
          Seq(2, "a2", 20.0, 1200, "a2: desc2"),
          Seq(3, "a3", 25.0, 1260, "a3: desc3")
        )

        // Partial updates using MERGE INTO statement with changed fields: "price", "_ts", "new_col"
        // This throws an error since the "UPDATE SET" clause contains the new column
        checkExceptionContain(
          s"""
             |merge into $tableName
             |using ( select 1 as id, 'a1' as name, 12.0 as price, 1001 as ts, 'x' as new_col
             |union select 3 as id, 'a3' as name, 25.0 as price, 1260 as ts, 'y' as new_col) s0
             |on $tableName.id = s0.id
             |when matched then update set price = s0.price, _ts = s0.ts, new_col = s0.new_col
             |""".stripMargin)(getExpectedUnresolvedColumnExceptionMessage("new_col", tableName))
      }
    }
  }

  def testPartialUpdate(tableType: String,
                        logDataBlockFormat: String): Unit = {
    testPartialUpdate(tableType, logDataBlockFormat, commitTimeOrdering = false)
  }

  def testPartialUpdate(tableType: String,
                        logDataBlockFormat: String,
                        commitTimeOrdering: Boolean): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> logDataBlockFormat,
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {
        val mergeMode = if (commitTimeOrdering) {
          RecordMergeMode.COMMIT_TIME_ORDERING.name()
        } else {
          RecordMergeMode.EVENT_TIME_ORDERING.name()
        }

        val preCombineString = if (commitTimeOrdering) {
          ""
        } else {
          "preCombineField = '_ts',"
        }
        // Create a table with five data fields
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts int,
             | description string
             |) using hudi
             |tblproperties(
             | type ='$tableType',
             | primaryKey = 'id',
             | $preCombineString
             | recordMergeMode = '$mergeMode'
             |)
             |location '$basePath'
        """.stripMargin)
        val structFields = scala.collection.immutable.List(
          StructField("id", IntegerType, nullable = true),
          StructField("name", StringType, nullable = true),
          StructField("price", DoubleType, nullable = true),
          StructField("_ts", IntegerType, nullable = true),
          StructField("description", StringType, nullable = true))
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2'), (3, 'a3', 30, 1250, 'a3: desc3')")
        validateTableSchema(tableName, structFields)

        // Partial updates using MERGE INTO statement with changed fields: "price" and "_ts"
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 1.0 as price, 100 as ts
             |union select 1 as id, 12.0 as price, 999 as ts
             |union select 3 as id, 25.0 as price, 1260 as ts) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price, _ts = s0.ts
             |""".stripMargin)
        validateTableSchema(tableName, structFields)
        if (commitTimeOrdering) {
          checkAnswer(s"select id, name, price, _ts, description from $tableName")(
            Seq(1, "a1", 12.0, 999, "a1: desc1"),
            Seq(2, "a2", 20.0, 1200, "a2: desc2"),
            Seq(3, "a3", 25.0, 1260, "a3: desc3")
          )
        } else {
          checkAnswer(s"select id, name, price, _ts, description from $tableName")(
            Seq(1, "a1", 10.0, 1000, "a1: desc1"),
            Seq(2, "a2", 20.0, 1200, "a2: desc2"),
            Seq(3, "a3", 25.0, 1260, "a3: desc3")
          )
        }
        if (tableType.equals("mor")) {
          validateLogBlock(basePath, 1, Seq(Seq("price", "_ts")), true)
        }

        // TODO: [HUDI-9375] get rid of this update and fix the rest of the test accordingly
        // showcase the difference between event time and commit time ordering
        // Partial updates using MERGE INTO statement with changed fields: "price" and "_ts"
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 12.0 as price, 1001 as ts) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price, _ts = s0.ts
             |""".stripMargin)

        checkAnswer(s"select id, name, price, _ts, description from $tableName")(
          Seq(1, "a1", 12.0, 1001, "a1: desc1"),
          Seq(2, "a2", 20.0, 1200, "a2: desc2"),
          Seq(3, "a3", 25.0, 1260, "a3: desc3")
        )
        if (tableType.equals("mor")) {
          validateLogBlock(basePath, 2, Seq(Seq("price", "_ts"), Seq("price", "_ts")), true)
        }

        // Partial updates using MERGE INTO statement with changed fields: "description" and "_ts"
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 'a1: updated desc1' as new_description, 1023 as _ts
             |union select 2 as id, 'a2' as name, 'a2: updated desc2' as new_description, 1270 as _ts) s0
             |on t0.id = s0.id
             |when matched then update set description = s0.new_description, _ts = s0._ts
             |""".stripMargin)

        validateTableSchema(tableName, structFields)
        checkAnswer(s"select id, name, price, _ts, description from $tableName")(
          Seq(1, "a1", 12.0, 1023, "a1: updated desc1"),
          Seq(2, "a2", 20.0, 1270, "a2: updated desc2"),
          Seq(3, "a3", 25.0, 1260, "a3: desc3")
        )

        if (tableType.equals("mor")) {
          validateLogBlock(basePath, 3, Seq(Seq("price", "_ts"), Seq("price", "_ts"), Seq("_ts", "description")), true)

          withSQLConf(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "4") {
            // Partial updates that trigger compaction
            spark.sql(
              s"""
                 |merge into $tableName t0
                 |using ( select 2 as id, '_a2' as name, 18.0 as _price, 1275 as _ts
                 |union select 3 as id, '_a3' as name, 28.0 as _price, 1280 as _ts) s0
                 |on t0.id = s0.id
                 |when matched then update set price = s0._price, _ts = s0._ts
                 |""".stripMargin)
            validateCompactionExecuted(basePath)
            validateTableSchema(tableName, structFields)
            checkAnswer(s"select id, name, price, _ts, description from $tableName")(
              Seq(1, "a1", 12.0, 1023, "a1: updated desc1"),
              Seq(2, "a2", 18.0, 1275, "a2: updated desc2"),
              Seq(3, "a3", 28.0, 1280, "a3: desc3")
            )
          }

          // trigger one more MIT and do inline clustering
          withSQLConf(
            HoodieClusteringConfig.INLINE_CLUSTERING.key() -> "true",
            HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key() -> "3"
          ) {
            spark.sql(
              s"""
                 |merge into $tableName t0
                 |using ( select 2 as id, '_a2' as name, 48.0 as _price, 1275 as _ts
                 |union select 3 as id, '_a3' as name, 58.0 as _price, 1280 as _ts) s0
                 |on t0.id = s0.id
                 |when matched then update set price = s0._price, _ts = s0._ts
                 |""".stripMargin)

            validateClusteringExecuted(basePath)
            validateTableSchema(tableName, structFields)
            checkAnswer(s"select id, name, price, _ts, description from $tableName")(
              Seq(1, "a1", 12.0, 1023, "a1: updated desc1"),
              Seq(2, "a2", 48.0, 1275, "a2: updated desc2"),
              Seq(3, "a3", 58.0, 1280, "a3: desc3")
            )
          }
        }
      }

      if (tableType.equals("cow")) {
        // No preCombine field
        val tableName2 = generateTableName
        val basePath2 = tmp.getCanonicalPath + "/" + tableName2
        spark.sql(
          s"""
             |create table $tableName2 (
             | id int,
             | name string,
             | price double
             |) using hudi
             |tblproperties(
             | type ='$tableType',
             | primaryKey = 'id'
             |)
             |location '$basePath2'
        """.stripMargin)
        spark.sql(s"insert into $tableName2 values(1, 'a1', 10)")

        withSQLConf(
          HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true",
          HoodieClusteringConfig.INLINE_CLUSTERING.key() -> "true",
          HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key() -> "1"
        ) {
          spark.sql(
            s"""
               |merge into $tableName2 t0
               |using ( select 1 as id, 'a2' as name, 12.0 as price) s0
               |on t0.id = s0.id
               |when matched then update set price = s0.price
               |""".stripMargin)

          validateClusteringExecuted(basePath2)
          checkAnswer(s"select id, name, price from $tableName2")(
            Seq(1, "a1", 12.0)
          )
        }
      }
    }
  }

  def testPartialUpdateWithInserts(tableType: String,
                                   logDataBlockFormat: String): Unit = {
    testPartialUpdateWithInserts(tableType, logDataBlockFormat, commitTimeOrdering = false)
  }

  def testPartialUpdateWithInserts(tableType: String,
                                   logDataBlockFormat: String,
                                   commitTimeOrdering: Boolean): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> logDataBlockFormat,
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {
        val mergeMode = if (commitTimeOrdering) {
          RecordMergeMode.COMMIT_TIME_ORDERING.name()
        } else {
          RecordMergeMode.EVENT_TIME_ORDERING.name()
        }

        val preCombineString = if (commitTimeOrdering) {
          ""
        } else {
          "preCombineField = '_ts',"
        }

        // Create a table with five data fields
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts int,
             | description string
             |) using hudi
             |tblproperties(
             | type ='$tableType',
             | primaryKey = 'id',
             | $preCombineString
             | recordMergeMode = '$mergeMode'
             |)
             |location '$basePath'
        """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2'), (3, 'a3', 30, 1250, 'a3: desc3')")

        // Partial updates with changed fields: "price" and "_ts" and inserts using MERGE INTO statement
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 12 as price, 1001 as _ts, '' as description
             |union select 3 as id, 'a3' as name, 25 as price, 1260 as _ts, '' as description
             |union select 4 as id, 'a4' as name, 70 as price, 1270 as _ts, 'a4: desc4' as description) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price, _ts = s0._ts
             |when not matched then insert *
             |""".stripMargin)

        checkAnswer(s"select id, name, price, _ts, description from $tableName")(
          Seq(1, "a1", 12.0, 1001, "a1: desc1"),
          Seq(2, "a2", 20.0, 1200, "a2: desc2"),
          Seq(3, "a3", 25.0, 1260, "a3: desc3"),
          Seq(4, "a4", 70.0, 1270, "a4: desc4")
        )

        if (tableType.equals("mor")) {
          validateLogBlock(basePath, 1, Seq(Seq("price", "_ts")), true)
        }
      }
    }
  }

  test("Test MergeInto Exception") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (
         | id int,
         | name string,
         | price double,
         | _ts long
         |) using hudi
         |tblproperties(
         | type = 'cow',
         | primaryKey = 'id',
         | preCombineField = '_ts'
         |)""".stripMargin)

    val failedToResolveErrorMessage = "No matching assignment found for target table ordering field `_ts"

    checkExceptionContain(
      s"""
         |merge into $tableName t0
         |using ( select 1 as id, 'a1' as name, 12 as price) s0
         |on t0.id = s0.id
         |when matched then update set price = s0.price
      """.stripMargin)(failedToResolveErrorMessage)

    val tableName2 = generateTableName
    spark.sql(
      s"""
         |create table $tableName2 (
         | id int,
         | name string,
         | price double,
         | _ts long
         |) using hudi
         |tblproperties(
         | type = 'mor',
         | primaryKey = 'id',
         | preCombineField = '_ts'
         |)""".stripMargin)
  }

  test("Partial updates for table version 6 and 8 handled gracefully in MIT") {
    Seq(HoodieTableVersion.SIX.versionCode(), HoodieTableVersion.EIGHT.versionCode()).foreach(
      tableVersion => withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             | CREATE TABLE $tableName (
             |   record_key STRING,
             |   name STRING,
             |   age INT,
             |   department STRING,
             |   salary DOUBLE,
             |   ts BIGINT
             | ) USING hudi
             | PARTITIONED BY (department)
             | LOCATION '${tmp.getCanonicalPath}'
             | TBLPROPERTIES (
             |   type = 'mor',
             |   hoodie.write.table.version = '$tableVersion',
             |   hoodie.index.type = 'GLOBAL_SIMPLE',
             |   hoodie.index.global.index.enable = 'true',
             |   hoodie.bloom.index.use.metadata = 'true',
             |   primaryKey = 'record_key',
             |   preCombineField = 'ts')""".stripMargin)

        spark.sql(
          s"""
             | INSERT INTO $tableName
             | SELECT * FROM (
             |   SELECT 'emp_001' as record_key, 'John Doe' as name, 30 as age,
             |          'Sales' as department, 80000.0 as salary, 1598886000 as ts
             |   UNION ALL
             |   SELECT 'emp_002', 'Jane Smith', 28, 'Sales', 75000.0, 1598886001
             |   UNION ALL
             |   SELECT 'emp_003', 'Bob Wilson', 35, 'Marketing', 85000.0, 1598886002
             |)""".stripMargin)

        spark.sql(
          s"""
             | UPDATE $tableName
             | SET
             |     ts = 1598000000
             | WHERE record_key = 'emp_001'
             |""".stripMargin)

        spark.sql(
          s"""
             | CREATE OR REPLACE TEMPORARY VIEW source_updates AS
             | SELECT * FROM (
             |   SELECT 'emp_001' as record_key, 'John Doe' as name, 30 as age,
             |          'Engineering' as department, CAST(95000.0 as DOUBLE) as salary, cast(1598886200 as BIGINT) as ts
             |   UNION ALL
             |   SELECT 'emp_004', 'Alice Brown', 29, 'Engineering', CAST(82000.0 as DOUBLE), cast(1598886201 as BIGINT)
             |)""".stripMargin)

        spark.sql(
          s"""
             | MERGE INTO $tableName t
             | USING source_updates s
             | ON t.record_key = s.record_key
             | WHEN MATCHED THEN
             |   UPDATE SET
             |     record_key = s.record_key,
             |     department = s.department,
             |     salary = s.salary,
             |     ts = s.ts
             | WHEN NOT MATCHED THEN
             |   INSERT *""".stripMargin)
      }
    )
  }

  test("Test MergeInto Partial Updates should fail with CUSTOM payload and merge mode") {
    withTempDir { tmp =>
      withSQLConf(
        "hoodie.index.type" -> "GLOBAL_SIMPLE",
        DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION.key() -> "upsert") {
        val tableName = generateTableName
        spark.sql(
          s"""
             | CREATE TABLE $tableName (
             |   record_key STRING,
             |   name STRING,
             |   age INT,
             |   department STRING,
             |   salary DOUBLE,
             |   ts BIGINT
             | ) USING hudi
             | PARTITIONED BY (department)
             | LOCATION '${tmp.getCanonicalPath}'
             | TBLPROPERTIES (
             |   type = 'mor',
             |   primaryKey = 'record_key',
             |   hoodie.write.record.merge.mode = 'CUSTOM',
             |   hoodie.datasource.write.payload.class = 'org.apache.hudi.common.testutils.reader.HoodieRecordTestPayload',
             |   preCombineField = 'ts')""".stripMargin)

        spark.sql(
          s"""
             | INSERT INTO $tableName
             | SELECT * FROM (
             |   SELECT 'emp_001' as record_key, 'John Doe' as name, 30 as age,
             |          'Sales' as department, 80000.0 as salary, 1598886000 as ts
             |   UNION ALL
             |   SELECT 'emp_002', 'Jane Smith', 28, 'Sales', 75000.0, 1598886001
             |   UNION ALL
             |   SELECT 'emp_003', 'Bob Wilson', 35, 'Marketing', 85000.0, 1598886002
             |)""".stripMargin)

        val failedToResolveError = "MERGE INTO field resolution error: No matching assignment found for target table"
        checkExceptionContain(
          s"""
             |merge into $tableName t0
             |using ( SELECT 'emp_001' as record_key, 'John Doe' as name, 35 as age, cast(1598886200 as BIGINT) as ts) s0
             |on t0.record_key = s0.record_key
             |when matched then update set age = s0.age
      """.stripMargin)(failedToResolveError)
      }
    }
  }

  def validateLogBlock(basePath: String,
                       expectedNumLogFile: Int,
                       changedFields: Seq[Seq[String]],
                       isPartial: Boolean): Unit = {
    val (metaClient, fsView) = getMetaClientAndFileSystemView(basePath)
    val fileSlice: Optional[FileSlice] = fsView.getAllFileSlices("")
      .filter((fileSlice: FileSlice) => {
        HoodieTestUtils.getLogFileListFromFileSlice(fileSlice).size() == expectedNumLogFile
      })
      .findFirst()
    assertTrue(fileSlice.isPresent)
    val logFilePathList: List[String] = HoodieTestUtils.getLogFileListFromFileSlice(fileSlice.get)
    Collections.sort(logFilePathList)

    val schema = new TableSchemaResolver(metaClient).getTableSchema
    for (i <- 0 until expectedNumLogFile) {
      val logReader = new HoodieLogFileReader(
        metaClient.getStorage, new HoodieLogFile(logFilePathList.get(i)),
        schema, 1024 * 1024, false, false,
        "id", null)
      assertTrue(logReader.hasNext)
      val logBlockHeader = logReader.next().getLogBlockHeader
      assertTrue(logBlockHeader.containsKey(HeaderMetadataType.SCHEMA))
      if (isPartial) {
        assertTrue(logBlockHeader.containsKey(HeaderMetadataType.IS_PARTIAL))
        assertTrue(logBlockHeader.get(HeaderMetadataType.IS_PARTIAL).toBoolean)
      } else {
        assertFalse(logBlockHeader.containsKey(HeaderMetadataType.IS_PARTIAL))
      }
      val actualSchema = HoodieSchema.parse(logBlockHeader.get(HeaderMetadataType.SCHEMA))
      val expectedSchema = HoodieSchemaUtils.addMetadataFields(HoodieSchemaUtils.generateProjectionSchema(
        schema, changedFields(i).asJava), false)
      assertEquals(expectedSchema, actualSchema)
    }
  }

  def validateClusteringExecuted(basePath: String): Unit = {
    val storageConf = HoodieTestUtils.getDefaultStorageConf
    val metaClient: HoodieTableMetaClient =
      HoodieTableMetaClient.builder.setConf(storageConf).setBasePath(basePath).build
    val lastCommit = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant().get()
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, lastCommit.getAction)
  }

  def validateCompactionExecuted(basePath: String): Unit = {
    val storageConf = HoodieTestUtils.getDefaultStorageConf
    val metaClient: HoodieTableMetaClient =
      HoodieTableMetaClient.builder.setConf(storageConf).setBasePath(basePath).build
    val lastCommit = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant().get()
    assertEquals(HoodieTimeline.COMMIT_ACTION, lastCommit.getAction)
    CompactionUtils.getCompactionPlan(metaClient, lastCommit.requestedTime())
  }
}
