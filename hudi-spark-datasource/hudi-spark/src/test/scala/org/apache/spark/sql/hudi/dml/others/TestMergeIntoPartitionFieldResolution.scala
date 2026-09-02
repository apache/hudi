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

import org.apache.hudi.common.table.timeline.TimelineUtils
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * Regression coverage for MERGE INTO partition-column resolution. Assertions check
 * `_hoodie_partition_path` as well as the row values: unchanged data is what a dropped record
 * produces, so asserting only that passes whether or not the merge did any work.
 */
class TestMergeIntoPartitionFieldResolution extends HoodieSparkSqlTestBase {

  private val expectedError = "Failed to resolve partition fields"

  private def createPartitionedTable(tableName: String, tableType: String, location: String): Unit =
    spark.sql(
      s"""
         |create table $tableName (
         |  id bigint,
         |  name string,
         |  amount double,
         |  ts bigint,
         |  dt string
         |) using hudi
         | partitioned by (dt)
         | tblproperties (
         |   type = '$tableType',
         |   primaryKey = 'id',
         |   preCombineField = 'ts'
         | )
         | location '$location'
       """.stripMargin)

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test MergeInto rejects an update whose source omits the partition column ($tableType)") {
      withTempDir { tmp =>
        val tableName = generateTableName
        createPartitionedTable(tableName, tableType, s"${tmp.getCanonicalPath}/$tableName")
        spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")

        // `dt` appears in neither the source output, the ON condition, nor the assignments.
        checkExceptionContain(
          s"""
             |merge into $tableName as t
             |using (
             |  select 1L as id, 15.0 as amount, 200L as ts
             |) as s
             |on t.id = s.id
             |when matched then update set t.amount = s.amount, t.ts = s.ts
       """.stripMargin)(expectedError)

        // Target untouched, and still a single row in its original partition.
        checkAnswer(s"select id, amount, ts, dt, _hoodie_partition_path from $tableName")(
          Seq(1L, 10.0, 1L, "2026-08-11", "dt=2026-08-11")
        )
      }
    }
  }

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test MergeInto with an INSERT clause cannot duplicate the key into the default partition ($tableType)") {
      withTempDir { tmp =>
        val tableName = generateTableName
        createPartitionedTable(tableName, tableType, s"${tmp.getCanonicalPath}/$tableName")
        spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")

        // With a NOT MATCHED clause a mis-partitioned record is inserted rather than dropped.
        checkExceptionContain(
          s"""
             |merge into $tableName as t
             |using (
             |  select 1L as id, 99.0 as amount, 9L as ts
             |) as s
             |on t.id = s.id
             |when matched then update set t.amount = s.amount, t.ts = s.ts
             |when not matched then insert (id, amount, ts) values (s.id, s.amount, s.ts)
       """.stripMargin)(expectedError)

        // Exactly one row, in the right partition - no duplicate key, no default-partition row.
        checkAnswer(s"select count(*) from $tableName")(Seq(1L))
        checkAnswer(s"select id, amount, dt, _hoodie_partition_path from $tableName")(
          Seq(1L, 10.0, "2026-08-11", "dt=2026-08-11")
        )
      }
    }
  }

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test MergeInto applies when the source projects the partition column ($tableType)") {
      withTempDir { tmp =>
        val tableName = generateTableName
        createPartitionedTable(tableName, tableType, s"${tmp.getCanonicalPath}/$tableName")
        spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")
        spark.sql(s"insert into $tableName values (2, 'b', 20.0, 1, '2026-08-11')")

        // The ON clause deliberately still matches on the record key alone - it is the source
        // projection, not the join condition, that governs partition-path resolution.
        spark.sql(
          s"""
             |merge into $tableName as t
             |using (
             |  select 1L as id, 15.0 as amount, 200L as ts, '2026-08-11' as dt
             |) as s
             |on t.id = s.id
             |when matched then update set t.amount = s.amount, t.ts = s.ts
       """.stripMargin)

        // The update landed on the existing record, in place, with no extra row created.
        checkAnswer(s"select id, name, amount, ts, dt, _hoodie_partition_path from $tableName order by id")(
          Seq(1L, "a", 15.0, 200L, "2026-08-11", "dt=2026-08-11"),
          Seq(2L, "b", 20.0, 1L, "2026-08-11", "dt=2026-08-11")
        )
      }
    }
  }

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test MergeInto rejects a partition value supplied only by a MERGE assignment ($tableType)") {
      withTempDir { tmp =>
        val tableName = generateTableName
        createPartitionedTable(tableName, tableType, s"${tmp.getCanonicalPath}/$tableName")
        spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")

        // An assignment states the record's NEW partition, not the one its existing version
        // occupies, and key generation runs over the source row before any assignment is
        // evaluated - so the assignment cannot supply the partition for tagging. Both shapes are
        // rejected: the value matching the current partition, and the value changing it.
        Seq("2026-08-11", "2026-08-12").foreach { assignedDt =>
          checkExceptionContain(
            s"""
               |merge into $tableName as t
               |using (
               |  select 1L as id, 15.0 as amount, 200L as ts, '$assignedDt' as src_dt
               |) as s
               |on t.id = s.id
               |when matched then update set t.amount = s.amount, t.ts = s.ts, t.dt = s.src_dt
       """.stripMargin)(expectedError)
        }

        // Accepting these would tag the record in the partition it moves TO, missing the existing
        // version.
        checkAnswer(s"select id, amount, ts, dt, _hoodie_partition_path from $tableName")(
          Seq(1L, 10.0, 1L, "2026-08-11", "dt=2026-08-11")
        )
      }
    }
  }

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test MergeInto on a non-partitioned table needs no partition column ($tableType)") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id bigint,
             |  name string,
             |  amount double,
             |  ts bigint
             |) using hudi
             | tblproperties (
             |   type = '$tableType',
             |   primaryKey = 'id',
             |   preCombineField = 'ts'
             | )
             | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)

        spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1)")

        // No partition columns exist, so the new resolution must not engage at all.
        spark.sql(
          s"""
             |merge into $tableName as t
             |using (
             |  select 1L as id, 15.0 as amount, 200L as ts
             |) as s
             |on t.id = s.id
             |when matched then update set t.amount = s.amount, t.ts = s.ts
       """.stripMargin)

        checkAnswer(s"select id, name, amount, ts from $tableName")(
          Seq(1L, "a", 15.0, 200L)
        )
      }
    }
  }

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test MergeInto rejects a delete whose source omits the partition column ($tableType)") {
      withTempDir { tmp =>
        val tableName = generateTableName
        createPartitionedTable(tableName, tableType, s"${tmp.getCanonicalPath}/$tableName")
        spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")

        // DeleteAction carries no assignments, so this reaches the resolution by its own path.
        checkExceptionContain(
          s"""
             |merge into $tableName as t
             |using (
             |  select 1L as id, 200L as ts
             |) as s
             |on t.id = s.id
             |when matched then delete
       """.stripMargin)(expectedError)

        checkAnswer(s"select id, amount, dt, _hoodie_partition_path from $tableName")(
          Seq(1L, 10.0, "2026-08-11", "dt=2026-08-11")
        )
      }
    }
  }

  // CoW only: on MOR the same statement hits a separate pre-existing writer defect first.
  // Each entry is (index type, the confs that put it in the re-keying configuration).
  private val rekeyingGlobalIndexes: Seq[(String, Map[String, String])] = Seq(
    ("GLOBAL_BLOOM", Map("hoodie.bloom.index.update.partition.path" -> "false")),
    ("GLOBAL_SIMPLE", Map("hoodie.simple.index.update.partition.path" -> "false")),
    ("RECORD_INDEX", Map(
      "hoodie.record.index.update.partition.path" -> "false",
      "hoodie.metadata.enable" -> "true",
      "hoodie.metadata.record.index.enable" -> "true")),
    ("GLOBAL_RECORD_LEVEL_INDEX", Map(
      "hoodie.record.index.update.partition.path" -> "false",
      "hoodie.metadata.enable" -> "true",
      "hoodie.metadata.record.index.enable" -> "true")))

  rekeyingGlobalIndexes.foreach { case (indexType, extraConfs) =>
    test(s"Test MergeInto allows a source without the partition column on a re-keying global index ($indexType, cow)") {
      withTempDir { tmp =>
        // A global index looks the record up by key across partitions, and with
        // update.partition.path disabled HoodieIndexUtils re-keys the incoming record onto the
        // partition its existing version occupies - so the partition value the source carries is
        // irrelevant and the merge is correct today. Rejecting it would be a regression.
        val confs = (extraConfs + ("hoodie.index.type" -> indexType)).toSeq
        withSQLConf(confs: _*) {
          val tableName = generateTableName
          createPartitionedTable(tableName, "cow", s"${tmp.getCanonicalPath}/$tableName")
          spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")

          spark.sql(
            s"""
               |merge into $tableName as t
               |using (
               |  select 1L as id, 15.0 as amount, 200L as ts
               |) as s
               |on t.id = s.id
               |when matched then update set t.amount = s.amount, t.ts = s.ts
       """.stripMargin)

          checkAnswer(s"select id, amount, ts, dt, _hoodie_partition_path from $tableName")(
            Seq(1L, 15.0, 200L, "2026-08-11", "dt=2026-08-11")
          )
        }
      }
    }
  }

  private val rekeyingConfsByType: Map[String, Map[String, String]] = rekeyingGlobalIndexes.toMap

  // The re-keying exemption covers MATCHED records only. HoodieIndexUtils#tagGlobalLocationBackToRecords
  // re-keys a record onto its existing partition, but a record the global lookup does not find is
  // returned untouched and keeps the partition it arrived with - so a WHEN NOT MATCHED ... INSERT
  // row still lands in the default partition, duplicating nothing but mis-placing the new key.
  // Whether a row matches is data-dependent, so the presence of the insert clause is what decides.
  Seq("GLOBAL_BLOOM", "GLOBAL_RECORD_LEVEL_INDEX").foreach { indexType =>
    test(s"Test MergeInto requires the partition column when a global-index merge can insert ($indexType, cow)") {
      withTempDir { tmp =>
        val confs = (rekeyingConfsByType(indexType) + ("hoodie.index.type" -> indexType)).toSeq
        withSQLConf(confs: _*) {
          val tableName = generateTableName
          createPartitionedTable(tableName, "cow", s"${tmp.getCanonicalPath}/$tableName")
          spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")

          checkExceptionContain(
            s"""
               |merge into $tableName as t
               |using (
               |  select 2L as id, 77.0 as amount, 7L as ts
               |) as s
               |on t.id = s.id
               |when matched then update set t.amount = s.amount, t.ts = s.ts
               |when not matched then insert (id, amount, ts) values (s.id, s.amount, s.ts)
       """.stripMargin)(expectedError)

          // A not-matched row admitted here would be written to dt=__HIVE_DEFAULT_PARTITION__.
          checkAnswer(s"select count(*) from $tableName")(Seq(1L))
          checkAnswer(s"select id, amount, dt, _hoodie_partition_path from $tableName")(
            Seq(1L, 10.0, "2026-08-11", "dt=2026-08-11")
          )
        }
      }
    }
  }

  // ... and the same shape is accepted once the source carries the column, so the narrowing above
  // rejects only what it must.
  test("Test MergeInto allows a global-index merge that can insert when the source carries the partition column (GLOBAL_BLOOM, cow)") {
    withTempDir { tmp =>
      withSQLConf(
        "hoodie.index.type" -> "GLOBAL_BLOOM",
        "hoodie.bloom.index.update.partition.path" -> "false") {
        val tableName = generateTableName
        createPartitionedTable(tableName, "cow", s"${tmp.getCanonicalPath}/$tableName")
        spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")

        spark.sql(
          s"""
             |merge into $tableName as t
             |using (
             |  select 2L as id, 'b' as name, 77.0 as amount, 7L as ts, '2026-08-12' as dt
             |) as s
             |on t.id = s.id
             |when matched then update set t.amount = s.amount, t.ts = s.ts
             |when not matched then insert (id, name, amount, ts, dt)
             |  values (s.id, s.name, s.amount, s.ts, s.dt)
       """.stripMargin)

        checkAnswer(s"select id, name, dt, _hoodie_partition_path from $tableName order by id")(
          Seq(1L, "a", "2026-08-11", "dt=2026-08-11"),
          Seq(2L, "b", "2026-08-12", "dt=2026-08-12")
        )
      }
    }
  }

  // The other edge of the same gate: with update.partition.path ENABLED the incoming partition
  // value does decide placement, so the guard must still engage rather than exempt the table.
  Seq("RECORD_INDEX", "GLOBAL_RECORD_LEVEL_INDEX").foreach { indexType =>
    test(s"Test MergeInto still requires the partition column when the global index updates it ($indexType, cow)") {
      withTempDir { tmp =>
        withSQLConf(
          "hoodie.index.type" -> indexType,
          "hoodie.record.index.update.partition.path" -> "true",
          "hoodie.metadata.enable" -> "true",
          "hoodie.metadata.record.index.enable" -> "true") {
          val tableName = generateTableName
          createPartitionedTable(tableName, "cow", s"${tmp.getCanonicalPath}/$tableName")
          spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")

          checkExceptionContain(
            s"""
               |merge into $tableName as t
               |using (
               |  select 1L as id, 15.0 as amount, 200L as ts
               |) as s
               |on t.id = s.id
               |when matched then update set t.amount = s.amount, t.ts = s.ts
       """.stripMargin)(expectedError)

          checkAnswer(s"select id, amount, ts, dt, _hoodie_partition_path from $tableName")(
            Seq(1L, 10.0, 1L, "2026-08-11", "dt=2026-08-11")
          )
        }
      }
    }
  }

  // The accepting half of the update.partition.path=true configuration: the source DOES carry the
  // partition column and names a different value, so the guard resolves it and the global index is
  // free to move the record. Worth pinning separately - the rejecting test above only shows the
  // guard engages, not that a partition-changing update still works once past it.
  test("Test MergeInto moves the record when the source carries a new partition value " +
    "(RECORD_INDEX, update.partition.path=true, cow)") {
    withTempDir { tmp =>
      withSQLConf(
        "hoodie.index.type" -> "RECORD_INDEX",
        "hoodie.record.index.update.partition.path" -> "true",
        "hoodie.metadata.enable" -> "true",
        "hoodie.metadata.record.index.enable" -> "true") {
        val tableName = generateTableName
        createPartitionedTable(tableName, "cow", s"${tmp.getCanonicalPath}/$tableName")
        spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")

        spark.sql(
          s"""
             |merge into $tableName as t
             |using (
             |  select 1L as id, 'a' as name, 15.0 as amount, 200L as ts, '2026-08-12' as dt
             |) as s
             |on t.id = s.id
             |when matched then update set
             |  t.name = s.name, t.amount = s.amount, t.ts = s.ts, t.dt = s.dt
       """.stripMargin)

        // Exactly one row, relocated - not one copy per partition, which is what a failure to
        // re-key would leave behind.
        checkAnswer(s"select id, amount, ts, dt, _hoodie_partition_path from $tableName")(
          Seq(1L, 15.0, 200L, "2026-08-12", "dt=2026-08-12")
        )
      }
    }
  }

  private def completedCommitCount(basePath: String): Int =
    createMetaClient(spark, basePath)
      .getActiveTimeline.getAllCommitsTimeline.filterCompletedInstants().countInstants()

  private def lastCommitRecordsWritten(basePath: String): Long = {
    val timeline = createMetaClient(spark, basePath)
      .getActiveTimeline.getAllCommitsTimeline.filterCompletedInstants()
    val lastInstant = timeline.lastInstant()
    assert(lastInstant.isPresent, "expected at least one completed commit on the timeline")
    TimelineUtils.getCommitMetadata(lastInstant.get(), timeline).fetchTotalRecordsWritten()
  }

  /**
   * Asserted against the commit timeline, not the rows: a commit with 0 records is
   * indistinguishable from a merge that legitimately changed nothing.
   */
  test("Test MergeInto e2e: a successful merge writes records, a rejected merge writes no commit") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      createPartitionedTable(tableName, "cow", basePath)
      spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")

      val commitsAfterInsert = completedCommitCount(basePath)

      // The duplicating shape must be rejected, and must leave the timeline untouched.
      checkExceptionContain(
        s"""
           |merge into $tableName as t
           |using (
           |  select 1L as id, 99.0 as amount, 9L as ts
           |) as s
           |on t.id = s.id
           |when matched then update set t.amount = s.amount, t.ts = s.ts
           |when not matched then insert (id, amount, ts) values (s.id, s.amount, s.ts)
       """.stripMargin)(expectedError)

      assert(completedCommitCount(basePath) == commitsAfterInsert,
        "a rejected MERGE INTO must not leave a commit on the timeline")

      // The equivalent statement with the partition column supplied must write a NON-EMPTY commit.
      spark.sql(
        s"""
           |merge into $tableName as t
           |using (
           |  select 1L as id, 99.0 as amount, 9L as ts, '2026-08-11' as dt
           |) as s
           |on t.id = s.id
           |when matched then update set t.amount = s.amount, t.ts = s.ts
       """.stripMargin)

      assert(completedCommitCount(basePath) == commitsAfterInsert + 1,
        "a successful MERGE INTO must write exactly one new commit")

      val recordsWritten = lastCommitRecordsWritten(basePath)
      assert(recordsWritten > 0,
        s"MERGE INTO reported success but wrote $recordsWritten records - the empty-commit regression")

      checkAnswer(s"select id, amount, ts, dt, _hoodie_partition_path from $tableName")(
        Seq(1L, 99.0, 9L, "2026-08-11", "dt=2026-08-11")
      )
    }
  }

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test MergeInto resolves the partition column from the ON condition ($tableType)") {
      withTempDir { tmp =>
        val tableName = generateTableName
        createPartitionedTable(tableName, tableType, s"${tmp.getCanonicalPath}/$tableName")
        spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")

        // The partition column is supplied only by the ON condition, under a different source name.
        // recordKeyAttributeToConditionExpression already back-fills this shape, so the partition
        // resolution must defer to it rather than rejecting the statement.
        spark.sql(
          s"""
             |merge into $tableName as t
             |using (
             |  select 1L as id, 15.0 as amount, 200L as ts, '2026-08-11' as event_dt
             |) as s
             |on t.id = s.id and t.dt = s.event_dt
             |when matched then update set t.amount = s.amount, t.ts = s.ts
       """.stripMargin)

        checkAnswer(s"select id, amount, ts, dt, _hoodie_partition_path from $tableName")(
          Seq(1L, 15.0, 200L, "2026-08-11", "dt=2026-08-11")
        )
      }
    }
  }

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test MergeInto resolves the partition column from the ON condition when an assignment also sets it ($tableType)") {
      withTempDir { tmp =>
        val tableName = generateTableName
        createPartitionedTable(tableName, tableType, s"${tmp.getCanonicalPath}/$tableName")
        spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")

        // A regression guard for the ON-condition path when an assignment names the same column.
        //
        // NOTE: this does NOT reach the dedupe - only the ON condition contributes an association
        //       for `dt`, so there is one entry. The case that does reach it is below.
        spark.sql(
          s"""
             |merge into $tableName as t
             |using (
             |  select 1L as id, 15.0 as amount, 200L as ts, '2026-08-11' as event_dt
             |) as s
             |on t.id = s.id and t.dt = s.event_dt
             |when matched then update set t.amount = s.amount, t.ts = s.ts, t.dt = s.event_dt
       """.stripMargin)

        checkAnswer(s"select id, amount, ts, dt, _hoodie_partition_path from $tableName")(
          Seq(1L, 15.0, 200L, "2026-08-11", "dt=2026-08-11")
        )
      }
    }
  }

  // The one overlap that reaches the dedupe: `id` is both the record key and the ordering field, so
  // the ON condition and the assignment each contribute (id -> s.sid) and each emits its own Alias.
  // The outcome is not asserted beyond identity - equal-ordering resolution differs across lines.
  Seq("cow", "mor").foreach { tableType =>
    test(s"Test MergeInto projects a record key that is also an ordering field only once ($tableType)") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id bigint,
             |  name string,
             |  amount double
             |) using hudi
             | tblproperties (
             |   type = '$tableType',
             |   primaryKey = 'id',
             |   preCombineField = 'id'
             | )
             | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)

        spark.sql(s"insert into $tableName values (1, 'a', 10.0)")

        spark.sql(
          s"""
             |merge into $tableName as t
             |using (
             |  select 1L as sid, 'b' as name, 20.0 as amount
             |) as s
             |on t.id = s.sid
             |when matched then update set t.id = s.sid, t.name = s.name, t.amount = s.amount
       """.stripMargin)

        checkAnswer(s"select count(*) from $tableName")(Seq(1L))
        checkAnswer(s"select id from $tableName")(Seq(1L))
        // Which of the two wins is the tie-break difference described above, but the surviving
        // value must be one of them; anything else is corruption rather than a resolution choice.
        val amount = spark.sql(s"select amount from $tableName").collect().head.getDouble(0)
        assert(amount == 10.0 || amount == 20.0, s"unexpected amount after merge: $amount")
      }
    }
  }

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test MergeInto on a primary-keyless table needs no partition column in the source ($tableType)") {
      withTempDir { tmp =>
        val tableName = generateTableName
        // No primaryKey: getProcessedInputDf left-outer-joins the target and projects its meta
        // columns, and MergeIntoKeyGenerator reads `_hoodie_partition_path` from that meta, so a
        // matched row is already placed correctly without the source carrying `dt`. The partition
        // resolution must not engage for this table shape.
        spark.sql(
          s"""
             |create table $tableName (
             |  id bigint,
             |  name string,
             |  amount double,
             |  ts bigint,
             |  dt string
             |) using hudi
             | partitioned by (dt)
             | tblproperties (
             |   type = '$tableType',
             |   preCombineField = 'ts'
             | )
             | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)

        spark.sql(s"insert into $tableName values (1, 'a', 10.0, 1, '2026-08-11')")

        spark.sql(
          s"""
             |merge into $tableName as t
             |using (
             |  select 1L as id, 15.0 as amount, 200L as ts
             |) as s
             |on t.id = s.id
             |when matched then update set t.amount = s.amount, t.ts = s.ts
       """.stripMargin)

        checkAnswer(s"select id, amount, ts, dt, _hoodie_partition_path from $tableName")(
          Seq(1L, 15.0, 200L, "2026-08-11", "dt=2026-08-11")
        )
      }
    }
  }
}
