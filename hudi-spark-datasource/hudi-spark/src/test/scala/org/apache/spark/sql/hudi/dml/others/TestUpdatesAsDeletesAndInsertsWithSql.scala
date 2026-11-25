/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.dml.others

import org.apache.hudi.DataSourceWriteOptions.{ENABLE_MERGE_INTO_PARTIAL_UPDATES, MERGE_INTO_CARDINALITY_CHECK, OPERATION, SPARK_SQL_INSERT_INTO_OPERATION, SPARK_SQL_OPTIMIZED_WRITES, UPSERT_OPERATION_OPT_VAL}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.table.log.HoodieLogFormat
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.core.io.storage.HoodieIOFactory
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.action.commit.BaseWriteHelper

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.getMetaClientAndFileSystemView
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Spark SQL DML on merge-on-read tables with [[HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS]]
 * enabled: every update and delete must reach the log as a positional delete.
 */
class TestUpdatesAsDeletesAndInsertsWithSql extends HoodieSparkSqlTestBase {

  Seq("SIMPLE", "GLOBAL_SIMPLE", "RECORD_INDEX").foreach { indexType =>
    test(s"Test MERGE INTO writing updates as deletes and inserts with $indexType index") {
      withTable(indexType) { (tableName, tablePath) =>
        // Every source row matches and there is no not-matched clause
        spark.sql(
          s"""
             |merge into $tableName t
             |using (select 1 as id, 'a1m' as name, 11.0 as price, 'p1' as dt
             |  union all select 2, 'a2m', 21.0, 'p1' union all select 3, 'a3m', 31.0, 'p1') s
             |on t.id = s.id
             |when matched then update set *
           """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(
          Seq(1, "a1m", 11.0, "p1"), Seq(2, "a2m", 21.0, "p1"), Seq(3, "a3m", 31.0, "p1"),
          Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"))
        assertEquals(Seq("1", "2", "3"), positionalDeletes(tablePath))

        spark.sql(
          s"""
             |merge into $tableName t
             |using (select 1 as id, 'x' as name, 0.0 as price, 'p1' as dt, 'del' as op
             |  union all select 2, 'a2u', 22.0, 'p1', 'upd'
             |  union all select 4, 'a4u', 44.0, 'p2', 'none'
             |  union all select 6, 'a6', 60.0, 'p2', 'ins'
             |  union all select 7, 'a7', 70.0, 'p2', 'none') s
             |on t.id = s.id
             |when matched and s.op = 'del' then delete
             |when matched and s.op = 'upd' then update set id = s.id, name = s.name, price = s.price, dt = s.dt
             |when not matched and s.op = 'ins' then insert (id, name, price, dt) values (s.id, s.name, s.price, s.dt)
           """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(
          Seq(2, "a2u", 22.0, "p1"), Seq(3, "a3m", 31.0, "p1"), Seq(4, "a4", 40.0, "p2"),
          Seq(5, "a5", 50.0, "p2"), Seq(6, "a6", 60.0, "p2"))
        assertEquals(Seq("1", "1", "2", "2", "3"), positionalDeletes(tablePath))
      }
    }
  }

  Seq("GLOBAL_SIMPLE", "RECORD_INDEX").foreach { indexType =>
    test(s"Test MERGE INTO and UPDATE moving records across partitions writing updates as deletes and inserts with $indexType index") {
      withSQLConf(HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE.key -> "true") {
        withTable(indexType) { (tableName, tablePath) =>
          // id 1 takes the partition the source names, id 2 one other than the source value it is looked up under
          spark.sql(
            s"""
               |merge into $tableName t
               |using (select 1 as id, 'a1m' as name, 11.0 as price, 'p2' as dt, 'p2' as new_dt
               |  union all select 2, 'a2m', 21.0, 'p1', 'p2') s
               |on t.id = s.id
               |when matched and s.id = 1 then update set id = s.id, name = s.name, price = s.price, dt = s.dt
               |when matched and s.id = 2 then update set id = s.id, name = s.name, price = s.price, dt = s.new_dt
             """.stripMargin)
          spark.sql(s"update $tableName set dt = 'p1' where id = 4")
          checkAnswer(s"select id, name, price, dt, _hoodie_partition_path from $tableName order by id")(
            Seq(1, "a1m", 11.0, "p2", "dt=p2"), Seq(2, "a2m", 21.0, "p2", "dt=p2"), Seq(3, "a3", 30.0, "p1", "dt=p1"),
            Seq(4, "a4", 40.0, "p1", "dt=p1"), Seq(5, "a5", 50.0, "p2", "dt=p2"))
          checkAnswer(s"select id from $tableName where dt = 'p1' order by id")(Seq(3), Seq(4))
          assertEquals(Seq("1", "2", "4"), positionalDeletes(tablePath))
        }
      }
    }
  }

  test("Test MERGE INTO and UPDATE moving records across partitions writing updates as deletes and inserts "
    + "on a table without a primary key") {
    withTable("SIMPLE", partitioned = true, keyed = false) { (tableName, tablePath) =>
      // id 1 takes the partition the source names, id 2 one its clause assigns, and id 3 stays
      spark.sql(
        s"""
           |merge into $tableName t
           |using (select 1 as id, 'a1m' as name, 11.0 as price, 'p2' as dt
           |  union all select 2, 'a2m', 21.0, 'p1' union all select 3, 'a3m', 31.0, 'p1') s
           |on t.id = s.id
           |when matched and s.id = 2 then update set name = s.name, price = s.price, dt = 'p2'
           |when matched then update set *
         """.stripMargin)
      spark.sql(s"update $tableName set dt = 'p1' where id = 4")
      checkAnswer(s"select id, name, price, dt, _hoodie_partition_path from $tableName order by id")(
        Seq(1, "a1m", 11.0, "p2", "dt=p2"), Seq(2, "a2m", 21.0, "p2", "dt=p2"), Seq(3, "a3m", 31.0, "p1", "dt=p1"),
        Seq(4, "a4", 40.0, "p1", "dt=p1"), Seq(5, "a5", 50.0, "p2", "dt=p2"))
      checkAnswer(s"select id from $tableName where dt = 'p1' order by id")(Seq(3), Seq(4))
      assertEquals(Seq(1, 2, 3, 4), positionallyDeletedIds(tablePath))
    }
  }

  test("Test MERGE INTO without an update clause writing updates as deletes and inserts") {
    withTable { (tableName, tablePath) =>
      // Delete only: id 2 fails the condition and id 9 has no match
      spark.sql(
        s"""
           |merge into $tableName t
           |using (select 1 as id, 'p1' as dt, 'del' as op union all select 2, 'p1', 'none' union all select 9, 'p1', 'del') s
           |on t.id = s.id
           |when matched and s.op = 'del' then delete
         """.stripMargin)
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"))
      assertEquals(Seq("1"), positionalDeletes(tablePath))

      // Insert only: the matched id 2 is neither inserted again nor deleted
      spark.sql(
        s"""
           |merge into $tableName t
           |using (select 2 as id, 'a2m' as name, 21.0 as price, 'p1' as dt union all select 6, 'a6', 60.0, 'p2') s
           |on t.id = s.id
           |when not matched then insert *
         """.stripMargin)
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"),
        Seq(6, "a6", 60.0, "p2"))
      assertEquals(Seq("1"), positionalDeletes(tablePath))

      // Delete and insert
      spark.sql(
        s"""
           |merge into $tableName t
           |using (select 4 as id, 'a4m' as name, 41.0 as price, 'p2' as dt union all select 7, 'a7', 70.0, 'p1') s
           |on t.id = s.id
           |when matched then delete
           |when not matched then insert *
         """.stripMargin)
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(5, "a5", 50.0, "p2"), Seq(6, "a6", 60.0, "p2"),
        Seq(7, "a7", 70.0, "p1"))
      assertEquals(Seq("1", "4"), positionalDeletes(tablePath))
    }
  }

  /**
   * A MERGE INTO whose matched clauses need the target record, which the split of an update reads: the statement
   * with its SQL confs, then the rows and the keys of the positional deletes it leaves.
   */
  private case class TargetRecordCase(name: String,
                                      statement: String => String,
                                      rows: Seq[Seq[Any]],
                                      deletedKeys: Seq[String],
                                      conf: Seq[(String, String)] = Seq.empty)

  private val targetConditionCases = Seq(
    // id 2 fails both conditions and id 6 has no match
    TargetRecordCase("update and delete conditions on target fields with an insert",
      tableName =>
        s"""
           |merge into $tableName t
           |using (select 1 as id, 'a1m' as name, 11.0 as price, 'p1' as dt union all select 2, 'a2m', 21.0, 'p1'
           |  union all select 3, 'a3m', 31.0, 'p1' union all select 5, 'a5m', 51.0, 'p2' union all select 6, 'a6', 60.0, 'p2') s
           |on t.id = s.id
           |when matched and t.price < 20 then update set *
           |when matched and t.price >= 30 then delete
           |when not matched then insert *
         """.stripMargin,
      Seq(Seq(1, "a1m", 11.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(6, "a6", 60.0, "p2")),
      Seq("1", "3", "5")),
    // id 2 fails the condition and id 9 has no match
    TargetRecordCase("a delete condition on a target field without an update clause",
      tableName =>
        s"""
           |merge into $tableName t
           |using (select 2 as id, 'p1' as dt union all select 3, 'p1' union all select 9, 'p1') s
           |on t.id = s.id
           |when matched and t.price > 25 then delete
         """.stripMargin,
      Seq(Seq(1, "a1", 10.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2")),
      Seq("3")))

  private val targetAssignmentCases = Seq(
    TargetRecordCase("an update that leaves out columns",
      tableName =>
        s"""
           |merge into $tableName t
           |using (select 1 as id, 12.0 as price, 'p1' as dt union all select 4, 42.0, 'p2') s
           |on t.id = s.id
           |when matched then update set price = s.price
         """.stripMargin,
      Seq(Seq(1, "a1", 12.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 42.0, "p2"),
        Seq(5, "a5", 50.0, "p2")),
      Seq("1", "4")),
    TargetRecordCase("an update that leaves out columns with partial updates off",
      tableName =>
        s"""
           |merge into $tableName t
           |using (select 2 as id, 'a2m' as name, 'p1' as dt union all select 5, 'a5m', 'p2') s
           |on t.id = s.id
           |when matched then update set id = s.id, name = s.name
         """.stripMargin,
      Seq(Seq(1, "a1", 10.0, "p1"), Seq(2, "a2m", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"),
        Seq(5, "a5m", 50.0, "p2")),
      Seq("2", "5"),
      Seq(ENABLE_MERGE_INTO_PARTIAL_UPDATES.key -> "false")),
    TargetRecordCase("an update that assigns from target fields",
      tableName =>
        s"""
           |merge into $tableName t
           |using (select 3 as id, 1.5 as price, 'p1' as dt) s
           |on t.id = s.id
           |when matched then update set price = t.price + s.price, name = concat(t.name, '-', t.dt)
         """.stripMargin,
      Seq(Seq(1, "a1", 10.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3-p1", 31.5, "p1"), Seq(4, "a4", 40.0, "p2"),
        Seq(5, "a5", 50.0, "p2")),
      Seq("3")))

  // A global index can merge a source record with the record it matches while it tags, which leaves a delete to the split
  Seq(("SIMPLE", true, targetConditionCases ++ targetAssignmentCases),
    ("SIMPLE", false, targetConditionCases ++ targetAssignmentCases.take(1)),
    ("GLOBAL_SIMPLE", true, targetConditionCases.take(1)),
    ("RECORD_INDEX", true, targetConditionCases.take(1))).foreach { case (indexType, partitioned, cases) =>
    cases.foreach { mergeCase =>
      test(s"Test MERGE INTO with ${mergeCase.name} writing updates as deletes and inserts with $indexType index"
        + s" on a ${if (partitioned) "partitioned" else "non-partitioned"} table") {
        withTable(indexType, partitioned) { (tableName, tablePath) =>
          withSQLConf(mergeCase.conf: _*) {
            spark.sql(mergeCase.statement(tableName))
          }
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(mergeCase.rows: _*)
          assertEquals(mergeCase.deletedKeys, positionalDeletes(tablePath, partitioned))

          withSQLConf(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1") {
            spark.sql(s"call run_compaction(op => 'run', table => '$tableName')")
          }
          assertEquals(1, HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf, tablePath)
            .getActiveTimeline.getCommitTimeline.filterCompletedInstants.countInstants)
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(mergeCase.rows: _*)
        }
      }
    }
  }

  test("Test MERGE INTO of a deleted record writing updates as deletes and inserts") {
    withTable { (tableName, tablePath) =>
      spark.sql(s"delete from $tableName where id = 1")
      // The index still locates the deleted copy of id 1 in its base file, which is no match
      spark.sql(
        s"""
           |merge into $tableName t
           |using (select 1 as id, 'a1m' as name, 11.0 as price, 'p1' as dt) s
           |on t.id = s.id
           |when matched then update set *
         """.stripMargin)
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"))
      assertEquals(Seq("1"), positionalDeletes(tablePath))

      spark.sql(
        s"""
           |merge into $tableName t
           |using (select 1 as id, 'a1n' as name, 12.0 as price, 'p1' as dt) s
           |on t.id = s.id
           |when matched then update set id = s.id, name = 'matched', price = s.price, dt = s.dt
           |when not matched then insert *
         """.stripMargin)
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1n", 12.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"),
        Seq(5, "a5", 50.0, "p2"))
      assertEquals(Seq("1"), positionalDeletes(tablePath))
    }
  }

  Seq(None, Some("false")).foreach { optimizedWrites =>
    test(s"Test UPDATE and DELETE writing updates as deletes and inserts with optimized writes ${optimizedWrites.getOrElse("default")}") {
      withSQLConf(optimizedWrites.map(SPARK_SQL_OPTIMIZED_WRITES.key() -> _).toSeq: _*) {
        withTable { (tableName, tablePath) =>
          spark.sql(s"update $tableName set price = price * 2 where id in (1, 4)")
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(1, "a1", 20.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"),
            Seq(4, "a4", 80.0, "p2"), Seq(5, "a5", 50.0, "p2"))
          assertEquals(Seq("1", "4"), positionalDeletes(tablePath))

          // The index also locates the tombstoned copy of key 1 in its old file group and deletes it again
          spark.sql(s"delete from $tableName where id in (1, 3)")
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(2, "a2", 20.0, "p1"), Seq(4, "a4", 80.0, "p2"), Seq(5, "a5", 50.0, "p2"))
          assertEquals(Seq("1", "1", "1", "3", "4"), positionalDeletes(tablePath))
        }
      }
    }
  }

  test("Test UPDATE, MERGE INTO and DELETE writing updates as deletes and inserts with the record index across a compaction") {
    val rows = Seq(Seq(2, "a2m", 21.0, "p1"), Seq(4, "a4", 80.0, "p2"), Seq(5, "a5", 50.0, "p2"))
    withTable("RECORD_INDEX") { (tableName, tablePath) =>
      // Inline compaction stays off until the explicit one, so the log files keep every positional delete
      withSQLConf(HoodieCompactionConfig.INLINE_COMPACT.key -> "false") {
        spark.sql(s"update $tableName set price = price * 2 where id in (1, 4)")
        assertEquals(Seq("1", "4"), positionalDeletes(tablePath))

        // The record index locates only the current copy of id 1, so its tombstoned copy is not deleted again
        spark.sql(s"update $tableName set name = 'a1u' where id = 1")
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(
          Seq(1, "a1u", 20.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"),
          Seq(4, "a4", 80.0, "p2"), Seq(5, "a5", 50.0, "p2"))
        assertEquals(Seq("1", "1", "4"), positionalDeletes(tablePath))

        spark.sql(
          s"""
             |merge into $tableName t
             |using (select 1 as id, 'x' as name, 0.0 as price, 'p1' as dt, 'none' as op
             |  union all select 2, 'a2m', 21.0, 'p1', 'upd'
             |  union all select 3, 'x', 0.0, 'p1', 'del'
             |  union all select 6, 'a6', 60.0, 'p2', 'ins') s
             |on t.id = s.id
             |when matched and s.op = 'del' then delete
             |when matched and s.op = 'upd' then update set id = s.id, name = s.name, price = s.price, dt = s.dt
             |when not matched and s.op = 'ins' then insert (id, name, price, dt) values (s.id, s.name, s.price, s.dt)
           """.stripMargin)
        spark.sql(s"delete from $tableName where id in (1, 6)")
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(rows: _*)
        assertEquals(Seq("1", "1", "1", "2", "3", "4", "6"), positionalDeletes(tablePath))
      }

      withSQLConf(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1") {
        spark.sql(s"call run_compaction(op => 'run', table => '$tableName')")
      }
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(rows: _*)
      assertEquals(Seq.empty, positionalDeletes(tablePath))

      // The compacted file groups keep their ids, so the record index still locates every key
      spark.sql(s"update $tableName set price = 99.0 where id = 2")
      spark.sql(s"delete from $tableName where id = 4")
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(2, "a2m", 99.0, "p1"), Seq(5, "a5", 50.0, "p2"))
      assertEquals(Seq("2", "4"), positionalDeletes(tablePath))
    }
  }

  // A table without a primary key always writes the rows its statements read as prepped records
  Seq(("SIMPLE", true, "true"), ("SIMPLE", false, "false"), ("RECORD_INDEX", true, "true")).foreach { case (indexType, partitioned, optimizedWrites) =>
    test("Test UPDATE, DELETE and MERGE INTO writing updates as deletes and inserts on a "
      + s"${if (partitioned) "partitioned" else "non-partitioned"} table without a primary key with $indexType index") {
      withTable(indexType, partitioned, keyed = false) { (tableName, tablePath) =>
        // Inline compaction stays off until the explicit one, so the log files keep every positional delete
        withSQLConf(HoodieCompactionConfig.INLINE_COMPACT.key -> "false", SPARK_SQL_OPTIMIZED_WRITES.key -> optimizedWrites) {
          spark.sql(s"update $tableName set price = price * 2 where id in (1, 4)")
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(1, "a1", 20.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"),
            Seq(4, "a4", 80.0, "p2"), Seq(5, "a5", 50.0, "p2"))
          assertEquals(Seq(1, 4), positionallyDeletedIds(tablePath, partitioned))

          // The new version of id 1 keeps its generated record key, which locates it again
          spark.sql(s"update $tableName set name = 'a1u' where id = 1")
          spark.sql(s"delete from $tableName where id = 3")
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(1, "a1u", 20.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(4, "a4", 80.0, "p2"), Seq(5, "a5", 50.0, "p2"))
          assertEquals(Seq(1, 1, 3, 4), positionallyDeletedIds(tablePath, partitioned))

          // id 4 and id 7 take no clause
          spark.sql(
            s"""
               |merge into $tableName t
               |using (select 1 as id, 'x' as name, 0.0 as price, 'p1' as dt, 'del' as op
               |  union all select 2, 'a2m', 21.0, 'p1', 'upd'
               |  union all select 4, 'a4m', 41.0, 'p2', 'none'
               |  union all select 6, 'a6', 60.0, 'p2', 'ins'
               |  union all select 7, 'a7', 70.0, 'p2', 'none') s
               |on t.id = s.id
               |when matched and s.op = 'del' then delete
               |when matched and s.op = 'upd' then update set name = s.name, price = t.price + s.price
               |when not matched and s.op = 'ins' then insert (id, name, price, dt) values (s.id, s.name, s.price, s.dt)
             """.stripMargin)
          val rows = Seq(Seq(2, "a2m", 41.0, "p1"), Seq(4, "a4", 80.0, "p2"), Seq(5, "a5", 50.0, "p2"), Seq(6, "a6", 60.0, "p2"))
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(rows: _*)
          assertEquals(Seq(1, 1, 1, 2, 3, 4), positionallyDeletedIds(tablePath, partitioned))
          checkAnswer(s"select count(distinct _hoodie_record_key) from $tableName")(Seq(4))

          withSQLConf(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1") {
            spark.sql(s"call run_compaction(op => 'run', table => '$tableName')")
          }
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(rows: _*)
          assertEquals(Seq.empty, positionalDeletes(tablePath, partitioned))

          spark.sql(s"update $tableName set price = 99.0 where id = 2")
          spark.sql(s"delete from $tableName where id = 4")
          // Without a matched clause the matched id 2 writes nothing
          spark.sql(
            s"""
               |merge into $tableName t
               |using (select 2 as id, 'x' as name, 0.0 as price, 'p1' as dt union all select 8, 'a8', 80.0, 'p2') s
               |on t.id = s.id
               |when not matched then insert *
             """.stripMargin)
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(2, "a2m", 99.0, "p1"), Seq(5, "a5", 50.0, "p2"), Seq(6, "a6", 60.0, "p2"), Seq(8, "a8", 80.0, "p2"))
          assertEquals(Seq(2, 4), positionallyDeletedIds(tablePath, partitioned))
        }
      }
    }
  }

  // The matched rows are id 1 for each condition, so ids 3 and 9 are inserted and the row of id 3 stays as it is
  Seq("t.price < 30", "s.price < 30", "t.price < s.price - 1.5").foreach { conjunct =>
    test(s"Test MERGE INTO matching on t.id = s.id and $conjunct writing updates as deletes and inserts "
      + "on a table without a primary key") {
      withTable("SIMPLE", partitioned = true, keyed = false) { (tableName, tablePath) =>
        spark.sql(
          s"""
             |merge into $tableName t
             |using (select 1 as id, 'a1m' as name, 12.0 as price, 'p1' as dt
             |  union all select 3, 'a3m', 31.0, 'p1' union all select 9, 'a9', 90.0, 'p2') s
             |on t.id = s.id and $conjunct
             |when matched then update set name = s.name
             |when not matched then insert *
           """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName order by id, name")(
          Seq(1, "a1m", 10.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(3, "a3m", 31.0, "p1"),
          Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"), Seq(9, "a9", 90.0, "p2"))
        assertEquals(Seq(1), positionallyDeletedIds(tablePath))
      }
    }
  }

  test("Test MERGE INTO of more than one source row for a target row writing updates as deletes and inserts "
    + "on a table without a primary key") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      withSQLConf(HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() -> "true") {
        spark.sql(
          s"""
             |create table $tableName (id int, name string, price double, ts int, dt string) using hudi
             | partitioned by (dt)
             | location '$tablePath'
             | tblproperties (type = 'mor', preCombineField = 'ts', 'hoodie.record.merge.mode' = 'COMMIT_TIME_ORDERING')
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 100, 'p1'), (2, 'a2', 20.0, 100, 'p1')")
        // Both source rows of id 1 match its row
        val statement =
          s"""
             |merge into $tableName t
             |using (select 1 as id, 'a1m' as name, 11.0 as price, 200 as ts, 'p1' as dt
             |  union all select 1, 'a1m', 11.0, 200, 'p1' union all select 3, 'a3', 30.0, 100, 'p1') s
             |on t.id = s.id
             |when matched then update set *
             |when not matched then insert *
           """.stripMargin
        checkExceptionContain(statement)(
          "MERGE statement matched a single row from the target table with multiple rows of the source table")
        checkAnswer(s"select id, name, price, ts, dt from $tableName order by id")(
          Seq(1, "a1", 10.0, 100, "p1"), Seq(2, "a2", 20.0, 100, "p1"))

        // Without the check the source rows are combined into one update, which stays located at the row of id 1
        withSQLConf(MERGE_INTO_CARDINALITY_CHECK.key -> "false") {
          spark.sql(statement)
        }
        checkAnswer(s"select id, name, price, ts, dt from $tableName order by id")(
          Seq(1, "a1m", 11.0, 200, "p1"), Seq(2, "a2", 20.0, 100, "p1"), Seq(3, "a3", 30.0, 100, "p1"))
        checkAnswer(s"select count(distinct _hoodie_record_key) from $tableName")(Seq(3))
      }
    }
  }

  test("Test UPDATE and DELETE of rows updated in log files before writing updates as deletes and inserts "
    + "on a table without a primary key") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (id int, name string, price double, dt string) using hudi
           | partitioned by (dt)
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (type = 'mor', 'hoodie.record.merge.mode' = 'COMMIT_TIME_ORDERING')
         """.stripMargin)
      spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 'p1'), (2, 'a2', 20.0, 'p1'), (3, 'a3', 30.0, 'p1')")
      // The new versions of ids 1 and 2 are in a log file, and their base file rows are what the positions refer to
      spark.sql(s"update $tableName set price = price + 1 where id in (1, 2)")
      withSQLConf(HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() -> "true") {
        spark.sql(s"update $tableName set name = 'a1u' where id = 1")
        spark.sql(s"delete from $tableName where id = 2")
      }
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1u", 11.0, "p1"), Seq(3, "a3", 30.0, "p1"))
    }
  }

  test("Test a datasource upsert of rows read from a table without a primary key refused writing updates as deletes and inserts") {
    withTable("SIMPLE", partitioned = true, keyed = false) { (tableName, tablePath) =>
      val instants = HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf, tablePath)
        .getActiveTimeline.getInstants.asScala.toList
      val exception = intercept[Exception] {
        spark.read.format("hudi").load(tablePath).where("id = 1").withColumn("price", lit(11.0))
          .write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key, "true")
          .option(OPERATION.key, UPSERT_OPERATION_OPT_VAL)
          .mode(SaveMode.Append)
          .save(tablePath)
      }
      assertTrue(exception.getMessage.contains("Use Spark SQL UPDATE, DELETE or MERGE INTO"), exception.getMessage)
      assertEquals(instants, HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf, tablePath)
        .getActiveTimeline.getInstants.asScala.toList)
    }
  }

  test("Test INSERT INTO that upserts refused on a table without a primary key writing updates as deletes and inserts") {
    withTable("SIMPLE", partitioned = true, keyed = false) { (tableName, tablePath) =>
      val instants = HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf, tablePath)
        .getActiveTimeline.getInstants.asScala.toList
      // The upsert would look the rows up by record keys it generates anew
      withSQLConf(SPARK_SQL_INSERT_INTO_OPERATION.key -> "upsert") {
        checkExceptionContain(s"insert into $tableName values (1, 'a1u', 11.0, 'p1')")(
          BaseWriteHelper.keylessTableNotSupported().getMessage)
      }
      // The statement is refused before it starts a commit
      assertEquals(instants, HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf, tablePath)
        .getActiveTimeline.getInstants.asScala.toList)
    }
  }

  /**
   * Creates a merge-on-read table with the write mode enabled, holding ids 1 to 3 with dt p1 and ids 4 and 5
   * with dt p2, which are its partitions unless it is not partitioned. Its primary key is id if it is keyed.
   */
  private def withTable(f: (String, String) => Unit): Unit = withTable("SIMPLE")(f)

  private def withTable(indexType: String)(f: (String, String) => Unit): Unit = withTable(indexType, partitioned = true)(f)

  private def withTable(indexType: String, partitioned: Boolean)(f: (String, String) => Unit): Unit =
    withTable(indexType, partitioned, keyed = true)(f)

  private def withTable(indexType: String, partitioned: Boolean, keyed: Boolean)(f: (String, String) => Unit): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      withSQLConf(HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() -> "true", "hoodie.index.type" -> indexType,
        HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> String.valueOf(indexType == "RECORD_INDEX")) {
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  dt string
             |) using hudi
             | ${if (partitioned) "partitioned by (dt)" else ""}
             | location '$tablePath'
             | tblproperties (
             |  type = 'mor',
             |  ${if (keyed) "primaryKey = 'id'," else ""}
             |  'hoodie.record.merge.mode' = 'COMMIT_TIME_ORDERING'
             | )
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 'p1'), (2, 'a2', 20.0, 'p1'), (3, 'a3', 30.0, 'p1'),"
          + " (4, 'a4', 40.0, 'p2'), (5, 'a5', 50.0, 'p2')")
        f(tableName, tablePath)
      }
    }
  }

  /**
   * The values of id in the rows the positional deletes remove, see [[positionalDeletes]], for a table whose record
   * keys are generated. Every copy of a row keeps its record key and its id.
   */
  private def positionallyDeletedIds(tablePath: String, partitioned: Boolean = true): Seq[Int] = {
    val (_, fsView) = getMetaClientAndFileSystemView(tablePath)
    val baseFiles = try {
      partitions(partitioned).flatMap(fsView.getAllBaseFiles(_).iterator().asScala).map(_.getPath)
    } finally {
      fsView.close()
    }
    val idsByRecordKey = spark.read.parquet(baseFiles: _*).select("_hoodie_record_key", "id").distinct().collect()
      .map(row => row.getString(0) -> row.getInt(1)).toMap
    positionalDeletes(tablePath, partitioned).map(idsByRecordKey).sorted
  }

  private def partitions(partitioned: Boolean): Seq[String] = if (partitioned) Seq("dt=p1", "dt=p2") else Seq("")

  /**
   * Asserts that every log block of the latest file slices is a delete block whose positions point at
   * the rows of the deleted keys in the slice's base file, and returns the sorted deleted keys.
   */
  private def positionalDeletes(tablePath: String, partitioned: Boolean = true): Seq[String] = {
    val (metaClient, fsView) = getMetaClientAndFileSystemView(tablePath)
    val schema = new TableSchemaResolver(metaClient).getTableSchema
    val deletedKeys = ArrayBuffer[String]()
    try {
      partitions(partitioned).flatMap(fsView.getLatestFileSlices(_).iterator().asScala).filter(_.hasLogFiles).foreach { slice =>
        assertTrue(slice.getBaseFile.isPresent, s"a file group with logs must have a base file: $slice")
        val baseFileKeys = ArrayBuffer[String]()
        val fileReader = HoodieIOFactory.getIOFactory(metaClient.getStorage)
          .getReaderFactory(HoodieRecordType.AVRO)
          .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, new StoragePath(slice.getBaseFile.get.getPath))
        try {
          fileReader.getRecordKeyIterator.asScala.foreach(baseFileKeys += _)
        } finally {
          fileReader.close()
        }
        slice.getLogFiles.iterator().asScala.foreach { logFile =>
          val logReader = HoodieLogFormat.newReader(metaClient, logFile, schema)
          try {
            while (logReader.hasNext) {
              val block = logReader.next()
              assertTrue(block.isInstanceOf[HoodieDeleteBlock],
                s"expected only delete blocks but got ${block.getClass.getSimpleName} in ${logFile.getFileName}")
              val deleteBlock = block.asInstanceOf[HoodieDeleteBlock]
              val deleteRecords = deleteBlock.getRecordsToDelete
              val positions = deleteBlock.getRecordPositionList.asScala
              assertEquals(deleteRecords.length, positions.size, "every delete record must carry a position")
              positions.zipWithIndex.foreach { case (position, i) =>
                assertEquals(deleteRecords(i).getRecordKey, baseFileKeys(position.intValue()),
                  "the base file row at a delete position must hold the deleted record key")
              }
              assertEquals(slice.getBaseFile.get.getCommitTime, deleteBlock.getBaseFileInstantTimeOfPositions,
                "positions must reference the file slice's base file")
              deleteRecords.foreach(deletedKeys += _.getRecordKey)
            }
          } finally {
            logReader.close()
          }
        }
      }
    } finally {
      fsView.close()
    }
    deletedKeys.sorted.toList
  }
}
