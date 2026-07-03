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

import org.apache.hudi.DataSourceWriteOptions.SPARK_SQL_OPTIMIZED_WRITES

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * Focused coverage for the MERGE INTO write path implemented by
 * [[org.apache.spark.sql.hudi.command.payload.ExpressionPayload]] and
 * [[org.apache.spark.sql.hudi.command.MergeIntoKeyGenerator]].
 *
 * The scenarios below intentionally combine, within a single statement, all of:
 *   - conditional matched UPDATE clauses (the update-condition evaluation loop),
 *   - matched DELETE clauses (both conditional and unconditional delete markers),
 *   - conditional NOT-MATCHED INSERT clauses (including a not-matched row that
 *     matches no insert condition and is therefore filtered),
 * so both the matched and not-matched evaluators, the delete-marker branch, and the
 * record-merge branch are exercised. Running against partitioned COW and MOR tables
 * with the row-writer path both enabled and disabled additionally drives the
 * record-key / partition-path extraction overloads of MergeIntoKeyGenerator (the
 * meta-field-populated branch for matched rows and the key-generator fallback for
 * freshly inserted rows).
 */
class TestMergeIntoWriteCoverage extends HoodieSparkSqlTestBase {

  test("Test MergeInto conditional update, delete-marker and insert on partitioned COW") {
    Seq(true, false).foreach { optimizedWrites =>
      withTempDir { tmp =>
        withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> optimizedWrites.toString) {
          val tableName = generateTableName
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts int,
               |  pt string
               |) using hudi
               | partitioned by (pt)
               | location '${tmp.getCanonicalPath}'
               | tblproperties (
               |  type = 'cow',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               | )
             """.stripMargin)

          // Seed: id 1 & 2 in partition p1, id 3 in partition p2.
          spark.sql(
            s"""insert into $tableName values
               | (1, 'a1', 10.0, 1, 'p1'),
               | (2, 'a2', 20.0, 1, 'p1'),
               | (3, 'a3', 30.0, 1, 'p2')
             """.stripMargin)

          // Single MERGE that exercises every clause type at once:
          //  - id 1 matches "flag = 'u'"      -> conditional UPDATE (record merge, higher ts wins)
          //  - id 2 matches "flag = 'd'"      -> conditional DELETE (delete marker)
          //  - id 3 matches neither cond      -> no matched clause fires, record retained
          //  - id 4 not matched, insert cond  -> INSERT into a brand new partition p3
          //  - id 5 not matched, no insert cond -> filtered out (not written)
          spark.sql(
            s"""
               | merge into $tableName t
               | using (
               |   select 1 as id, 'a1_u' as name, 11.0 as price, 2 as ts, 'p1' as pt, 'u' as flag union all
               |   select 2 as id, 'a2_d' as name, 22.0 as price, 2 as ts, 'p1' as pt, 'd' as flag union all
               |   select 3 as id, 'a3_x' as name, 33.0 as price, 2 as ts, 'p2' as pt, 'x' as flag union all
               |   select 4 as id, 'a4_i' as name, 40.0 as price, 2 as ts, 'p3' as pt, 'i' as flag union all
               |   select 5 as id, 'a5_i' as name, 50.0 as price, 2 as ts, 'p3' as pt, 'n' as flag
               | ) s
               | on t.id = s.id
               | when matched and s.flag = 'u' then update set
               |   name = s.name, price = s.price, ts = s.ts
               | when matched and s.flag = 'd' then delete
               | when not matched and s.flag = 'i' then insert
               |   (id, name, price, ts, pt) values (s.id, s.name, s.price, s.ts, s.pt)
             """.stripMargin)

          checkAnswer(s"select id, name, price, ts, pt from $tableName order by id")(
            Seq(1, "a1_u", 11.0, 2, "p1"), // conditionally updated
            Seq(3, "a3", 30.0, 1, "p2"),   // no matched clause fired -> unchanged
            Seq(4, "a4_i", 40.0, 2, "p3")  // conditionally inserted into a new partition
            // id 2 deleted, id 5 filtered by the insert condition
          )
        }
      }
    }
  }

  test("Test MergeInto unconditional delete-marker on partitioned COW") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts int,
           |  pt string
           |) using hudi
           | partitioned by (pt)
           | location '${tmp.getCanonicalPath}'
           | tblproperties (type = 'cow', primaryKey = 'id', preCombineField = 'ts')
         """.stripMargin)
      spark.sql(
        s"""insert into $tableName values
           | (1, 'a1', 1, 'p1'),
           | (2, 'a2', 1, 'p1')
         """.stripMargin)

      // The source must expose the ordering field (ts) so MERGE can resolve the
      // table's ordering-field association, and the partition field (pt) so the
      // delete can be routed to the record's partition. The delete also carries a
      // higher ordering value than the stored record so event-time ordering applies
      // the delete rather than discarding it as stale.
      spark.sql(
        s"""
           | merge into $tableName t
           | using (select 1 as id, 'a1' as name, 2 as ts, 'p1' as pt) s
           | on t.id = s.id
           | when matched then delete
         """.stripMargin)

      checkAnswer(s"select id, name from $tableName order by id")(
        Seq(2, "a2")
      )
    }
  }

  test("Test MergeInto conditional update, delete and insert on partitioned MOR") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts int,
           |  pt string
           |) using hudi
           | partitioned by (pt)
           | location '${tmp.getCanonicalPath}'
           | tblproperties (type = 'mor', primaryKey = 'id', preCombineField = 'ts')
         """.stripMargin)
      spark.sql(
        s"""insert into $tableName values
           | (1, 'a1', 10.0, 1, 'p1'),
           | (2, 'a2', 20.0, 1, 'p1'),
           | (3, 'a3', 30.0, 1, 'p2')
         """.stripMargin)

      // On MOR the matched records flow through ExpressionPayload.getInsertValue as
      // update-records, exercising the MOR-specific matched/delete handling.
      spark.sql(
        s"""
           | merge into $tableName t
           | using (
           |   select 1 as id, 'a1_u' as name, 11.0 as price, 2 as ts, 'p1' as pt, 'u' as flag union all
           |   select 2 as id, 'a2_d' as name, 22.0 as price, 2 as ts, 'p1' as pt, 'd' as flag union all
           |   select 4 as id, 'a4_i' as name, 40.0 as price, 2 as ts, 'p2' as pt, 'i' as flag
           | ) s
           | on t.id = s.id
           | when matched and s.flag = 'u' then update set
           |   name = s.name, price = s.price, ts = s.ts
           | when matched and s.flag = 'd' then delete
           | when not matched then insert
           |   (id, name, price, ts, pt) values (s.id, s.name, s.price, s.ts, s.pt)
         """.stripMargin)

      checkAnswer(s"select id, name, price, ts, pt from $tableName order by id")(
        Seq(1, "a1_u", 11.0, 2, "p1"),
        Seq(3, "a3", 30.0, 1, "p2"),
        Seq(4, "a4_i", 40.0, 2, "p2")
      )
    }
  }

  test("Test MergeInto update with OverwriteWithLatestAvroPayload record merge") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Using the overwrite payload drives the "always pick incoming" record-merge
      // branch of ExpressionPayload.doRecordMerge (as opposed to the ordering-based
      // DefaultHoodieRecordPayload branch covered by the other cases).
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts int,
           |  pt string
           |) using hudi
           | partitioned by (pt)
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  hoodie.datasource.write.payload.class = 'org.apache.hudi.common.model.OverwriteWithLatestAvroPayload'
           | )
         """.stripMargin)
      spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 5, 'p1')")

      // Incoming ts (2) is lower than the persisted ts (5); the overwrite payload must
      // still pick the incoming record.
      spark.sql(
        s"""
           | merge into $tableName t
           | using (select 1 as id, 'a1_new' as name, 99.0 as price, 2 as ts, 'p1' as pt) s
           | on t.id = s.id
           | when matched then update set
           |   name = s.name, price = s.price, ts = s.ts
         """.stripMargin)

      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1_new", 99.0, 2)
      )
    }
  }
}
