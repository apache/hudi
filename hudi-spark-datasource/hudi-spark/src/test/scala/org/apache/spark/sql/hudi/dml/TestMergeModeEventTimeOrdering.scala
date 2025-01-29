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

package org.apache.spark.sql.hudi.dml

import org.apache.hudi.DataSourceWriteOptions

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestMergeModeEventTimeOrdering extends HoodieSparkSqlTestBase {

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test $tableType table with EVENT_TIME_ORDERING merge mode") {
      withSparkSqlSessionConfig(
        "hoodie.merge.small.file.group.candidates.limit" -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key -> "true"
      ) {
        withRecordType()(withTempDir { tmp =>
          val tableName = generateTableName
          // Create table with EVENT_TIME_ORDERING
          spark.sql(
            s"""
               | create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               | ) using hudi
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts',
               |  hoodie.record.merge.mode = 'EVENT_TIME_ORDERING'
               | )
               | location '${tmp.getCanonicalPath}'
             """.stripMargin)

          // Insert initial records with ts=100
          spark.sql(
            s"""
               | insert into $tableName
               | select 1 as id, 'A' as name, 10.0 as price, 100 as ts
               | union all
               | select 2, 'B', 20.0, 100
             """.stripMargin)

          // Verify inserting records with the same ts value are visible
          spark.sql(
            s"""
               | insert into $tableName
               | select 1 as id, 'A_equal' as name, 60.0 as price, 100 as ts
               | union all
               | select 2, 'B_equal', 70.0, 100
            """.stripMargin)

          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(1, "A_equal", 60.0, 100),
            Seq(2, "B_equal", 70.0, 100)
          )

          // Verify updating records with the same ts value are visible
          spark.sql(
            s"""
               | update $tableName
               | set price = 50.0, ts = 100
               | where id = 1
             """.stripMargin)

          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(1, "A_equal", 50.0, 100),
            Seq(2, "B_equal", 70.0, 100)
          )

          // Insert records with lower ts=99 (should not be visible)
          spark.sql(
            s"""
               | insert into $tableName
               | select 1 as id, 'A' as name, 30.0 as price, 99 as ts
               | union all
               | select 2, 'B', 40.0, 99
             """.stripMargin)

          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(1, "A_equal", 50.0, 100),
            Seq(2, "B_equal", 70.0, 100)
          )

          // Update record with a lower ts=98 (should not be visible)
          spark.sql(
            s"""
               | update $tableName
               | set price = 50.0, ts = 98
               | where id = 1
             """.stripMargin)

          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(1, "A_equal", 50.0, 100),
            Seq(2, "B_equal", 70.0, 100)
          )

          // Insert records with higher ts=101 (should be visible)
          spark.sql(
            s"""
               | insert into $tableName
               | select 1 as id, 'A' as name, 30.0 as price, 101 as ts
               | union all
               | select 2, 'B', 40.0, 101
             """.stripMargin)

          // Verify records with ts=101 are visible
          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(1, "A", 30.0, 101),
            Seq(2, "B", 40.0, 101)
          )

          // Update with a higher ts=102 is visible
          spark.sql(
            s"""
               | update $tableName
               | set price = 50.0, ts = 102
               | where id = 1
             """.stripMargin)

          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(1, "A", 50.0, 102),
            Seq(2, "B", 40.0, 101)
          )

          // Insert records with missing ts is not allowed
          checkExceptionContain(
            s"""
               | insert into $tableName
               | select 1 as id, 'A_missing_ts' as name, 31.0 as price
               | union all
               | select 2, 'B_missing_ts', 41.0
             """.stripMargin)("not enough data columns")

          // Update with missing ts is the same as COMMIT_TIME_ORDERING (should be visible)
          spark.sql(
            s"""
               | update $tableName
               | set price = 53.0
               | where id = 1
             """.stripMargin)

          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(1, "A", 53.0, 102),
            Seq(2, "B", 40.0, 101)
          )
          // Delete record with no ts.
          spark.sql(s"delete from $tableName where id = 1")
          // Verify deletion
          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(2, "B", 40.0, 101)
          )
        })
      }
    }
  }

  Seq("mor").foreach { tableType =>
    // [HUDI-8915]: COW MIT delete does not honor event time ordering. For update we have the coverage in
    // "Test MergeInto with commit time/event time ordering coverage".
    //  Seq("cow", "mor").foreach { tableType =>
    test(s"Test merge operations with EVENT_TIME_ORDERING for $tableType table") {
      withSparkSqlSessionConfig("hoodie.merge.small.file.group.candidates.limit" -> "0") {
        withRecordType()(withTempDir { tmp =>
          val tableName = generateTableName
          // Create table with EVENT_TIME_ORDERING
          spark.sql(
            s"""
               | create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               | ) using hudi
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts',
               |  hoodie.record.merge.mode = 'EVENT_TIME_ORDERING'
               | )
               | location '${tmp.getCanonicalPath}'
             """.stripMargin)

          // Insert initial records with ts=100
          spark.sql(
            s"""
               | insert into $tableName
               | select 0 as id, 'A0' as name, 0.0 as price, 100L as ts union all
               | select 1, 'A', 10.0, 100L union all
               | select 2, 'B', 20.0, 100L union all
               | select 3, 'C', 30.0, 100L union all
               | select 4, 'D', 40.0, 100L union all
               | select 5, 'E', 50.0, 100L union all
               | select 6, 'F', 60.0, 100L
             """.stripMargin)

          // Merge operation - delete with arbitrary ts value (lower, equal and higher). Lower ts won't take effect.
          spark.sql(
            s"""
               | merge into $tableName t
               | using (
               |   select 0 as id, 'B2' as name, 25.0 as price, 100L as ts union all
               |   select 1 as id, 'B2' as name, 25.0 as price, 101L as ts union all
               |   select 2 as id, 'B2' as name, 25.0 as price, 99L as ts
               | ) s
               | on t.id = s.id
               | when matched then delete
             """.stripMargin)

          // Merge operation - update with mixed ts values (only equal or higher ts should take effect)
          spark.sql(
            s"""
               | merge into $tableName t
               | using (
               |   select 4 as id, 'D2' as name, 45.0 as price, 101L as ts union all
               |   select 5, 'E2', 55.0, 99L as ts union all
               |   select 6, 'F2', 65.0, 100L as ts
               | ) s
               | on t.id = s.id
               | when matched then update set *
             """.stripMargin)

          // Verify state after merges
          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(2, "B", 20.0, 100),
            Seq(3, "C", 30.0, 100),
            Seq(4, "D2", 45.0, 101),
            Seq(5, "E", 50.0, 100),
            Seq(6, "F2", 65.0, 100)
          )

          // Insert new records through merge
          spark.sql(
            s"""
               | merge into $tableName t
               | using (
               |   select 7 as id, 'G' as name, 70.0 as price, 99 as ts union all
               |   select 8, 'H', 80.0, 99 as ts
               | ) s
               | on t.id = s.id
               | when not matched then insert *
             """.stripMargin)

          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(2, "B", 20.0, 100),
            Seq(3, "C", 30.0, 100),
            Seq(4, "D2", 45.0, 101),
            Seq(5, "E", 50.0, 100),
            Seq(6, "F2", 65.0, 100),
            Seq(7, "G", 70.0, 99),
            Seq(8, "H", 80.0, 99)
          )
        })
      }
    }
  }
}
