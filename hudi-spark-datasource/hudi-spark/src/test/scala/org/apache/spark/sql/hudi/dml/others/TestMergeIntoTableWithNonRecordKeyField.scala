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

import org.apache.hudi.DataSourceWriteOptions.{MERGE_INTO_CARDINALITY_CHECK, SPARK_SQL_OPTIMIZED_WRITES}
import org.apache.hudi.ScalaAssertionSupport
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.hudi.command.MergeIntoHoodieTableCommand
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestMergeIntoTableWithNonRecordKeyField extends HoodieSparkSqlTestBase with ScalaAssertionSupport {

  test("Test Merge into extra cond") {
    Seq(true, false).foreach { sparkSqlOptimizedWrites =>
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  primaryKey ='id',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)
        val tableName2 = generateTableName
        spark.sql(
          s"""
             |create table $tableName2 (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName2'
             | tblproperties (
             |  primaryKey ='id',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)

        spark.sql(
          s"""
             |insert into $tableName values
             |    (1, 'a1', 10, 100),
             |    (2, 'a2', 20, 200),
             |    (3, 'a3', 20, 100)
             |""".stripMargin)
        spark.sql(
          s"""
             |insert into $tableName2 values
             |    (1, 'u1', 10, 999),
             |    (3, 'u3', 30, 9999),
             |    (4, 'u4', 40, 99999)
             |""".stripMargin)

        // test with optimized sql merge enabled / disabled.
        withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> sparkSqlOptimizedWrites.toString) {

          spark.sql(
            s"""
               |merge into $tableName as oldData
               |using $tableName2
               |on oldData.id = $tableName2.id
               |when matched and oldData.price = $tableName2.price then update set oldData.name = $tableName2.name, oldData.ts = $tableName2.ts
               |
               |""".stripMargin)

          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "u1", 10.0, 999),
            Seq(3, "a3", 20.0, 100),
            Seq(2, "a2", 20.0, 200)
          )

          spark.sql(
            s"""
               |merge into $tableName as oldData
               |using $tableName2
               |on oldData.id = $tableName2.id and oldData.price = $tableName2.price
               |when matched then update set oldData.name = $tableName2.name, oldData.ts = $tableName2.ts
               |when not matched then insert *
               |""".stripMargin)

          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "u1", 10.0, 999),
            Seq(2, "a2", 20.0, 200),
            Seq(3, "u3", 20.0, 9999),
            Seq(4, "u4", 40.0, 99999)
          )

          //test with multiple pks
          val tableName3 = generateTableName
          spark.sql(
            s"""
               |create table $tableName3 (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName3'
               | tblproperties (
               |  primaryKey ='id,name',
               |  preCombineField = 'ts'
               | )
       """.stripMargin)

          spark.sql(
            s"""
               |insert into $tableName3 values
               |    (1, 'a1', 10, 100),
               |    (2, 'a2', 20, 200),
               |    (3, 'u3', 20, 100)
               |""".stripMargin)

          val errorMessage2 = "Hudi tables with record key are required to match on all record key columns. Column: 'name' not found"
          checkException(
            s"""
               | merge into $tableName3 as t0
               | using (
               |  select * from $tableName2
               | ) as s0
               | on t0.id = s0.id
               | when matched then update set id = t0.id, name = t0.name,
               |  price = t0.price, ts = s0.ts
               | when not matched then insert (id,name,price,ts) values(s0.id, s0.name, s0.price, s0.ts)
         """.stripMargin)(errorMessage2)
        }
      }
    }
  }

  test("Test MergeInto on a table with record key must match on every record key column") {
    // None leaves the optimized writes config unset, so it takes its default
    Seq(None, Some("true"), Some("false")).foreach { sparkSqlOptimizedWrites =>
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |  primaryKey ='id',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000), (2, 'a2', 20, 1000)")

        withSQLConf(sparkSqlOptimizedWrites.map(SPARK_SQL_OPTIMIZED_WRITES.key() -> _).toSeq: _*) {
          // The source row matches target row 1 on name, while its id points at target row 2
          checkException(
            s"""
               |merge into $tableName t
               |using (select 2 as id, 'a1' as name, 11.0 as price, 1001L as ts) s
               |on t.name = s.name
               |when matched then update set price = s.price, ts = s.ts
               |""".stripMargin)(
            "Hudi tables with record key are required to match on all record key columns. Column: 'id' not found")
        }
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 20.0, 1000))
      }
    }
  }

  test("Test pkless complex merge cond") {
    withRecordType()(withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true",
        SPARK_SQL_OPTIMIZED_WRITES.key() -> "true") {
        val tableName = generateTableName
        // Create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts int
             |) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |  preCombineField = 'ts'
             | )
       """.stripMargin)

        spark.sql(
          s"""
             |insert into $tableName values
             |    (1, 'a1', 10, 100),
             |    (2, 'a2', 20, 100),
             |    (3, 'a3', 30, 100),
             |    (4, 'a4', 40, 100),
             |    (5, 'a5', 50, 100),
             |    (6, 'a6', 60, 100)
             |""".stripMargin)

        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 2 as id, 'a1' as name, 50 as price, 999 as ts
             | ) s0
             | on $tableName.price < s0.price and $tableName.id >= s0.id
             | when matched then update set ts = s0.ts
             | when not matched then insert *
       """.stripMargin)
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 100),
          Seq(2, "a2", 20.0, 999),
          Seq(3, "a3", 30.0, 999),
          Seq(4, "a4", 40.0, 999),
          Seq(5, "a5", 50.0, 100),
          Seq(6, "a6", 60.0, 100)
        )

        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 1 as id, 'a1' as name, 50 as price, 999 as ts
             | ) s0
             | on $tableName.id < s0.id
             | when matched then update set ts = s0.ts
             | when not matched then insert *
       """.stripMargin)
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 100),
          Seq(2, "a2", 20.0, 999),
          Seq(3, "a3", 30.0, 999),
          Seq(4, "a4", 40.0, 999),
          Seq(5, "a5", 50.0, 100),
          Seq(6, "a6", 60.0, 100),
          Seq(1, "a1", 50.0, 999)
        )
      }
    })
  }

  test("Test pkless multiple source match") {
    for (withPrecombine <- Seq(true, false)) {
      withTempDir { tmp =>
        withSQLConf("hoodie.payload.combined.schema.validate" -> "true") {
          val tableName = generateTableName

          val prekstr = if (withPrecombine) "tblproperties (preCombineField = 'ts')" else ""
          // Create table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts int
               |) using hudi
               | location '${tmp.getCanonicalPath}'
               | $prekstr
         """.stripMargin)

          spark.sql(
            s"""
               |insert into $tableName values
               |    (1, 'a1', 10, 100)
               |""".stripMargin)

          val updateStr = if (withPrecombine) "price = s0.price, ts = s0.ts" else "price = s0.price"
          val mergeStatement =
            s"""
               | merge into $tableName
               | using (
               |  select 1 as id, 'a1' as name, 20 as price, 200 as ts
               |  union all
               |  select 2 as id, 'a1' as name, 30 as price, 100 as ts
               | ) s0
               | on $tableName.name = s0.name
               | when matched then update set $updateStr
               | when not matched then insert *
         """.stripMargin
          // Both source rows match the target row, which fails with the cardinality check on
          withSQLConf(MERGE_INTO_CARDINALITY_CHECK.key -> "true") {
            checkExceptionContain(mergeStatement)(
              "MERGE statement matched a single row from the target table with multiple rows of the source table")
          }
          checkAnswer(s"select id, name, price, ts from $tableName")(Seq(1, "a1", 10.0, 100))
          withSQLConf(MERGE_INTO_CARDINALITY_CHECK.key -> "false") {
            spark.sql(mergeStatement)
          }
          if (withPrecombine) {
            checkAnswer(s"select id, name, price, ts from $tableName")(
              Seq(1, "a1", 20.0, 200)
            )
          } else {
            checkAnswer(s"select id, name, price, ts from $tableName")(
              Seq(1, "a1", 30.0, 100)
            )
          }
        }
      }
    }

  }

  test("Test MergeInto Basic pkless") {
    withTempDir { tmp =>
      withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> "true",
        "hoodie.payload.combined.schema.validate" -> "true") {
        val tableName = generateTableName
        // Create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts int
             |) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |  preCombineField = 'ts'
             | )
       """.stripMargin)

        // First merge with a extra input field 'flag' (insert a new record)
        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '1' as flag
             | ) s0
             | on s0.id = $tableName.id
             | when matched and flag = '1' then update set
             | id = s0.id, name = s0.name, price = s0.price, ts = s0.ts
             | when not matched and flag = '1' then insert *
       """.stripMargin)
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        // Second merge (update the record)
        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 1 as id, 'a1' as name, 10 as price, 1001 as ts
             | ) s0
             | on s0.id = $tableName.id
             | when matched then update set
             | id = s0.id, name = s0.name, price = s0.price + $tableName.price, ts = s0.ts
             | when not matched then insert *
       """.stripMargin)
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 20.0, 1001)
        )

        // the third time merge (update & insert the record)
        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select * from (
             |  select 1 as id, 'a1' as name, 10 as price, 1002 as ts
             |  union all
             |  select 2 as id, 'a2' as name, 12 as price, 1001 as ts
             |  )
             | ) s0
             | on s0.id = $tableName.id
             | when matched then update set
             | id = s0.id, name = s0.name, price = s0.price + $tableName.price, ts = s0.ts
             | when not matched and s0.id % 2 = 0 then insert *
       """.stripMargin)
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 30.0, 1002),
          Seq(2, "a2", 12.0, 1001)
        )

        // the fourth merge (delete the record)
        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 1 as id, 'a1' as name, 12 as price, 1003 as ts
             | ) s0
             | on s0.id = $tableName.id
             | when matched and s0.id != 1 then update set
             |    id = s0.id, name = s0.name, price = s0.price, ts = s0.ts
             | when matched and s0.id = 1 then delete
             | when not matched then insert *
       """.stripMargin)
        val cnt = spark.sql(s"select * from $tableName where id = 1").count()
        assertResult(0)(cnt)
      }
    }
  }

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test MergeInto without update clause on pkless $tableType table without ordering field") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  dt string
             | ) using hudi
             | tblproperties (type = '$tableType')
             | partitioned by(dt)
             | location '${tmp.getCanonicalPath}'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 'p1'), (2, 'a2', 20.0, 'p1'), (3, 'a3', 30.0, 'p1'),"
          + " (4, 'a4', 40.0, 'p2'), (5, 'a5', 50.0, 'p2')")

        // Delete only with a condition: id 2 fails the condition and id 9 has no match
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 1 as id, 'del' as op union all select 2, 'keep' union all select 9, 'del') s
             | on t.id = s.id
             | when matched and s.op = 'del' then delete
           """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName")(
          Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"))

        // Delete only without a condition: id 8 has no match
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 2 as id union all select 8) s
             | on t.id = s.id
             | when matched then delete
           """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName")(
          Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"))

        // Insert only without a condition: the matched id 3 is not inserted again, both unmatched id 6 are
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 3 as id, 'a3n' as name, 31.0 as price, 'p1' as dt
             |  union all select 6, 'a6', 60.0, 'p2' union all select 6, 'a6b', 61.0, 'p2') s
             | on t.id = s.id
             | when not matched then insert *
           """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName")(
          Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"), Seq(6, "a6", 60.0, "p2"),
          Seq(6, "a6b", 61.0, "p2"))

        // Insert only with a condition: id 4 is matched and the unmatched id 10 fails the condition
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 4 as id, 'a4n' as name, 71.0 as price, 'p2' as dt
             |  union all select 7, 'a7', 70.0, 'p1' union all select 10, 'a10', 5.0, 'p1') s
             | on t.id = s.id
             | when not matched and s.price > 50 then insert *
           """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName")(
          Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"), Seq(6, "a6", 60.0, "p2"),
          Seq(6, "a6b", 61.0, "p2"), Seq(7, "a7", 70.0, "p1"))

        // Delete and insert: the matched id 3 fails the delete condition and is not inserted again
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 3 as id, 'a3n' as name, 32.0 as price, 'p1' as dt
             |  union all select 5, 'a5n', 52.0, 'p2' union all select 11, 'a11', 110.0, 'p1') s
             | on t.id = s.id
             | when matched and s.id = 5 then delete
             | when not matched then insert *
           """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName")(
          Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(6, "a6", 60.0, "p2"), Seq(6, "a6b", 61.0, "p2"),
          Seq(7, "a7", 70.0, "p1"), Seq(11, "a11", 110.0, "p1"))
      }
    }
  }

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test MergeInto moves a matched record to the partition its update gives it in pkless $tableType table") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             | create table $tableName (id int, name string, dep string) using hudi
             | tblproperties (type = '$tableType')
             | partitioned by (dep)
             | location '$tablePath'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 'd1'), (6, 'a6', 'd6'), (7, 'a7', 'd1'), (8, 'a8', 'd1')")

        // id 1 takes the partition the source names, id 7 one its clause assigns, id 8 stays, id 6 is deleted and the
        // unmatched id 2 is inserted
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 2 as id, 'a2' as name, 'd2' as dep union all select 1, 'a1m', 'd9'
             |   union all select 6, 'a6', 'd6' union all select 7, 'a7m', 'd1' union all select 8, 'a8m', 'd1') s
             | on t.id = s.id
             | when matched and s.id = 1 then update set *
             | when matched and s.id = 6 then delete
             | when matched and s.id = 7 then update set id = s.id, name = s.name, dep = 'd8'
             | when matched and s.id = 8 then update set *
             | when not matched and s.id = 2 then insert *
           """.stripMargin)
        checkPartitionedRows(tableName, "dep")(
          Seq(1, "a1m", "d9"), Seq(2, "a2", "d2"), Seq(7, "a7m", "d8"), Seq(8, "a8m", "d1"))

        // The moved records are updated in their new partition, and id 7 is moved again by a clause that assigns
        // the partition field only
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 1 as id, 'a1n' as name, 'd9' as dep union all select 7, 'a7n', 'd8') s
             | on t.id = s.id
             | when matched then update set *
           """.stripMargin)
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 1 as id, 'd9' as dep union all select 7, 'd1') s
             | on t.id = s.id
             | when matched then update set dep = s.dep
           """.stripMargin)
        val expected = Seq(Seq(1, "a1n", "d9"), Seq(2, "a2", "d2"), Seq(7, "a7n", "d1"), Seq(8, "a8m", "d1"))
        checkPartitionedRows(tableName, "dep")(expected: _*)
        if (tableType == "mor") {
          compactAndCheck(tableName)
          checkPartitionedRows(tableName, "dep")(expected: _*)
        }
      }
    }

    test(s"Test MergeInto deletes a matched record of a pkless $tableType table with an ordering field") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (id int, name string, ts long, dep string) using hudi
             | tblproperties (type = '$tableType', preCombineField = 'ts')
             | partitioned by (dep)
             | location '${tmp.getCanonicalPath}/$tableName'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 'd1'), (2, 'a2', 10, 'd1'), (3, 'a3', 10, 'd1')")

        // id 1 takes the delete clause and id 2 the update clause
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 1 as id, 'a1m' as name, 20L as ts, 'd1' as dep union all select 2, 'a2m', 20L, 'd1') s
             | on t.id = s.id
             | when matched and s.id = 2 then update set *
             | when matched then delete
           """.stripMargin)
        checkAnswer(s"select id, name, ts, dep from $tableName order by id")(Seq(2, "a2m", 20L, "d1"), Seq(3, "a3", 10L, "d1"))

        // A statement with a delete clause only
        spark.sql(s"merge into $tableName t using (select 3 as id, 20L as ts) s on t.id = s.id when matched then delete")
        checkAnswer(s"select id, name, ts, dep from $tableName order by id")(Seq(2, "a2m", 20L, "d1"))
      }
    }

    test(s"Test MergeInto moves a matched record of a pkless $tableType table with an ordering field only when its update applies") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (id int, name string, ts long, dep string) using hudi
             | tblproperties (type = '$tableType', preCombineField = 'ts')
             | partitioned by (dep)
             | location '${tmp.getCanonicalPath}/$tableName'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 'd1'), (2, 'a2', 10, 'd1')")

        // The update of id 1 has a lower ordering value than the record, which stays as it is
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 1 as id, 'a1m' as name, 5L as ts, 'd2' as dep union all select 2, 'a2m', 20L, 'd2') s
             | on t.id = s.id
             | when matched then update set *
           """.stripMargin)
        checkAnswer(s"select id, name, ts, dep, _hoodie_partition_path from $tableName order by id")(
          Seq(1, "a1", 10L, "d1", "dep=d1"), Seq(2, "a2m", 20L, "d2", "dep=d2"))
      }
    }
  }

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test MergeInto fails when more than one source record matches a record it moves in pkless $tableType table") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (id int, name string, dep string) using hudi
             | tblproperties (type = '$tableType')
             | partitioned by (dep)
             | location '${tmp.getCanonicalPath}/$tableName'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 'd1'), (2, 'a2', 'd1')")

        // Two source records match id 1, and one of them moves it
        val e = intercept[HoodieException] {
          spark.sql(
            s"""
               | merge into $tableName t
               | using (select 1 as id, 'a1x' as name, 'd2' as dep union all select 1, 'a1y', 'd1'
               |   union all select 2, 'a2m', 'd1') s
               | on t.id = s.id
               | when matched then update set *
             """.stripMargin)
        }
        assertResult(MergeIntoHoodieTableCommand.MOVED_RECORD_CARDINALITY_VIOLATION)(e.getMessage)
        checkPartitionedRows(tableName, "dep")(Seq(1, "a1", "d1"), Seq(2, "a2", "d1"))
      }
    }
  }

  test("Test MergeInto refuses to move a matched record of a pkless table when the ordering values of its source and update disagree") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName (id int, name string, ts long, dep string) using hudi
           | tblproperties (type = 'cow', preCombineField = 'ts')
           | partitioned by (dep)
           | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)
      spark.sql(s"insert into $tableName values (1, 'a1', 10, 'd1'), (2, 'a2', 10, 'd1')")

      // The source record of id 1 has a higher ordering value than the record and its update a lower one, and the
      // other way round for id 2
      Seq((1, 20L, 5L), (2, 5L, 20L)).foreach { case (id, ts, newTs) =>
        val e = intercept[HoodieException] {
          spark.sql(
            s"""
               | merge into $tableName t
               | using (select $id as id, 'm' as name, ${ts}L as ts, ${newTs}L as new_ts, 'd2' as dep) s
               | on t.id = s.id
               | when matched then update set id = s.id, name = s.name, ts = s.new_ts, dep = s.dep
             """.stripMargin)
        }
        assertResult("MERGE INTO cannot change the partition field(s) `dep` of a matched record in a table without a record " +
          "key when it cannot tell whether the update applies: the ordering value the update gives the record and that " +
          "of the source record must both apply it or both keep the record, under the EVENT_TIME_ORDERING merge mode")(e.getMessage)
      }
      checkAnswer(s"select id, name, ts, dep, _hoodie_partition_path from $tableName order by id")(
        Seq(1, "a1", 10L, "d1", "dep=d1"), Seq(2, "a2", 10L, "d1", "dep=d1"))

      // Both apply the update of id 1, which moves it, and both keep the record of id 2
      spark.sql(
        s"""
           | merge into $tableName t
           | using (select 1 as id, 'a1m' as name, 20L as ts, 30L as new_ts, 'd2' as dep
           |   union all select 2, 'a2m', 5L, 6L, 'd2') s
           | on t.id = s.id
           | when matched then update set id = s.id, name = s.name, ts = s.new_ts, dep = s.dep
         """.stripMargin)
      checkAnswer(s"select id, name, ts, dep, _hoodie_partition_path from $tableName order by id")(
        Seq(1, "a1m", 30L, "d2", "dep=d2"), Seq(2, "a2", 10L, "d1", "dep=d1"))
    }
  }
}
