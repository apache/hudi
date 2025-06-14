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
import org.apache.hudi.ScalaAssertionSupport

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
        spark.sql(s"set ${SPARK_SQL_OPTIMIZED_WRITES.key()}=$sparkSqlOptimizedWrites")

        spark.sql(
          s"""
             |merge into $tableName as oldData
             |using $tableName2
             |on oldData.id = $tableName2.id
             |when matched and oldData.price = $tableName2.price then update set oldData.name = $tableName2.name
             |
             |""".stripMargin)

        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "u1", 10.0, 100),
          Seq(3, "a3", 20.0, 100),
          Seq(2, "a2", 20.0, 200)
        )

        spark.sql(
          s"""
             |merge into $tableName as oldData
             |using $tableName2
             |on oldData.id = $tableName2.id and oldData.price = $tableName2.price
             |when matched then update set oldData.name = $tableName2.name
             |when not matched then insert *
             |""".stripMargin)

        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "u1", 10.0, 100),
          Seq(2, "a2", 20.0, 200),
          Seq(3, "u3", 20.0, 100),
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

        if (sparkSqlOptimizedWrites) {
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
        } else {
          spark.sql(
            s"""
               | merge into $tableName3 as t0
               | using (
               |  select * from $tableName2
               | ) as s0
               | on t0.id = s0.id
               | when matched then update set id = t0.id, name = t0.name,
               |  price = t0.price, ts = s0.ts
               | when not matched then insert (id,name,price,ts) values(s0.id, s0.name, s0.price, s0.ts)
           """.stripMargin
          )
          checkAnswer(s"select id, name, price, ts from $tableName3")(
            Seq(1, "a1", 10.0, 100),
            Seq(1, "u1", 10.0, 999),
            Seq(2, "a2", 20.0, 200),
            Seq(3, "u3", 20.0, 9999),
            Seq(4, "u4", 40.0, 99999)
          )
        }
      }
    }
  }

  test("Test pkless complex merge cond") {
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      spark.sql(s"set ${SPARK_SQL_OPTIMIZED_WRITES.key()}=true")
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

    })
  }

  test("Test pkless multiple source match") {
    for (withPrecombine <- Seq(true, false)) {
      withTempDir { tmp =>
        spark.sql("set hoodie.payload.combined.schema.validate = true")
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

        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 1 as id, 'a1' as name, 20 as price, 200 as ts
             |  union all
             |  select 2 as id, 'a1' as name, 30 as price, 100 as ts
             | ) s0
             | on $tableName.name = s0.name
             | when matched then update set price = s0.price
             | when not matched then insert *
         """.stripMargin)
        if (withPrecombine) {
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 20.0, 100)
          )
        } else {
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 30.0, 100)
          )
        }
      }
    }

  }

  test("Test MergeInto Basic pkless") {
    withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      spark.sql(s"set ${SPARK_SQL_OPTIMIZED_WRITES.key()}=true")
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
