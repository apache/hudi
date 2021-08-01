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

package org.apache.spark.sql.hudi

import org.apache.hudi.common.table.HoodieTableMetaClient

class TestMergeIntoTable2 extends TestHoodieSqlBase {

  test("Test MergeInto for MOR table 2") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a mor partitioned table.
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           | ) using hudi
           | options (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
           | partitioned by(dt)
           | location '${tmp.getCanonicalPath}'
         """.stripMargin)
      // Insert data which matched insert-condition.
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when not matched and s0.id % 2 = 1 then insert *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 10, "2021-03-21")
      )

      // Insert data which not matched insert-condition.
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 2 as id, 'a2' as name, 10 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when not matched and s0.id % 2 = 1 then insert *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 10, "2021-03-21")
      )

      // Update data which not matched update-condition
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 11 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched and s0.id % 2 = 0 then update set *
           | when matched and s0.id % 3 = 2 then delete
           | when not matched then insert *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 10, "2021-03-21")
      )

      // Update data which matched update-condition
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 11 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched and s0.id % 2 = 1 then update set id = s0.id, name = s0.name,
           |  price = s0.price * 2, ts = s0.ts, dt = s0.dt
           | when not matched then insert (id,name,price,ts,dt) values(s0.id, s0.name, s0.price, s0.ts, s0.dt)
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 22, "2021-03-21")
      )

      // Delete data which matched update-condition
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 11 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched and s0.id % 2 = 0 then update set id = s0.id, name = s0.name,
           |  price = s0.price * 2, ts = s0.ts, dt = s0.dt
           | when matched and s0.id % 2 = 1 then delete
           | when not matched then insert (id,name,price,ts,dt) values(s0.id, s0.name, s0.price, s0.ts, s0.dt)
         """.stripMargin
      )
      checkAnswer(s"select count(1) from $tableName")(
        Seq(0)
      )

      checkException(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched and s0.id % 2 = 1 then update set id = s0.id, name = s0.name,
           |  price = s0.price + t0.price, ts = s0.ts, dt = s0.dt
         """.stripMargin
      )("assertion failed: Target table's field(price) cannot be the right-value of the update clause for MOR table.")
    }
  }

  test("Test Merge Into CTAS Table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName using hudi
           |options(primaryKey = 'id')
           |location '${tmp.getCanonicalPath}'
           |as
           |select 1 as id, 'a1' as name
           |""".stripMargin
      )
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tmp.getCanonicalPath)
        .setConf(spark.sessionState.newHadoopConf())
        .build()
      // check record key in hoodie.properties
      assertResult("id")(metaClient.getTableConfig.getRecordKeyFields.get().mkString(","))

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           | select 1 as s_id, 'a1_1' as name
           |) s0
           |on h0.id = s0.s_id
           |when matched then update set *
           |""".stripMargin
      )
      checkAnswer(s"select id, name from $tableName")(
        Seq(1, "a1_1")
      )
    }
  }

  test("Test MergeInto For Source Table With ColumnAliases") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath.replaceAll("\\\\", "/")}/$tableName'
           | options (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // First merge with a extra input field 'flag' (insert a new record)
      spark.sql(
        s"""
           | merge into $tableName
           | using (
           |  select 1, 'a1', 10, 1000, '1'
           | ) s0 (id,name,price,ts,flag)
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
           |  select 1 , 'a1', 10 , 1001
           | ) s0 (id,name,price,ts)
           | on s0.id = $tableName.id
           | when matched then update set
           | id = s0.id, name = s0.name, price = s0.price + $tableName.price, ts = s0.ts
           | when not matched then insert *
             """.stripMargin)
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 20.0, 1001)
      )

      // Third merge (update & insert the record)
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select * from (
           |  select 1, 'a1', 10, 1002
           |  union all
           |  select 2, 'a2', 12, 1001
           |  )
           | ) s0 (id,name,price,ts)
           | on s0.id = t0.id
           | when matched then update set
           | t0.id = s0.id, t0.name = s0.name, t0.price = s0.price + t0.price, t0.ts = s0.ts
           | when not matched and s0.id % 2 = 0 then insert *
             """.stripMargin)
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 30.0, 1002),
        Seq(2, "a2", 12.0, 1001)
      )

      // Fourth merge (delete the record)
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1, 'a1', 12, 1003
           | ) s0 (id,name,price,ts)
           | on s0.id = t0.id
           | when matched and id != 1 then update set
           | t0.id = s0.id, t0.name = s0.name, t0.price = s0.price, t0.ts = s0.ts
           | when matched and s0.id = 1 then delete
           | when not matched then insert *
             """.stripMargin)
      val cnt = spark.sql(s"select * from $tableName where id = 1").count()
      assertResult(0)(cnt)
    }
  }
}
