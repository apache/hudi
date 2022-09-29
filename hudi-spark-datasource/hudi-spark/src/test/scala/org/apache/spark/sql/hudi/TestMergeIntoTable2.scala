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

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.Row

class TestMergeIntoTable2 extends HoodieSparkSqlTestBase {

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
           | tblproperties (
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
           |tblproperties(primaryKey = 'id')
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

  test("Test Merge With Complex Data Type") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  s_value struct<f0: int, f1: string>,
           |  a_value array<string>,
           |  m_value map<string, string>,
           |  ts long
           | ) using hudi
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
           | location '${tmp.getCanonicalPath}'
         """.stripMargin)

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |select
           |  1 as id,
           |  'a1' as name,
           |  struct(1, '10') as s_value,
           |  split('a0,a1', ',') as a_value,
           |  map('k0', 'v0') as m_value,
           |  1000 as ts
           |) s0
           |on h0.id = s0.id
           |when not matched then insert *
           |""".stripMargin)

      checkAnswer(s"select id, name, s_value, a_value, m_value, ts from $tableName")(
        Seq(1, "a1", Row(1, "10"), Seq("a0", "a1"), Map("k0" -> "v0"), 1000)
      )
      // update value
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |select
           |  1 as id,
           |  'a1' as name,
           |  struct(1, '12') as s_value,
           |  split('a0,a1,a2', ',') as a_value,
           |  map('k1', 'v1') as m_value,
           |  1000 as ts
           |) s0
           |on h0.id = s0.id
           |when matched then update set *
           |when not matched then insert *
           |""".stripMargin)
      checkAnswer(s"select id, name, s_value, a_value, m_value, ts from $tableName")(
        Seq(1, "a1", Row(1, "12"), Seq("a0", "a1", "a2"), Map("k1" -> "v1"), 1000)
      )
    }
  }

  test("Test column name matching for insert * and update set *") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // Insert data
      spark.sql(s"insert into $tableName select 1, 'a1', 1, 10, '2021-03-21'")
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 1.0, 10, "2021-03-21")
      )

      // Test the order of column types in sourceTable is similar to that in targetTable
      spark.sql(
        s"""
           |merge into $tableName as t0
           |using (
           |  select 1 as id, '2021-05-05' as dt, 1002 as ts, 97 as price, 'a1' as name union all
           |  select 1 as id, '2021-05-05' as dt, 1003 as ts, 98 as price, 'a2' as name union all
           |  select 2 as id, '2021-05-05' as dt, 1001 as ts, 99 as price, 'a3' as name
           | ) as s0
           |on t0.id = s0.id
           |when matched then update set *
           |when not matched then insert *
           |""".stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a2", 98.0, 1003, "2021-05-05"),
        Seq(2, "a3", 99.0, 1001, "2021-05-05")
      )
      // Test the order of the column types of sourceTable is different from the column types of targetTable
      spark.sql(
        s"""
           |merge into $tableName as t0
           |using (
           |  select 1 as id, 'a1' as name, 1004 as ts, '2021-05-05' as dt, 100 as price union all
           |  select 2 as id, 'a5' as name, 1000 as ts, '2021-05-05' as dt, 101 as price union all
           |  select 3 as id, 'a3' as name, 1000 as ts, '2021-05-05' as dt, 102 as price
           | ) as s0
           |on t0.id = s0.id
           |when matched then update set *
           |when not matched then insert *
           |""".stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 100.0, 1004, "2021-05-05"),
        Seq(2, "a3", 99.0, 1001, "2021-05-05"),
        Seq(3, "a3", 102.0, 1000, "2021-05-05")
      )

      // Test an extra input field 'flag'
      spark.sql(
        s"""
           |merge into $tableName as t0
           |using (
           |  select 1 as id, 'a6' as name, 1006 as ts, '2021-05-05' as dt, 106 as price, '0' as flag union all
           |  select 4 as id, 'a4' as name, 1000 as ts, '2021-05-06' as dt, 100 as price, '1' as flag
           | ) as s0
           |on t0.id = s0.id
           |when matched and flag = '1' then update set *
           |when not matched and flag = '1' then insert *
           |""".stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 100.0, 1004, "2021-05-05"),
        Seq(2, "a3", 99.0, 1001, "2021-05-05"),
        Seq(3, "a3", 102.0, 1000, "2021-05-05"),
        Seq(4, "a4", 100.0, 1000, "2021-05-06")
      )
    }
  }

  test("Test MergeInto For Source Table With Column Aliases") {
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
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // Merge with an extra input field 'flag' (insert a new record)
      val mergeSql =
        s"""
           | merge into $tableName
           | using (
           |  select 1, 'a1', 10, 1000, '1'
           | ) s0(id,name,price,ts,flag)
           | on s0.id = $tableName.id
           | when matched and flag = '1' then update set
           | id = s0.id, name = s0.name, price = s0.price, ts = s0.ts
           | when not matched and flag = '1' then insert *
           |""".stripMargin

      if (HoodieSparkUtils.isSpark3) {
        checkExceptionContain(mergeSql)("Columns aliases are not allowed in MERGE")
      } else {
        spark.sql(mergeSql)
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )
      }
    }
  }

  test("Test MergeInto When PrimaryKey And PreCombineField Of Source Table And Target Table Differ In Case Only") {
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
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      spark.sql(
        s"""
           | merge into $tableName
           | using (
           |  select 1 as ID, 'a1' as NAME, 10 as PRICE, 1000 as TS, '1' as FLAG
           | ) s0
           | on s0.ID = $tableName.id
           | when matched and FLAG = '1' then update set
           | id = s0.ID, name = s0.NAME, price = s0.PRICE, ts = s0.TS
           | when not matched and FLAG = '1' then insert *
           |""".stripMargin)
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 10.0, 1000)
      )

      // Test the case of the column names of condition and action is different from that of source table
      spark.sql(
        s"""
           | merge into $tableName
           | using (
           |  select 1 as ID, 'a1' as NAME, 11 as PRICE, 1001 as TS, '1' as FLAG
           | ) s0
           | on s0.id = $tableName.id
           | when matched and FLAG = '1' then update set
           | id = s0.id, name = s0.NAME, price = s0.PRICE, ts = s0.ts
           | when not matched and FLAG = '1' then insert *
           |""".stripMargin)
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 11.0, 1001)
      )

      // Test the case of the column names of cast condition is different from that of source table
      spark.sql(
        s"""
           | merge into $tableName
           | using (
           |  select 2 as ID, 'a2' as NAME, 12 as PRICE, 1002 as TS, '1' as FLAG
           | ) s0
           | on cast(s0.id as int) = $tableName.id
           | when matched and FLAG = '1' then update set
           | id = s0.id, name = s0.NAME, price = s0.PRICE, ts = s0.ts
           | when not matched and FLAG = '1' then insert *
           |""".stripMargin)
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 11.0, 1001),
        Seq(2, "a2", 12.0, 1002)
      )
    }
  }

  test("Test ignoring case") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create table
      spark.sql(
        s"""
           |create table $tableName (
           |  ID int,
           |  name string,
           |  price double,
           |  TS long,
           |  DT string
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | options (
           |  primaryKey ='ID',
           |  preCombineField = 'TS'
           | )
       """.stripMargin)

      // First merge with a extra input field 'flag' (insert a new record)
      spark.sql(
        s"""
           | merge into $tableName
           | using (
           |  select 1 as id, 'a1' as name, 10 as PRICE, 1000 as ts, '2021-05-05' as dt, '1' as flag
           | ) s0
           | on s0.id = $tableName.id
           | when matched and flag = '1' then update set
           | id = s0.id, name = s0.name, PRICE = s0.price, ts = s0.ts, dt = s0.dt
           | when not matched and flag = '1' then insert *
       """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 10.0, 1000, "2021-05-05")
      )

      // Second merge (update the record)
      spark.sql(
        s"""
           | merge into $tableName
           | using (
           |  select 1 as id, 'a1' as name, 20 as PRICE, '2021-05-05' as dt, 1001 as ts
           | ) s0
           | on s0.id = $tableName.id
           | when matched then update set
           | id = s0.id, name = s0.name, PRICE = s0.price, ts = s0.ts, dt = s0.dt
           | when not matched then insert *
       """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 20.0, 1001, "2021-05-05")
      )

      // Test ignoring case when column name matches
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 1111 as ts, '2021-05-05' as dt, 111 as PRICE union all
           |  select 2 as id, 'a2' as name, 1112 as ts, '2021-05-05' as dt, 112 as PRICE
           | ) as s0
           | on t0.id = s0.id
           | when matched then update set *
           | when not matched then insert *
           |""".stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 111.0, 1111, "2021-05-05"),
        Seq(2, "a2", 112.0, 1112, "2021-05-05")
      )
    }
  }

  test("Test ignoring case for MOR table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a mor partitioned table.
      spark.sql(
        s"""
           | create table $tableName (
           |  ID int,
           |  NAME string,
           |  price double,
           |  TS long,
           |  dt string
           | ) using hudi
           | options (
           |  type = 'mor',
           |  primaryKey = 'ID',
           |  preCombineField = 'TS'
           | )
           | partitioned by(dt)
           | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)

      // Test ignoring case when column name matches
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as NAME, 1111 as ts, '2021-05-05' as DT, 111 as price
           | ) as s0
           | on t0.id = s0.id
           | when matched then update set *
           | when not matched then insert *
         """.stripMargin
      )
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 111.0, 1111, "2021-05-05")
      )
    }
  }

  test("Test only insert when source table contains history") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // Insert data
      spark.sql(s"insert into $tableName select 1, 'a1', 1, 10, '2022-08-18'")
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 1.0, 10, "2022-08-18")
      )

      // Insert data which not matched insert-condition.
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 11 as price, 110 as ts, '2022-08-19' as dt union all
           |  select 2 as id, 'a2' as name, 10 as price, 100 as ts, '2022-08-18' as dt
           | ) as s0
           | on t0.id = s0.id
           | when not matched then insert *
         """.stripMargin
      )

      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 1.0, 10, "2022-08-18"),
        Seq(2, "a2", 10.0, 100, "2022-08-18")
      )
    }
  }

  test("Test only insert when source table contains history and target table has multiple keys") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create table with multiple keys
      spark.sql(
        s"""
           |create table $tableName (
           |  id1 int,
           |  id2 int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey ='id1,id2',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      spark.sql("set hoodie.merge.allow.duplicate.on.inserts = true")
      // Insert data
      spark.sql(s"insert into $tableName select 1, 1, 'a1', 1, 10, '2022-08-18'")
      checkAnswer(s"select id1, id2, name, price, ts, dt from $tableName")(
        Seq(1, 1, "a1", 1.0, 10, "2022-08-18")
      )

      // Insert data which not matched insert-condition.
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id1, 1 as id2, 'a1' as name, 11 as price, 110 as ts, '2022-08-19' as dt union all
           |  select 1 as id1, 2 as id2, 'a2' as name, 10 as price, 100 as ts, '2022-08-18' as dt
           | ) as s0
           | on t0.id1 = s0.id1
           | when not matched then insert *
         """.stripMargin
      )

      checkAnswer(s"select id1, id2, name, price, ts, dt from $tableName")(
        Seq(1, 1, "a1", 1.0, 10, "2022-08-18"),
        Seq(1, 2, "a2", 10.0, 100, "2022-08-18")
      )
    }
  }

  test("Test Merge Into For Source Table With Different Column Order") {
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
           | tblproperties (
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
           |  select 'a1' as name, 1 as id, 10 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when not matched and s0.id % 2 = 1 then insert *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 10, "2021-03-21")
      )
    }
  }

  test("Test Merge into with String cast to Double") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a cow partitioned table.
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           | ) using hudi
           | tblproperties (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
           | partitioned by(dt)
           | location '${tmp.getCanonicalPath}'
         """.stripMargin)
      // Insert data
      spark.sql(s"insert into $tableName select 1, 'a1', cast(10.0 as double), 999, '2021-03-21'")
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 'a1' as name, 1 as id, '10.1' as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched then update set t0.price = s0.price, t0.ts = s0.ts
           | when not matched then insert *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 10.1, "2021-03-21")
      )
    }
  }
  test("Test Merge into where manually set DefaultHoodieRecordPayload") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a cow partitioned table.
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  ts long
           | ) using hudi
           | tblproperties (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  hoodie.datasource.write.payload.class = 'org.apache.hudi.common.model.DefaultHoodieRecordPayload'
           | ) location '${tmp.getCanonicalPath}'
         """.stripMargin)
      // Insert data
      spark.sql(s"insert into $tableName select 1, 'a1', 999")
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 'a2' as name, 1 as id, 1000 as ts
           | ) as s0
           | on t0.id = s0.id
           | when matched then update set t0.name = s0.name, t0.ts = s0.ts
           | when not matched then insert *
         """.stripMargin
      )
      checkAnswer(s"select id,name,ts from $tableName")(
        Seq(1, "a2", 1000)
      )
    }
  }
}
