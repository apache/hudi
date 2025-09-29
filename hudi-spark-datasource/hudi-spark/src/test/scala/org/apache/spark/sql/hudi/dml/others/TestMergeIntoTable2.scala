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

import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkUtils}
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.config.HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Disabled
import org.slf4j.LoggerFactory

class TestMergeIntoTable2 extends HoodieSparkSqlTestBase {
  private val log = LoggerFactory.getLogger(getClass)

  test("Test MergeInto for MOR table 2") {
    spark.sql(s"set ${MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key} = 0")
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val tableName = generateTableName
      // Create a mor partitioned table.
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts int,
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

      val errorMsg = if (HoodieSparkUtils.gteqSpark4_0)
        "[INTERNAL_ERROR] Eagerly executed command failed. You hit a bug in Spark or the Spark plugins you use. Please, report this bug to the corresponding communities or vendors, and provide the full stack trace. SQLSTATE: XX000"
      else
        "assertion failed: Target table's field(price) cannot be the right-value of the update clause for MOR table."

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
      )(errorMsg)
    })
  }

  test("Test Merge Into CTAS Table") {
    withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
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
      val metaClient = createMetaClient(spark, tmp.getCanonicalPath)
      // check record key in hoodie.properties
      assertResult("id")(metaClient.getTableConfig.getRecordKeyFields.get().mkString(","))

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           | select 1 as id, 'a1_1' as name
           |) s0
           |on h0.id = s0.id
           |when matched then update set *
           |""".stripMargin
      )
      checkAnswer(s"select id, name from $tableName")(
        Seq(1, "a1_1")
      )
    }
  }

  test("Test Merge With Complex Data Type") {
    spark.sql(s"set ${MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key} = 0")
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  s_value struct<f0: int, f1: string>,
           |  a_value array<string>,
           |  m_value map<string, string>,
           |  ts int
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
    })
  }

  test("Test column name matching for insert * and update set *") {
    withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val tableName = generateTableName
      // Create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts int,
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

      // NOTE: When using star update/insert clauses (ie `insert *` or `update *`) order of the
      //       columns in the source and target table _have to_ match (Spark won't be applying any column
      //       column resolution logic)
      spark.sql(
        s"""
           |merge into $tableName as t0
           |using (
           |  select 1 as id, 'a1' as name, 97 as price, 1002 as ts, '2021-05-05' as dt union all
           |  select 1 as id, 'a2' as name, 98 as price, 1003 as ts, '2021-05-05' as dt union all
           |  select 2 as id, 'a3' as name, 99 as price, 1001 as ts, '2021-05-05' as dt
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
           |when matched then update set t0.name = s0.name, t0.ts = s0.ts, t0.dt = s0.dt, t0.price = s0.price
           |when not matched then insert (id, name, ts, dt, price) values (s0.id, s0.name, s0.ts, s0.dt, s0.price)
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
           |  select 1 as id, 'a6' as name, 106 as price, 1006 as ts, '2021-05-05' as dt, '0' as flag union all
           |  select 4 as id, 'a4' as name, 100 as price, 1000 as ts, '2021-05-06' as dt, '1' as flag
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
      spark.sql("set hoodie.payload.combined.schema.validate = true")
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
      if (HoodieSparkUtils.gteqSpark4_0) {
        checkExceptionContain(mergeSql)("[COLUMN_ALIASES_NOT_ALLOWED] Column aliases are not allowed in MERGE")
      } else if (HoodieSparkUtils.gteqSpark3_3_2) {
        checkExceptionContain(mergeSql)("Columns aliases are not allowed in MERGE")
      } else {
        spark.sql(mergeSql)
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )
      }
    }
  }

  /* TODO [HUDI-6472]
  test("Test MergeInto When PrimaryKey And PreCombineField Of Source Table And Target Table Differ In Case Only") {
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
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
           | when matched and FLAG = '1' then update set *
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
           | when matched and FLAG = '1' then update set id = s0.id, name = s0.NAME, price = s0.PRICE, ts = s0.ts
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
           | when matched and FLAG = '1' then update set id = s0.id, name = s0.NAME, price = s0.PRICE, ts = s0.ts
           | when not matched and FLAG = '1' then insert *
           |""".stripMargin)
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 11.0, 1001),
        Seq(2, "a2", 12.0, 1002)
      )
    })
  }

  test("Test ignoring case") {
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val tableName = generateTableName
      // Create table
      spark.sql(
        s"""
           |create table $tableName (
           |  ID int,
           |  name string,
           |  price double,
           |  ts int,
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
           |  select 1 as id, 'a1' as name, 20 as PRICE, 1001 as ts, '2021-05-05' as dt
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
           |  select 1 as id, 'a1' as name, 111 as PRICE, 1111 as ts, '2021-05-05' as dt union all
           |  select 2 as id, 'a2' as name, 112 as PRICE, 1112 as ts, '2021-05-05' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched then update set *
           | when not matched then insert *
           |""".stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 111.0, 1111, "2021-05-05"),
        Seq(2, "a2", 112.0, 1112, "2021-05-05")
      )
    })
  }

  test("Test ignoring case for MOR table") {
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val tableName = generateTableName
      // Create a mor partitioned table.
      spark.sql(
        s"""
           | create table $tableName (
           |  ID int,
           |  NAME string,
           |  price double,
           |  ts int,
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
           |  select 1 as id, 'a1' as NAME, 111 as price, 1111 as ts, '2021-05-05' as DT
           | ) as s0
           | on t0.id = s0.id
           | when matched then update set *
           | when not matched then insert *
         """.stripMargin
      )
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 111.0, 1111, "2021-05-05")
      )
    })
  }
*/

  test("Test only insert when source table contains history") {
    withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val tableName = generateTableName
      // Create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts int,
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
    withRecordType()(withTempDir { tmp =>
      spark.sql("set hoodie.payload.combined.schema.validate = true")
      val tableName = generateTableName
      // Create table with multiple keys
      spark.sql(
        s"""
           |create table $tableName (
           |  id1 int,
           |  id2 int,
           |  name string,
           |  price double,
           |  ts int,
           |  dt string
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey ='id1,id2',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
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
           | on t0.id1 = s0.id1 and t0.id2 = s0.id2
           | when not matched then insert *
         """.stripMargin
      )

      checkAnswer(s"select id1, id2, name, price, ts, dt from $tableName")(
        Seq(1, 1, "a1", 1.0, 10, "2022-08-18"),
        Seq(1, 2, "a2", 10.0, 100, "2022-08-18")
      )
    })
  }

  test("Test Merge Into For Source Table With Different Column Order") {
    spark.sql(s"set ${MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key} = 0")
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a mor partitioned table.
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts int,
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
    }
  }

  test("Test Merge into with String cast to Double") {
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      // Create a cow partitioned table.
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts int,
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
    })
  }

  test("Test Merge into where manually set DefaultHoodieRecordPayload") {
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      // Create a cow table with default payload class, check whether it will be overwritten by ExpressionPayload.
      // if not, this ut cannot pass since DefaultHoodieRecordPayload can not promotion int to long when insert a ts with Integer value
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  ts int
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
           |  select 1 as id, 'a2' as name, 1000 as ts
           | ) as s0
           | on t0.id = s0.id
           | when matched then update set t0.name = s0.name, t0.ts = s0.ts
           | when not matched then insert *
         """.stripMargin
      )
      checkAnswer(s"select id,name,ts from $tableName")(
        Seq(1, "a2", 1000)
      )
    })
  }

  test("Test only insert for source table in dup key with preCombineField") {
    spark.sql(s"set ${MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key} = 0")
    Seq("cow", "mor").foreach {
      tableType => {
        withTempDir { tmp =>
          val tableName = generateTableName
          // Create a cow partitioned table with preCombineField
          spark.sql(
            s"""
               | create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts int,
               |  dt string
               | ) using hudi
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               | )
               | partitioned by(dt)
               | location '${tmp.getCanonicalPath}'
         """.stripMargin)
          // Insert data without match condition
          spark.sql(
            s"""
               | merge into $tableName as t0
               | using (
               |  select 1 as id, 'a1' as name, 10.1 as price, 1000 as ts, '2021-03-21' as dt
               |  union all
               |  select 1 as id, 'a2' as name, 10.2 as price, 1002 as ts, '2021-03-21' as dt
               | ) as s0
               | on t0.id = s0.id
               | when not matched then insert *
         """.stripMargin
          )
          checkAnswer(s"select id, name, price, ts, dt from $tableName")(
            Seq(1, "a2", 10.2, 1002, "2021-03-21")
          )

          // Insert data with match condition
          spark.sql(
            s"""
               | merge into $tableName as t0
               | using (
               |  select 1 as id, 'a1_new' as name, 10.1 as price, 1003 as ts, '2021-03-21' as dt
               |  union all
               |  select 3 as id, 'a3' as name, 10.3 as price, 1003 as ts, '2021-03-21' as dt
               |  union all
               |  select 3 as id, 'a3' as name, 10.3 as price, 1003 as ts, '2021-03-21' as dt
               | ) as s0
               | on t0.id = s0.id
               | when matched then update set *
               | when not matched then insert *
         """.stripMargin
          )
          checkAnswer(s"select id, name, price, ts, dt from $tableName")(
            Seq(1, "a1_new", 10.1, 1003, "2021-03-21"),
            Seq(3, "a3", 10.3, 1003, "2021-03-21")
          )
        }
      }
    }
  }

  /**
   * This test relies on duplicate entries with a record key and will be re-enabled as part of HUDI-9708
    */
  ignore("Test only insert for source table in dup key without preCombineField") {
    spark.sql(s"set ${MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key} = ${MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.defaultValue()}")
    Seq("cow", "mor").foreach {
      tableType => {
        withTempDir { tmp =>
          val tableName = generateTableName
          spark.sql(
            s"""
               | create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts int,
               |  dt string
               | ) using hudi
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id'
               | )
               | partitioned by(dt)
               | location '${tmp.getCanonicalPath}'
         """.stripMargin)
          // append records to small file is use update bucket, set this conf use concat handler
          spark.sql("set hoodie.merge.allow.duplicate.on.inserts = true")

          // Insert data without matched condition
          spark.sql(
            s"""
               | merge into $tableName as t0
               | using (
               |  select 1 as id, 'a1' as name, 10.1 as price, 1000 as ts, '2021-03-21' as dt
               |  union all
               |  select 1 as id, 'a2' as name, 10.2 as price, 1002 as ts, '2021-03-21' as dt
               | ) as s0
               | on t0.id = s0.id
               | when not matched then insert *
         """.stripMargin
          )
          checkAnswer(s"select id, name, price, ts, dt from $tableName")(
            Seq(1, "a1", 10.1, 1000, "2021-03-21"),
            Seq(1, "a2", 10.2, 1002, "2021-03-21")
          )

          // Insert data with matched condition
          spark.sql(
            s"""
               | merge into $tableName as t0
               | using (
               |  select 3 as id, 'a3' as name, 10.3 as price, 1003 as ts, '2021-03-21' as dt
               |  union all
               |  select 1 as id, 'a2' as name, 10.4 as price, 1004 as ts, '2021-03-21' as dt
               | ) as s0
               | on t0.id = s0.id
               | when matched then update set *
               | when not matched then insert *
         """.stripMargin
          )
          checkAnswer(s"select id, name, price, ts, dt from $tableName")(
            Seq(1, "a2", 10.4, 1004, "2021-03-21"),
            Seq(1, "a2", 10.4, 1004, "2021-03-21"),
            Seq(3, "a3", 10.3, 1003, "2021-03-21")
          )
        }
      }
    }
  }

  test("Test Merge into with RuntimeReplaceable func such as nvl") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a cow partitioned table with preCombineField
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts int,
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
      spark.sql("set hoodie.merge.allow.duplicate.on.inserts = false")

      spark.sql(
        s"""
           |insert into $tableName values
           |  (1, 'a1', 10, 1000, '2021-03-21'),
           |  (3, 'a3', 10, 2000, '2021-03-21')
         """.stripMargin)

      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 10.1 as price, 1003 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched then update set t0.ts = s0.ts,
           |      t0.price = if(nvl(s0.price,0) <> nvl(t0.price,0), s0.price, t0.price)
           | when not matched then insert *
        """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 10.1, 1003, "2021-03-21"),
        Seq(3, "a3", 10.0, 2000, "2021-03-21")
      )
    }
  }

  test("Test MOR Table with create empty partitions") {
    withTempDir { tmp =>

      val sourceTable = generateTableName
      val path1 = tmp.getCanonicalPath.concat("/source")
      spark.sql(
        s"""
           | create table $sourceTable (
           |  id int,
           |  name string,
           |  price double,
           |  ts int,
           |  dt string
           | ) using hudi
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
           | partitioned by(dt)
           | location '${path1}'
         """.stripMargin)
      spark.sql("set hoodie.merge.allow.duplicate.on.inserts = false")

      spark.sql(s"insert into $sourceTable values(1, 'a1', cast(3.01 as double), 11, '2022-09-26'),(2, 'a2', cast(3.02 as double), 12, '2022-09-27'),(3, 'a3', cast(3.03 as double), 13, '2022-09-28'),(4, 'a4', cast(3.04 as double), 14, '2022-09-29')")

      checkAnswer(s"select id, name, price, ts, dt from $sourceTable order by id")(
        Seq(1, "a1", 3.01, 11,"2022-09-26"),
        Seq(2, "a2", 3.02, 12,"2022-09-27"),
        Seq(3, "a3", 3.03, 13,"2022-09-28"),
        Seq(4, "a4", 3.04, 14,"2022-09-29")
      )

      val path2 = tmp.getCanonicalPath.concat("/target")
      val destTable = generateTableName
      spark.sql(
        s"""
           | create table $destTable (
           |  id int,
           |  name string,
           |  price double,
           |  ts int,
           |  dt string
           | ) using hudi
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
           | partitioned by(dt)
           | location '${path2}'
         """.stripMargin)

      spark.sql(s"insert into $destTable values(1, 'd1', cast(3.01 as double), 11, '2022-09-26'),(2, 'd2', cast(3.02 as double), 12, '2022-09-26'),(3, 'd3', cast(3.03 as double), 13, '2022-09-26')")

      checkAnswer(s"select id, name, price, ts, dt from $destTable order by id")(
        Seq(1, "d1", 3.01, 11,"2022-09-26"),
        Seq(2, "d2", 3.02, 12,"2022-09-26"),
        Seq(3, "d3", 3.03, 13,"2022-09-26")
      )

      // merge operation
      spark.sql(
        s"""
           |merge into $destTable h0
           |using (
           | select id, name, price, ts, dt from $sourceTable
           | ) s0
           | on h0.id = s0.id and h0.dt = s0.dt
           | when matched then update set *
           |""".stripMargin)

      checkAnswer(s"select id, name, price, ts, dt from $destTable order by id")(
        Seq(1, "a1", 3.01, 11,"2022-09-26"),
        Seq(2, "d2", 3.02, 12,"2022-09-26"),
        Seq(3, "d3", 3.03, 13,"2022-09-26")
      )
      // check partitions
      checkAnswer(s"show partitions $destTable")(Seq("dt=2022-09-26"))
    }
  }

  test("Test MergeInto Anti-Patterns of assignment clauses") {
    Seq("cow", "mor").foreach { tableType =>
      Seq("COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING").foreach { mergeMode =>
        withRecordType()(withTempDir { tmp =>
          withSparkSqlSessionConfig(DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key -> "false") {
            log.info(s"Testing table type $tableType with merge mode $mergeMode")

            val tableName = generateTableName
            // Create table with primaryKey and preCombineField
            spark.sql(
              s"""
                 |create table $tableName (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  ts int,
                 |  dt string
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id',
                 |  orderingFields = 'ts',
                 |  recordMergeMode = '$mergeMode'
                 | )
                 | partitioned by(dt)
                 | location '${tmp.getCanonicalPath}'
           """.stripMargin)

            // Insert initial data
            spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, '2021-03-21')")

            // Test 1: Update statements where at least one misses primary key assignment
            if (tableType.equals("mor")) {
              checkException(
                s"""
                   |merge into $tableName as t0
                   |using (
                   |  select 1 as id, 'a1' as name, 11 as price, 1001 as ts, '2021-03-21' as dt
                   |) as s0
                   |on t0.id = s0.id
                   |when matched and s0.id = 1 then update set
                   |  name = s0.name,
                   |  price = s0.price,
                   |  ts = s0.ts,
                   |  dt = s0.dt
                   |when matched and s0.id = 2 then update set *
               """.stripMargin
              )("MERGE INTO field resolution error: No matching assignment found for target table record key field `id`")

              checkException(
                s"""
                   |merge into $tableName as t0
                   |using (
                   |  select 1 as id, 'a1' as name, 11 as price, 1001 as ts, '2021-03-21' as dt
                   |) as s0
                   |on t0.id = s0.id
                   |when matched then update set
                   |  name = s0.name,
                   |  price = s0.price,
                   |  ts = s0.ts,
                   |  dt = s0.dt
               """.stripMargin
              )("MERGE INTO field resolution error: No matching assignment found for target table record key field `id`")
            }

            // Test 2: At least one partial insert assignment clause misses primary key.
            checkException(
              s"""
                 |merge into $tableName as t0
                 |using (
                 |  select 2 as id, 'a2' as name, 12 as price, 1002 as ts, '2021-03-21' as dt
                 |) as s0
                 |on t0.id = s0.id
                 |when not matched and s0.id = 1 then insert (name, price, ts, dt)
                 |values (s0.name, s0.price, s0.ts, s0.dt)
                 |when not matched and s0.id = 2 then insert *
               """.stripMargin
            )("MERGE INTO field resolution error: No matching assignment found for target table record key field `id`")

            checkException(
              s"""
                 |merge into $tableName as t0
                 |using (
                 |  select 2 as id, 'a2' as name, 12 as price, 1002 as ts, '2021-03-21' as dt
                 |) as s0
                 |on t0.id = s0.id
                 |when not matched then insert (name, price, ts, dt)
                 |values (s0.name, s0.price, s0.ts, s0.dt)
               """.stripMargin
            )("MERGE INTO field resolution error: No matching assignment found for target table record key field `id`")

            // Test 3: Partial insert missing preCombineField - only validate for EVENT_TIME_ORDERING
            val mergeStmt =
              s"""
                 |merge into $tableName as t0
                 |using (
                 |  select 2 as id, 'a2' as name, 12 as price, 1002 as ts, '2021-03-21' as dt
                 |) as s0
                 |on t0.id = s0.id
                 |when not matched and s0.id = 1 then insert (id, name, price, dt)
                 |values (s0.id, s0.name, s0.price, s0.dt)
                 |when not matched and s0.id = 2 then insert *
               """.stripMargin

            if (mergeMode == "EVENT_TIME_ORDERING") {
              checkException(mergeStmt)(
                "MERGE INTO field resolution error: No matching assignment found for target table ordering field `ts`"
              )
            } else {
              // For COMMIT_TIME_ORDERING, this should execute without error
              spark.sql(mergeStmt)
            }

            // Verify data state
            if (mergeMode == "COMMIT_TIME_ORDERING") {
              checkAnswer(s"select id, name, price, ts, dt from $tableName order by id")(
                Seq(1, "a1", 10.0, 1000, "2021-03-21"),
                Seq(2, "a2", 12.0, 1002, "2021-03-21")
              )
            } else {
              // For EVENT_TIME_ORDERING, original data should be unchanged due to exception
              checkAnswer(s"select id, name, price, ts, dt from $tableName")(
                Seq(1, "a1", 10.0, 1000, "2021-03-21")
              )
            }
          }
        })
      }
    }
  }

  test("Test merge into Allowed-patterns of assignment clauses") {
    withRecordType()(withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        Seq("COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING").foreach { mergeMode =>
          withSparkSqlSessionConfig(DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key -> "true") {
            val tableName = generateTableName
            spark.sql(
              s"""
                 |create table $tableName (
                 |  id int,
                 |  name string,
                 |  value int,
                 |  ts int
                 |) using hudi
                 | location '${tmp.getCanonicalPath}/$tableName'
                 | partitioned by(value)
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey ='id',
                 |  preCombineField = 'ts',
                 |  recordMergeMode = '$mergeMode'
                 | )
            """.stripMargin)

            spark.sql(s"insert into $tableName select 1 as id, 'a1' as name, 10 as value, 1000 as ts")

            // Test case 1: COW primary key column is not mandatory in the update assignment clause.
            // Covered by test "Test MergeInto with more than once update actions"

            // Test case 2: When partial update feature is on, primary key column is not mandatory in the update
            // assignment clause. Covered by test class org/apache/spark/sql/hudi/dml/TestPartialUpdateForMergeInto.scala.

            // Test case 3: Precombine key column is not mandatory in the update assignment clause.
            val updateStatement =
              s"""
                 |merge into $tableName h0
                 |using (
                 |  select 1 as id, 1003 as ts
                 | ) s0
                 | on h0.id = s0.id
                 | when matched then update set h0.id = s0.id
                 |""".stripMargin


            if (mergeMode == "EVENT_TIME_ORDERING") {
              // For EVENT_TIME_ORDERING, the ordering field is required
              checkException(updateStatement)(
                "MERGE INTO field resolution error: No matching assignment found for target table ordering field `ts`"
              )
            } else {
              spark.sql(
                updateStatement)
              // For COMMIT_TIME_ORDERING, ts should be updated to 1003
              checkAnswer(s"select id, name, value, ts from $tableName")(
                Seq(1, "a1", 10, 1000)
              )
            }
          }
        }
      }
    })
  }

  test("Test MergeInto with commit time/event time ordering coverage") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        Seq(RecordMergeMode.COMMIT_TIME_ORDERING.name(),
          RecordMergeMode.EVENT_TIME_ORDERING.name()).foreach { recordMergeMode =>
          val targetTable = generateTableName
          spark.sql(
            s"""
               |create table $targetTable (
               |  id INT,
               |  name STRING,
               |  price INT,
               |  ts BIGINT
               |) using hudi
               |TBLPROPERTIES (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts',
               |  recordMergeMode = '$recordMergeMode'
               | )
               |LOCATION '${tmp.getCanonicalPath}/$targetTable'
               |""".stripMargin)

          spark.sql(
            s"""
               |INSERT INTO $targetTable
               |SELECT id, name, price, ts
               |FROM (
               |    SELECT 1 as id, 'John Doe' as name, 19 as price, 1598886001 as ts
               |     UNION ALL
               |     SELECT 2, 'Jane Doe', 24, 1598972400
               |     UNION ALL
               |     SELECT 3, 'Bob Smith', 14, 1599058800
               |)
               |""".stripMargin)

          spark.sql(
            s"""
               |MERGE INTO $targetTable t
               |USING (
               | SELECT
               |   CAST(1 AS INT) as id,
               |   CAST('John Doe' AS STRING) as name,
               |   CAST(19 AS INT) as price,
               |   CAST(1 AS BIGINT) as ts
               | UNION ALL
               | SELECT
               |   CAST(4 AS INT),
               |   CAST('Alice Johnson' AS STRING),
               |   CAST(49 AS INT),
               |   CAST(2 AS BIGINT)
               |) s
               |ON t.price = s.price
               |WHEN MATCHED THEN UPDATE SET
               |    t.id = s.id,
               |    t.name = s.name,
               |    t.price = s.price,
               |    t.ts = s.ts
               |WHEN NOT MATCHED THEN INSERT
               |    (id, name, price, ts)
               |VALUES
               |    (s.id, s.name, s.price, s.ts)
               |""".stripMargin)

          checkAnswer(s"select id, name, price, ts from $targetTable ORDER BY id")(
            Seq(1, "John Doe", 19, if (recordMergeMode == RecordMergeMode.EVENT_TIME_ORDERING.name()) 1598886001L else 1L),
            Seq(2, "Jane Doe", 24, 1598972400L),
            Seq(3, "Bob Smith", 14, 1599058800L),
            Seq(4, "Alice Johnson", 49, 2L))
        }
      }
    }
  }
}
