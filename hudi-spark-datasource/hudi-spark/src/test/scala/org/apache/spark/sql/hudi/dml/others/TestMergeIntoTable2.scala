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
import org.apache.hudi.config.HoodieCompactionConfig
import org.apache.hudi.config.HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicLong

class TestMergeIntoTable2 extends HoodieSparkSqlTestBase {
  private val log = LoggerFactory.getLogger(getClass)

  test("Test MergeInto for MOR table 2") {
    withRecordType()(withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true", MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0") {
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

        // Update data from a field of the target table
        spark.sql(
          s"""
             | merge into $tableName as t0
             | using (
             |  select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.id
             | when matched and s0.id % 2 = 1 then update set id = s0.id, name = s0.name,
             |  price = s0.price + t0.price, ts = s0.ts, dt = s0.dt
         """.stripMargin
        )
        checkAnswer(s"select id,name,price,dt from $tableName")(
          Seq(1, "a1", 32, "2021-03-21")
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
      }
    })
  }

  test("Test Merge Into CTAS Table") {
    withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true") {
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
  }

  test("Test Merge With Complex Data Type") {
    withRecordType()(withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true", MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0") {
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
      }
    })
  }

  test("Test column name matching for insert * and update set *") {
    withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true") {
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
  }

  test("Test MergeInto For Source Table With Column Aliases") {
    withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true") {
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
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true") {
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
  }

  test("Test only insert when source table contains history and target table has multiple keys") {
    withRecordType()(withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true") {
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
    }
      })
  }

  test("Test Merge Into For Source Table With Different Column Order") {
    withSQLConf(MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0") {
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

  testWithMergeHandles("Test Merge into where manually set DefaultHoodieRecordPayload") {
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
    withSQLConf(MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0") {
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
  }

  /**
   * This test relies on duplicate entries with a record key and will be re-enabled as part of HUDI-9708
    */
  ignore("Test only insert for source table in dup key without preCombineField") {
    withSQLConf(MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.defaultValue().toString) {
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
            withSQLConf("hoodie.merge.allow.duplicate.on.inserts" -> "true") {
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
    }
  }

  /**
   * Matched clauses that refer to fields of the target table, each with the rows it leaves in a table that holds
   * ids 1 to 3 with dt p1 and ids 4 and 5 with dt p2. The source of the first two statements has a record
   * without a match in p1 and only matched records in p2.
   */
  private val targetFieldCases: Seq[(String, String, Seq[Seq[Any]])] = Seq(
    // id 2 fails the condition, and id 4 passes it on another field than id 1
    ("an update condition",
      """using (select 1 as id, 'a1m' as name, 11.0 as price, 'p1' as dt union all select 2, 'a2m', 21.0, 'p1'
        |  union all select 4, 'a4m', 41.0, 'p2' union all select 9, 'a9', 90.0, 'p1') s
        |on t.id = s.id
        |when matched and (t.price < 15 or t.name = 'a4') then update set *""".stripMargin,
      Seq(Seq(1, "a1m", 11.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4m", 41.0, "p2"),
        Seq(5, "a5", 50.0, "p2"))),
    // id 2 fails both conditions
    ("update and delete conditions with an insert",
      """using (select 1 as id, 'a1m' as name, 11.0 as price, 'p1' as dt union all select 2, 'a2m', 21.0, 'p1'
        |  union all select 3, 'a3m', 31.0, 'p1' union all select 5, 'a5m', 51.0, 'p2' union all select 6, 'a6', 60.0, 'p1') s
        |on t.id = s.id
        |when matched and t.price < 20 then update set *
        |when matched and t.price >= 30 then delete
        |when not matched then insert *""".stripMargin,
      Seq(Seq(1, "a1m", 11.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(6, "a6", 60.0, "p1"))),
    ("assigned values",
      """using (select 3 as id, 1.5 as price, 'p1' as dt union all select 4, 2.5, 'p2') s
        |on t.id = s.id
        |when matched then update set price = t.price + s.price, name = concat(t.name, '-', t.dt)""".stripMargin,
      Seq(Seq(1, "a1", 10.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3-p1", 31.5, "p1"), Seq(4, "a4-p2", 42.5, "p2"),
        Seq(5, "a5", 50.0, "p2"))))

  Seq(("SIMPLE", true, targetFieldCases), ("SIMPLE", false, targetFieldCases.slice(1, 2)),
    ("GLOBAL_SIMPLE", true, targetFieldCases), ("RECORD_INDEX", true, targetFieldCases),
    ("BUCKET", true, targetFieldCases.slice(1, 2))).foreach { case (indexType, partitioned, cases) =>
    cases.foreach { case (name, clauses, rows) =>
      test(s"Test MergeInto with target fields in $name on mor table with $indexType index"
        + s" on a ${if (partitioned) "partitioned" else "non-partitioned"} table") {
        withTempDir { tmp =>
          withSQLConf("hoodie.index.type" -> indexType, "hoodie.bucket.index.num.buckets" -> "2",
            "hoodie.metadata.global.record.level.index.enable" -> (indexType == "RECORD_INDEX").toString) {
            val tableName = generateTableName
            spark.sql(
              s"""
                 | create table $tableName (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  dt string
                 | ) using hudi
                 | tblproperties (
                 |  type = 'mor',
                 |  primaryKey = 'id'
                 | )
                 | ${if (partitioned) "partitioned by(dt)" else ""}
                 | location '${tmp.getCanonicalPath}'
               """.stripMargin)
            spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 'p1'), (2, 'a2', 20.0, 'p1'), (3, 'a3', 30.0, 'p1'),"
              + " (4, 'a4', 40.0, 'p2'), (5, 'a5', 50.0, 'p2')")

            spark.sql(s"merge into $tableName t $clauses")
            checkAnswer(s"select id, name, price, dt from $tableName order by id")(rows: _*)

            withSQLConf(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1") {
              spark.sql(s"call run_compaction(op => 'run', table => '$tableName')")
            }
            assertResult(1)(createMetaClient(spark, tmp.getCanonicalPath).getActiveTimeline.getCommitTimeline
              .filterCompletedInstants.countInstants)
            checkAnswer(s"select id, name, price, dt from $tableName order by id")(rows: _*)
          }
        }
      }
    }
  }

  test("Test MergeInto with target fields in a condition on mor table after a delete in the log") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  price double
           | ) using hudi
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id'
           | )
           | location '${tmp.getCanonicalPath}'
         """.stripMargin)
      spark.sql(s"insert into $tableName values (1, 'a1', 10.0), (2, 'a2', 20.0)")
      // The delete goes to a log file, so the index still locates id 1 through the base file
      spark.sql(s"delete from $tableName where id = 1")
      spark.sql(
        s"""
           | merge into $tableName t
           | using (select 1 as id, 'a1n' as name, 11.0 as price union all select 2, 'a2n', 21.0) s
           | on t.id = s.id
           | when matched and t.price < 25 then update set *
           | when not matched then insert *
         """.stripMargin)
      checkAnswer(s"select id, name, price from $tableName order by id")(Seq(1, "a1n", 11.0), Seq(2, "a2n", 21.0))
    }
  }

  // Under commit time ordering a source record of a key deleted in the log is inserted at the location of the deleted
  // row. Under event time ordering the delete has the ordering value of the source record, and such a record is not
  // written, as the insert could lose to the deleted row, or the statement is refused with the bucket index, which
  // also locates new keys.
  Seq("SIMPLE", "BUCKET").foreach { indexType =>
    Seq(true, false).foreach { commitTimeOrdering =>
      test(s"Test MergeInto with target fields in a condition on mor table with $indexType index and "
        + s"${if (commitTimeOrdering) "commit" else "event"} time ordering after a delete in the log") {
        withTempDir { tmp =>
          withSQLConf("hoodie.index.type" -> indexType, "hoodie.bucket.index.num.buckets" -> "1") {
            val mergeMode = if (commitTimeOrdering) ", recordMergeMode = 'COMMIT_TIME_ORDERING'" else ""
            // The statement that deletes id 1 and the ordering value of its source record
            Seq(("merge", 50L), ("merge", 200L), ("sql", 0L)).foreach { case (delete, deleteOrdering) =>
              val tableName = generateTableName
              spark.sql(
                s"""
                   | create table $tableName (
                   |  id int,
                   |  name string,
                   |  price double,
                   |  ts long
                   | ) using hudi
                   | tblproperties (
                   |  type = 'mor',
                   |  primaryKey = 'id',
                   |  preCombineField = 'ts'$mergeMode
                   | )
                   | location '${tmp.getCanonicalPath}/$tableName'
                 """.stripMargin)
              spark.sql(s"insert into $tableName select 1, 'a1', 10.0D, 100L union all select 2, 'a2', 20.0D, 100L")
              if (delete == "sql") {
                spark.sql(s"delete from $tableName where id = 1")
              } else {
                spark.sql(
                  s"""
                     | merge into $tableName t
                     | using (select 1 as id, 'a1d' as name, 10.0D as price, ${deleteOrdering}L as ts) s
                     | on t.id = s.id
                     | when matched and t.price < 15 then delete
                   """.stripMargin)
              }
              // Under event time ordering a delete with a lower ordering value than the row loses to it
              val deleted = commitTimeOrdering || delete == "sql" || deleteOrdering > 100L
              checkAnswer(s"select id, name, price from $tableName order by id")(
                (if (deleted) Seq.empty else Seq(Seq(1, "a1", 10.0))) :+ Seq(2, "a2", 20.0): _*)
              val reinsert =
                s"""
                   | merge into $tableName t
                   | using (select 1 as id, 'a1n' as name, 5.0D as price, 50L as ts) s
                   | on t.id = s.id
                   | when matched and t.price < 15 then update set *
                   | when not matched then insert *
                 """.stripMargin
              if (deleted && indexType == "BUCKET" && !commitTimeOrdering) {
                checkExceptionContain(reinsert)("is only supported with commit time ordering")
              } else if (deleted) {
                spark.sql(reinsert)
                val expected = (if (commitTimeOrdering) Seq(Seq(1, "a1n", 5.0)) else Seq.empty) :+ Seq(2, "a2", 20.0)
                checkAnswer(s"select id, name, price from $tableName order by id")(expected: _*)
                withSQLConf(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1") {
                  spark.sql(s"call run_compaction(op => 'run', table => '$tableName')")
                }
                spark.sql(s"refresh table $tableName")
                checkAnswer(s"select id, name, price from $tableName order by id")(expected: _*)
              }
            }
          }
        }
      }
    }
  }

  // The read of such a record gives it the default ordering value, which has another type than the source's
  Seq("SIMPLE", "GLOBAL_SIMPLE").foreach { indexType =>
    Seq((", recordMergeMode = 'COMMIT_TIME_ORDERING'", "100L"), ("", "cast(null as bigint)")).foreach { case (mergeMode, existing) =>
      test(s"Test MergeInto with target fields in a condition deletes a record read without an ordering value ($existing) on mor table"
        + s" with $indexType index") {
        withTempDir { tmp =>
          withSQLConf("hoodie.index.type" -> indexType) {
            val tableName = generateTableName
            spark.sql(
              s"""
                 | create table $tableName (
                 |  id int,
                 |  price double,
                 |  ts long
                 | ) using hudi
                 | tblproperties (
                 |  type = 'mor',
                 |  primaryKey = 'id',
                 |  preCombineField = 'ts'$mergeMode
                 | )
                 | location '${tmp.getCanonicalPath}'
               """.stripMargin)
            spark.sql(s"insert into $tableName select 1, 10.0D, $existing union all select 2, 20.0D, $existing")
            spark.sql(
              s"""
                 | merge into $tableName t
                 | using (select 1 as id, 10.0D as price, 50L as ts) s
                 | on t.id = s.id
                 | when matched and t.price < 15 then delete
               """.stripMargin)
            checkAnswer(s"select id, price from $tableName")(Seq(2, 20.0))
          }
        }
      }
    }
  }

  test("Test MergeInto with target fields and an insert clause on mor table with BUCKET index needs commit time ordering") {
    withTempDir { tmp =>
      withSQLConf("hoodie.index.type" -> "BUCKET", "hoodie.bucket.index.num.buckets" -> "1") {
        Seq(true, false).foreach { commitTimeOrdering =>
          val tableName = generateTableName
          val mergeMode = if (commitTimeOrdering) ", recordMergeMode = 'COMMIT_TIME_ORDERING'" else ""
          spark.sql(
            s"""
               | create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               | ) using hudi
               | tblproperties (
               |  type = 'mor',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'$mergeMode
               | )
               | location '${tmp.getCanonicalPath}/$tableName'
             """.stripMargin)
          spark.sql(s"insert into $tableName select 1, 'a1', 10.0D, 100L")
          // id 9 is a new key in the existing bucket
          val statement =
            s"""
               | merge into $tableName t
               | using (select 1 as id, 'a1m' as name, 11.0D as price, 200L as ts union all select 9, 'a9', 90.0D, 200L) s
               | on t.id = s.id
               | when matched and t.price < 15 then update set *
               | when not matched then insert *
             """.stripMargin
          if (commitTimeOrdering) {
            spark.sql(statement)
            checkAnswer(s"select id, name, price from $tableName order by id")(Seq(1, "a1m", 11.0), Seq(9, "a9", 90.0))
          } else {
            checkExceptionContain(statement)("is only supported with commit time ordering")
            checkAnswer(s"select id, name, price from $tableName order by id")(Seq(1, "a1", 10.0))
          }
        }
      }
    }
  }

  test("Test MergeInto with target fields on mor table whose partitions hold the same bucket file ID") {
    withTempDir { tmp =>
      // The file ID of a bucket is the same in each partition under non-blocking concurrency control
      withSQLConf("hoodie.index.type" -> "BUCKET", "hoodie.bucket.index.num.buckets" -> "1",
        "hoodie.write.concurrency.mode" -> "NON_BLOCKING_CONCURRENCY_CONTROL",
        "hoodie.clean.failed.writes.policy" -> "LAZY",
        "hoodie.write.lock.provider" -> "org.apache.hudi.client.transaction.lock.InProcessLockProvider") {
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (
             |  id int,
             |  price double,
             |  dt string
             | ) using hudi
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id'
             | )
             | partitioned by(dt)
             | location '${tmp.getCanonicalPath}'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 10.0, 'd1'), (1, 20.0, 'd2')")
        val timeline = createMetaClient(spark, tmp.getCanonicalPath).getActiveTimeline
        val lastInstant = timeline.getCommitsTimeline.filterCompletedInstants.lastInstant.get
        assertResult(1)(timeline.readCommitMetadata(lastInstant).getFileIdAndRelativePaths.size)

        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 1 as id, 1.0 as price, 'd1' as dt union all select 1, 2.0, 'd2') s
             | on t.id = s.id and t.dt = s.dt
             | when matched then update set price = t.price + s.price
           """.stripMargin)
        checkAnswer(s"select id, price, dt from $tableName order by dt")(Seq(1, 11.0, "d1"), Seq(1, 22.0, "d2"))
      }
    }
  }

  test("Test MergeInto with target fields in a condition packs inserts into small base files on mor table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  price double
           | ) using hudi
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id'
           | )
           | location '${tmp.getCanonicalPath}'
         """.stripMargin)
      spark.sql(s"insert into $tableName values (1, 'a1', 10.0)")
      def merge(source: String): Unit = spark.sql(
        s"""
           | merge into $tableName t
           | using ($source) s
           | on t.id = s.id
           | when matched and t.price < 15 then update set name = concat(t.name, s.name), price = s.price
           | when not matched then insert *
         """.stripMargin)
      def checkFileGroups(expected: Int): Unit =
        checkAnswer(s"select count(distinct split(_hoodie_file_name, '_')[0]) from $tableName")(Seq(expected))

      // An inline compaction would leave the file group of id 1 without log files, and take inserts again
      withSQLConf(HoodieCompactionConfig.INLINE_COMPACT.key -> "false") {
        (2 to 4).foreach(id => merge(s"select $id as id, 'a$id' as name, ${id * 10}.0 as price"))
        checkFileGroups(1)

        // The file group of id 1 takes an update, so id 5 goes to another one, which then takes id 6
        merge("select 1 as id, 'm' as name, 11.0 as price union all select 5, 'a5', 50.0")
        merge("select 6 as id, 'a6' as name, 60.0 as price")
      }
      checkAnswer(s"select id, name, price from $tableName order by id")(
        Seq(1, "a1m", 11.0), Seq(2, "a2", 20.0), Seq(3, "a3", 30.0), Seq(4, "a4", 40.0), Seq(5, "a5", 50.0), Seq(6, "a6", 60.0))
      checkAnswer(s"select count(distinct split(_hoodie_file_name, '_')[0]) from $tableName where id in (5, 6)")(Seq(1))
      checkAnswer(s"select count(distinct split(_hoodie_file_name, '_')[0]) from $tableName where id < 5")(Seq(1))
    }
  }

  test("Test MergeInto with target fields reads the source once and shuffles only matched rows of the table") {
    withTempDir { tmp =>
      // The bucket index tags without a shuffle, and without an ordering field the source rows are not combined
      withSQLConf("hoodie.index.type" -> "BUCKET", "hoodie.bucket.index.num.buckets" -> "1") {
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (
             |  id int,
             |  price double
             | ) using hudi
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id'
             | )
             | location '${tmp.getCanonicalPath}'
           """.stripMargin)
        spark.sql(s"insert into $tableName select cast(id as int), cast(10 as double) from range(0, 1000, 1, 1)")

        val evaluated = spark.sparkContext.longAccumulator("evaluatedSourceRows")
        spark.udf.register("count_source_row", (price: Double) => {
          evaluated.add(1)
          price
        })
        val shuffled = new AtomicLong()
        val listener = new SparkListener {
          override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit =
            Option(taskEnd.taskMetrics).foreach(metrics => shuffled.addAndGet(metrics.shuffleWriteMetrics.recordsWritten))
        }
        spark.sparkContext.addSparkListener(listener)
        try {
          spark.sql(
            s"""
               | merge into $tableName t
               | using (select cast(id as int) as id, count_source_row(cast(1 as double) + id * 0) as price from range(0, 10, 1, 1)) s
               | on t.id = s.id
               | when matched then update set price = t.price + s.price
             """.stripMargin)
          spark.sparkContext.listenerBus.waitUntilEmpty()
        } finally {
          spark.sparkContext.removeSparkListener(listener)
        }
        checkAnswer(s"select count(*), sum(price) from $tableName")(Seq(1000, 10010.0))
        // The write may also probe the source for a first row
        assert(evaluated.value < 20, s"The 10 source rows were evaluated ${evaluated.value} times")
        assert(shuffled.get < 500, s"${shuffled.get} records were shuffled for 10 source rows")
      }
    }
  }

  Seq(Seq("keep", "purge"), Seq("purge", "keep")).foreach { statuses =>
    test(s"Test MergeInto with target fields in a condition on mor table holding a key twice in one base file "
      + s"(${statuses.mkString(", ")})") {
      withTempDir { tmp =>
        val tableName = generateTableName
        withSQLConf("hoodie.index.type" -> "SIMPLE") {
          spark.sql(
            s"""
               | create table $tableName (
               |  id int,
               |  status string
               | ) using hudi
               | tblproperties (
               |  type = 'mor',
               |  primaryKey = 'id'
               | )
               | location '${tmp.getCanonicalPath}'
             """.stripMargin)
          // An insert keeps both rows of id 1
          withSQLConf("hoodie.spark.sql.insert.into.operation" -> "insert") {
            spark.sql(s"insert into $tableName values (1, '${statuses.head}'), (1, '${statuses(1)}'), (2, 'keep')")
          }
          checkAnswer(s"select count(distinct _hoodie_file_name) from $tableName")(Seq(1))

          // Each row of id 1 is evaluated against the clause, also once a log file holds the first update
          Seq(("purge", "updated"), ("updated", "twice")).foreach { case (from, to) =>
            spark.sql(
              s"""
                 | merge into $tableName t
                 | using (select 1 as id) s
                 | on t.id = s.id
                 | when matched and t.status = '$from' then update set status = '$to'
               """.stripMargin)
            checkAnswer(s"select id, status from $tableName order by id, status")(
              Seq(1, "keep"), Seq(1, to), Seq(2, "keep"))
          }

          withSQLConf(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1") {
            spark.sql(s"call run_compaction(op => 'run', table => '$tableName')")
          }
          checkAnswer(s"select id, status from $tableName order by id, status")(
            Seq(1, "keep"), Seq(1, "twice"), Seq(2, "keep"))
        }
      }
    }
  }

  test("Test MergeInto with a matched condition on a target field refused on mor table without record key") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  price double
           | ) using hudi
           | tblproperties (
           |  type = 'mor'
           | )
           | location '${tmp.getCanonicalPath}'
         """.stripMargin)
      spark.sql(s"insert into $tableName values (1, 'a1', 10.0), (2, 'a2', 20.0)")
      // The source records are written with the locations the join with the table gives them, and their matched
      // clauses are evaluated without the records they match, so the statements are refused before a commit starts
      val instants = createMetaClient(spark, tmp.getCanonicalPath).getActiveTimeline.getInstants
      Seq("update set *", "delete").foreach { action =>
        checkExceptionContain(
          s"""
             | merge into $tableName t
             | using (select 1 as id, 'a1m' as name, 11.0 as price union all select 2, 'a2m', 21.0) s
             | on t.id = s.id
             | when matched and t.price < 15 then $action
           """.stripMargin)(
          "Target table's field(price) cannot be used in the condition of a WHEN MATCHED clause for MOR table without a record key")
      }
      assertResult(instants)(createMetaClient(spark, tmp.getCanonicalPath).getActiveTimeline.getInstants)
      checkAnswer(s"select id, name, price from $tableName order by id")(Seq(1, "a1", 10.0), Seq(2, "a2", 20.0))
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
      withSQLConf("hoodie.merge.allow.duplicate.on.inserts" -> "false") {
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
      withSQLConf("hoodie.merge.allow.duplicate.on.inserts" -> "false") {
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
              // `value` is the partition column and is supplied by neither the source, the ON
              // condition, nor the assignments, so the statement is rejected.
              checkExceptionContain(updateStatement)(
                "Failed to resolve partition fields"
              )
              checkAnswer(s"select id, name, value, ts from $tableName")(
                Seq(1, "a1", 10, 1000)
              )
            }
          }
        }
      }
    })
  }

  testWithMergeHandles("Test MergeInto with commit time/event time ordering coverage") {
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

  test("Test MergeInto With A Merge Condition That Is Always False") {
    withTempDir { tmp =>
      // (record key, table type, index type, whether the index finds every key the table holds)
      val keyProp = "primaryKey = 'id',"
      Seq(("", "cow", "SIMPLE", true), ("", "mor", "SIMPLE", true), (keyProp, "cow", "SIMPLE", true),
        (keyProp, "cow", "BLOOM", true), (keyProp, "mor", "RECORD_INDEX", true), (keyProp, "mor", "SIMPLE", false),
        (keyProp, "mor", "BUCKET", false), (keyProp, "cow", "BUCKET", false)).foreach {
        case (recordKeyProp, tableType, indexType, findsEveryKey) =>
          withSQLConf("hoodie.index.type" -> indexType, "hoodie.bucket.index.num.buckets" -> "2",
            "hoodie.metadata.global.record.level.index.enable" -> (indexType == "RECORD_INDEX").toString) {
            val tableName = generateTableName
            spark.sql(
              s"""
                 | create table $tableName (id int, name string, price double) using hudi
                 | tblproperties ($recordKeyProp type = '$tableType')
                 | location '${tmp.getCanonicalPath}/$tableName'
               """.stripMargin)
            spark.sql(s"insert into $tableName values (1, 'a1', 10.0)")
            def source(ids: Int*): String =
              ids.map(id => s"select $id as id, 'a$id' as name, ${id * 10.0}D as price").mkString("(", " union all ", ")")
            def insertOnly(ids: Int*): String =
              s"merge into $tableName t using ${source(ids: _*)} s on 1 != 1 when not matched then insert *"

            if (!findsEveryKey) {
              // The index cannot tell whether the table holds a key, so the statement stays refused
              checkExceptionContain(insertOnly(2))(
                "Currently only equality predicates are supported in MERGE INTO statement on record key table")
              checkAnswer(s"select id, name, price from $tableName")(Seq(1, "a1", 10.0))
            } else {
              // No source record matches, so the not-matched clause inserts all of them, the one with the id of the
              // table included on a table without a record key
              val ids = if (recordKeyProp.isEmpty) Seq(1, 2, 3) else Seq(2, 3)
              spark.sql(
                s"""
                   | merge into $tableName t using ${source(ids: _*)} s on 1 != 1
                   | when matched then update set *
                   | when not matched then insert *
                 """.stripMargin)
              val inserted = Seq(1, "a1", 10.0) +: ids.map(id => Seq(id, s"a$id", id * 10.0))
              checkAnswer(s"select id, name, price from $tableName order by id, price")(inserted: _*)

              // The first not-matched clause that accepts a source record inserts it, and no clause may accept it
              spark.sql(
                s"""
                   | merge into $tableName t using ${source(5, 6, 7)} s on false
                   | when matched then delete
                   | when not matched and s.id = 5 then insert (id, name) values (s.id, concat(s.name, '-first'))
                   | when not matched and s.id < 7 then insert *
                 """.stripMargin)
              // Without a not-matched clause the statement writes nothing
              spark.sql(s"merge into $tableName t using ${source(8)} s on false when matched then delete")
              val afterClauses = inserted ++ Seq(Seq(5, "a5-first", null), Seq(6, "a6", 60.0))
              checkAnswer(s"select id, name, price from $tableName order by id, price")(afterClauses: _*)

              if (recordKeyProp.nonEmpty) {
                // A key the table already holds fails the insert, whatever the duplicate key policy of an INSERT INTO
                Seq("none", "drop").foreach { policy =>
                  withSQLConf("hoodie.datasource.insert.dup.policy" -> policy) {
                    checkExceptionContain(insertOnly(6, 9))("Duplicate key found for insert statement, key is: ")
                  }
                }
                checkAnswer(s"select id, name, price from $tableName order by id, price")(afterClauses: _*)

                // Two inserted source records with one key the table does not hold fail the insert too
                val sameKey = s"(select 10 as id, 'new' as name, 1.0D as price union all " +
                  "select 10 as id, 'other' as name, 2.0D as price)"
                checkExceptionContain(s"merge into $tableName t using $sameKey s on false when not matched then insert *")(
                  "Duplicate key found for insert statement, key is: 10")
                checkAnswer(s"select id, name, price from $tableName order by id, price")(afterClauses: _*)
                // A source record no clause inserts does not count
                spark.sql(s"merge into $tableName t using $sameKey s on false when not matched and s.name = 'new' then insert *")
                checkAnswer(s"select id, name, price from $tableName order by id, price")(
                  afterClauses :+ Seq(10, "new", 1.0): _*)

                // Another merge condition that is not an equality on the record key stays refused
                Seq("true", "t.id > s.id").foreach { condition =>
                  checkExceptionContain(
                    s"merge into $tableName t using ${source(9)} s on $condition when not matched then insert *")(
                    "Currently only equality predicates are supported in MERGE INTO statement on record key table")
                }
              }
            }
          }
      }

      // The insert clauses must assign the ordering field of an event time ordering table, as in any MERGE INTO
      val orderedTable = generateTableName
      spark.sql(
        s"""
           | create table $orderedTable (id int, name string, ts long) using hudi
           | tblproperties (primaryKey = 'id', preCombineField = 'ts', type = 'cow')
           | location '${tmp.getCanonicalPath}/$orderedTable'
         """.stripMargin)
      checkExceptionContain(s"merge into $orderedTable t using (select 1 as id, 'a1' as name, 1L as ts) s on false " +
        "when not matched then insert (id, name) values (s.id, s.name)")(
        "No matching assignment found for target table ordering field `ts`")
      checkAnswer(s"select id from $orderedTable")()
    }
  }

  test("Test MergeInto With A Merge Condition That Is Always False Resolves Partition Fields And Evaluates Clause Conditions Once") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName (id int, name string, dt string) using hudi
           | tblproperties (primaryKey = 'id', type = 'cow')
           | partitioned by (dt)
           | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)
      spark.sql(s"insert into $tableName values (1, 'a1', 'p1')")

      // The source has to supply the partition field, as in a merge on the record key
      checkExceptionContain(s"merge into $tableName t using (select 2 as id, 'a2' as name) s on false " +
        "when not matched then insert (id, name) values (s.id, s.name)")("Failed to resolve partition fields `dt`")
      checkAnswer(s"select id, name, dt from $tableName")(Seq(1, "a1", "p1"))

      // A clause condition that is not deterministic decides each source record once
      spark.sql(s"merge into $tableName t using (select cast(id as int) as id, concat('a', id) as name, 'p2' as dt " +
        "from range(2, 42)) s on false when not matched and rand(7) < 0.5 then insert *")
      val inserted = spark.sql(s"select id, name, dt from $tableName where id > 1").collect()
      assert(inserted.nonEmpty && inserted.length < 40, s"inserted ${inserted.length} of 40 source records")
      assert(inserted.forall(row => row.getString(1) == s"a${row.getInt(0)}" && row.getString(2) == "p2"))

      // A clause that leaves out the partition field takes it from the source, and one that assigns it keeps its value
      spark.sql(s"merge into $tableName t using (select 60 as id, 'a60' as name, 'p3' as dt union all " +
        "select 61 as id, 'a61' as name, 'p3' as dt) s on false " +
        "when not matched and s.id = 60 then insert (id, name) values (s.id, s.name) " +
        "when not matched then insert (id, name, dt) values (s.id, s.name, 'p4')")
      checkAnswer(s"select id, name, dt, _hoodie_partition_path from $tableName where id >= 60 order by id")(
        Seq(60, "a60", "p3", "dt=p3"), Seq(61, "a61", "p4", "dt=p4"))

      // The clause is refused before the statement runs
      if (HoodieSparkUtils.gteqSpark3_4) {
        checkExceptionContain(s"merge into $tableName t using (select 50 as id, 'a50' as name, 'p1' as dt) s on false " +
          "when not matched then insert * when not matched by source then delete")(
          "WHEN NOT MATCHED BY SOURCE clause is not supported in MERGE INTO statement on a Hudi table")
      }
    }
  }

  test("Test MergeInto With A Merge Condition That Is Always False Checks Inserted Records By The Key They Are Written With") {
    withTempDir { tmp =>
      // Two records of which the key generator makes one record key, from null and from empty key fields
      val compositeTable = generateTableName
      spark.sql(
        s"""
           | create table $compositeTable (id string, part string, price double) using hudi
           | tblproperties (primaryKey = 'id,part', type = 'cow')
           | location '${tmp.getCanonicalPath}/$compositeTable'
         """.stripMargin)
      spark.sql(s"insert into $compositeTable values ('x', 'p1', 10.0)")
      Seq(("cast(null as string)", "cast(null as string)", "id:a,part:__null__"), ("''", "'__empty__'", "id:a,part:__empty__")).foreach {
        case (firstPart, secondPart, recordKey) =>
          checkExceptionContain(s"merge into $compositeTable t using (select 'a' as id, $firstPart as part, 1.0D as price " +
            s"union all select 'a', $secondPart, 2.0D) s on false when not matched then insert *")(
            s"Duplicate key found for insert statement, key is: $recordKey")
      }
      checkAnswer(s"select id, part, price from $compositeTable")(Seq("x", "p1", 10.0))

      // Two equal values of which the key generator makes two record keys
      val doubleTable = generateTableName
      spark.sql(
        s"""
           | create table $doubleTable (id double, name string) using hudi
           | tblproperties (primaryKey = 'id', type = 'cow')
           | location '${tmp.getCanonicalPath}/$doubleTable'
         """.stripMargin)
      spark.sql(s"insert into $doubleTable values (1.0D, 'one')")
      spark.sql(s"merge into $doubleTable t using (select 0.0D as id, 'positive' as name union all " +
        "select cast('-0.0' as double), 'negative') s on false when not matched then insert *")
      checkAnswer(s"select name, _hoodie_record_key from $doubleTable order by name")(
        Seq("negative", "-0.0"), Seq("one", "1.0"), Seq("positive", "0.0"))

      // One record key in two partitions is two keys to a partition-local index and one key to a global index
      Seq(("SIMPLE", false), ("BLOOM", false), ("GLOBAL_SIMPLE", true), ("GLOBAL_BLOOM", true),
        ("RECORD_LEVEL_INDEX", false), ("GLOBAL_RECORD_LEVEL_INDEX", true)).foreach {
        case (indexType, isGlobal) =>
          withSQLConf("hoodie.index.type" -> indexType,
            "hoodie.metadata.global.record.level.index.enable" -> (indexType == "GLOBAL_RECORD_LEVEL_INDEX").toString,
            "hoodie.metadata.record.level.index.enable" -> (indexType == "RECORD_LEVEL_INDEX").toString) {
            val tableName = generateTableName
            spark.sql(
              s"""
                 | create table $tableName (id int, name string, dt string) using hudi
                 | tblproperties (primaryKey = 'id', type = 'cow')
                 | partitioned by (dt)
                 | location '${tmp.getCanonicalPath}/$tableName'
               """.stripMargin)
            spark.sql(s"insert into $tableName values (1, 'a1', 'p1')")
            def insertBoth(firstDt: String, secondDt: String): String =
              s"merge into $tableName t using (select 2 as id, 'a2' as name, '$firstDt' as dt " +
                s"union all select 2, 'b2', '$secondDt') s on false when not matched then insert *"

            checkExceptionContain(insertBoth("p2", "p2"))("Duplicate key found for insert statement, key is: 2")
            checkAnswer(s"select id, name, dt from $tableName")(Seq(1, "a1", "p1"))
            if (isGlobal) {
              checkExceptionContain(insertBoth("p1", "p2"))("Duplicate key found for insert statement, key is: 2")
              checkAnswer(s"select id, name, dt from $tableName")(Seq(1, "a1", "p1"))
            } else {
              spark.sql(insertBoth("p1", "p2"))
              checkAnswer(s"select id, name, dt from $tableName order by id, dt")(
                Seq(1, "a1", "p1"), Seq(2, "a2", "p1"), Seq(2, "b2", "p2"))
            }
          }
      }
    }
  }

  test("Test MergeInto With A Merge Condition That Is Always False Needs An Index That Finds Every Key") {
    withTempDir { tmp =>
      // Until the record level index is built, a lookup falls back to an index that reads the keys of the base files
      Seq(("cow", true), ("mor", false)).foreach { case (tableType, findsEveryKey) =>
        withSQLConf("hoodie.index.type" -> "RECORD_INDEX",
          "hoodie.metadata.global.record.level.index.enable" -> "false") {
          val tableName = generateTableName
          spark.sql(
            s"""
               | create table $tableName (id int, name string, price double) using hudi
               | tblproperties (primaryKey = 'id', type = '$tableType')
               | location '${tmp.getCanonicalPath}/$tableName'
             """.stripMargin)
          spark.sql(s"insert into $tableName values (1, 'a1', 10.0)")
          def insertOnly(id: Int): String =
            s"merge into $tableName t using (select $id as id, 'b$id' as name, 20.0D as price) s on false when not matched then insert *"
          if (findsEveryKey) {
            checkExceptionContain(insertOnly(1))("Duplicate key found for insert statement, key is: 1")
            spark.sql(insertOnly(2))
            checkAnswer(s"select id, name, price from $tableName order by id")(Seq(1, "a1", 10.0), Seq(2, "b2", 20.0))
          } else {
            Seq(1, 2).foreach { id =>
              checkExceptionContain(insertOnly(id))(
                "Currently only equality predicates are supported in MERGE INTO statement on record key table")
            }
            checkAnswer(s"select id, name, price from $tableName")(Seq(1, "a1", 10.0))
          }
        }
      }

      // A write that disables the metadata table drops the record level index that is built
      withSQLConf("hoodie.index.type" -> "RECORD_INDEX", "hoodie.metadata.global.record.level.index.enable" -> "true") {
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (id int, name string, price double) using hudi
             | tblproperties (primaryKey = 'id', type = 'mor')
             | location '${tmp.getCanonicalPath}/$tableName'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0)")
        // An index that can index log files writes the insert to a log file
        withSQLConf("hoodie.index.type" -> "INMEMORY") {
          spark.sql(s"insert into $tableName values (2, 'a2', 20.0)")
        }
        val insertOnly =
          s"merge into $tableName t using (select 2 as id, 'b2' as name, 30.0D as price) s on false when not matched then insert *"
        checkExceptionContain(insertOnly)("Duplicate key found for insert statement, key is: 2")
        withSQLConf("hoodie.metadata.enable" -> "false") {
          checkExceptionContain(insertOnly)(
            "Currently only equality predicates are supported in MERGE INTO statement on record key table")
        }
        checkAnswer(s"select id, name, price from $tableName order by id")(Seq(1, "a1", 10.0), Seq(2, "a2", 20.0))
      }

      // A write that does not enable the record level index drops the built one when it rolls a failed write back
      withSQLConf("hoodie.index.type" -> "RECORD_INDEX", "hoodie.metadata.global.record.level.index.enable" -> "true") {
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (id int, name string, price double) using hudi
             | tblproperties (primaryKey = 'id', type = 'mor')
             | location '${tmp.getCanonicalPath}/$tableName'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0)")
        withSQLConf("hoodie.index.type" -> "INMEMORY") {
          spark.sql(s"insert into $tableName values (2, 'a2', 20.0)")
        }
        val insertOnly =
          s"merge into $tableName t using (select 2 as id, 'b2' as name, 30.0D as price) s on false when not matched then insert *"
        withSQLConf("hoodie.metadata.global.record.level.index.enable" -> "false") {
          Seq("Duplicate key found for insert statement, key is: 2",
            "the record level index is not available, so the write cannot tell whether the table holds a record key",
            "Currently only equality predicates are supported in MERGE INTO statement on record key table").foreach { error =>
            checkExceptionContain(insertOnly)(error)
            checkAnswer(s"select id, name, price from $tableName order by id")(Seq(1, "a1", 10.0), Seq(2, "a2", 20.0))
          }
        }
      }

      // Nor does a write that disables the metadata table read the record level index of a copy-on-write table
      withSQLConf("hoodie.index.type" -> "RECORD_INDEX", "hoodie.metadata.global.record.level.index.enable" -> "true") {
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (id int, name string, price double) using hudi
             | tblproperties (primaryKey = 'id', type = 'cow')
             | location '${tmp.getCanonicalPath}/$tableName'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0)")
        withSQLConf("hoodie.metadata.enable" -> "false") {
          checkExceptionContain(
            s"merge into $tableName t using (select 2 as id, 'b2' as name, 30.0D as price) s on false when not matched then insert *")(
            "Currently only equality predicates are supported in MERGE INTO statement on record key table")
        }
        checkAnswer(s"select id, name, price from $tableName")(Seq(1, "a1", 10.0))
      }

      // An index class takes precedence over the index type
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName (id int, name string, price double) using hudi
           | tblproperties (primaryKey = 'id', type = 'cow')
           | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)
      spark.sql(s"insert into $tableName values (1, 'a1', 10.0)")
      withSQLConf("hoodie.index.class" -> "org.apache.hudi.index.HoodieInMemoryHashIndex") {
        checkExceptionContain(
          s"merge into $tableName t using (select 2 as id, 'b2' as name, 20.0D as price) s on false when not matched then insert *")(
          "Currently only equality predicates are supported in MERGE INTO statement on record key table")
      }
      checkAnswer(s"select id, name, price from $tableName")(Seq(1, "a1", 10.0))
    }
  }

}
