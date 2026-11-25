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
import org.apache.hudi.HoodieSparkUtils.gteqSpark3_5
import org.apache.hudi.common.config.{HoodieMetadataConfig, RecordMergeMode}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig}
import org.apache.hudi.config.HoodieWriteConfig.{MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT, WRITE_UPDATES_AS_DELETES_AND_INSERTS}
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.slf4j.LoggerFactory

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
        // Two source rows match target row 1, which only passes with the cardinality check off
        withSQLConf(DataSourceWriteOptions.MERGE_INTO_CARDINALITY_CHECK.key -> "false") {
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
        }

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

  test("Test Merge Into Applies Store Assignment Policy") {
    if (gteqSpark3_5) {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  v int,
             |  s struct<n1:int not null, n2:struct<dn1:int, dn2:int>>
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (primaryKey = 'id', type = 'cow', 'hoodie.table.format' = 'test-format')
           """.stripMargin)
        spark.sql(s"insert into $tableName select 1, 10, named_struct('n1', 1, 'n2', named_struct('dn1', 3, 'dn2', 4))")
        spark.sql(
          """
            |create or replace temporary view store_assignment_source as
            |select 1 as id, named_struct('n1', 1) as c2, 'str' as c3, named_struct('dn3', 1, 'dn1', 2) as c4,
            |  cast(20 as bigint) as c5
            |union all
            |select 2, named_struct('n1', 2), 'str', named_struct('dn3', 3, 'dn1', 4), cast(30 as bigint)
          """.stripMargin)
        val merge = (action: String) =>
          s"merge into $tableName t using store_assignment_source s on t.id = s.id when $action"

        withSQLConf("spark.sql.storeAssignmentPolicy" -> "ansi") {
          checkExceptionContain(merge("matched then update set t.s = s.c2"))(
            "Cannot find data for the output column `s`.`n2`")
          checkExceptionContain(merge("matched then update set t.s.n1 = s.c3"))(
            "Cannot safely cast `s`.`n1` \"STRING\" to \"INT\"")
          checkExceptionContain(merge("matched then update set t.s.n2 = s.c4"))(
            "Cannot find data for the output column `s`.`n2`.`dn2`")
          checkExceptionContain(merge("matched then update set t.s.n1 = null"))("value appeared in non-nullable field")
          checkExceptionContain(merge("not matched then insert (id, v, s) values (s.id, s.c3, null)"))(
            "Cannot safely cast `v` \"STRING\" to \"INT\"")
          spark.sql(merge(
            "matched then update set t.v = s.c5, t.s = named_struct('n2', named_struct('dn2', 6, 'dn1', 5), 'n1', 1)"))
        }
        withSQLConf("spark.sql.storeAssignmentPolicy" -> "strict") {
          checkExceptionContain(merge("matched then update set t.v = s.c5"))("Cannot safely cast `v` \"BIGINT\" to \"INT\"")
          checkExceptionContain(merge("matched then update set t.s.n1 = null"))(
            "Cannot safely cast `s`.`n1` \"VOID\" to \"INT\"")
          checkExceptionContain(merge("not matched then insert (id, v, s) values (s.id, s.c5, null)"))(
            "Cannot safely cast `v` \"BIGINT\" to \"INT\"")
          checkExceptionContain(merge("not matched then insert (id, v, s) values (s.id, 30, null)"))(
            "Cannot safely cast `s` \"VOID\" to \"STRUCT<")
          spark.sql(merge("not matched then insert (id, v, s) values " +
            "(s.id, 30, named_struct('n1', 3, 'n2', named_struct('dn1', 7, 'dn2', 8)))"))
        }
        withSQLConf("spark.sql.storeAssignmentPolicy" -> "legacy") {
          checkExceptionContain(merge("matched then update set t.v = s.c5"))("LEGACY store assignment policy is disallowed")
        }
        checkAnswer(s"select id, v, s from $tableName order by id")(
          Seq(1, 20, Row(1, Row(5, 6))),
          Seq(2, 30, Row(3, Row(7, 8))))

        // A table of the native format keeps casting the values to the types of the columns
        val nativeTable = generateTableName
        spark.sql(
          s"""
             |create table $nativeTable (id int, v int) using hudi
             | location '${tmp.getCanonicalPath}/$nativeTable'
             | tblproperties (primaryKey = 'id', type = 'cow')
           """.stripMargin)
        spark.sql(s"insert into $nativeTable select 1, 10")
        withSQLConf("spark.sql.storeAssignmentPolicy" -> "strict") {
          spark.sql(s"merge into $nativeTable t using store_assignment_source s on t.id = s.id " +
            "when matched then update set t.v = s.c5")
        }
        checkAnswer(s"select id, v from $nativeTable")(Seq(1, 20))
      }
    }
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

  Seq("cow", "mor").foreach { tableType =>
    Seq(false, true).foreach { updatesAsDeletesAndInserts =>
      test(s"Test MergeInto without update clause on $tableType table without ordering field"
        + s" writing updates as deletes and inserts $updatesAsDeletesAndInserts") {
        withTempDir { tmp =>
          withSQLConf(WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() -> updatesAsDeletesAndInserts.toString) {
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
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | partitioned by(dt)
                 | location '${tmp.getCanonicalPath}'
               """.stripMargin)
            spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 'p1'), (2, 'a2', 20.0, 'p1'), (3, 'a3', 30.0, 'p1'),"
              + " (4, 'a4', 40.0, 'p2'), (5, 'a5', 50.0, 'p2')")

            // Delete only with a condition: id 2 fails the condition and id 9 has no match
            spark.sql(
              s"""
                 | merge into $tableName t
                 | using (select 1 as id, 'p1' as dt, 'del' as op union all select 2, 'p1', 'keep' union all select 9, 'p1', 'del') s
                 | on t.id = s.id
                 | when matched and s.op = 'del' then delete
               """.stripMargin)
            checkAnswer(s"select id, name, price, dt from $tableName")(
              Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"))

            // Delete only without a condition: id 8 has no match
            spark.sql(
              s"""
                 | merge into $tableName t
                 | using (select 2 as id, 'p1' as dt union all select 8, 'p2') s
                 | on t.id = s.id
                 | when matched then delete
               """.stripMargin)
            checkAnswer(s"select id, name, price, dt from $tableName")(
              Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"))

            // Insert only without a condition: the matched id 3 is neither inserted again nor updated
            spark.sql(
              s"""
                 | merge into $tableName t
                 | using (select 3 as id, 'a3n' as name, 31.0 as price, 'p1' as dt union all select 6, 'a6', 60.0, 'p2') s
                 | on t.id = s.id
                 | when not matched then insert *
               """.stripMargin)
            checkAnswer(s"select id, name, price, dt from $tableName")(
              Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"), Seq(6, "a6", 60.0, "p2"))

            // Insert only with a condition: id 4 is matched, the unmatched id 10 fails the condition, and so do
            // two of the three source records of the unmatched id 7
            spark.sql(
              s"""
                 | merge into $tableName t
                 | using (select 4 as id, 'a4n' as name, 71.0 as price, 'p2' as dt union all select 7, 'a7x', 5.0, 'p1'
                 |  union all select 7, 'a7', 70.0, 'p1' union all select 7, 'a7y', 6.0, 'p1' union all select 10, 'a10', 5.0, 'p1') s
                 | on t.id = s.id
                 | when not matched and s.price > 50 then insert *
               """.stripMargin)
            checkAnswer(s"select id, name, price, dt from $tableName")(
              Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"), Seq(6, "a6", 60.0, "p2"),
              Seq(7, "a7", 70.0, "p1"))

            // Delete and insert: the matched id 3 fails the delete condition and is not inserted again
            spark.sql(
              s"""
                 | merge into $tableName t
                 | using (select 5 as id, 'a5n' as name, 51.0 as price, 'p2' as dt, 'del' as op
                 |  union all select 3, 'a3n', 31.0, 'p1', 'keep' union all select 11, 'a11', 110.0, 'p1', 'ins') s
                 | on t.id = s.id
                 | when matched and s.op = 'del' then delete
                 | when not matched then insert (id, name, price, dt) values (s.id, s.name, s.price, s.dt)
               """.stripMargin)
            checkAnswer(s"select id, name, price, dt from $tableName")(
              Seq(3, "a3", 30.0, "p1"), Seq(4, "a4", 40.0, "p2"), Seq(6, "a6", 60.0, "p2"), Seq(7, "a7", 70.0, "p1"),
              Seq(11, "a11", 110.0, "p1"))
          }
        }
      }
    }
  }

  /**
   * Matched clauses that refer to fields of the target table, each with the rows it leaves in a table that holds
   * ids 1 to 3 with dt p1 and ids 4 and 5 with dt p2. The source of the first three statements has a record
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
    // id 2 fails the condition
    ("a delete condition",
      """using (select 2 as id, 'p1' as dt union all select 3, 'p1' union all select 5, 'p2' union all select 9, 'p1') s
        |on t.id = s.id
        |when matched and t.price > 25 then delete""".stripMargin,
      Seq(Seq(1, "a1", 10.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(4, "a4", 40.0, "p2"))),
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

  Seq(("SIMPLE", true, targetFieldCases), ("SIMPLE", false, targetFieldCases.slice(2, 3)),
    ("GLOBAL_SIMPLE", true, targetFieldCases), ("RECORD_INDEX", true, targetFieldCases),
    ("BUCKET", true, targetFieldCases.slice(2, 3))).foreach { case (indexType, partitioned, cases) =>
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
               |ON t.id = s.id
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

  Seq(("cow", true, false), ("cow", false, false), ("mor", true, false), ("mor", true, true), ("mor", false, false)).foreach {
    case (tableType, keyed, updatesAsDeletesAndInserts) =>
      test(s"Test MergeInto with the target table as source on $tableType table with record key $keyed"
        + s" writing updates as deletes and inserts $updatesAsDeletesAndInserts") {
        withTempDir { tmp =>
          withSQLConf(WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() -> updatesAsDeletesAndInserts.toString) {
            Seq(false, true).foreach { cached =>
              val tableName = generateTableName
              val keyProp = if (keyed) ", primaryKey = 'id'" else ""
              spark.sql(
                s"""
                   | create table $tableName (id int, v string) using hudi
                   | tblproperties (type = '$tableType' $keyProp)
                   | location '${tmp.getCanonicalPath}/$tableName'
                 """.stripMargin)
              spark.sql(s"insert into $tableName values (1, 'v1'), (2, 'v2')")
              if (cached) {
                spark.sql(s"cache table $tableName")
              }
              spark.sql(
                s"""
                   | merge into $tableName t
                   | using $tableName s
                   | on t.id = s.id
                   | when matched and s.id = 1 then update set id = s.id, v = 'x'
                   | when not matched then insert *
                 """.stripMargin)
              checkAnswer(s"select id, v from $tableName order by id")(Seq(1, "x"), Seq(2, "v2"))
              if (cached) {
                spark.sql(s"uncache table $tableName")
              }
            }
          }
        }
      }
  }

  test("Test MergeInto refuses to reassign a partition field of a table with record key and a non-global index") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (id int, name string, dt string) using hudi
             | tblproperties (type = '$tableType', primaryKey = 'id')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}/$tableName'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 'p1'), (2, 'a2', 'p1')")

        checkExceptionContain(
          s"""
             | merge into $tableName t
             | using (select 1 as id, 'a1n' as name, 'p1' as dt, 'p9' as new_dt) s
             | on t.id = s.id
             | when matched then update set id = s.id, name = s.name, dt = s.new_dt
           """.stripMargin)(
          "Detected disallowed assignment clause in MERGE INTO statement for partition field `dt`")
        // The source value the record is looked up under can still be assigned
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 1 as id, 'a1n' as name, 'p1' as dt) s
             | on t.id = s.id
             | when matched then update set *
           """.stripMargin)
        checkAnswer(s"select id, name, dt, _hoodie_partition_path from $tableName order by id")(
          Seq(1, "a1n", "p1", "dt=p1"), Seq(2, "a2", "p1", "dt=p1"))
      }
    }
  }

  test("Test MergeInto With Nested Field Assignments") {
    withTempDir { tmp =>
      Seq(("cow", "primaryKey = 'id',"), ("cow", ""), ("mor", "primaryKey = 'id',")).foreach { case (tableType, keyProp) =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  s struct<c1:int, c2:struct<a:array<int>, m:map<string, string>>>
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties ($keyProp type = '$tableType')
           """.stripMargin)
        spark.sql(s"insert into $tableName select 1, named_struct('c1', 2, 'c2', named_struct('a', array(1, 2), 'm', map('a', 'b')))")
        spark.sql(s"insert into $tableName select 2, null")
        val mergeNested =
          s"""
             |merge into $tableName t
             |using (select 1 as id, -2 as c1 union all select 2, 5) s
             |on t.id = s.id
             |when matched then update set t.s.c1 = s.c1, t.s.c2.a = array(-1, -2)
           """.stripMargin

        if (!gteqSpark3_5) {
          checkExceptionContain(mergeNested)("Assigning a nested field requires Spark 3.5 or later")
        } else {
          spark.sql(mergeNested)
          checkAnswer(s"select id, s from $tableName order by id")(
            Seq(1, Row(-2, Row(Seq(-1, -2), Map("a" -> "b")))),
            Seq(2, Row(5, Row(Seq(-1, -2), null))))

          checkExceptionContain(
            s"""
               |merge into $tableName t using (select 3 as id) s on t.id = s.id
               |when not matched then insert (id, s.c1) values (s.id, 1)
             """.stripMargin)("INSERT assignment keys cannot be nested fields")
          checkExceptionContain(
            s"""
               |merge into $tableName t using (select 1 as id) s on t.id = s.id
               |when matched then update set t.s.c1 = 1, t.s.c1 = 2
             """.stripMargin)("Multiple assignments for 's.c1'")
        }
      }
    }
  }

  test("Test MergeInto And Update Refuse Assignments To A Field Nested In An Array Or A Map") {
    withTempDir { tmp =>
      // a nested field assignment needs Spark 3.5
      Seq("primaryKey = 'id',", "").filter(_ => gteqSpark3_5).foreach { keyProp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  a array<struct<c1:int, c2:int>>,
             |  m map<string, string>
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties ($keyProp type = 'cow')
           """.stripMargin)
        spark.sql(s"insert into $tableName select 1, array(named_struct('c1', 2, 'c2', 3)), map('k', 'v')")
        val merge = s"merge into $tableName t using (select 1 as id, -2 as c2) s on t.id = s.id when matched then "

        Seq(
          merge + "update set t.a.c1 = s.c2",
          merge + "update set t.a.c1 = array(-2)",
          merge + "update set t.m.key = 'new_key'",
          s"update $tableName set a.c1 = 1",
          s"update $tableName set m.key = 'new_key'").foreach { statement =>
          checkExceptionContain(statement)("Updating nested fields is only supported for StructType")
        }
        checkAnswer(s"select id, a, m from $tableName")(Seq(1, Seq(Row(2, 3)), Map("k" -> "v")))
      }
    }
  }

  test("Test MergeInto Refuses Conditions The Write Cannot Evaluate Per Record") {
    withTempDir { tmp =>
      Seq("primaryKey = 'id',", "").foreach { keyProp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  v int
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties ($keyProp type = 'cow')
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 10), (2, 20)")
        val source = "(select 1 as id, -1 as v union all select 3, -3)"

        checkExceptionContain(
          s"merge into $tableName t using $source s on t.id = s.id and rand() > 0.5 when matched then delete")(
          "MERGE operation contains unsupported SEARCH condition. Non-deterministic expressions are not allowed")
        checkExceptionContain(
          s"merge into $tableName t using $source s on t.id = s.id when matched and rand() > 0.5 then delete")(
          "MERGE operation contains unsupported DELETE condition. Non-deterministic expressions are not allowed")
        checkExceptionContain(
          s"merge into $tableName t using $source s on t.id = s.id when not matched and rand() > 0.5 then insert *")(
          "MERGE operation contains unsupported INSERT condition. Non-deterministic expressions are not allowed")
        checkExceptionContain(
          s"merge into $tableName t using $source s on t.id = s.id and max(t.id) = 1 when matched then delete")(
          "MERGE operation contains unsupported SEARCH condition. Aggregates are not allowed")
        checkExceptionContain(
          s"""
             |merge into $tableName t using $source s on t.id = s.id
             |when matched and t.id < (select max(id) from $source) then update set v = s.v
           """.stripMargin)("MERGE operation contains unsupported UPDATE condition. Subqueries are not allowed")
        checkExceptionContain(
          s"""
             |merge into $tableName t using $source s on t.id = s.id
             |when not matched and s.id in (select id from $source) then insert *
           """.stripMargin)("MERGE operation contains unsupported INSERT condition. Subqueries are not allowed")
        checkExceptionContain(
          s"merge into $tableName t using $source s on t.id = s.id when matched and sum(t.id) < 1 then delete")(
          "MERGE operation contains unsupported DELETE condition. Aggregates are not allowed")
        checkAnswer(s"select id, v from $tableName order by id")(Seq(1, 10), Seq(2, 20))

        if (keyProp.isEmpty) {
          // the merge condition of a table without a record key is a join condition, which evaluates a subquery
          spark.sql(
            s"""
               |merge into $tableName t using $source s on t.id = s.id and t.id < (select max(id) from $source)
               |when matched then update set v = s.v
               |when not matched then insert *
             """.stripMargin)
          checkAnswer(s"select id, v from $tableName order by id")(Seq(1, -1), Seq(2, 20), Seq(3, -3))
        }
      }
    }
  }

  test("Test MergeInto With A Source Key Of Another Integral Type") {
    withTempDir { tmp =>
      // a wider source key type relies on Spark 3.5 casting an inserted value with an ANSI cast
      for (tableType <- Seq("cow", "mor"); keyProp <- Seq("primaryKey = 'id',", "") if gteqSpark3_5) {
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  v int
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties ($keyProp type = '$tableType')
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 10), (2, 20)")

        spark.sql(
          s"""
             |merge into $tableName t
             |using (select 1L as id, 11 as v union all select 3L, 30) s
             |on t.id = s.id
             |when matched then update set *
             |when not matched then insert *
           """.stripMargin)
        checkAnswer(s"select id, v from $tableName order by id")(Seq(1, 11), Seq(2, 20), Seq(3, 30))

        // a source key out of the range of the key column cannot be inserted, and on a table with a record
        // key, whose record key is rendered from the source key, it fails the statement
        val outOfRange = "(select 4294967297L as id, 40 as v)"
        checkExceptionContain(
          s"merge into $tableName t using $outOfRange s on t.id = s.id when not matched then insert *")("overflow")
        if (keyProp.nonEmpty) {
          checkExceptionContain(
            s"merge into $tableName t using $outOfRange s on t.id = s.id when matched then update set v = s.v")("overflow")
          checkExceptionContain(
            s"""
               |merge into $tableName t using $outOfRange s on t.id = s.id
               |when not matched then insert (id, v) values (cast(s.id as int), s.v)
             """.stripMargin)("overflow")
          checkExceptionContain(
            s"""
               |merge into $tableName t using (select 4294967297L as sid, 40 as v) s on t.id = s.sid
               |when not matched then insert (id, v) values (cast(s.sid as int), s.v)
             """.stripMargin)("overflow")
          // a source that is not a local relation, whose key is only read when the statement runs
          spark.range(4294967297L, 4294967298L).selectExpr("id", "40 as v").createOrReplaceTempView(s"${tableName}_src")
          checkExceptionContain(
            s"merge into $tableName t using ${tableName}_src s on t.id = s.id when matched then update set v = s.v")("overflow")
          checkExceptionContain(
            s"merge into $tableName t using ${tableName}_src s on t.id = s.id when not matched then insert *")("overflow")
        }
        checkAnswer(s"select id, v from $tableName order by id")(Seq(1, 11), Seq(2, 20), Seq(3, 30))

        spark.sql(
          s"""
             |merge into $tableName t
             |using (select 2L as sid, 21 as v union all select 4L, 40) s
             |on t.id = s.sid
             |when matched then update set id = s.sid, v = s.v
             |when not matched then insert (id, v) values (s.sid, s.v)
           """.stripMargin)
        checkAnswer(s"select id, v from $tableName order by id")(Seq(1, 11), Seq(2, 21), Seq(3, 30), Seq(4, 40))
      }

      if (gteqSpark3_5) {
        // every record key field is checked, including one of a composite record key
        val compositeKeyTable = generateTableName
        spark.sql(
          s"""
             |create table $compositeKeyTable (
             |  id int,
             |  k int,
             |  v int
             |) using hudi
             | location '${tmp.getCanonicalPath}/$compositeKeyTable'
             | tblproperties (primaryKey = 'id,k', type = 'cow')
           """.stripMargin)
        spark.sql(s"insert into $compositeKeyTable values (1, 1, 10)")
        checkExceptionContain(
          s"""
             |merge into $compositeKeyTable t using (select 2 as id, 4294967297L as k, 40 as v) s
             |on t.id = s.id and t.k = s.k
             |when not matched then insert (id, k, v) values (s.id, cast(s.k as int), s.v)
           """.stripMargin)("overflow")
        checkAnswer(s"select id, k, v from $compositeKeyTable")(Seq(1, 1, 10))
      }

      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id bigint,
           |  v int
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (primaryKey = 'id', type = 'cow')
         """.stripMargin)
      spark.sql(s"insert into $tableName values (1, 10)")
      spark.sql(
        s"""
           |merge into $tableName t
           |using (select 1 as id, 11 as v union all select 3, 30) s
           |on t.id = s.id
           |when matched then update set *
           |when not matched then insert *
         """.stripMargin)
      checkAnswer(s"select id, v from $tableName order by id")(Seq(1L, 11), Seq(3L, 30))
    }
  }

  test("Test MergeInto and Update move a record across partitions of a pluggable-format table with a record key") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             | create table $tableName (id int, name string, dep string) using hudi
             | tblproperties (type = '$tableType', primaryKey = 'id', 'hoodie.table.format' = 'test-format',
             |   'hoodie.table.format.test.metadata.table.enabled' = 'true')
             | partitioned by (dep)
             | location '$tablePath'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 'd1'), (2, 'a2', 'd1'), (3, 'a3', 'd2'), (4, 'a4', 'd2')")

        // id 1 takes the partition the source names, id 2 one other than the source value it is looked up under
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 1 as id, 'a1m' as name, 'd2' as dep, 'd2' as new_dep
             |   union all select 2, 'a2m', 'd1', 'd3' union all select 4, 'a4m', 'd2', 'd2') s
             | on t.id = s.id
             | when matched and s.id = 1 then update set *
             | when matched and s.id = 2 then update set id = s.id, name = s.name, dep = s.new_dep
             | when matched and s.id = 4 then update set id = s.id, name = s.name, dep = s.new_dep
           """.stripMargin)
        spark.sql(s"update $tableName set dep = 'd1', name = 'a3u' where id = 3")
        checkPartitionedRows(tableName, "dep")(
          Seq(1, "a1m", "d2"), Seq(2, "a2m", "d3"), Seq(3, "a3u", "d1"), Seq(4, "a4m", "d2"))
        if (tableType == "mor") {
          compactAndCheck(tableName)
          checkPartitionedRows(tableName, "dep")(
            Seq(1, "a1m", "d2"), Seq(2, "a2m", "d3"), Seq(3, "a3u", "d1"), Seq(4, "a4m", "d2"))
        }
      }
    }
  }

  test("Test MergeInto finds a record that Update moves across partitions through the record index") {
    withSQLConf("hoodie.index.type" -> "RECORD_INDEX", HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true",
      HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE.key -> "true") {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          val tableName = generateTableName
          spark.sql(
            s"""
               | create table $tableName (id int, name string, dep string) using hudi
               | tblproperties (type = '$tableType', primaryKey = 'id')
               | partitioned by (dep)
               | location '${tmp.getCanonicalPath}/$tableName'
             """.stripMargin)
          spark.sql(s"insert into $tableName values (1, 'a1', 'd1'), (2, 'a2', 'd1')")
          spark.sql(s"update $tableName set dep = 'd2' where id = 1")
          checkPartitionedRows(tableName, "dep")(Seq(1, "a1", "d2"), Seq(2, "a2", "d1"))

          // The source names the partition id 1 was moved from, and one id 2 moves to
          spark.sql(
            s"""
               | merge into $tableName t
               | using (select 1 as id, 'a1m' as name, 'd1' as dep union all select 2, 'a2m', 'd3') s
               | on t.id = s.id
               | when matched and s.id = 1 then update set id = s.id, name = s.name, dep = t.dep
               | when matched and s.id = 2 then update set *
               | when not matched then insert *
             """.stripMargin)
          checkPartitionedRows(tableName, "dep")(Seq(1, "a1m", "d2"), Seq(2, "a2m", "d3"))
        }
      }
    }
  }

  test("Test MergeInto and Update refuse to move a record of a pluggable-format table with a non-global index") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             | create table $tableName (id int, name string, dep string) using hudi
             | tblproperties (type = '$tableType', primaryKey = 'id', 'hoodie.table.format' = 'test-format',
             |   'hoodie.table.format.test.metadata.table.enabled' = 'true', 'hoodie.index.type' = 'SIMPLE')
             | partitioned by (dep)
             | location '$tablePath'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 'd1')")
        checkExceptionContain(
          s"""
             | merge into $tableName t
             | using (select 1 as id, 'a1m' as name, 'd1' as dep, 'd9' as new_dep) s
             | on t.id = s.id
             | when matched then update set id = s.id, name = s.name, dep = s.new_dep
           """.stripMargin)(
          "Detected disallowed assignment clause in MERGE INTO statement for partition field `dep`")
        checkExceptionContain(s"update $tableName set dep = 'd9' where id = 1")(
          "Detected disallowed assignment clause in UPDATE statement for partition field `dep`")
        checkPartitionedRows(tableName, "dep")(Seq(1, "a1", "d1"))
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

  test("Test MergeInto applies the first matched clause that holds, in the order written") {
    val deleteClause = "when matched and s.flag in ('both', 'delete') then delete"
    val updateClause = "when matched and s.flag in ('both', 'update') then update set " +
      "id = s.id, name = s.name, price = s.price, ts = s.ts"
    for (tableType <- Seq("cow", "mor"); deleteFirst <- Seq(true, false)) {
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
             | tblproperties (type = '$tableType', primaryKey = 'id', preCombineField = 'ts')
             | location '${tmp.getCanonicalPath}'
             |""".stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000), (2, 'a2', 20, 1000), (3, 'a3', 30, 1000)")
        val clauses = if (deleteFirst) s"$deleteClause $updateClause" else s"$updateClause $deleteClause"
        spark.sql(
          s"""
             |merge into $tableName t
             |using (
             |  select 1 as id, 'u1' as name, 11D as price, 1001L as ts, 'both' as flag
             |  union all select 2, 'u2', 22D, 1001L, 'update'
             |  union all select 3, 'u3', 33D, 1001L, 'delete'
             |) s
             |on t.id = s.id
             |$clauses
             |""".stripMargin)
        val updatedFirstRow = if (deleteFirst) Seq.empty else Seq(Seq(1, "u1", 11.0, 1001))
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          updatedFirstRow :+ Seq(2, "u2", 22.0, 1001): _*)
      }
    }
  }

  test("Test MergeInto applies the first of several matched update clauses that holds") {
    def updateClause(condition: String, name: String): String =
      s"when matched and $condition then update set id = s.id, name = '$name', price = s.price, ts = s.ts"
    Seq("cow", "mor").foreach { tableType =>
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
             | tblproperties (type = '$tableType', primaryKey = 'id', preCombineField = 'ts')
             | location '${tmp.getCanonicalPath}'
             |""".stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000)")
        val clauses = Seq(
          updateClause("s.flag = 'a'", "c1"),
          updateClause("s.flag = 'x'", "c2"),
          updateClause("s.flag = 'x'", "c3"),
          updateClause("s.price > 1", "c4"),
          updateClause("s.price > 2", "c5"),
          updateClause("s.price > 3", "c6"))
        spark.sql(
          s"""
             |merge into $tableName t
             |using (select 1 as id, 11D as price, 1001L as ts, 'x' as flag) s
             |on t.id = s.id
             |${clauses.mkString("\n")}
             |""".stripMargin)
        checkAnswer(s"select id, name, price, ts from $tableName")(Seq(1, "c2", 11.0, 1001))
      }
    }
  }

  test("Test MergeInto without update clause inserts a source row whose key was deleted") {
    withTempDir { tmp =>
      for (tableType <- Seq("cow", "mor"); indexType <- Seq("SIMPLE", "BLOOM")) {
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (
             |  id int,
             |  name string,
             |  price double
             | ) using hudi
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  hoodie.index.type = '$indexType'
             | )
             | location '${tmp.getCanonicalPath}/$tableName'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0), (2, 'a2', 20.0)")
        // On a mor table the delete goes to a log file, while the base file still holds id 1
        spark.sql(s"delete from $tableName where id = 1")
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 1 as id, 'n1' as name, 11.0 as price
             |  union all select 2, 'n2', 21.0 union all select 3, 'n3', 31.0) s
             | on t.id = s.id
             | when not matched then insert *
           """.stripMargin)
        checkAnswer(s"select id, name, price from $tableName")(
          Seq(1, "n1", 11.0), Seq(2, "a2", 20.0), Seq(3, "n3", 31.0))
      }
    }
  }

  test("Test MergeInto With Only Insert Clauses Leaves A Matched Record Out Of The Insert Condition") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withSQLConf("spark.sql.ansi.enabled" -> "true") {
          val tableName = generateTableName
          spark.sql(
            s"""
               | create table $tableName (id int, value string, ts long) using hudi
               | tblproperties (primaryKey = 'id', preCombineField = 'ts', type = '$tableType')
               | location '${tmp.getCanonicalPath}/$tableName'
             """.stripMargin)
          spark.sql(s"insert into $tableName values (1, 'bad', 1)")
          def mergeFrom(rows: (Int, String)*): Unit = spark.sql(
            s"""
               | merge into $tableName t
               | using (${rows.map { case (id, value) => s"select $id as id, '$value' as value, 2L as ts" }.mkString(" union all ")}) s
               | on t.id = s.id
               | when not matched and cast(s.value as int) > 0 then insert *
             """.stripMargin)

          // The insert condition cannot be evaluated on the source record of id 1, which matches a record of the
          // table and so writes nothing, as no clause applies to a matched record. The ordering field makes the
          // statement an upsert, which looks the records up before it evaluates the insert condition
          mergeFrom((1, "bad"), (2, "5"), (3, "0"))
          checkAnswer(s"select id, value from $tableName order by id")(Seq(1, "bad"), Seq(2, "5"))
          // A source record that matches no record still has its insert condition evaluated
          checkNestedExceptionContains(new Runnable {
            override def run(): Unit = mergeFrom((4, "bad"))
          })("CAST_INVALID_INPUT")
          checkAnswer(s"select id, value from $tableName order by id")(Seq(1, "bad"), Seq(2, "5"))
        }
      }
    }
  }

  test("Test MergeInto With A Source Key Column The Merge Condition Does Not Match On") {
    withTempDir { tmp =>
      for (tableType <- Seq("cow", "mor");
           source <- Seq("select 2L as id, 1L as match_id, 99 as v", "select 2 as id, 1 as match_id, 99 as v");
           updateClause <- Seq("update set v = s.v", "update set *")) {
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  v int
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (primaryKey = 'id', type = '$tableType')
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 10), (2, 20)")
        // the source column id would render the record key 2, while the merge condition matches id 1
        checkExceptionContain(
          s"merge into $tableName t using ($source) s on t.id = s.match_id when matched then $updateClause")(
          "Invalid MERGE INTO matching condition")
        checkAnswer(s"select id, v from $tableName order by id")(Seq(1, 10), Seq(2, 20))
      }
    }
  }

  test("Test MergeInto deletes by record key from a source without the partition field through a global index that keeps records in their partition") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName (id int, name string, ts long, dep string) using hudi
           | tblproperties (type = 'cow', primaryKey = 'id', preCombineField = 'ts', 'hoodie.index.type' = 'GLOBAL_SIMPLE',
           |   '${HoodieIndexConfig.SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE.key}' = 'false')
           | partitioned by (dep)
           | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)
      spark.sql(s"insert into $tableName values (1, 'a1', 10, 'd1'), (2, 'a2', 10, 'd2')")

      spark.sql(
        s"""
           | merge into $tableName t
           | using (select 1 as id, 20L as ts) s
           | on t.id = s.id
           | when matched then delete
         """.stripMargin)
      checkAnswer(s"select id, name, ts, dep from $tableName order by id")(Seq(2, "a2", 10L, "d2"))
    }
  }

  test("Test MergeInto and Update move a record across partitions of a table with a record key and a global index") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (id int, name string, dep string) using hudi
             | tblproperties (type = '$tableType', primaryKey = 'id', 'hoodie.index.type' = 'GLOBAL_SIMPLE')
             | partitioned by (dep)
             | location '${tmp.getCanonicalPath}/$tableName'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 'd1'), (2, 'a2', 'd1'), (3, 'a3', 'd2'), (4, 'a4', 'd2')")

        // id 1 takes the partition the source names, id 2 one other than the source value it is looked up under
        spark.sql(
          s"""
             | merge into $tableName t
             | using (select 1 as id, 'a1m' as name, 'd2' as dep, 'd2' as new_dep
             |   union all select 2, 'a2m', 'd1', 'd3' union all select 4, 'a4m', 'd2', 'd2') s
             | on t.id = s.id
             | when matched and s.id = 1 then update set *
             | when matched and s.id = 2 then update set id = s.id, name = s.name, dep = s.new_dep
             | when matched and s.id = 4 then update set id = s.id, name = s.name, dep = s.new_dep
           """.stripMargin)
        spark.sql(s"update $tableName set dep = 'd1', name = 'a3u' where id = 3")
        val expected = Seq(Seq(1, "a1m", "d2"), Seq(2, "a2m", "d3"), Seq(3, "a3u", "d1"), Seq(4, "a4m", "d2"))
        checkPartitionedRows(tableName, "dep")(expected: _*)
        if (tableType == "mor") {
          compactAndCheck(tableName)
          checkPartitionedRows(tableName, "dep")(expected: _*)
        }
      }
    }
  }

  test("Test MergeInto and Update refuse to move a record of a table with a record key and an index that does not move records") {
    withTempDir { tmp =>
      // The record index moves records only with hoodie.record.index.update.partition.path on
      Seq("SIMPLE", "RECORD_INDEX").foreach { indexType =>
        withSQLConf("hoodie.index.type" -> indexType,
          HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> (indexType == "RECORD_INDEX").toString) {
          val tableName = generateTableName
          spark.sql(
            s"""
               | create table $tableName (id int, name string, dep string) using hudi
               | tblproperties (primaryKey = 'id')
               | partitioned by (dep)
               | location '${tmp.getCanonicalPath}/$tableName'
             """.stripMargin)
          spark.sql(s"insert into $tableName values (1, 'a1', 'd1')")
          checkExceptionContain(
            s"""
               | merge into $tableName t
               | using (select 1 as id, 'a1m' as name, 'd1' as dep, 'd9' as new_dep) s
               | on t.id = s.id
               | when matched then update set id = s.id, name = s.name, dep = s.new_dep
             """.stripMargin)(
            "Detected disallowed assignment clause in MERGE INTO statement for partition field `dep`")
          checkExceptionContain(s"update $tableName set dep = 'd9' where id = 1")(
            "Detected disallowed assignment clause in UPDATE statement for partition field `dep`")
          checkPartitionedRows(tableName, "dep")(Seq(1, "a1", "d1"))
        }
      }
    }
  }
}
