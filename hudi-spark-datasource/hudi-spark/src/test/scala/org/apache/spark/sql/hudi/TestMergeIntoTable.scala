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

import org.apache.hudi.{DataSourceReadOptions, HoodieDataSourceHelpers}
import org.apache.hudi.common.fs.FSUtils

class TestMergeIntoTable extends HoodieSparkSqlTestBase {

  test("Test MergeInto Basic") {
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
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
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

  test("Test MergeInto with ignored record") {
    withTempDir {tmp =>
      val sourceTable = generateTableName
      val targetTable = generateTableName
      // Create source table
      spark.sql(
        s"""
           | create table $sourceTable (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           | ) using parquet
           | location '${tmp.getCanonicalPath}/$sourceTable'
         """.stripMargin)
      // Create target table
      spark.sql(
        s"""
           |create table $targetTable (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$targetTable'
           | tblproperties (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // Insert data to source table
      spark.sql(s"insert into $sourceTable values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $sourceTable values(2, 'a2', 11, 1000)")

      spark.sql(
        s"""
           | merge into $targetTable as t0
           | using (select * from $sourceTable) as s0
           | on t0.id = s0.id
           | when matched then update set *
           | when not matched and s0.name = 'a1' then insert *
         """.stripMargin)
      // The record of "name = 'a2'" will be filter
      checkAnswer(s"select id, name, price, ts from $targetTable")(
        Seq(1, "a1", 10.0, 1000)
      )

      spark.sql(s"insert into $targetTable select 3, 'a3', 12, 1000")
      checkAnswer(s"select id, name, price, ts from $targetTable")(
        Seq(1, "a1", 10.0, 1000),
        Seq(3, "a3", 12, 1000)
      )

      spark.sql(
        s"""
           | merge into $targetTable as t0
           | using (
           |  select * from (
           |    select 1 as s_id, 'a1' as name, 20 as price, 1001 as ts
           |    union all
           |    select 3 as s_id, 'a3' as name, 20 as price, 1003 as ts
           |    union all
           |    select 4 as s_id, 'a4' as name, 10 as price, 1004 as ts
           |  )
           | ) s0
           | on s0.s_id = t0.id
           | when matched and s0.ts = 1001 then update set id = s0.s_id, name = t0.name, price =
           | s0.price, ts = s0.ts
         """.stripMargin
      )
      // Ignore the update for id = 3
      checkAnswer(s"select id, name, price, ts from $targetTable")(
        Seq(1, "a1", 20.0, 1001),
        Seq(3, "a3", 12.0, 1000)
      )
    }
  }

  test("Test MergeInto for MOR table ") {
    withTempDir {tmp =>
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
      // Insert data
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
      // Update data when matched-condition not matched.
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 12 as price, 1001 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched and s0.id % 2 = 0 then update set *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 10, "2021-03-21")
      )
      // Update data when matched-condition matched.
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 12 as price, 1001 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched and s0.id % 2 = 1 then update set *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 12, "2021-03-21")
      )
      // Insert a new data.
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 2 as id, 'a2' as name, 10 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when not matched and s0.id % 2 = 0 then insert *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName order by id")(
        Seq(1, "a1", 12, "2021-03-21"),
        Seq(2, "a2", 10, "2021-03-21")
      )
      // Update with different source column names.
      spark.sql(
        s"""
           | merge into $tableName t0
           | using (
           |  select 2 as s_id, 'a2' as s_name, 15 as s_price, 1001 as s_ts, '2021-03-21' as dt
           | ) s0
           | on t0.id = s0.s_id
           | when matched and s_ts = 1001 then update set *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName order by id")(
        Seq(1, "a1", 12, "2021-03-21"),
        Seq(2, "a2", 15, "2021-03-21")
      )

      // Delete with condition expression.
      spark.sql(
        s"""
           | merge into $tableName t0
           | using (
           |  select 1 as s_id, 'a2' as s_name, 15 as s_price, 1001 as s_ts, '2021-03-21' as dt
           | ) s0
           | on t0.id = s0.s_id + 1
           | when matched and s_ts = 1001 then delete
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName order by id")(
        Seq(1, "a1", 12, "2021-03-21")
      )
    }
  }

  test("Test MergeInto with insert only") {
    withTempDir {tmp =>
      // Create a partitioned mor table
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName (
           |  id bigint,
           |  name string,
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

      spark.sql(s"insert into $tableName select 1, 'a1', 10, '2021-03-21'")
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 2 as id, 'a2' as name, 10 as price, 1000 as ts, '2021-03-20' as dt
           | ) s0
           | on s0.id = t0.id
           | when not matched and s0.id % 2 = 0 then insert (id,name,price,dt)
           | values(s0.id,s0.name,s0.price,s0.dt)
         """.stripMargin)
      checkAnswer(s"select id,name,price,dt from $tableName order by id")(
        Seq(1, "a1", 10, "2021-03-21"),
        Seq(2, "a2", 10, "2021-03-20")
      )

      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 3 as id, 'a3' as name, 10 as price, 1000 as ts, '2021-03-20' as dt
           | ) s0
           | on s0.id = t0.id
           | when not matched and s0.id % 2 = 0 then insert (id,name,price,dt)
           | values(s0.id,s0.name,s0.price,s0.dt)
         """.stripMargin)
      // id = 3 should not write to the table as it has filtered by id % 2 = 0
      checkAnswer(s"select id,name,price,dt from $tableName order by id")(
        Seq(1, "a1", 10, "2021-03-21"),
        Seq(2, "a2", 10, "2021-03-20")
      )
    }
  }

  test("Test MergeInto For PreCombineField") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName1 = generateTableName
        // Create a mor partitioned table.
        spark.sql(
          s"""
             | create table $tableName1 (
             |  id int,
             |  name string,
             |  price double,
             |  v long,
             |  dt string
             | ) using hudi
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'v',
             |  hoodie.compaction.payload.class = 'org.apache.hudi.common.model.DefaultHoodieRecordPayload'
             | )
             | partitioned by(dt)
             | location '${tmp.getCanonicalPath}/$tableName1'
         """.stripMargin)
        // Insert data
        spark.sql(
          s"""
             | merge into $tableName1 as t0
             | using (
             |  select 1 as id, 'a1' as name, 10 as price, 1001 as v, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.id
             | when not matched and s0.id % 2 = 1 then insert *
         """.stripMargin
        )
        checkAnswer(s"select id,name,price,dt,v from $tableName1")(
          Seq(1, "a1", 10, "2021-03-21", 1001)
        )

        // Update data with a smaller version value
        spark.sql(
          s"""
             | merge into $tableName1 as t0
             | using (
             |  select 1 as id, 'a1' as name, 11 as price, 1000 as v, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.id
             | when matched and s0.id % 2 = 1 then update set *
         """.stripMargin
        )
        // Update failed as v = 1000 < 1001
        checkAnswer(s"select id,name,price,dt,v from $tableName1")(
          Seq(1, "a1", 10, "2021-03-21", 1001)
        )

        // Update data with a bigger version value
        spark.sql(
          s"""
             | merge into $tableName1 as t0
             | using (
             |  select 1 as id, 'a1' as name, 12 as price, 1002 as v, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.id
             | when matched and s0.id % 2 = 1 then update set *
         """.stripMargin
        )
        // Update success
        checkAnswer(s"select id,name,price,dt,v from $tableName1")(
          Seq(1, "a1", 12, "2021-03-21", 1002)
        )
      }
    }
  }

  test("Test MergeInto with preCombine field expression") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName1 = generateTableName
        spark.sql(
          s"""
             | create table $tableName1 (
             |  id int,
             |  name string,
             |  price double,
             |  v long,
             |  dt string
             | ) using hudi
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'v'
             | )
             | partitioned by(dt)
             | location '${tmp.getCanonicalPath}/$tableName1'
         """.stripMargin)
        // Insert data
        spark.sql(s"""insert into $tableName1 values(1, 'a1', 10, 1000, '2021-03-21')""")

        // Update data with a value expression on preCombine field
        // 1) set source column name to be same as target column
        spark.sql(
          s"""
             | merge into $tableName1 as t0
             | using (
             |  select 1 as id, 'a1' as name, 11 as price, 999 as v, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.id
             | when matched then update set id=s0.id, name=s0.name, price=s0.price*2, v=s0.v+2, dt=s0.dt
         """.stripMargin
        )
        // Update success as new value 1001 is bigger than original value 1000
        checkAnswer(s"select id,name,price,dt,v from $tableName1")(
          Seq(1, "a1", 22, "2021-03-21", 1001)
        )

        // 2) set source column name to be different with target column
        spark.sql(
          s"""
             | merge into $tableName1 as t0
             | using (
             |  select 1 as s_id, 'a1' as s_name, 12 as s_price, 1000 as s_v, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.s_id
             | when matched then update set id=s0.s_id, name=s0.s_name, price=s0.s_price*2, v=s0.s_v+2, dt=s0.dt
         """.stripMargin
        )
        // Update success as new value 1002 is bigger than original value 1001
        checkAnswer(s"select id,name,price,dt,v from $tableName1")(
          Seq(1, "a1", 24, "2021-03-21", 1002)
        )
      }
    }
  }

  test("Test MergeInto with primaryKey expression") {
    withTempDir { tmp =>
      val tableName1 = generateTableName
      spark.sql(
        s"""
           | create table $tableName1 (
           |  id int,
           |  name string,
           |  price double,
           |  v long,
           |  dt string
           | ) using hudi
           | tblproperties (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'v'
           | )
           | partitioned by(dt)
           | location '${tmp.getCanonicalPath}/$tableName1'
         """.stripMargin)
      // Insert data
      spark.sql(s"""insert into $tableName1 values(3, 'a3', 30, 3000, '2021-03-21')""")
      spark.sql(s"""insert into $tableName1 values(2, 'a2', 20, 2000, '2021-03-21')""")
      spark.sql(s"""insert into $tableName1 values(1, 'a1', 10, 1000, '2021-03-21')""")

      // Delete data with a condition expression on primaryKey field
      // 1) set source column name to be same as target column
      spark.sql(
        s"""
           | merge into $tableName1 t0
           | using (
           |  select 1 as id, 'a1' as name, 15 as price, 1001 as v, '2021-03-21' as dt
           | ) s0
           | on t0.id = s0.id + 1
           | when matched then delete
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,v,dt from $tableName1 order by id")(
        Seq(1, "a1", 10, 1000, "2021-03-21"),
        Seq(3, "a3", 30, 3000, "2021-03-21")
      )

      // 2) set source column name to be different with target column
      spark.sql(
        s"""
           | merge into $tableName1 t0
           | using (
           |  select 2 as s_id, 'a1' as s_name, 15 as s_price, 1001 as s_v, '2021-03-21' as dt
           | ) s0
           | on t0.id = s0.s_id + 1
           | when matched then delete
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,v,dt from $tableName1 order by id")(
        Seq(1, "a1", 10, 1000, "2021-03-21")
      )
    }
  }

  test("Test MergeInto with combination of delete update insert") {
    withTempDir { tmp =>
      val sourceTable = generateTableName
      val targetTable = generateTableName
      // Create source table
      spark.sql(
        s"""
           | create table $sourceTable (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           | ) using parquet
           | location '${tmp.getCanonicalPath}/$sourceTable'
         """.stripMargin)
      spark.sql(s"insert into $sourceTable values(8, 's8', 80, 2008, '2021-03-21')")
      spark.sql(s"insert into $sourceTable values(9, 's9', 90, 2009, '2021-03-21')")
      spark.sql(s"insert into $sourceTable values(10, 's10', 100, 2010, '2021-03-21')")
      spark.sql(s"insert into $sourceTable values(11, 's11', 110, 2011, '2021-03-21')")
      spark.sql(s"insert into $sourceTable values(12, 's12', 120, 2012, '2021-03-21')")
      // Create target table
      spark.sql(
        s"""
           |create table $targetTable (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | tblproperties (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
           | partitioned by(dt)
           | location '${tmp.getCanonicalPath}/$targetTable'
       """.stripMargin)
      spark.sql(s"insert into $targetTable values(7, 'a7', 70, 1007, '2021-03-21')")
      spark.sql(s"insert into $targetTable values(8, 'a8', 80, 1008, '2021-03-21')")
      spark.sql(s"insert into $targetTable values(9, 'a9', 90, 1009, '2021-03-21')")
      spark.sql(s"insert into $targetTable values(10, 'a10', 100, 1010, '2021-03-21')")

      spark.sql(
        s"""
           | merge into $targetTable as t0
           | using $sourceTable as s0
           | on t0.id = s0.id
           | when matched and id = 10 then delete
           | when matched and id < 10 then update set name='sxx', price=s0.price*2, ts=s0.ts+10000, dt=s0.dt
           | when not matched and id > 10 then insert *
         """.stripMargin)
      checkAnswer(s"select id,name,price,ts,dt from $targetTable order by id")(
        Seq(7, "a7", 70, 1007, "2021-03-21"),
        Seq(8, "sxx", 160, 12008, "2021-03-21"),
        Seq(9, "sxx", 180, 12009, "2021-03-21"),
        Seq(11, "s11", 110, 2011, "2021-03-21"),
        Seq(12, "s12", 120, 2012, "2021-03-21")
      )
    }
  }

  test("Merge Hudi to Hudi") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val sourceTable = generateTableName
        spark.sql(
          s"""
             |create table $sourceTable (
             | id int,
             | name string,
             | price double,
             | _ts long
             |) using hudi
             |tblproperties(
             | type ='$tableType',
             | primaryKey = 'id',
             | preCombineField = '_ts'
             |)
             |location '${tmp.getCanonicalPath}/$sourceTable'
          """.stripMargin)

        val targetTable = generateTableName
        val targetBasePath = s"${tmp.getCanonicalPath}/$targetTable"
        spark.sql(
          s"""
             |create table $targetTable (
             | id int,
             | name string,
             | price double,
             | _ts long
             |) using hudi
             |tblproperties(
             | type ='$tableType',
             | primaryKey = 'id',
             | preCombineField = '_ts'
             |)
             |location '$targetBasePath'
          """.stripMargin)

        // First merge
        spark.sql(s"insert into $sourceTable values(1, 'a1', 10, 1000)")
        spark.sql(
          s"""
             |merge into $targetTable t0
             |using $sourceTable s0
             |on t0.id = s0.id
             |when not matched then insert *
          """.stripMargin)

        checkAnswer(s"select id, name, price, _ts from $targetTable")(
          Seq(1, "a1", 10, 1000)
        )
        val fs = FSUtils.getFs(targetBasePath, spark.sessionState.newHadoopConf())
        val firstCommitTime = HoodieDataSourceHelpers.latestCommit(fs, targetBasePath)

        // Second merge
        spark.sql(s"update $sourceTable set price = 12, _ts = 1001 where id = 1")
        spark.sql(
          s"""
             |merge into $targetTable t0
             |using $sourceTable s0
             |on t0.id = s0.id
             |when matched and cast(s0._ts as string) > '1000' then update set *
           """.stripMargin)
        checkAnswer(s"select id, name, price, _ts from $targetTable")(
          Seq(1, "a1", 12, 1001)
        )
        // Test incremental query
        val hudiIncDF1 = spark.read.format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "000")
          .option(DataSourceReadOptions.END_INSTANTTIME.key, firstCommitTime)
          .load(targetBasePath)
        hudiIncDF1.createOrReplaceTempView("inc1")
        checkAnswer(s"select id, name, price, _ts from inc1")(
          Seq(1, "a1", 10, 1000)
        )
        val secondCommitTime = HoodieDataSourceHelpers.latestCommit(fs, targetBasePath)
        // Third merge
        spark.sql(s"insert into $sourceTable values(2, 'a2', 10, 1001)")
        spark.sql(
          s"""
             |merge into $targetTable t0
             |using $sourceTable s0
             |on t0.id = s0.id
             |when matched then update set *
             |when not matched and s0.name = 'a2' then insert *
           """.stripMargin)
        checkAnswer(s"select id, name, price, _ts from $targetTable order by id")(
          Seq(1, "a1", 12, 1001),
          Seq(2, "a2", 10, 1001)
        )
        // Test incremental query
        val hudiIncDF2 = spark.read.format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, secondCommitTime)
          .load(targetBasePath)
        hudiIncDF2.createOrReplaceTempView("inc2")
        checkAnswer(s"select id, name, price, _ts from inc2 order by id")(
          Seq(1, "a1", 12, 1001),
          Seq(2, "a2", 10, 1001)
        )
      }
    }
  }

  test("Test Different Type of PreCombineField") {
    withTempDir { tmp =>
      val typeAndValue = Seq(
        ("string", "'1000'"),
        ("int", 1000),
        ("bigint", 10000),
        ("timestamp", "'2021-05-20 00:00:00'"),
        ("date", "'2021-05-20'")
      )
      typeAndValue.foreach { case (dataType, dataValue) =>
        val tableName = generateTableName
        // Create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  c $dataType
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  primaryKey ='id',
             |  preCombineField = 'c'
             | )
       """.stripMargin)

        // First merge with a extra input field 'flag' (insert a new record)
        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 1 as id, 'a1' as name, 10 as price, $dataValue as c0, '1' as flag
             | ) s0
             | on s0.id = $tableName.id
             | when matched and flag = '1' then update set
             | id = s0.id, name = s0.name, price = s0.price, c = s0.c0
             | when not matched and flag = '1' then insert *
       """.stripMargin)
        checkAnswer(s"select id, name, price from $tableName")(
          Seq(1, "a1", 10.0)
        )
        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 1 as id, 'a1' as name, 10 as price, $dataValue as c
             | ) s0
             | on s0.id = $tableName.id
             | when matched then update set
             | id = s0.id, name = s0.name, price = s0.price + $tableName.price, c = s0.c
             | when not matched then insert *
       """.stripMargin)
        checkAnswer(s"select id, name, price from $tableName")(
          Seq(1, "a1", 20.0)
        )
      }
    }
  }

  test("Test MergeInto For MOR With Compaction On") {
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
           |  type = 'mor',
           |  preCombineField = 'ts',
           |  hoodie.compact.inline = 'true'
           | )
       """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
      spark.sql(s"insert into $tableName values(4, 'a4', 10, 1000)")
      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 10.0, 1000),
        Seq(2, "a2", 10.0, 1000),
        Seq(3, "a3", 10.0, 1000),
        Seq(4, "a4",10.0, 1000)
      )

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           | select 4 as id, 'a4' as name, 11 as price, 1000 as ts
           | ) s0
           | on h0.id = s0.id
           | when matched then update set *
           |""".stripMargin)

      // 5 commits will trigger compaction.
      checkAnswer(s"select id, name, price, ts from $tableName order by id")(
        Seq(1, "a1", 10.0, 1000),
        Seq(2, "a2", 10.0, 1000),
        Seq(3, "a3", 10.0, 1000),
        Seq(4, "a4", 11.0, 1000)
      )
    }
  }

  test("Test MereInto With Null Fields") {
    withTempDir { tmp =>
      val types = Seq(
        "string" ,
        "int",
        "bigint",
        "double",
        "float",
        "timestamp",
        "date",
        "decimal"
      )
      types.foreach { dataType =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  value $dataType,
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
             |merge into $tableName h0
             |using (
             | select 1 as id, 'a1' as name, cast(null as $dataType) as value, 1000 as ts
             | ) s0
             | on h0.id = s0.id
             | when not matched then insert *
             |""".stripMargin)
        checkAnswer(s"select id, name, value, ts from $tableName")(
          Seq(1, "a1", null, 1000)
        )
      }
    }
  }

  test("Test MergeInto With All Kinds Of DataType") {
    withTempDir { tmp =>
      val dataAndTypes = Seq(
        ("string", "'a1'"),
        ("int", "10"),
        ("bigint", "10"),
        ("double", "10.0"),
        ("float", "10.0"),
        ("decimal(5,2)", "10.11"),
        ("decimal(5,0)", "10"),
        ("timestamp", "'2021-05-20 00:00:00'"),
        ("date", "'2021-05-20'")
      )
      dataAndTypes.foreach { case (dataType, dataValue) =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  value $dataType,
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
             |merge into $tableName h0
             |using (
             | select 1 as id, 'a1' as name, cast($dataValue as $dataType) as value, 1000 as ts
             | ) s0
             | on h0.id = s0.id
             | when not matched then insert *
             |""".stripMargin)
        checkAnswer(s"select id, name, cast(value as string), ts from $tableName")(
          Seq(1, "a1", extractRawValue(dataValue), 1000)
        )
      }
    }
  }

  test("Test MergeInto with no-full fields source") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  value int,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           | select 1 as id, 1001 as ts
           | ) s0
           | on h0.id = s0.id
           | when matched then update set h0.ts = s0.ts
           |""".stripMargin)
      checkAnswer(s"select id, name, value, ts from $tableName")(
        Seq(1, "a1", 10, 1001)
      )
    }
  }
}
