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

import org.apache.commons.lang3.StringUtils
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.exception.{HoodieDuplicateKeyException, HoodieException}
import org.apache.spark.sql.functions.lit

class TestQueryTable extends HoodieSparkSqlTestBase {

  test("Test PruneColumns") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a partitioned table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | tblproperties (primaryKey = 'id')
           | partitioned by (dt)
           | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)
      //  Insert overwrite dynamic partition
      spark.sql(
        s"""
           | insert overwrite table $tableName
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-01-05' as dt
        """.stripMargin)

      //  Insert overwrite dynamic partition
      spark.sql(
        s"""
           | insert overwrite table $tableName
           | select 2 as id, 'a2' as name, 10 as price, 1000 as ts, '2021-01-06' as dt
        """.stripMargin)

      // Insert overwrite static partition
      spark.sql(
        s"""
           | insert into table $tableName partition(dt = '2021-01-05')
           | select * from (select 2 , 'a2', 12, 1000) limit 10
        """.stripMargin)

      spark.sql(s"set hoodie.datasource.v2.read.enable=true")

      val query = s"select id, name from $tableName " +
        s"where dt ='2021-01-05' and id = 1"

      spark.sql(query).explain(true)

      checkAnswer(query)(
        Seq(1, "a1")
      )
      spark.sql(s"set hoodie.datasource.v2.read.enable=false")

    }

  }

  test("Test RuntimeFiltering") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a partitioned table
      println(spark.version)
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | tblproperties (primaryKey = 'id', type = 'cow')
           | partitioned by (name)
           | location '${tmp.getCanonicalPath}'
       """.stripMargin)
      // Insert into dynamic partition
      spark.sql(
        s"""
           | insert into $tableName
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-01-05' as dt
        """.stripMargin)

      spark.sql(
        s"""
           | insert into $tableName
           | select 10 as id, 'a10' as name, 100 as price, 10000 as ts, '2021-01-06' as dt
        """.stripMargin)


      spark.sql(
        s"""
           | insert into $tableName
           | select 2 as id, 'a2' as name, 20 as price, 2000 as ts, '2021-02-05' as dt
        """.stripMargin)

      val dimDf = spark.range(1, 4)
        .withColumn("name", lit("a1"))
        .select("id", "name")

      dimDf.show(false)

      spark.sql("CREATE TABLE dim (id int, name string) USING parquet")
      dimDf.coalesce(1).write.mode("append").insertInto("dim")


      spark.sql(s"set hoodie.datasource.v2.read.enable=true")

      val query =String.format("SELECT f.id, f.price, f.ts, f.dt, f.name FROM %s f JOIN dim d ON f.name = d.name AND d.id = 1 ORDER BY id", tableName)
      val output = spark.sql("EXPLAIN EXTENDED " + query).collectAsList()
      val actualFilterCount = StringUtils.countMatches(output.get(0).getString(0), "dynamicpruningexpression")
      checkAnswer(query)(
        Seq(1, 10.0, 1000, "2021-01-05", "a1")
      )

      if (HoodieSparkUtils.isSpark3_2) {
        assertResult(actualFilterCount)(1)
      }

      spark.sql("DROP TABLE IF EXISTS dim")
      spark.sql(s"set hoodie.datasource.v2.read.enable=false")
    }
  }

  test("Test Query None Partitioned Table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(s"set hoodie.sql.insert.mode=strict")
      spark.sql(s"set hoodie.datasource.v2.read.enable=true")

      // Create none partitioned cow table
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
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 10.0, 1000)
      )

      spark.sql(s"insert into $tableName select 2, 'a2', 12, 1000")
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 10.0, 1000),
        Seq(2, "a2", 12.0, 1000)
      )

      checkAnswer(s"select id, name from default.$tableName where id = 1")(
        Seq(1, "a1")
      )

      assertThrows[HoodieDuplicateKeyException] {
        try {
          spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
        } catch {
          case e: Exception =>
            var root: Throwable = e
            while (root.getCause != null) {
              root = root.getCause
            }
            throw root
        }
      }

      // Create table with dropDup is true
      val tableName2 = generateTableName
      spark.sql("set hoodie.datasource.write.insert.drop.duplicates = true")
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
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      spark.sql(s"insert into $tableName2 values(1, 'a1', 10, 1000)")
      // This record will be drop when dropDup is true
      spark.sql(s"insert into $tableName2 values(2, 'a2', 1000, 1000)")
      checkAnswer(s"select id, name, price, ts from $tableName2 where name = 'a1'")(
        Seq(1, "a1", 10.0, 1000)
      )

      // disable this config to avoid affect other test in this class.
      spark.sql("set hoodie.datasource.write.insert.drop.duplicates = false")
      spark.sql(s"set hoodie.sql.insert.mode=upsert")
      spark.sql(s"set hoodie.datasource.v2.read.enable=false")
    }
  }

  test("Test Query Partitioned Table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a partitioned table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | tblproperties (primaryKey = 'id')
           | partitioned by (dt)
           | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)
      //  Insert overwrite dynamic partition
      spark.sql(
        s"""
           | insert overwrite table $tableName
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-01-05' as dt
        """.stripMargin)

      //  Insert overwrite dynamic partition
      spark.sql(
        s"""
           | insert overwrite table $tableName
           | select 2 as id, 'a2' as name, 10 as price, 1000 as ts, '2021-01-06' as dt
        """.stripMargin)

      // Insert overwrite static partition
      spark.sql(
        s"""
           | insert overwrite table $tableName partition(dt = '2021-01-05')
           | select * from (select 2 , 'a2', 12, 1000) limit 10
        """.stripMargin)

      // Insert data from another table
      val tblNonPartition = generateTableName
      spark.sql(
        s"""
           | create table $tblNonPartition (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           | ) using hudi
           | tblproperties (primaryKey = 'id')
           | location '${tmp.getCanonicalPath}/$tblNonPartition'
         """.stripMargin)
      spark.sql(s"insert into $tblNonPartition select 1, 'a1', 10, 1000")
      spark.sql(
        s"""
           | insert overwrite table $tableName partition(dt ='2021-01-04')
           | select * from $tblNonPartition limit 10
        """.stripMargin)

      spark.sql(
        s"""
           | insert overwrite table $tableName
           | select id + 2, name, price, ts , '2021-01-04' from $tblNonPartition limit 10
        """.stripMargin)

      // test insert overwrite non-partitioned table
      spark.sql(s"insert overwrite table $tblNonPartition select 2, 'a2', 10, 1000")

      spark.sql(s"set hoodie.datasource.v2.read.enable=true")

      checkAnswer(s"select id, name, price, ts, dt from $tableName order by id,dt")(
        Seq(2, "a2", 10.0, 1000, "2021-01-06"),
        Seq(2, "a2", 12.0, 1000, "2021-01-05"),
        Seq(3, "a1", 10.0, 1000, "2021-01-04")
      )

      checkAnswer(s"select id, name, price, ts, dt from $tableName " +
        s"where dt >='2021-01-05' and dt <= '2021-01-06' order by id,dt")(
        Seq(2, "a2", 12.0, 1000, "2021-01-05"),
        Seq(2, "a2", 10.0, 1000, "2021-01-06")
      )

      checkAnswer(s"select id, name, price, ts from $tblNonPartition")(
        Seq(2, "a2", 10.0, 1000)
      )

      spark.sql(s"set hoodie.datasource.v2.read.enable=false")
    }
  }

  test("Test Qeury Exception") {
    val tableName = generateTableName
    spark.sql(s"set hoodie.datasource.v2.read.enable=true")
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  dt string
         |) using hudi
         | tblproperties (primaryKey = 'id', type = 'mor')
         | partitioned by (dt)
       """.stripMargin)

    if (HoodieSparkUtils.isSpark3_2) {
      assertThrows[HoodieException] {
        try {
          spark.sql(s"select * from $tableName").show()
        } catch {
          case e: Exception =>
            var root: Throwable = e
            while (root.getCause != null) {
              root = root.getCause
            }
            throw root
        }
      }
    }
    spark.sql(s"set hoodie.datasource.v2.read.enable=false")
  }

  test("Test Query SQL Join") {
    withTempDir { tmp =>
      val tableName1 = generateTableName
      // Create a partitioned table
      spark.sql(
        s"""
           |create table $tableName1 (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | tblproperties (primaryKey = 'id', type = 'cow')
           | partitioned by (dt)
           | location '${tmp.getCanonicalPath}/$tableName1'
       """.stripMargin)
      // Insert into dynamic partition
      spark.sql(
        s"""
           | insert into $tableName1
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-01-05' as dt
        """.stripMargin)

      spark.sql(
        s"""
           | insert into $tableName1
           | select 10 as id, 'a111' as name, 10 as price, 1000 as ts, '2021-01-06' as dt
        """.stripMargin)

      spark.sql(
        s"""
           | insert into $tableName1
           | select 2 as id, 'a2' as name, 20 as price, 2000 as ts, '2021-02-05' as dt
        """.stripMargin)

      val dimDf = spark.range(1, 4)
        .withColumn("name", lit("a1"))
        .select("id", "name")

      val tableName2 = generateTableName
      spark.sql(
        s"""
           |create table $tableName2 (
           |  id int,
           |  name string
           |) using hudi
           | tblproperties (primaryKey = 'id', type = 'cow')
           | location '${tmp.getCanonicalPath}/$tableName2'
       """.stripMargin)

      dimDf.coalesce(1)
        .write.format("org.apache.hudi").mode("append").insertInto(tableName2)

      spark.sql(s"set hoodie.datasource.v2.read.enable=true")
      val query =String.format("SELECT f.id, f.name, f.ts, f.dt" +
        " FROM %s f JOIN %s d ON f.name = d.name AND d.id = 1 ORDER BY id", tableName1, tableName2)
      checkAnswer(query)(
        Seq(1, "a1", 1000, "2021-01-05")
      )
      spark.sql(s"set hoodie.datasource.v2.read.enable=false")
    }
  }
}
