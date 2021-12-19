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
import org.apache.hudi.exception.HoodieDuplicateKeyException

class TestInsertTable extends TestHoodieSqlBase {

  test("Test Insert Into") {
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
           | location '${tmp.getCanonicalPath}'
       """.stripMargin)
      // Insert into dynamic partition
      spark.sql(
        s"""
           | insert into $tableName
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-01-05' as dt
        """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 10.0, 1000, "2021-01-05")
      )
      // Insert into static partition
      spark.sql(
        s"""
           | insert into $tableName partition(dt = '2021-01-05')
           | select 2 as id, 'a2' as name, 10 as price, 1000 as ts
        """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 10.0, 1000, "2021-01-05"),
        Seq(2, "a2", 10.0, 1000, "2021-01-05")
      )
    }
  }

  test("Test Insert Into None Partitioned Table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(s"set hoodie.sql.insert.mode=strict")
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
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      spark.sql(s"insert into $tableName2 select 1, 'a1', 10, 1000")
      // This record will be drop when dropDup is true
      spark.sql(s"insert into $tableName2 select 1, 'a1', 12, 1000")
      checkAnswer(s"select id, name, price, ts from $tableName2")(
        Seq(1, "a1", 10.0, 1000)
      )
      // disable this config to avoid affect other test in this class.
      spark.sql("set hoodie.datasource.write.insert.drop.duplicates = false")
      spark.sql(s"set hoodie.sql.insert.mode=upsert")
    }
  }

  test("Test Insert Overwrite") {
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
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 10.0, 1000, "2021-01-05")
      )

      //  Insert overwrite dynamic partition
      spark.sql(
        s"""
           | insert overwrite table $tableName
           | select 2 as id, 'a2' as name, 10 as price, 1000 as ts, '2021-01-06' as dt
        """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName order by id")(
        Seq(1, "a1", 10.0, 1000, "2021-01-05"),
        Seq(2, "a2", 10.0, 1000, "2021-01-06")
      )

      // Insert overwrite static partition
      spark.sql(
        s"""
           | insert overwrite table $tableName partition(dt = '2021-01-05')
           | select * from (select 2 , 'a2', 12, 1000) limit 10
        """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName order by dt")(
        Seq(2, "a2", 12.0, 1000, "2021-01-05"),
        Seq(2, "a2", 10.0, 1000, "2021-01-06")
      )

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
      checkAnswer(s"select id, name, price, ts, dt from $tableName order by id,dt")(
        Seq(1, "a1", 10.0, 1000, "2021-01-04"),
        Seq(2, "a2", 12.0, 1000, "2021-01-05"),
        Seq(2, "a2", 10.0, 1000, "2021-01-06")
      )

      spark.sql(
        s"""
           | insert overwrite table $tableName
           | select id + 2, name, price, ts , '2021-01-04' from $tblNonPartition limit 10
        """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName " +
        s"where dt >='2021-01-04' and dt <= '2021-01-06' order by id,dt")(
        Seq(2, "a2", 12.0, 1000, "2021-01-05"),
        Seq(2, "a2", 10.0, 1000, "2021-01-06"),
        Seq(3, "a1", 10.0, 1000, "2021-01-04")
      )

      // test insert overwrite non-partitioned table
      spark.sql(s"insert overwrite table $tblNonPartition select 2, 'a2', 10, 1000")
      checkAnswer(s"select id, name, price, ts from $tblNonPartition")(
        Seq(2, "a2", 10.0, 1000)
      )
    }
  }

  test("Test Different Type of Partition Column") {
    withTempDir { tmp =>
      val typeAndValue = Seq(
        ("string", "'1000'"),
        ("int", 1000),
        ("bigint", 10000),
        ("timestamp", "'2021-05-20 00:00:00'"),
        ("date", "'2021-05-20'")
      )
      typeAndValue.foreach { case (partitionType, partitionValue) =>
        val tableName = generateTableName
        // Create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  dt $partitionType
             |) using hudi
             | tblproperties (primaryKey = 'id')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)
        spark.sql(s"insert into $tableName partition(dt = $partitionValue) select 1, 'a1', 10")
        spark.sql(s"insert into $tableName select 2, 'a2', 10, $partitionValue")
        checkAnswer(s"select id, name, price, cast(dt as string) from $tableName order by id")(
          Seq(1, "a1", 10, removeQuotes(partitionValue).toString),
          Seq(2, "a2", 10, removeQuotes(partitionValue).toString)
        )
      }
    }
  }

  test("Test insert for uppercase table name") {
    withTempDir{ tmp =>
      val tableName = s"H_$generateTableName"

      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double
           |) using hudi
           | tblproperties (primaryKey = 'id')
           | location '${tmp.getCanonicalPath}'
       """.stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10)")
      checkAnswer(s"select id, name, price from $tableName")(
        Seq(1, "a1", 10.0)
      )
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tmp.getCanonicalPath)
        .setConf(spark.sessionState.newHadoopConf())
        .build()
      assertResult(metaClient.getTableConfig.getTableName)(tableName)
    }
  }

  test("Test Insert Exception") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  dt string
         |) using hudi
         | tblproperties (primaryKey = 'id')
         | partitioned by (dt)
       """.stripMargin)
    checkException(s"insert into $tableName partition(dt = '2021-06-20')" +
      s" select 1, 'a1', 10, '2021-06-20'") (
      "assertion failed: Required select columns count: 4, Current select columns(including static partition column)" +
        " count: 5，columns: (1,a1,10,2021-06-20,dt)"
    )
    checkException(s"insert into $tableName select 1, 'a1', 10")(
      "assertion failed: Required select columns count: 4, Current select columns(including static partition column)" +
        " count: 3，columns: (1,a1,10)"
    )
    spark.sql("set hoodie.sql.bulk.insert.enable = true")
    spark.sql("set hoodie.sql.insert.mode = strict")

    val tableName2 = generateTableName
    spark.sql(
      s"""
         |create table $tableName2 (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         |) using hudi
         | tblproperties (
         |   primaryKey = 'id',
         |   preCombineField = 'ts'
         | )
       """.stripMargin)
    checkException(s"insert into $tableName2 values(1, 'a1', 10, 1000)")(
      "Table with primaryKey can not use bulk insert in strict mode."
    )

    spark.sql("set hoodie.sql.insert.mode = non-strict")
    val tableName3 = generateTableName
    spark.sql(
      s"""
         |create table $tableName3 (
         |  id int,
         |  name string,
         |  price double,
         |  dt string
         |) using hudi
         | tblproperties (primaryKey = 'id')
         | partitioned by (dt)
       """.stripMargin)
    checkException(s"insert overwrite table $tableName3 values(1, 'a1', 10, '2021-07-18')")(
      "Insert Overwrite Partition can not use bulk insert."
    )
    spark.sql("set hoodie.sql.bulk.insert.enable = false")
    spark.sql("set hoodie.sql.insert.mode = upsert")
  }

  test("Test bulk insert") {
    spark.sql("set hoodie.sql.insert.mode = non-strict")
    withTempDir { tmp =>
      Seq("cow", "mor").foreach {tableType =>
        // Test bulk insert for single partition
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  dt string
             |) using hudi
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id'
             | )
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)
        spark.sql("set hoodie.datasource.write.insert.drop.duplicates = false")

        // Enable the bulk insert
        spark.sql("set hoodie.sql.bulk.insert.enable = true")
        spark.sql(s"insert into $tableName values(1, 'a1', 10, '2021-07-18')")

        checkAnswer(s"select id, name, price, dt from $tableName")(
          Seq(1, "a1", 10.0, "2021-07-18")
        )
        // Disable the bulk insert
        spark.sql("set hoodie.sql.bulk.insert.enable = false")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, '2021-07-18')")

        checkAnswer(s"select id, name, price, dt from $tableName order by id")(
          Seq(1, "a1", 10.0, "2021-07-18"),
          Seq(2, "a2", 10.0, "2021-07-18")
        )

        // Test bulk insert for multi-level partition
        val tableMultiPartition = generateTableName
        spark.sql(
          s"""
             |create table $tableMultiPartition (
             |  id int,
             |  name string,
             |  price double,
             |  dt string,
             |  hh string
             |) using hudi
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id'
             | )
             | partitioned by (dt, hh)
             | location '${tmp.getCanonicalPath}/$tableMultiPartition'
       """.stripMargin)

        // Enable the bulk insert
        spark.sql("set hoodie.sql.bulk.insert.enable = true")
        spark.sql(s"insert into $tableMultiPartition values(1, 'a1', 10, '2021-07-18', '12')")

        checkAnswer(s"select id, name, price, dt, hh from $tableMultiPartition")(
          Seq(1, "a1", 10.0, "2021-07-18", "12")
        )
        // Disable the bulk insert
        spark.sql("set hoodie.sql.bulk.insert.enable = false")
        spark.sql(s"insert into $tableMultiPartition " +
          s"values(2, 'a2', 10, '2021-07-18','12')")

        checkAnswer(s"select id, name, price, dt, hh from $tableMultiPartition order by id")(
          Seq(1, "a1", 10.0, "2021-07-18", "12"),
          Seq(2, "a2", 10.0, "2021-07-18", "12")
        )
        // Test bulk insert for non-partitioned table
        val nonPartitionedTable = generateTableName
        spark.sql(
          s"""
             |create table $nonPartitionedTable (
             |  id int,
             |  name string,
             |  price double
             |) using hudi
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id'
             | )
             | location '${tmp.getCanonicalPath}/$nonPartitionedTable'
       """.stripMargin)
        spark.sql("set hoodie.sql.bulk.insert.enable = true")
        spark.sql(s"insert into $nonPartitionedTable values(1, 'a1', 10)")
        checkAnswer(s"select id, name, price from $nonPartitionedTable")(
          Seq(1, "a1", 10.0)
        )
        spark.sql(s"insert overwrite table $nonPartitionedTable values(2, 'a2', 10)")
        checkAnswer(s"select id, name, price from $nonPartitionedTable")(
          Seq(2, "a2", 10.0)
        )
        spark.sql("set hoodie.sql.bulk.insert.enable = false")

        // Test CTAS for bulk insert
        val tableName2 = generateTableName
        spark.sql(
          s"""
             |create table $tableName2
             |using hudi
             |tblproperties(
             | type = '$tableType',
             | primaryKey = 'id'
             |)
             | location '${tmp.getCanonicalPath}/$tableName2'
             | as
             | select * from $tableName
             |""".stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName2 order by id")(
          Seq(1, "a1", 10.0, "2021-07-18"),
          Seq(2, "a2", 10.0, "2021-07-18")
        )
      }
    }
    spark.sql("set hoodie.sql.insert.mode = upsert")
  }

  test("Test combine before insert") {
    spark.sql("set hoodie.sql.bulk.insert.enable = false")
    withTempDir{tmp =>
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
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      spark.sql(
        s"""
           |insert overwrite table $tableName
           |select * from (
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
           | union all
           | select 1 as id, 'a1' as name, 11 as price, 1001 as ts
           | )
           |""".stripMargin
      )
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 11.0, 1001)
      )
    }
  }

  test("Test insert pk-table") {
    spark.sql("set hoodie.sql.bulk.insert.enable = false")
    withTempDir{tmp =>
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
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(1, 'a1', 11, 1000)")
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 11.0, 1000)
      )

    }
  }

}
