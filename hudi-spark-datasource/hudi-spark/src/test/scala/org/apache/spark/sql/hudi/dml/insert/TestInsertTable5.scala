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

package org.apache.spark.sql.hudi.dml.insert

import org.apache.hudi.DataSourceWriteOptions.{DROP_INSERT_DUP_POLICY, FAIL_INSERT_DUP_POLICY, NONE_INSERT_DUP_POLICY}
import org.apache.hudi.HoodieSparkUtils

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestInsertTable5 extends HoodieSparkSqlTestBase {
  test("Test insert into with special cols") {
    withTempDir { tmp =>
      val targetTableA = generateTableName
      val tablePathA = s"${tmp.getCanonicalPath}/$targetTableA"
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }

      spark.sql(
        s"""
           |create table if not exists $targetTableA (
           | id bigint,
           | name string,
           | price double
           |) using hudi
           |tblproperties (
           | primaryKey = 'id',
           | type = 'mor',
           | preCombineField = 'name'
           |) location '$tablePathA'
           |""".stripMargin)

      spark.sql(s"insert into $targetTableA (id, price, name) values (1, 12.1, 'aaa')")

      checkAnswer(s"select id, price, name from $targetTableA")(
        Seq(1, 12.1, "aaa")
      )

      val targetTableB = generateTableName
      val tablePathB = s"${tmp.getCanonicalPath}/$targetTableB"

      spark.sql(
        s"""
           |create table if not exists $targetTableB (
           | id bigint,
           | name string,
           | price double,
           | day string,
           | hour string
           |) using hudi
           |tblproperties (
           | primaryKey = 'id',
           | type = 'mor',
           | preCombineField = 'name'
           |) partitioned by (day, hour)
           |location '$tablePathB'
           |""".stripMargin)

      spark.sql(s"insert into $targetTableB (id, day, price, name, hour) " +
        s"values (2, '01', 12.2, 'bbb', '02')")

      spark.sql(s"insert into $targetTableB (id, day, price, name, hour) " +
        s"select id, '01' as dt, price, name, '03' as hour from $targetTableA")

      spark.sql(s"insert into $targetTableB partition(day='02', hour) (id, hour, price, name) " +
        s"values (3, '01', 12.3, 'ccc')")

      spark.sql(s"insert into $targetTableB partition(day='02', hour='02') (id, price, name) " +
        s"values (4, 12.4, 'ddd')")

      checkAnswer(s"select id, price, name, day, hour from $targetTableB")(
        Seq(2, 12.2, "bbb", "01", "02"),
        Seq(1, 12.1, "aaa", "01", "03"),
        Seq(3, 12.3, "ccc", "02", "01"),
        Seq(4, 12.4, "ddd", "02", "02")
      )

      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = true")
        checkExceptionContain(s"insert into $targetTableB (id, day, price, name, hour) " +
          s"select id, '01' as dt, price, name, '03' as hour from $targetTableA")(
          "hudi not support specified cols when enable default columns")
      }
    }
  }

  test("Test insert overwrite with special cols") {
    withTempDir { tmp =>
      val targetTableA = generateTableName
      val tablePathA = s"${tmp.getCanonicalPath}/$targetTableA"
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }

      spark.sql(
        s"""
           |create table if not exists $targetTableA (
           | id bigint,
           | name string,
           | price double
           |) using hudi
           |tblproperties (
           | primaryKey = 'id',
           | type = 'mor',
           | preCombineField = 'name'
           |) location '$tablePathA'
           |""".stripMargin)

      spark.sql(s"insert overwrite $targetTableA (id, price, name) values (1, 12.1, 'aaa')")

      checkAnswer(s"select id, price, name from $targetTableA")(
        Seq(1, 12.1, "aaa")
      )

      val targetTableB = generateTableName
      val tablePathB = s"${tmp.getCanonicalPath}/$targetTableB"

      spark.sql(
        s"""
           |create table if not exists $targetTableB (
           | id bigint,
           | name string,
           | price double,
           | day string,
           | hour string
           |) using hudi
           |tblproperties (
           | primaryKey = 'id',
           | type = 'mor',
           | preCombineField = 'name'
           |) partitioned by (day, hour)
           |location '$tablePathB'
           |""".stripMargin)

      spark.sql(s"insert overwrite $targetTableB (id, day, price, name, hour) " +
        s"values (2, '01', 12.2, 'bbb', '02')")

      checkAnswer(s"select id, price, name, day, hour from $targetTableB")(
        Seq(2, 12.2, "bbb", "01", "02")
      )

      spark.sql(s"insert overwrite $targetTableB (id, day, price, name, hour) " +
        s"select id, '01' as dt, price, name, '03' as hour from $targetTableA")

      spark.sql(s"insert overwrite $targetTableB partition(day='02', hour) (id, hour, price, name) " +
        s"values (3, '01', 12.3, 'ccc')")

      spark.sql(s"insert overwrite $targetTableB partition(day='02', hour='02') (id, price, name) " +
        s"values (4, 12.4, 'ddd')")

      checkAnswer(s"select id, price, name, day, hour from $targetTableB")(
        Seq(1, 12.1, "aaa", "01", "03"),
        Seq(3, 12.3, "ccc", "02", "01"),
        Seq(4, 12.4, "ddd", "02", "02")
      )

      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = true")
        checkExceptionContain(s"insert overwrite $targetTableB (id, day, price, name, hour) " +
          s"select id, '01' as dt, price, name, '03' as hour from $targetTableA")(
          "hudi not support specified cols when enable default columns")
      }
    }
  }

  test("Test SparkKeyGenerator When Bulk Insert") {
    withSQLConf("hoodie.sql.bulk.insert.enable" -> "true", "hoodie.sql.insert.mode" -> "non-strict") {
      withRecordType()(withTempDir { tmp =>
        val tableName = generateTableName
        // Create a multi-level partitioned table
        // Specify wrong keygenarator by setting hoodie.datasource.write.keygenerator.class = 'org.apache.hudi.keygen.ComplexAvroKeyGenerator'
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  dt string,
             |  pt string
             |) using hudi
             |tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  preCombineField = 'ts',
             |  hoodie.table.keygenerator.class = 'org.apache.hudi.keygen.ComplexAvroKeyGenerator',
             |  hoodie.datasource.write.keygenerator.class = 'org.apache.hudi.keygen.ComplexAvroKeyGenerator'
             |)
             | partitioned by (dt, pt)
             | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)
        //Insert data and check the same
        spark.sql(
          s"""insert into $tableName  values
             |(1, 'a', 31, 1000, '2021-01-05', 'A'),
             |(2, 'b', 18, 1000, '2021-01-05', 'A')
             |""".stripMargin)
        checkAnswer(s"select id, name, price, ts, dt, pt from $tableName order by dt")(
          Seq(1, "a", 31, 1000, "2021-01-05", "A"),
          Seq(2, "b", 18, 1000, "2021-01-05", "A")
        )
      })
    }
  }

  test("Test table with insert dup policy") {
    withTempDir { tmp =>
      Seq(
        NONE_INSERT_DUP_POLICY,
        FAIL_INSERT_DUP_POLICY,
        DROP_INSERT_DUP_POLICY).foreach(policy => {
        val targetTable = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

        spark.sql(s"set hoodie.datasource.insert.dup.policy=$policy")
        spark.sql(
          s"""
             |create table ${targetTable} (
             |  `id` string,
             |  `name` string,
             |  `dt` bigint,
             |  `day` STRING,
             |  `hour` INT
             |) using hudi
             |tblproperties (
             |  'primaryKey' = 'id',
             |  'type' = 'MOR',
             |  'preCombineField'= 'dt',
             |  'hoodie.bucket.index.hash.field' = 'id',
             |  'hoodie.bucket.index.num.buckets'= 512
             | )
           partitioned by (`day`,`hour`)
           location '${tablePath}'
           """.stripMargin)

        spark.sql("set spark.sql.shuffle.partitions = 11")
        spark.sql(
          s"""
             |insert into ${targetTable}
             |select '1' as id, 'aa' as name, 123 as dt, '2024-02-19' as `day`, 10 as `hour`
             |""".stripMargin)
        if (policy.equals(FAIL_INSERT_DUP_POLICY)) {
          checkExceptionContain(
            () => spark.sql(
              s"""
                 |insert into ${targetTable}
                 |select '1' as id, 'aa' as name, 1234 as dt, '2024-02-19' as `day`, 10 as `hour`
                 |""".stripMargin))(s"Duplicate key found for insert statement, key is: id:1")
        } else {
          spark.sql(s"set hoodie.datasource.write.operation=insert")
          spark.sql(
            s"""
               |insert into ${targetTable}
               |select '1' as id, 'aa' as name, 1234 as dt, '2024-02-19' as `day`, 10 as `hour`
               |""".stripMargin)
          if (policy.equals(NONE_INSERT_DUP_POLICY)) {
            checkAnswer(s"select id, name, dt, day, hour from $targetTable limit 10")(
              Seq("1", "aa", 123, "2024-02-19", 10),
              Seq("1", "aa", 1234, "2024-02-19", 10)
            )
          } else {
            checkAnswer(s"select id, name, dt, day, hour from $targetTable limit 10")(
              Seq("1", "aa", 123, "2024-02-19", 10)
            )
          }
        }
      })
    }
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
  }

  test("Test throwing error on schema evolution in INSERT INTO") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        // Create a partitioned table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  dt string,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (primaryKey = 'id', type = '$tableType')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
             | """.stripMargin)

        // INSERT INTO with same set of columns as that in the table schema works
        spark.sql(
          s"""
             | insert into $tableName partition(dt = '2024-01-14')
             | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
             | union
             | select 2 as id, 'a2' as name, 20 as price, 1002 as ts
             | """.stripMargin)
        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1", 10.0, 1000, "2024-01-14"),
          Seq(2, "a2", 20.0, 1002, "2024-01-14")
        )

        // INSERT INTO with an additional column that does not exist in the table schema
        // throws an error, as INSERT INTO does not allow schema evolution
        val sqlStatement =
          s"""
             | insert into $tableName partition(dt = '2024-01-14')
             | select 3 as id, 'a3' as name, 30 as price, 1003 as ts, 'x' as new_col
             | union
             | select 2 as id, 'a2_updated' as name, 25 as price, 1005 as ts, 'y' as new_col
             | """.stripMargin
        val expectedExceptionMessage = if (HoodieSparkUtils.gteqSpark3_5) {
          "[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] " +
            s"Cannot write to `spark_catalog`.`default`.`$tableName`, " +
            "the reason is too many data columns:\n" +
            "Table columns: `id`, `name`, `price`, `ts`.\n" +
            "Data columns: `id`, `name`, `price`, `ts`, `new_col`."
        } else {
          val endingStr = if (HoodieSparkUtils.gteqSpark3_4) "." else ""
          val tableId = if (HoodieSparkUtils.gteqSpark3_4) {
            s"spark_catalog.default.$tableName"
          } else {
            s"default.$tableName"
          }
          s"Cannot write to '$tableId', too many data columns:\n" +
            s"Table columns: 'id', 'name', 'price', 'ts'$endingStr\n" +
            s"Data columns: 'id', 'name', 'price', 'ts', 'new_col'$endingStr"
        }
        checkExceptionContain(sqlStatement)(expectedExceptionMessage)
      }
    }
  }
}
