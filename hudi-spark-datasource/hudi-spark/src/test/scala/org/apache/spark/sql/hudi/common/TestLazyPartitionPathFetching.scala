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

package org.apache.spark.sql.hudi.common

class TestLazyPartitionPathFetching extends HoodieSparkSqlTestBase {

  test("Test querying with string column + partition pruning") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  date_par date
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
           | PARTITIONED BY (date_par)
       """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, date '2023-02-27')")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, date '2023-02-28')")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000, date '2023-03-01')")

      checkAnswer(s"select id, name, price, ts from $tableName where date_par='2023-03-01' order by id")(
        Seq(3, "a3", 10.0, 1000)
      )

      withSQLConf("spark.sql.session.timeZone" -> "UTC+2") {
        checkAnswer(s"select id, name, price, ts from $tableName where date_par='2023-03-01' order by id")(
          Seq(3, "a3", 10.0, 1000)
        )
      }
    }
  }

  test("Test querying with date column + partition pruning") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  grass_date date
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
           | PARTITIONED BY (grass_date)
         """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, date('2023-02-27'))")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, date('2023-02-28'))")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000, date('2023-03-01'))")

      checkAnswer(s"select id, name, price, ts from $tableName where grass_date = date'2023-03-01' order by id")(
        Seq(3, "a3", 10.0, 1000)
      )
    }
  }

  test("Test querying with date column + partition pruning (multi-level partitioning)") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  country string,
           |  date_par date
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
           | PARTITIONED BY (country, date_par)
         """.stripMargin)
      spark.sql(
        s"""
           |ALTER TABLE $tableName
           |SET TBLPROPERTIES (hoodie.write.complex.keygen.validation.enable = 'false')
           |""".stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, 'ID', date '2023-02-27')")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, 'ID', date '2023-02-28')")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000, 'ID', date '2023-03-01')")

      // for lazy fetching partition path & file slice to be enabled, filter must be applied on all partitions
      checkAnswer(s"select id, name, price, ts from $tableName " +
        s"where date_par='2023-03-01' and country='ID' order by id")(
        Seq(3, "a3", 10.0, 1000)
      )

      withSQLConf("spark.sql.session.timeZone" -> "UTC+2") {
        checkAnswer(s"select id, name, price, ts from $tableName " +
          s"where date_par='2023-03-01' and country='ID' order by id")(
          Seq(3, "a3", 10.0, 1000)
        )
      }
    }
  }
}
