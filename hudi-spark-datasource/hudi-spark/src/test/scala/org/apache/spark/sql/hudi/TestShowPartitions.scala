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

import org.apache.spark.sql.Row

class TestShowPartitions extends TestHoodieSqlBase {

  test("Test Show Non Partitioned Table's Partitions") {
    val tableName = generateTableName
    // Create a non-partitioned table
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         |) using hudi
         |options (
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         )
       """.stripMargin)
    // Insert data
    spark.sql(
      s"""
         | insert into $tableName
         | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
        """.stripMargin)
    checkAnswer(s"show partitions $tableName")(Seq.empty: _*)
  }

  test("Test Show Partitioned Table's Partitions") {
    val tableName = generateTableName
    // Create a partitioned table
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         | price double,
         |  ts long,
         |  dt string
         ) using hudi
         | partitioned by (dt)
         | options (
         |   primaryKey = 'id',
         |   preCombineField = 'ts'
         | )
       """.stripMargin)
    // Empty partitions
    checkAnswer(s"show partitions $tableName")(Seq.empty: _*)

    // Insert into dynamic partition
    spark.sql(
      s"""
         | insert into $tableName
         | values (1, 'a1', 10, 1000, '2021-01-01')
        """.stripMargin)
    checkAnswer(s"show partitions $tableName")(Seq("dt=2021-01-01"))

    // Insert into static partition
    spark.sql(
      s"""
         | insert into $tableName partition(dt = '2021-01-02')
         | select 2 as id, 'a2' as name, 10 as price, 1000 as ts
        """.stripMargin)
    checkAnswer(s"show partitions $tableName partition(dt='2021-01-02')")(Seq("dt=2021-01-02"))

    // Insert into null partition
    spark.sql(
      s"""
         | insert into $tableName
         | select 3 as id, 'a3' as name, 10 as price, 1000 as ts, null as dt
        """.stripMargin)
    checkAnswer(s"show partitions $tableName")(
      Seq("dt=2021-01-01"), Seq("dt=2021-01-02"), Seq("dt=default")
    )
  }

  test("Test Show Table's Partitions with MultiLevel Partitions") {
    val tableName = generateTableName
    // Create a multi-level partitioned table
    spark.sql(
      s"""
         | create table $tableName (
         |   id int,
         |   name string,
         |   price double,
         |   ts long,
         |   year string,
         |   month string,
         |   day string
         | ) using hudi
         | partitioned by (year, month, day)
         | options (
         |   primaryKey = 'id',
         |   preCombineField = 'ts'
         | )
       """.stripMargin)
    // Empty partitions
    checkAnswer(s"show partitions $tableName")(Seq.empty: _*)

    // Insert into dynamic partition
    spark.sql(
      s"""
         | insert into $tableName
         | values
         |   (1, 'a1', 10, 1000, '2021', '01', '01'),
         |   (2, 'a2', 10, 1000, '2021', '01', '02'),
         |   (3, 'a3', 10, 1000, '2021', '02', '01'),
         |   (4, 'a4', 10, 1000, '2021', '02', null),
         |   (5, 'a5', 10, 1000, '2021', null, '01'),
         |   (6, 'a6', 10, 1000, null, '01', '02'),
         |   (7, 'a6', 10, 1000, '2022', null, null),
         |   (8, 'a6', 10, 1000, null, '01', null),
         |   (9, 'a6', 10, 1000, null, null, '01')
        """.stripMargin)

    // check all partitions
    checkAnswer(s"show partitions $tableName")(
      Seq("year=2021/month=01/day=01"),
      Seq("year=2021/month=01/day=02"),
      Seq("year=2021/month=02/day=01"),
      Seq("year=2021/month=02/day=default"),
      Seq("year=2021/month=default/day=01"),
      Seq("year=default/month=01/day=default"),
      Seq("year=default/month=01/day=02"),
      Seq("year=default/month=default/day=01"),
      Seq("year=2022/month=default/day=default")
    )

    // check partial partitions
    checkAnswer(s"show partitions $tableName partition(year='2021', month='01', day='01')")(
      Seq("year=2021/month=01/day=01")
    )
    checkAnswer(s"show partitions $tableName partition(year='2021', month='02')")(
      Seq("year=2021/month=02/day=default"),
      Seq("year=2021/month=02/day=01")
    )
    checkAnswer(s"show partitions $tableName partition(day=01)")(
      Seq("year=2021/month=02/day=01"),
      Seq("year=2021/month=default/day=01"),
      Seq("year=2021/month=01/day=01"),
      Seq("year=default/month=default/day=01")
    )
  }
}
