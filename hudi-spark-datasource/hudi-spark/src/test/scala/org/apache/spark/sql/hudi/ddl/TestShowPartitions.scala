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

package org.apache.spark.sql.hudi.ddl

import org.apache.hudi.common.util.PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestShowPartitions extends HoodieSparkSqlTestBase {

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
         |tblproperties (
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
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
         | tblproperties (
         |   primaryKey = 'id',
         |   orderingFields = 'ts'
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
      Seq("dt=2021-01-01"), Seq("dt=2021-01-02"), Seq("dt=%s".format(DEFAULT_PARTITION_PATH))
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
         | tblproperties (
         |   primaryKey = 'id',
         |   orderingFields = 'ts'
         | )
       """.stripMargin)
    // Empty partitions
    checkAnswer(s"show partitions $tableName")(Seq.empty: _*)

    withSparkSqlSessionConfig(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false") {
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
    }

    // check all partitions
    checkAnswer(s"show partitions $tableName")(
      Seq("year=2021/month=01/day=01"),
      Seq("year=2021/month=01/day=02"),
      Seq("year=2021/month=02/day=01"),
      Seq("year=2021/month=02/day=%s".format(DEFAULT_PARTITION_PATH)),
      Seq("year=2021/month=%s/day=01".format(DEFAULT_PARTITION_PATH)),
      Seq("year=%s/month=01/day=%s".format(DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH)),
      Seq("year=%s/month=01/day=02".format(DEFAULT_PARTITION_PATH)),
      Seq("year=%s/month=%s/day=01".format(DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH)),
      Seq("year=2022/month=%s/day=%s".format(DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH))
    )

    // check partial partitions
    checkAnswer(s"show partitions $tableName partition(year='2021', month='01', day='01')")(
      Seq("year=2021/month=01/day=01")
    )
    checkAnswer(s"show partitions $tableName partition(year='2021', month='02')")(
      Seq("year=2021/month=02/day=%s".format(DEFAULT_PARTITION_PATH)),
      Seq("year=2021/month=02/day=01")
    )
    checkAnswer(s"show partitions $tableName partition(day='01')")(
      Seq("year=2021/month=02/day=01"),
      Seq("year=2021/month=%s/day=01".format(DEFAULT_PARTITION_PATH)),
      Seq("year=2021/month=01/day=01"),
      Seq("year=%s/month=%s/day=01".format(DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH))
    )
  }

  test("Test alter table show partitions which are dropped before") {
    Seq("true", "false").foreach { enableMetadata =>
      withSQLConf("hoodie.metadata.enable" -> enableMetadata) {
        withTable(generateTableName) { tableName =>
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
               | tblproperties (
               |   primaryKey = 'id',
               |   orderingFields = 'ts'
               | )
             """.stripMargin)
          spark.sql(s"alter table $tableName add partition(year='2023', month='06', day='06')")
          checkAnswer(s"show partitions $tableName")(
            Seq("year=2023/month=06/day=06")
          )
          // Lazily drop that partition
          spark.sql(s"alter table $tableName drop partition(year='2023', month='06', day='06')")
          checkAnswer(s"show partitions $tableName")(Seq.empty: _*)
          // rewrite data to the dropped partition
          withSparkSqlSessionConfig(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false") {
            spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, '2023', '06', '06')")
          }
          checkAnswer(s"show partitions $tableName")(
            Seq("year=2023/month=06/day=06")
          )
        }
      }
    }
  }

  test("Test show partitions after table being overwritten") {
    withTable(generateTableName) { tableName =>
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
           | tblproperties (
           |   primaryKey = 'id',
           |   orderingFields = 'ts'
           | )
         """.stripMargin)

      withSparkSqlSessionConfig(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false") {
        // Insert into dynamic partition
        spark.sql(
          s"""
             | insert into $tableName
             | values
             |   (1, 'a1', 10, 1000, '2023', '12', '01'),
             |   (2, 'a2', 10, 1000, '2023', '12', '02'),
             |   (3, 'a3', 10, 1000, '2023', '12', '03')
        """.stripMargin)
        checkAnswer(s"show partitions $tableName")(
          Seq("year=2023/month=12/day=01"),
          Seq("year=2023/month=12/day=02"),
          Seq("year=2023/month=12/day=03")
        )

        // Insert overwrite table
        spark.sql(
          s"""
             | insert overwrite table $tableName
             | values
             |   (4, 'a4', 10, 1000, '2023', '12', '01'),
             |   (2, 'a2', 10, 1000, '2023', '12', '04')
        """.stripMargin)
        checkAnswer(s"show partitions $tableName")(
          Seq("year=2023/month=12/day=01"),
          Seq("year=2023/month=12/day=04")
        )
      }
    }
  }

  test("Test show partitions in static partition overwrite") {
    withSQLConf("hoodie.datasource.overwrite.mode" -> "STATIC") {
      withTable(generateTableName) { tableName =>
        spark.sql(
          s"""
             | create table $tableName (
             |   id int,
             |   name string,
             |   price double,
             |   ts long,
             |   dt string
             | ) using hudi
             | partitioned by (dt)
             | tblproperties (
             |   primaryKey = 'id',
             |   orderingFields = 'ts'
             | )
         """.stripMargin)

        // Insert into dynamic partition
        spark.sql(
          s"""
             | insert into $tableName
             | values
             |   (1, 'a1', 10, 1000, '2023-12-01'),
             |   (2, 'a2', 10, 1000, '2023-12-02'),
             |   (3, 'a3', 10, 1000, '2023-12-03')
        """.stripMargin)
        checkAnswer(s"show partitions $tableName")(
          Seq("dt=2023-12-01"),
          Seq("dt=2023-12-02"),
          Seq("dt=2023-12-03")
        )

        // Insert overwrite static partitions
        spark.sql(
          s"""
             | insert overwrite table $tableName partition(dt='2023-12-01')
             | values
             |   (4, 'a4', 10, 1000),
             |   (2, 'a2', 10, 1000)
        """.stripMargin)
        checkAnswer(s"show partitions $tableName")(
          Seq("dt=2023-12-01"),
          Seq("dt=2023-12-02"),
          Seq("dt=2023-12-03")
        )
      }
    }
  }
}
