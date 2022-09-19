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

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.table.HoodieTableMetaClient

class TestTimeTravelTable extends HoodieSparkSqlTestBase {
  test("Test Insert and Update Record with time travel") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        val tableName1 = generateTableName
        spark.sql(
          s"""
             |create table $tableName1 (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  type = 'cow',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | location '${tmp.getCanonicalPath}/$tableName1'
       """.stripMargin)

        spark.sql(s"insert into $tableName1 values(1, 'a1', 10, 1000)")

        val metaClient1 = HoodieTableMetaClient.builder()
          .setBasePath(s"${tmp.getCanonicalPath}/$tableName1")
          .setConf(spark.sessionState.newHadoopConf())
          .build()

        val instant1 = metaClient1.getActiveTimeline.getAllCommitsTimeline
          .lastInstant().get().getTimestamp

        spark.sql(s"insert into $tableName1 values(1, 'a2', 20, 2000)")

        checkAnswer(s"select id, name, price, ts from $tableName1")(
          Seq(1, "a2", 20.0, 2000)
        )

        // time travel from instant1
        checkAnswer(
          s"select id, name, price, ts from $tableName1 TIMESTAMP AS OF '$instant1'")(
          Seq(1, "a1", 10.0, 1000)
        )
      }
    }
  }

  test("Test Insert Into Records with time travel To new Table") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        // Create Non-Partitioned table
        val tableName1 = generateTableName
        spark.sql(
          s"""
             |create table $tableName1 (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  type = 'cow',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | location '${tmp.getCanonicalPath}/$tableName1'
       """.stripMargin)

        spark.sql(s"insert into $tableName1 values(1, 'a1', 10, 1000)")

        val metaClient1 = HoodieTableMetaClient.builder()
          .setBasePath(s"${tmp.getCanonicalPath}/$tableName1")
          .setConf(spark.sessionState.newHadoopConf())
          .build()

        val instant1 = metaClient1.getActiveTimeline.getAllCommitsTimeline
          .lastInstant().get().getTimestamp


        val tableName2 = generateTableName
        // Create a partitioned table
        spark.sql(
          s"""
             |create table $tableName2 (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  dt string
             |) using hudi
             | tblproperties (primaryKey = 'id')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}/$tableName2'
       """.stripMargin)

        // Insert into dynamic partition
        spark.sql(
          s"""
             | insert into $tableName2
             | select id, name, price, ts, '2022-02-14' as dt
             | from $tableName1 TIMESTAMP AS OF '$instant1'
        """.stripMargin)
        checkAnswer(s"select id, name, price, ts, dt from $tableName2")(
          Seq(1, "a1", 10.0, 1000, "2022-02-14")
        )

        // Insert into static partition
        spark.sql(
          s"""
             | insert into $tableName2 partition(dt = '2022-02-15')
             | select 2 as id, 'a2' as name, price, ts
             | from $tableName1 TIMESTAMP AS OF '$instant1'
        """.stripMargin)
        checkAnswer(
          s"select id, name, price, ts, dt from $tableName2")(
          Seq(1, "a1", 10.0, 1000, "2022-02-14"),
          Seq(2, "a2", 10.0, 1000, "2022-02-15")
        )
      }
    }
  }

  test("Test Two Table's Union Join with time travel") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          val tableName = generateTableName

          val basePath = tmp.getCanonicalPath
          val tableName1 = tableName + "_1"
          val tableName2 = tableName + "_2"
          val path1 = s"$basePath/$tableName1"
          val path2 = s"$basePath/$tableName2"

          spark.sql(
            s"""
               |create table $tableName1 (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               | )
               | location '$path1'
       """.stripMargin)

          spark.sql(
            s"""
               |create table $tableName2 (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               | )
               | location '$path2'
       """.stripMargin)

          spark.sql(s"insert into $tableName1 values(1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName1 values(2, 'a2', 20, 1000)")

          checkAnswer(s"select id, name, price, ts from $tableName1")(
            Seq(1, "a1", 10.0, 1000),
            Seq(2, "a2", 20.0, 1000)
          )

          checkAnswer(s"select id, name, price, ts from $tableName1")(
            Seq(1, "a1", 10.0, 1000),
            Seq(2, "a2", 20.0, 1000)
          )

          spark.sql(s"insert into $tableName2 values(3, 'a3', 10, 1000)")
          spark.sql(s"insert into $tableName2 values(4, 'a4', 20, 1000)")

          checkAnswer(s"select id, name, price, ts from $tableName2")(
            Seq(3, "a3", 10.0, 1000),
            Seq(4, "a4", 20.0, 1000)
          )

          val metaClient1 = HoodieTableMetaClient.builder()
            .setBasePath(path1)
            .setConf(spark.sessionState.newHadoopConf())
            .build()

          val metaClient2 = HoodieTableMetaClient.builder()
            .setBasePath(path2)
            .setConf(spark.sessionState.newHadoopConf())
            .build()

          val instant1 = metaClient1.getActiveTimeline.getAllCommitsTimeline
            .lastInstant().get().getTimestamp

          val instant2 = metaClient2.getActiveTimeline.getAllCommitsTimeline
            .lastInstant().get().getTimestamp

          val sql =
            s"""
               |select id, name, price, ts from $tableName1 TIMESTAMP AS OF '$instant1' where id=1
               |union
               |select id, name, price, ts from $tableName2 TIMESTAMP AS OF '$instant2' where id>1
               |""".stripMargin

          checkAnswer(sql)(
            Seq(1, "a1", 10.0, 1000),
            Seq(3, "a3", 10.0, 1000),
            Seq(4, "a4", 20.0, 1000)
          )
        }
      }
    }
  }

  test("Test Unsupported syntax can be parsed") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      checkAnswer("select 1 distribute by 1")(Seq(1))
      withTempDir { dir =>
        val path = dir.toURI.getPath
        spark.sql(s"insert overwrite local directory '$path' using parquet select 1")
        // Requires enable hive support, so didn't test it
        // spark.sql(s"insert overwrite local directory '$path' stored as orc select 1")
      }
    }
  }

  test("Test Select Record with time travel and Repartition") {
    if (HoodieSparkUtils.gteqSpark3_2) {
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
             | tblproperties (
             |  type = 'cow',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(s"${tmp.getCanonicalPath}/$tableName")
          .setConf(spark.sessionState.newHadoopConf())
          .build()

        val instant = metaClient.getActiveTimeline.getAllCommitsTimeline
          .lastInstant().get().getTimestamp
        spark.sql(s"insert into $tableName values(1, 'a2', 20, 2000)")

        checkAnswer(s"select id, name, price, ts from $tableName distribute by cast(rand() * 2 as int)")(
          Seq(1, "a2", 20.0, 2000)
        )

        // time travel from instant
        checkAnswer(
          s"select id, name, price, ts from $tableName TIMESTAMP AS OF '$instant' distribute by cast(rand() * 2 as int)")(
          Seq(1, "a1", 10.0, 1000)
        )
      }
    }
  }

  test("Test Time Travel With Schema Evolution") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        spark.sql("set hoodie.schema.on.read.enable=true")
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(s"${tmp.getCanonicalPath}/$tableName")
          .setConf(spark.sessionState.newHadoopConf())
          .build()
        val instant1 = metaClient.reloadActiveTimeline().getAllCommitsTimeline
          .lastInstant().get().getTimestamp

        // add column
        spark.sql(s"alter table $tableName add columns (company string)")
        spark.sql(s"insert into $tableName values(2, 'a2', 11, 1100, 'hudi')")
        val instant2 = metaClient.reloadActiveTimeline().getAllCommitsTimeline
          .lastInstant().get().getTimestamp

        // drop column
        spark.sql(s"alter table $tableName drop column price")

        val result1 = spark.sql(s"select * from ${tableName} timestamp as of $instant1 order by id")
          .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name").collect()
        checkAnswer(result1)(Seq(1, "a1", 10.0, 1000))

        val result2 = spark.sql(s"select * from ${tableName} timestamp as of $instant2 order by id")
          .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name").collect()
        checkAnswer(result2)(
          Seq(1, "a1", 10.0, 1000, null),
          Seq(2, "a2", 11.0, 1100, "hudi")
        )

        val result3 = spark.sql(s"select * from ${tableName} order by id")
          .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name").collect()
        checkAnswer(result3)(
          Seq(1, "a1", 1000, null),
          Seq(2, "a2", 1100, "hudi")
        )
      }
    }
  }
}
