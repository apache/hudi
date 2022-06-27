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
}
