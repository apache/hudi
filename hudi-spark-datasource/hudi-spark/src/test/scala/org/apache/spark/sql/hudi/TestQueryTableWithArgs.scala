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
import org.apache.spark.sql.execution.{ExtendedMode, SimpleMode}

class TestQueryTableWithArgs extends HoodieSparkSqlTestBase {
  test("Test query table with snapshot args") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        val tableName1 = generateTableName
        println(HoodieSparkUtils.getSparkVersion)
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
        // query plan like below
        // == Parsed Logical Plan ==
        // 'Project ['id, 'name, 'price, 'ts]
        //+- 'TableArgumentRelation 'UnresolvedRelation [h0], [], false, [hoodie.datasource.query.type=snapshot, as.of.instant=$instant1]
        val querySql =
        s"select id, name, price, ts from $tableName1 ['hoodie.datasource.query.type'='snapshot','as.of.instant'='$instant1']"

        checkAnswer(querySql)(
          Seq(1, "a1", 10.0, 1000)
        )

        checkAnswer(
          s"select id, name, price, ts from $tableName1 TIMESTAMP AS OF '$instant1'")(
          Seq(1, "a1", 10.0, 1000)
        )

      }
    }
  }

  test("Test query table with no query args") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        val tableName1 = generateTableName
        println(HoodieSparkUtils.getSparkVersion)
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

        checkAnswer(
          s"select id, name, price, ts from $tableName1")(
          Seq(1, "a1", 10.0, 1000)
        )

        checkExceptionContain(s"select id, name, price, ts from $tableName1 []")("mismatched input '[' expecting")

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

        // time travel from instant1 with empty table args
        checkAnswer(
          s"select id, name, price, ts from $tableName1 [] TIMESTAMP AS OF '$instant1'")(
          Seq(1, "a1", 10.0, 1000)
        )

      }
    }
  }

  test("Test query table with query args and time travel") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        val tableName1 = generateTableName
        println(HoodieSparkUtils.getSparkVersion)
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

        checkAnswer(
          s"select id, name, price, ts from $tableName1")(
          Seq(1, "a1", 10.0, 1000)
        )

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

        // time travel from instant1 with empty table args
        checkAnswer(
          s"select id, name, price, ts from $tableName1 [] TIMESTAMP AS OF '$instant1'")(
          Seq(1, "a1", 10.0, 1000)
        )

        // time travel from instant1 with table args

        val errorMsg = s"Only one table parameter ['hoodie.datasource.query.type'='snapshot','as.of.instant'='$instant1'] and snapshot query TIMESTAMPASOF'$instant1' can exist("

        checkExceptionContain(
          s"select id, name, price, ts from $tableName1 ['hoodie.datasource.query.type'='snapshot','as.of.instant'='$instant1'] TIMESTAMP AS OF '$instant1'")(errorMsg)
      }
    }
  }


  test("Test Insert Into Records with snapshot args To new Table") {
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

//        == Parsed Logical Plan ==
//        'InsertIntoStatement 'UnresolvedRelation [h1], [], false, false, false
//        +- 'Project ['id, 'name, 'price, 'ts, 2022-02-14 AS dt#43]
//        +- 'TableArgumentRelation 'UnresolvedRelation [h0], [], false, [hoodie.datasource.query.type=snapshot, as.of.instant=20221109081017574]

//        spark.sql(
//          s"""
//             | insert into $tableName2
//             | select id, name, price, ts, '2022-02-14' as dt
//             | from $tableName1 ['hoodie.datasource.query.type'='snapshot','as.of.instant'='$instant1']
//        """.stripMargin).explain(ExtendedMode.name)


        println("before read")
        // Insert into static partition
//        == Parsed Logical Plan ==
//        'InsertIntoStatement 'UnresolvedRelation [h1], [], false, [dt=Some(2022-02-15)], false, false
//        +- 'Project [2 AS id#88, a2 AS name#89, 'price, 'ts]
//        +- 'UnresolvedRelation [h0], [], false

        spark.sql(
          s"""
             | insert into $tableName2 partition (dt='2022-02-15')
             |  select 2 as id, 'a2' as name, price, ts
             |  from $tableName1 timestamp as of $instant1
        """.stripMargin).explain(ExtendedMode.name)

      }
    }
  }

}
