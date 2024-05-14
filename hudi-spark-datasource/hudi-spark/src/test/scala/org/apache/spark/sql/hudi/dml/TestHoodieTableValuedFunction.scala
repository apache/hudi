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

package org.apache.spark.sql.hudi.dml

import org.apache.hudi.DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION
import org.apache.hudi.HoodieSparkUtils
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestHoodieTableValuedFunction extends HoodieSparkSqlTestBase {

  test(s"Test hudi_query Table-Valued Function") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          val tableName = generateTableName
          spark.sql("set " + SPARK_SQL_INSERT_INTO_OPERATION.key + "=upsert")
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               |tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               |)
               |location '${tmp.getCanonicalPath}/$tableName'
               |""".stripMargin
          )

          spark.sql(
            s"""
               | insert into $tableName
               | values (1, 'a1', 10, 1000), (2, 'a2', 20, 1000), (3, 'a3', 30, 1000)
               | """.stripMargin
          )

          checkAnswer(s"select id, name, price, ts from hudi_query('$tableName', 'read_optimized')")(
            Seq(1, "a1", 10.0, 1000),
            Seq(2, "a2", 20.0, 1000),
            Seq(3, "a3", 30.0, 1000)
          )

          spark.sql(
            s"""
               | insert into $tableName
               | values (1, 'a1_1', 10, 1100), (2, 'a2_2', 20, 1100), (3, 'a3_3', 30, 1100)
               | """.stripMargin
          )

          if (tableType == "cow") {
            checkAnswer(s"select id, name, price, ts from hudi_query('$tableName', 'read_optimized')")(
              Seq(1, "a1_1", 10.0, 1100),
              Seq(2, "a2_2", 20.0, 1100),
              Seq(3, "a3_3", 30.0, 1100)
            )
          } else {
            checkAnswer(s"select id, name, price, ts from hudi_query('$tableName', 'read_optimized')")(
              Seq(1, "a1", 10.0, 1000),
              Seq(2, "a2", 20.0, 1000),
              Seq(3, "a3", 30.0, 1000)
            )
          }
        }
      }
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
  }

  test(s"Test hudi_table_changes latest_state") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        Seq(
          ("cow", true),
          ("mor", true),
          ("cow", false),
          ("mor", false)
        ).foreach { parameters =>
          val tableType = parameters._1
          val isTableId = parameters._2
          val tableName = generateTableName
          val tablePath = s"${tmp.getCanonicalPath}/$tableName"
          val identifier = if (isTableId) tableName else tablePath
          spark.sql("set hoodie.sql.insert.mode = non-strict")
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               |tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               |)
               |location '$tablePath'
               |""".stripMargin
          )

          spark.sql(
            s"""
               | insert into $tableName
               | values (1, 'a1', 10, 1000), (2, 'a2', 20, 1000), (3, 'a3', 30, 1000)
               | """.stripMargin
          )

          val firstInstant = spark.sql(s"select min(_hoodie_commit_time) as commitTime from  $tableName order by commitTime").first().getString(0)

          checkAnswer(
            s"""select id,
               |name,
               |price,
               |ts
               |from hudi_table_changes('$identifier', 'latest_state', 'earliest')
               |""".stripMargin
          )(
            Seq(1, "a1", 10.0, 1000),
            Seq(2, "a2", 20.0, 1000),
            Seq(3, "a3", 30.0, 1000)
          )

          spark.sql(
            s"""
               | insert into $tableName
               | values (1, 'a1_1', 10, 1100), (2, 'a2_2', 20, 1100), (3, 'a3_3', 30, 1100)
               | """.stripMargin
          )
          val secondInstant = spark.sql(s"select max(_hoodie_commit_time) as commitTime from  $tableName order by commitTime").first().getString(0)

          checkAnswer(
            s"""select id,
               |name,
               |price,
               |ts
               |from hudi_table_changes(
               |'$identifier',
               |'latest_state',
               |'$firstInstant')
               |""".stripMargin
          )(
            Seq(1, "a1_1", 10.0, 1100),
            Seq(2, "a2_2", 20.0, 1100),
            Seq(3, "a3_3", 30.0, 1100)
          )

          spark.sql(
            s"""
               | insert into $tableName
               | values (1, 'a1_1', 10, 1200), (2, 'a2_2', 20, 1200), (3, 'a3_3', 30, 1200)
               | """.stripMargin
          )

          // should not include the first and latest instant
          checkAnswer(
            s"""select id,
               | name,
               | price,
               | ts
               | from hudi_table_changes(
               | '$identifier',
               | 'latest_state',
               | '$firstInstant',
               | '$secondInstant')
               | """.stripMargin
          )(
            Seq(1, "a1_1", 10.0, 1100),
            Seq(2, "a2_2", 20.0, 1100),
            Seq(3, "a3_3", 30.0, 1100)
          )
        }
      }
    }
  }

  test(s"Test hudi_table_changes cdc") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        Seq(
          ("cow", true),
          ("mor", true),
          ("cow", false),
          ("mor", false)
        ).foreach { parameters =>
          val tableType = parameters._1
          val isTableId = parameters._2
          val tableName = generateTableName
          val tablePath = s"${tmp.getCanonicalPath}/$tableName"
          val identifier = if (isTableId) tableName else tablePath
          spark.sql("set hoodie.sql.insert.mode = upsert")
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               |tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts',
               |  'hoodie.table.cdc.enabled' = 'true',
               |  'hoodie.table.cdc.supplemental.logging.mode' = 'data_before_after'
               |)
               |location '$tablePath'
               |""".stripMargin
          )

          spark.sql(
            s"""
               | insert into $tableName
               | values (1, 'a1', 10, 1000), (2, 'a2', 20, 1000), (3, 'a3', 30, 1000)
               | """.stripMargin
          )
          val originSchema = spark.read.format("hudi").load(tablePath).schema
          val firstInstant = spark.sql(s"select min(_hoodie_commit_time) as commitTime from  $tableName order by commitTime").first().getString(0)

          val cdcDataOnly1 = spark.sql(
            s"""select
               | op,
               | before,
               | after
               |from hudi_table_changes('$identifier', 'cdc', 'earliest')
               |""".stripMargin
          )

          val change1 = cdcDataOnly1.select(
            col("op"),
            col("before"),
            from_json(col("after"), originSchema).as("after")
          ).select(
            col("op"),
            col("before"),
            col("after.id"),
            col("after.name"),
            col("after.price"),
            col("after.ts")
          ).orderBy("after.id").collect()
          checkAnswer(change1)(
            Seq("i", null, 1, "a1", 10.0, 1000),
            Seq("i", null, 2, "a2", 20.0, 1000),
            Seq("i", null, 3, "a3", 30.0, 1000)
          )

          spark.sql(
            s"""
               | insert into $tableName
               | values (1, 'a1_1', 10, 1100), (2, 'a2_2', 20, 1100), (3, 'a3_3', 30, 1100)
               | """.stripMargin
          )
          val secondInstant = spark.sql(s"select max(_hoodie_commit_time) as commitTime from  $tableName order by commitTime").first().getString(0)

          val cdcDataOnly2 = spark.sql(
            s"""select
               | op,
               | before,
               | after
               |from hudi_table_changes(
               |'$identifier',
               |'cdc',
               |'$firstInstant')
               |""".stripMargin
          )

          val change2 = cdcDataOnly2.select(
            col("op"),
            from_json(col("before"), originSchema).as("before"),
            from_json(col("after"), originSchema).as("after")
          ).select(
            col("op"),
            col("before.id"),
            col("before.name"),
            col("before.price"),
            col("before.ts"),
            col("after.id"),
            col("after.name"),
            col("after.price"),
            col("after.ts")
          ).orderBy("after.id").collect()
          checkAnswer(change2)(
            Seq("u", 1, "a1", 10.0, 1000, 1, "a1_1", 10.0, 1100),
            Seq("u", 2, "a2", 20.0, 1000, 2, "a2_2", 20.0, 1100),
            Seq("u", 3, "a3", 30.0, 1000, 3, "a3_3", 30.0, 1100)
          )

          spark.sql(
            s"""
               | insert into $tableName
               | values (1, 'a1_1', 11, 1200), (2, 'a2_2', 21, 1200), (3, 'a3_3', 31, 1200)
               | """.stripMargin
          )

          // should not include the first and latest instant
          val cdcDataOnly3 = spark.sql(
            s"""select
               | op,
               | before,
               | after
               | from hudi_table_changes(
               | '$identifier',
               | 'cdc',
               | '$firstInstant',
               | '$secondInstant')
               | """.stripMargin
          )

          val change3 = cdcDataOnly3.select(
            col("op"),
            from_json(col("before"), originSchema).as("before"),
            from_json(col("after"), originSchema).as("after")
          ).select(
            col("op"),
            col("before.id"),
            col("before.name"),
            col("before.price"),
            col("before.ts"),
            col("after.id"),
            col("after.name"),
            col("after.price"),
            col("after.ts")
          ).orderBy("after.id").collect()
          checkAnswer(change3)(
            Seq("u", 1, "a1", 10.0, 1000, 1, "a1_1", 10.0, 1100),
            Seq("u", 2, "a2", 20.0, 1000, 2, "a2_2", 20.0, 1100),
            Seq("u", 3, "a3", 30.0, 1000, 3, "a3_3", 30.0, 1100)
          )
        }
      }
    }
  }
}
