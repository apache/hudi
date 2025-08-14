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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestTruncateTable extends HoodieSparkSqlTestBase {

  test("Test Truncate non-partitioned Table") {
    Seq("cow", "mor").foreach { tableType =>
      val tableName = generateTableName
      // Create table
      spark.sql(
        s"""
            |create table $tableName (
            |  id int,
            |  name string,
            |  price double,
            |  ts long
            |) using hudi
            | options (
            |  type = '$tableType',
            |  primaryKey = 'id',
            |  preCombineField = 'ts'
            | )
       """.stripMargin)
      // Insert data
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      // Truncate table
      spark.sql(s"truncate table $tableName")
      checkAnswer(s"select count(1) from $tableName")(Seq(0))

      // Insert data to the truncated table.
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 10.0, 1000)
      )
    }
  }

  Seq(false, true).foreach { urlencode =>
    test(s"Test Truncate single-partition table' partitions, urlencode: $urlencode") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        import spark.implicits._
        val df = Seq((1, "z3", "v1", "2021/10/01"), (2, "l4", "v1", "2021/10/02"))
          .toDF("id", "name", "ts", "dt")

        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(TABLE_TYPE.key, MOR_TABLE_TYPE_OPT_VAL)
          .option(RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "dt")
          .option(URL_ENCODE_PARTITIONING.key(), urlencode)
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        // register meta to spark catalog by creating table
        spark.sql(
          s"""
             |create table $tableName using hudi
             |location '$tablePath'
             |""".stripMargin)

        // truncate 2021-10-01 partition
        spark.sql(s"truncate table $tableName partition (dt='2021/10/01')")

        checkAnswer(s"select dt from $tableName")(Seq(s"2021/10/02"))

        // Truncate table
        spark.sql(s"truncate table $tableName")
        checkAnswer(s"select count(1) from $tableName")(Seq(0))
      }
    }
  }

  Seq(false, true).foreach { hiveStyle =>
    test(s"Test Truncate multi-level partitioned table's partitions, isHiveStylePartitioning: $hiveStyle") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        import spark.implicits._
        val df = Seq((1, "z3", "v1", "2021", "10", "01"), (2, "l4", "v1", "2021", "10","02"))
          .toDF("id", "name", "ts", "year", "month", "day")

        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(TABLE_TYPE.key, COW_TABLE_TYPE_OPT_VAL)
          .option(RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "year,month,day")
          .option(HIVE_STYLE_PARTITIONING.key, hiveStyle)
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        // register meta to spark catalog by creating table
        spark.sql(
          s"""
             |create table $tableName using hudi
             |location '$tablePath'
             |""".stripMargin)

        // not specified all partition column
        checkExceptionContain(s"truncate table $tableName partition (year='2021', month='10')")(
          "All partition columns need to be specified for Hoodie's partition"
        )

        // truncate 2021-10-01 partition
        spark.sql(s"truncate table $tableName partition (year='2021', month='10', day='01')")

        checkAnswer(s"select id, name, ts, year, month, day from $tableName")(
          Seq(2, "l4", "v1", "2021", "10", "02")
        )

        // Truncate table
        spark.sql(s"truncate table $tableName")
        checkAnswer(s"select count(1) from $tableName")(Seq(0))
      }
    }
  }
}
