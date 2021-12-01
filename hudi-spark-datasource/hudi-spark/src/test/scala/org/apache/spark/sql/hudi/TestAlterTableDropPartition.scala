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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.{ComplexKeyGenerator, SimpleKeyGenerator}
import org.apache.spark.sql.SaveMode

import scala.util.control.NonFatal

class TestAlterTableDropPartition extends TestHoodieSqlBase {

  test("Drop non-partitioned table") {
    val tableName = generateTableName
    // create table
    spark.sql(
      s"""
         | create table $tableName (
         |  id bigint,
         |  name string,
         |  ts string,
         |  dt string
         | )
         | using hudi
         | tblproperties (
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         | )
         |""".stripMargin)
    // insert data
    spark.sql(s"""insert into $tableName values (1, "z3", "v1", "2021-10-01"), (2, "l4", "v1", "2021-10-02")""")

    checkExceptionContain(s"alter table $tableName drop partition (dt='2021-10-01')")(
      s"dt is not a valid partition column in table")
  }

  Seq(false, true).foreach { urlencode =>
    test(s"Drop single-partition table' partitions, urlencode: $urlencode") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        import spark.implicits._
        val df = Seq((1, "z3", "v1", "2021/10/01"), (2, "l4", "v1", "2021/10/02"))
          .toDF("id", "name", "ts", "dt")

        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(TABLE_TYPE.key, COW_TABLE_TYPE_OPT_VAL)
          .option(RECORDKEY_FIELD.key, "id")
          .option(PRECOMBINE_FIELD.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "dt")
          .option(URL_ENCODE_PARTITIONING.key(), urlencode)
          .option(KEYGENERATOR_CLASS_NAME.key, classOf[SimpleKeyGenerator].getName)
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        // register meta to spark catalog by creating table
        spark.sql(
          s"""
             |create table $tableName using hudi
             |tblproperties (
             | primaryKey = 'id',
             | preCombineField = 'ts'
             |)
             |partitioned by (dt)
             |location '$tablePath'
             |""".stripMargin)

        // drop 2021-10-01 partition
        spark.sql(s"alter table $tableName drop partition (dt='2021/10/01')")

        checkAnswer(s"select dt from $tableName") (Seq(s"2021/10/02"))
      }
    }
  }

  test("Drop single-partition table' partitions created by sql") {
    val tableName = generateTableName
    // create table
    spark.sql(
      s"""
         | create table $tableName (
         |  id bigint,
         |  name string,
         |  ts string,
         |  dt string
         | )
         | using hudi
         | tblproperties (
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         | )
         | partitioned by (dt)
         |""".stripMargin)
    // insert data
    spark.sql(s"""insert into $tableName values (1, "z3", "v1", "2021-10-01"), (2, "l4", "v1", "2021-10-02")""")

    // specify duplicate partition columns
    checkExceptionContain(s"alter table $tableName drop partition (dt='2021-10-01', dt='2021-10-02')")(
      "Found duplicate keys 'dt'")

    // drop 2021-10-01 partition
    spark.sql(s"alter table $tableName drop partition (dt='2021-10-01')")

    checkAnswer(s"select id, name, ts, dt from $tableName") (Seq(2, "l4", "v1", "2021-10-02"))
  }

  Seq(false, true).foreach { hiveStyle =>
    test(s"Drop multi-level partitioned table's partitions, isHiveStylePartitioning: $hiveStyle") {
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
          .option(PRECOMBINE_FIELD.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "year,month,day")
          .option(HIVE_STYLE_PARTITIONING.key, hiveStyle)
          .option(KEYGENERATOR_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName)
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        // register meta to spark catalog by creating table
        spark.sql(
          s"""
             |create table $tableName using hudi
             |tblproperties (
             | primaryKey = 'id',
             | preCombineField = 'ts'
             |)
             |partitioned by (year, month, day)
             |location '$tablePath'
             |""".stripMargin)

        // not specified all partition column
        checkExceptionContain(s"alter table $tableName drop partition (year='2021', month='10')")(
          "All partition columns need to be specified for Hoodie's dropping partition"
        )
        // drop 2021-10-01 partition
        spark.sql(s"alter table $tableName drop partition (year='2021', month='10', day='01')")

        checkAnswer(s"select id, name, ts, year, month, day from $tableName")(
          Seq(2, "l4", "v1", "2021", "10", "02")
        )
      }
    }
  }
}
