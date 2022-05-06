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
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.spark.sql.SaveMode

class TestDeleteTable extends HoodieSparkSqlTestBase {

  test("Test Delete Table") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach {tableType =>
        val tableName = generateTableName
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)
        // insert data to table
        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        // delete data from table
        spark.sql(s"delete from $tableName where id = 1")
        checkAnswer(s"select count(1) from $tableName") (
          Seq(0)
        )

        spark.sql(s"insert into $tableName select 2, 'a2', 10, 1000")
        spark.sql(s"delete from $tableName where id = 1")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(2, "a2", 10.0, 1000)
        )

        spark.sql(s"delete from $tableName")
        checkAnswer(s"select count(1) from $tableName")(
          Seq(0)
        )
      }
    }
  }

  test("Test Delete Table On Non-PK Condition") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach {tableType =>
        /** non-partitioned table */
        val tableName = generateTableName
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
          """.stripMargin)

        // insert data to table
        spark.sql(
          s"""
             |insert into $tableName
             |values (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 1000), (3, 'a2', 30.0, 1000)
          """.stripMargin)
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 20.0, 1000),
          Seq(3, "a2", 30.0, 1000)
        )

        // delete data on non-pk condition
        spark.sql(s"delete from $tableName where name = 'a2'")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        /** partitioned table */
        val ptTableName = generateTableName + "_pt"
        // create table
        spark.sql(
          s"""
             |create table $ptTableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  pt string
             |) using hudi
             | location '${tmp.getCanonicalPath}/$ptTableName'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | partitioned by (pt)
          """.stripMargin)

        // insert data to table
        spark.sql(
          s"""
             |insert into $ptTableName
             |values (1, 'a1', 10.0, 1000, "2021"), (2, 'a2', 20.0, 1000, "2021"), (3, 'a2', 30.0, 1000, "2022")
          """.stripMargin)
        checkAnswer(s"select id, name, price, ts, pt from $ptTableName")(
          Seq(1, "a1", 10.0, 1000, "2021"),
          Seq(2, "a2", 20.0, 1000, "2021"),
          Seq(3, "a2", 30.0, 1000, "2022")
        )

        // delete data on non-pk condition
        spark.sql(s"delete from $ptTableName where name = 'a2'")
        checkAnswer(s"select id, name, price, ts, pt from $ptTableName")(
          Seq(1, "a1", 10.0, 1000, "2021")
        )

        spark.sql(s"delete from $ptTableName where pt = '2021'")
        checkAnswer(s"select id, name, price, ts, pt from $ptTableName")(
          Seq.empty: _*
        )
      }
    }
  }

  test("Test Delete Table with op upsert") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach {tableType =>
        val tableName = generateTableName
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts',
             |  hoodie.datasource.write.operation = 'upsert'
             | )
       """.stripMargin)
        // insert data to table
        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        // delete data from table
        spark.sql(s"delete from $tableName where id = 1")
        checkAnswer(s"select count(1) from $tableName") (
          Seq(0)
        )

        spark.sql(s"insert into $tableName select 2, 'a2', 10, 1000")
        spark.sql(s"delete from $tableName where id = 1")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(2, "a2", 10.0, 1000)
        )

        spark.sql(s"delete from $tableName")
        checkAnswer(s"select count(1) from $tableName")(
          Seq(0)
        )
      }
    }
  }

  Seq(false, true).foreach { urlencode =>
    test(s"Test Delete single-partition table' partitions, urlencode: $urlencode") {
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
             |location '$tablePath'
             |""".stripMargin)

        // delete 2021-10-01 partition
        if (urlencode) {
          spark.sql(s"""delete from $tableName where dt="2021/10/01"""")
        } else {
          spark.sql(s"delete from $tableName where dt='2021/10/01'")
        }

        checkAnswer(s"select dt from $tableName")(Seq(s"2021/10/02"))
      }
    }
  }
}
