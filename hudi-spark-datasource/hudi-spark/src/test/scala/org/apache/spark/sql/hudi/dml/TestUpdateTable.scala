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

import org.apache.hudi.DataSourceWriteOptions.SPARK_SQL_OPTIMIZED_WRITES
import org.apache.hudi.HoodieSparkUtils.isSpark2
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.assertEquals

class TestUpdateTable extends HoodieSparkSqlTestBase {

  test("Test Update Table") {
    withRecordType()(withTempDir { tmp =>
      Seq(true, false).foreach { sparkSqlOptimizedWrites =>
        Seq("cow", "mor").foreach { tableType =>
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

          // test with optimized sql writes enabled / disabled.
          spark.sql(s"set ${SPARK_SQL_OPTIMIZED_WRITES.key()}=$sparkSqlOptimizedWrites")

          // update data
          spark.sql(s"update $tableName set price = 20 where id = 1")
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 20.0, 1000)
          )

          // update data
          spark.sql(s"update $tableName set price = price * 2 where id = 1")
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 40.0, 1000)
          )
        }
      }
    })
  }

  test("Test Update Table Without Primary Key") {
    withRecordType()(withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        Seq(true, false).foreach { isPartitioned =>
          val tableName = generateTableName
          val partitionedClause = if (isPartitioned) {
            "PARTITIONED BY (name)"
          } else {
            ""
          }
          // create table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  price double,
               |  ts long,
               |  name string
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
               | $partitionedClause
            """.stripMargin)

          // insert data to table
          spark.sql(s"insert into $tableName select 1,10, 1000, 'a1'")
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 10.0, 1000)
          )

          // test with optimized sql writes enabled.
          spark.sql(s"set ${SPARK_SQL_OPTIMIZED_WRITES.key()}=true")

          // update data
          spark.sql(s"update $tableName set price = 20 where id = 1")
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 20.0, 1000)
          )

          // update data
          spark.sql(s"update $tableName set price = price * 2 where id = 1")
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 40.0, 1000)
          )

          // verify default compaction w/ MOR
          if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
            spark.sql(s"update $tableName set price = price * 2 where id = 1")
            spark.sql(s"update $tableName set price = price * 2 where id = 1")
            spark.sql(s"update $tableName set price = price * 2 where id = 1")
            // verify compaction is complete
            val metaClient = createMetaClient(spark, tmp.getCanonicalPath + "/" + tableName)
            assertEquals(metaClient.getActiveTimeline.getLastCommitMetadataWithValidData.get.getLeft.getAction, "commit")
          }
        }
      }
    })
  }

  test("Test Update Table On Non-PK Condition") {
    withRecordType()(withTempDir { tmp =>
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
        if (isSpark2) {
          spark.sql(s"insert into $tableName values (1, 'a1', cast(10.0 as double), 1000), (2, 'a2', cast(20.0 as double), 1000)")
        } else {
          spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 1000)")
        }

        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 20.0, 1000)
        )

        // update data on non-pk condition
        spark.sql(s"update $tableName set price = 11.0, ts = 1001 where name = 'a1'")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 11.0, 1001),
          Seq(2, "a2", 20.0, 1000)
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
        if (isSpark2) {
          spark.sql(
            s"""
               |insert into $ptTableName
               |values (1, 'a1', cast(10.0 as double), 1000, "2021"), (2, 'a2', cast(20.0 as double), 1000, "2021"), (3, 'a2', cast(30.0 as double), 1000, "2022")
               |""".stripMargin)
        } else {
          spark.sql(
            s"""
               |insert into $ptTableName
               |values (1, 'a1', 10.0, 1000, "2021"), (2, 'a2', 20.0, 1000, "2021"), (3, 'a2', 30.0, 1000, "2022")
               |""".stripMargin)
        }

        checkAnswer(s"select id, name, price, ts, pt from $ptTableName")(
          Seq(1, "a1", 10.0, 1000, "2021"),
          Seq(2, "a2", 20.0, 1000, "2021"),
          Seq(3, "a2", 30.0, 1000, "2022")
        )

        // update data on non-pk condition
        spark.sql(s"update $ptTableName set price = price * 1.1, ts = ts + 1 where name = 'a2'")
        checkAnswer(s"select id, name, price, ts, pt from $ptTableName")(
          Seq(1, "a1", 10.0, 1000, "2021"),
          Seq(2, "a2", 22.0, 1001, "2021"),
          Seq(3, "a2", 33.0, 1001, "2022")
        )

        spark.sql(s"update $ptTableName set price = price + 5, ts = ts + 1 where pt = '2021'")
        checkAnswer(s"select id, name, price, ts, pt from $ptTableName")(
          Seq(1, "a1", 15.0, 1001, "2021"),
          Seq(2, "a2", 27.0, 1002, "2021"),
          Seq(3, "a2", 33.0, 1001, "2022")
        )
      }
    })
  }

  test("Test ignoring case for Update Table") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |  ID int,
             |  NAME string,
             |  PRICE double,
             |  TS long
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | options (
             |  type = '$tableType',
             |  primaryKey = 'ID',
             |  preCombineField = 'TS'
             | )
       """.stripMargin)
        // insert data to table
        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        // update data
        spark.sql(s"update $tableName set PRICE = 20 where ID = 1")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 20.0, 1000)
        )

        // update data
        spark.sql(s"update $tableName set PRICE = PRICE * 2 where ID = 1")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 40.0, 1000)
        )
      }
    }
  }

  test("Test decimal type") {
    withTempDir { tmp =>
      Seq(true, false).foreach { sparkSqlOptimizedWrites =>
        val tableName = generateTableName
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  ff decimal(38, 10)
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
     """.stripMargin)

        // insert data to table
        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000, 10.0")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        // test with optimized sql writes enabled / disabled.
        spark.sql(s"set ${SPARK_SQL_OPTIMIZED_WRITES.key()}=$sparkSqlOptimizedWrites")

        spark.sql(s"update $tableName set price = 22 where id = 1")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 22.0, 1000)
        )
      }
    }
  }
}
