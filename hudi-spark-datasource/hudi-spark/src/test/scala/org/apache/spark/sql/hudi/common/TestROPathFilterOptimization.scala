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

package org.apache.spark.sql.hudi.common

/**
 * Tests for ROPathFilter optimization for file listing.
 * This optimization helps prevent OOM on driver when tables have multiple versions
 * of the same file in the same partition.
 */
class TestROPathFilterOptimization extends HoodieSparkSqlTestBase {

  val RO_PATH_FILTER_OPT_KEY = "hoodie.datasource.read.file.index.list.file.statuses.using.ro.path.filter"

  test("Test ROPathFilter optimization with COW table - basic functionality") {
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
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // Insert initial data
      spark.sql(s"insert into $tableName values(1, 'a1', 10.0, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 20.0, 2000)")
      spark.sql(s"insert into $tableName values(3, 'a3', 30.0, 3000)")

      // Update data to create multiple file versions
      spark.sql(s"update $tableName set price = 15.0 where id = 1")
      spark.sql(s"update $tableName set price = 25.0 where id = 2")

      // Query with ROPathFilter enabled via Spark conf
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "true") {
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 15.0, 1000),
          Seq(2, "a2", 25.0, 2000),
          Seq(3, "a3", 30.0, 3000)
        )
      }

      // Query without ROPathFilter should give same results
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "false") {
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 15.0, 1000),
          Seq(2, "a2", 25.0, 2000),
          Seq(3, "a3", 30.0, 3000)
        )
      }
    }
  }

  test("Test ROPathFilter optimization with partitioned COW table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
           | partitioned by (dt)
       """.stripMargin)

      // Insert data across multiple partitions
      spark.sql(s"insert into $tableName values(1, 'a1', 10.0, 1000, '2024-01-01')")
      spark.sql(s"insert into $tableName values(2, 'a2', 20.0, 2000, '2024-01-01')")
      spark.sql(s"insert into $tableName values(3, 'a3', 30.0, 3000, '2024-01-02')")
      spark.sql(s"insert into $tableName values(4, 'a4', 40.0, 4000, '2024-01-02')")

      // Create multiple file versions by updating
      spark.sql(s"update $tableName set price = 15.0 where id = 1")
      spark.sql(s"update $tableName set price = 25.0 where id = 2")
      spark.sql(s"update $tableName set price = 35.0 where id = 3")

      // Query with ROPathFilter enabled and partition filtering
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "true") {
        checkAnswer(s"select id, name, price, ts, dt from $tableName where dt = '2024-01-01' order by id")(
          Seq(1, "a1", 15.0, 1000, "2024-01-01"),
          Seq(2, "a2", 25.0, 2000, "2024-01-01")
        )
      }
    }
  }

  test("Test ROPathFilter with multiple updates creating many file versions") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  counter int,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // Insert initial data
      spark.sql(s"insert into $tableName values(1, 'a1', 0, 1000)")

      // Create many file versions through updates
      for (i <- 1 to 5) {
        spark.sql(s"update $tableName set counter = $i where id = 1")
      }

      // Query with ROPathFilter enabled - should only return latest version
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "true") {
        checkAnswer(s"select id, name, counter, ts from $tableName")(
          Seq(1, "a1", 5, 1000)
        )
      }
    }
  }

  test("Test ROPathFilter disabled by default") {
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
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10.0, 1000)")
      spark.sql(s"update $tableName set price = 15.0 where id = 1")

      // Default behavior - should still return correct results
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 15.0, 1000)
      )
    }
  }

  test("Test ROPathFilter with multi-level partitioning") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  year string,
           |  month string
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
           | partitioned by (year, month)
       """.stripMargin)

      // Insert data
      spark.sql(s"insert into $tableName values(1, 'a1', 10.0, 1000, '2024', '01')")
      spark.sql(s"insert into $tableName values(2, 'a2', 20.0, 2000, '2024', '01')")
      spark.sql(s"insert into $tableName values(3, 'a3', 30.0, 3000, '2024', '02')")

      // Create multiple versions
      spark.sql(s"update $tableName set price = 15.0 where id = 1")

      // Query with ROPathFilter
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "true") {
        checkAnswer(s"select id, name, price, ts, year, month from $tableName where year = '2024' and month = '01' order by id")(
          Seq(1, "a1", 15.0, 1000, "2024", "01"),
          Seq(2, "a2", 20.0, 2000, "2024", "01")
        )
      }
    }
  }

  test("Test ROPathFilter with delete operations") {
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
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // Insert data
      spark.sql(s"insert into $tableName values(1, 'a1', 10.0, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 20.0, 2000)")
      spark.sql(s"insert into $tableName values(3, 'a3', 30.0, 3000)")

      // Delete some records
      spark.sql(s"delete from $tableName where id = 2")

      // Query with ROPathFilter enabled - should not return deleted record
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "true") {
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(3, "a3", 30.0, 3000)
        )
      }
    }
  }

  test("Test ROPathFilter consistency between SQL and DataFrame") {
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
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // Insert and update data
      spark.sql(s"insert into $tableName values(1, 'a1', 10.0, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 20.0, 2000)")
      spark.sql(s"update $tableName set price = 15.0 where id = 1")

      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "true") {
        // Query via SQL
        val sqlResult = spark.sql(s"select id, name, price, ts from $tableName order by id").collect()

        // Query via DataFrame API
        val dfResult = spark.read
          .format("hudi")
          .option(RO_PATH_FILTER_OPT_KEY, "true")
          .load(tmp.getCanonicalPath)
          .selectExpr("id", "name", "price", "ts")
          .orderBy("id")
          .collect()

        // Both should return the same results
        assert(sqlResult.length == dfResult.length)
        sqlResult.zip(dfResult).foreach { case (sql, df) =>
          assert(sql.equals(df))
        }
      }
    }
  }
}
