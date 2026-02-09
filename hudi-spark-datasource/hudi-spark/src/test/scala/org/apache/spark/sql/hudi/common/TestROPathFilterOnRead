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
 * Tests for ROPathFilter optimization with advanced scenarios and edge cases.
 */
class TestROPathFilterAdvanced extends HoodieSparkSqlTestBase {

  val RO_PATH_FILTER_OPT_KEY = "hoodie.datasource.read.file.index.list.file.statuses.using.ro.path.filter"

  test("Test ROPathFilter with empty table") {
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

      // Query empty table with ROPathFilter enabled
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "true") {
        val result = spark.sql(s"select * from $tableName").collect()
        assert(result.length == 0)
      }
    }
  }

  test("Test ROPathFilter with partition pruning") {
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

      // Query empty table with ROPathFilter enabled
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "true") {
        val result = spark.sql(s"select * from $tableName").collect()
        assert(result.length == 0)
      }

      // Insert data across multiple partitions
      spark.sql(s"""insert into $tableName values(1, "a1", 10.0, 1000, "2024-01-01")""")
      spark.sql(s"""insert into $tableName values(2, "a2", 20.0, 2000, "2024-01-02")""")

      // Update data in first partition
      spark.sql(s"update $tableName set price = 15.0 where id = 1")

      // Query single partition with ROPathFilter
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "true") {
        checkAnswer(s"select id, name, price, ts, dt from $tableName where dt = '2024-01-01'")(
          Seq(1, "a1", 15.0, 1000, "2024-01-01")
        )
      }
    }
  }

  test("Test ROPathFilter with concurrent inserts to different partitions") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  region string
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
           | partitioned by (region)
       """.stripMargin)

      // Insert data to different partitions
      spark.sql(s"""insert into $tableName values(1, "a1", 10.0, 1000, "US")""")
      spark.sql(s"""insert into $tableName values(2, "a2", 20.0, 2000, "EU")""")
      spark.sql(s"""insert into $tableName values(3, "a3", 30.0, 3000, "APAC")""")
      spark.sql(s"""insert into $tableName values(4, "a4", 40.0, 4000, "US")""")

      // Query all data with ROPathFilter
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "true") {
        checkAnswer(s"select id, name, price, ts, region from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000, "US"),
          Seq(2, "a2", 20.0, 2000, "EU"),
          Seq(3, "a3", 30.0, 3000, "APAC"),
          Seq(4, "a4", 40.0, 4000, "US")
        )
      }
    }
  }

  test("Test ROPathFilter with multiple deletes and updates") {
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
      for (i <- 1 to 10) {
        spark.sql(s"""insert into $tableName values($i, "name$i", ${i * 10.0}, ${i * 1000})""")
      }

      // Perform mix of updates and deletes
      spark.sql(s"update $tableName set price = price * 2 where id % 2 = 0")
      spark.sql(s"delete from $tableName where id % 3 = 0")

      // Query with ROPathFilter
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "true") {
        val result = spark.sql(s"select id, name, price, ts from $tableName order by id").collect()
        // Should have deleted records where id % 3 = 0 (3, 6, 9)
        // Should have doubled price for even ids (2, 4, 8, 10)
        assert(result.length == 7) // 10 - 3 deleted = 7

        // Check a few specific values
        val row2 = result.find(_.getInt(0) == 2).get
        assert(row2.getDouble(2) == 40.0) // doubled from 20.0

        val row5 = result.find(_.getInt(0) == 5).get
        assert(row5.getDouble(2) == 50.0) // not doubled (odd)
      }
    }
  }

  test("Test ROPathFilter with mixed partition and non-partition columns in filter") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  category string
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
           | partitioned by (category)
       """.stripMargin)

      // Insert data
      spark.sql(s"""insert into $tableName values(1, "a1", 10.0, 1000, "electronics")""")
      spark.sql(s"""insert into $tableName values(2, "a2", 20.0, 2000, "electronics")""")
      spark.sql(s"""insert into $tableName values(3, "a3", 30.0, 3000, "books")""")
      spark.sql(s"""insert into $tableName values(4, "a4", 40.0, 4000, "books")""")

      // Update some records
      spark.sql(s"update $tableName set price = 15.0 where id = 1")

      // Query with both partition and data filters
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "true") {
        checkAnswer(s"select id, name, price, ts, category from $tableName where category = 'electronics' and price > 12.0 order by id")(
          Seq(1, "a1", 15.0, 1000, "electronics"),
          Seq(2, "a2", 20.0, 2000, "electronics")
        )
      }
    }
  }

  test("Test ROPathFilter correctness with complex update patterns") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  version int,
           |  data string,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // Insert and update the same record multiple times
      spark.sql(s"""insert into $tableName values(1, 1, "initial", 1000)""")
      spark.sql(s"""update $tableName set version = 2, data = "updated_v2" where id = 1""")
      spark.sql(s"""update $tableName set version = 3, data = "updated_v3" where id = 1""")
      spark.sql(s"""update $tableName set version = 4, data = "updated_v4" where id = 1""")

      // Insert another record
      spark.sql(s"""insert into $tableName values(2, 1, "second_record", 2000)""")

      // Query with ROPathFilter should return only latest versions
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "true") {
        checkAnswer(s"select id, version, data, ts from $tableName order by id")(
          Seq(1, 4, "updated_v4", 1000),
          Seq(2, 1, "second_record", 2000)
        )
      }

      // Without ROPathFilter should still return same results (correct filtering)
      withSQLConf(s"spark.$RO_PATH_FILTER_OPT_KEY" -> "false") {
        checkAnswer(s"select id, version, data, ts from $tableName order by id")(
          Seq(1, 4, "updated_v4", 1000),
          Seq(2, 1, "second_record", 2000)
        )
      }
    }
  }
}
