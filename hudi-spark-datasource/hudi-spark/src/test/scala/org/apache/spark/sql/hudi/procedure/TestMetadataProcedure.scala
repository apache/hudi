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

package org.apache.spark.sql.hudi.procedure

class TestMetadataProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call create_metadata_table then create_metadata_table") {
    withTempDir { tmp =>
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
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // The first step is delete the metadata
      val deleteResult = spark.sql(s"""call delete_metadata_table(table => '$tableName')""").collect()
      assertResult(1) {
        deleteResult.length
      }

      // The second step is create the metadata
      val createResult = spark.sql(s"""call create_metadata_table(table => '$tableName')""").collect()
      assertResult(1) {
        createResult.length
      }
    }
  }

  test("Test Call create_metadata_table then create_metadata_table with mutiltables") {
    withTempDir { tmp =>
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
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      val tableName_1 = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName_1 (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName_1'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      val tables = s"$tableName,$tableName_1"

      // The first step is delete the metadata
      val ret = spark.sql(s"""call delete_metadata_table(table => '$tables')""").collect()
      assertResult(1) {
        ret.length
      }

      // The second step is create the metadata
      val createResult = spark.sql(s"""call create_metadata_table(table => '$tableName')""").collect()
      assertResult(1) {
        createResult.length
      }
    }
  }

  test("Test Call init_metadata_table Procedure") {
    withTempDir { tmp =>
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
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // read only, no initialize
      val readResult = spark.sql(s"""call init_metadata_table(table => '$tableName', read_only => true)""").collect()
      assertResult(1) {
        readResult.length
      }

      // initialize metadata
      val initResult = spark.sql(s"""call init_metadata_table(table => '$tableName')""").collect()
      assertResult(1) {
        initResult.length
      }
    }
  }

  test("Test Call show_metadata_table_column_stats Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  c1 int,
           |  c2 boolean,
           |  c3 binary,
           |  c4 date,
           |  c5 decimal(10,1),
           |  c6 double,
           |  c7 float,
           |  c8 long,
           |  c9 string,
           |  c10 timestamp
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'c1',
           |  orderingFields = 'c8',
           |  hoodie.metadata.enable="true",
           |  hoodie.metadata.index.column.stats.enable="true"
           | )
       """.stripMargin)
      // insert data to table

      spark.sql(
        s"""
           |insert into table $tableName
           |values (1, true, CAST('binary data' AS BINARY), CAST('2021-01-01' AS DATE), CAST(10.5 AS DECIMAL(10,1)), CAST(3.14 AS DOUBLE), CAST(2.5 AS FLOAT), 1000, 'example string', CAST('2021-01-01 00:00:00' AS TIMESTAMP))
           |""".stripMargin)
      spark.sql(
        s"""
           |insert into table $tableName
           |values (10, false, CAST('binary data' AS BINARY), CAST('2022-02-02' AS DATE),  CAST(20.5 AS DECIMAL(10,1)), CAST(6.28 AS DOUBLE), CAST(3.14 AS FLOAT), 2000, 'another string', CAST('2022-02-02 00:00:00' AS TIMESTAMP))
           |""".stripMargin)

      // Only numerical and string types are compared for clarity on min/max values.
      val expectedValues = Map(
        1 -> ("1", "10"),
        2 -> ("false", "true"),
        6 -> ("3.14", "6.28"),
        7 -> ("2.5", "3.14"),
        8 -> ("1000", "2000"),
        9 -> ("another string", "example string")
      )

      for (i <- 1 to 10) {
        val columnName = s"c$i"
        val metadataStats = spark.sql(s"""call show_metadata_table_column_stats(table => '$tableName', targetColumns => '$columnName')""").collect()
        assertResult(1)(metadataStats.length)
        val minVal: String = metadataStats(0).getAs[String]("min_value")
        val maxVal: String = metadataStats(0).getAs[String]("max_value")

        expectedValues.get(i) match {
          case Some((expectedMin, expectedMax)) =>
            assertResult(expectedMin)(minVal)
            assertResult(expectedMax)(maxVal)
          case None => // Do nothing if no expected values found
        }
      }
    }
  }

  test("Test Call show_metadata_column_stats_overlap Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  c1 int,
           |  c2 boolean,
           |  c3 date,
           |  c4 double,
           |  c5 float,
           |  c6 long,
           |  c7 string,
           |  c8 timestamp
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  hoodie.metadata.enable="true",
           |  hoodie.metadata.index.column.stats.enable="true"
           | )
       """.stripMargin)
      // insert data to table
      withSQLConf("hoodie.parquet.small.file.limit" -> "0") {
        spark.sql(
          s"""
             |insert into table $tableName
             |values (1, true, CAST('2021-01-01' AS DATE), CAST(3.14 AS DOUBLE), CAST(2.5 AS FLOAT),1000, 'example string', CAST('2021-02-02 00:00:00' AS TIMESTAMP))
             |""".stripMargin)

        spark.sql(
          s"""
             |insert into table $tableName
             |values
             |(10, false, CAST('2022-02-02' AS DATE),CAST(6.28 AS DOUBLE), CAST(3.14 AS FLOAT), 2000, 'another string', CAST('2022-02-02 00:00:00' AS TIMESTAMP)),
             |(0, false, CAST('2020-02-02' AS DATE), CAST(7.28 AS DOUBLE), CAST(2.1 AS FLOAT), 3000, 'third string', CAST('2021-01-01 00:00:00' AS TIMESTAMP))
             |""".stripMargin)
      }

      val maxResult = Array(2, 1, 2, 1, 2, 1, 2, 2)
      val valueResult = Array(3, 2, 3, 3, 3, 3, 3, 3)
      // collect column stats for table
      for (i <- maxResult.indices) {
        val columnName = s"c${i + 1}"
        val metadataStats = spark.sql(s"""call show_metadata_column_stats_overlap(table => '$tableName', targetColumns => '$columnName')""").collect()
        assertResult(1) {
          metadataStats.length
        }
        assertResult(maxResult(i)) {
          metadataStats(0)(3)
        }
        assertResult(valueResult(i)) {
          metadataStats(0)(9)
        }
      }
    }
  }

  test("Test Call show_metadata_table_stats Procedure") {
    withTempDir { tmp =>
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
           |  primaryKey = 'id',
           |  orderingFields = 'ts',
           |  hoodie.metadata.metrics.enable = 'true'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // collect metadata stats for table
      val metadataStats = spark.sql(s"""call show_metadata_table_stats(table => '$tableName')""").collect()
      assertResult(0) {
        metadataStats.length
      }
    }
  }

  test("Test Call show_metadata_table_partitions Procedure") {
    withTempDir { tmp =>
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
           | partitioned by (ts)
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // collect metadata partitions for table
      val partitions = spark.sql(s"""call show_metadata_table_partitions(table => '$tableName')""").collect()
      assertResult(2) {
        partitions.length
      }
    }
  }

  test("Test Call show_metadata_table_files Procedure") {
    withTempDir { tmp =>
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
           | partitioned by (ts)
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // collect metadata partitions for table
      val partitions = spark.sql(s"""call show_metadata_table_partitions(table => '$tableName')""").collect()
      assertResult(2) {
        partitions.length
      }

      // collect metadata files for a partition of a table
      val partition = partitions(0).get(0).toString
      var filesResult = spark.sql(s"""call show_metadata_table_files(table => '$tableName', partition => '$partition')""").collect()
      assertResult(1) {
        filesResult.length
      }

      filesResult = spark.sql(s"""call show_metadata_table_files(table => '$tableName', partition => '$partition', limit => 0)""").collect()
      assertResult(0) {
        filesResult.length
      }
    }
  }

  test("Test Call validate_metadata_table_files Procedure") {
    withTempDir { tmp =>
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
           | partitioned by (ts)
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // collect validate metadata files result
      val validateFilesResult = spark.sql(s"""call validate_metadata_table_files(table => '$tableName')""").collect()
      assertResult(0) {
        validateFilesResult.length
      }

      // collect validate metadata files result with verbose
      val validateFilesVerboseResult = spark.sql(s"""call validate_metadata_table_files(table => '$tableName', verbose => true)""").collect()
      assertResult(2) {
        validateFilesVerboseResult.length
      }
    }
  }
}
