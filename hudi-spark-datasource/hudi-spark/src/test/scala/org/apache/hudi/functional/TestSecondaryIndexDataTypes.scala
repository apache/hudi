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

package org.apache.hudi.functional
import org.apache.hudi.exception.HoodieMetadataIndexException
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.SparkConf
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue, fail}

/**
 * Test secondary index creation with various data types.
 *
 * Given: A table with all Spark SQL data types
 * When: Creating secondary indexes on each column
 * Then: Should succeed for supported types (String, Int, BigInt, Long, Float, Double, timestamp types)
 *       and fail for unsupported types
 */
@Tag("functional")
class TestSecondaryIndexDataTypes extends SparkClientFunctionalTestHarness {
  override def conf: SparkConf = {
    conf(SparkClientFunctionalTestHarness.getSparkSqlConf)
  }

  /**
   * Test secondary index creation with all data types and verify query behavior.
   *
   * Given: A table with columns of all data types including basic types and logical types
   * When: Attempting to create secondary index on each column
   * Then: Should succeed for supported types (including Float and Double) and fail with exception for unsupported types
   *       Queries using secondary index should return correct results
   */
  @Test
  def testSecondaryIndexWithAllDataTypes(): Unit = {
    val tableName = "test_si_all_data_types"
    // Create table with all data types
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id INT,
         |  -- Supported types
         |  col_string STRING,
         |  col_int INT,
         |  col_bigint BIGINT,
         |  col_long LONG,
         |  col_smallint SMALLINT,
         |  col_tinyint TINYINT,
         |  col_timestamp TIMESTAMP,
         |  col_date DATE,
         |  -- Unsupported types
         |  col_boolean BOOLEAN,
         |  col_float FLOAT,
         |  col_double DOUBLE,
         |  col_decimal DECIMAL(10,2),
         |  col_binary BINARY,
         |  col_array ARRAY<STRING>,
         |  col_map MAP<STRING, INT>,
         |  col_struct STRUCT<field1: STRING, field2: INT>,
         |  -- Partition column
         |  partition_col STRING,
         |  ts BIGINT
         |) USING HUDI
         |OPTIONS (
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  'hoodie.metadata.enable' = 'true',
         |  'hoodie.metadata.record.index.enable' = 'true',
         |  'hoodie.datasource.write.partitionpath.field' = 'partition_col'
         |)
         |PARTITIONED BY (partition_col)
         |LOCATION '$basePath/$tableName'
       """.stripMargin)
    // Insert test data using SELECT with CAST
    spark.sql(
      s"""
         |INSERT INTO $tableName
         |SELECT
         |  1 as id,
         |  'string1' as col_string,
         |  cast(100 as int) as col_int,
         |  cast(1000 as bigint) as col_bigint,
         |  cast(1000 as long) as col_long,
         |  cast(10 as smallint) as col_smallint,
         |  cast(1 as tinyint) as col_tinyint,
         |  cast('2023-01-01 10:00:00' as timestamp) as col_timestamp,
         |  cast('2023-01-01' as date) as col_date,
         |  true as col_boolean,
         |  cast(1.111 as float) as col_float,
         |  cast(1.1111 as double) as col_double,
         |  cast(11.11 as decimal(10,2)) as col_decimal,
         |  cast('binary1' as binary) as col_binary,
         |  array('a', 'b') as col_array,
         |  map('k1', 1) as col_map,
         |  named_struct('field1', 'f1', 'field2', 1) as col_struct,
         |  'p1' as partition_col,
         |  1 as ts
       """.stripMargin)
    spark.sql(
      s"""
         |INSERT INTO $tableName
         |SELECT
         |  2 as id,
         |  'string2' as col_string,
         |  cast(200 as int) as col_int,
         |  cast(2000 as bigint) as col_bigint,
         |  cast(2000 as long) as col_long,
         |  cast(20 as smallint) as col_smallint,
         |  cast(2 as tinyint) as col_tinyint,
         |  cast('2023-01-02 10:00:00' as timestamp) as col_timestamp,
         |  cast('2023-01-02' as date) as col_date,
         |  false as col_boolean,
         |  cast(2.222 as float) as col_float,
         |  cast(2.2222 as double) as col_double,
         |  cast(22.22 as decimal(10,2)) as col_decimal,
         |  cast('binary2' as binary) as col_binary,
         |  array('c', 'd') as col_array,
         |  map('k2', 2) as col_map,
         |  named_struct('field1', 'f2', 'field2', 2) as col_struct,
         |  'p2' as partition_col,
         |  2 as ts
       """.stripMargin)
    spark.sql(
      s"""
         |INSERT INTO $tableName
         |SELECT
         |  3 as id,
         |  'string3' as col_string,
         |  cast(300 as int) as col_int,
         |  cast(3000 as bigint) as col_bigint,
         |  cast(3000 as long) as col_long,
         |  cast(30 as smallint) as col_smallint,
         |  cast(3 as tinyint) as col_tinyint,
         |  cast('2023-01-03 10:00:00' as timestamp) as col_timestamp,
         |  cast('2023-01-03' as date) as col_date,
         |  true as col_boolean,
         |  cast(3.333 as float) as col_float,
         |  cast(3.3333 as double) as col_double,
         |  cast(33.33 as decimal(10,2)) as col_decimal,
         |  cast('binary3' as binary) as col_binary,
         |  array('e', 'f') as col_array,
         |  map('k3', 3) as col_map,
         |  named_struct('field1', 'f3', 'field2', 3) as col_struct,
         |  'p1' as partition_col,
         |  3 as ts
       """.stripMargin)
    // Define supported and unsupported columns
    val supportedColumns = Seq(
//      ("col_string", "'string2'"),
//      ("col_int", "200"),
//      ("col_bigint", "2000"),
//      ("col_long", "2000"),
//      ("col_smallint", "20"),
//      ("col_tinyint", "2"),
//      ("col_float", 2.222f),
      ("col_double", 2.2222000),
//      ("col_timestamp", "cast('2023-01-02 10:00:00' as timestamp)"),
//      ("col_date", "cast('2023-01-02' as date)")
    )
    val unsupportedColumns = Seq(
      "col_decimal",
      "col_boolean",
      "col_binary",
      "col_array",
      "col_map",
      "col_struct"
    )
    // Test creating indexes on supported columns
    supportedColumns.foreach { case (colName, testValue) =>
      val indexName = s"idx_$colName"
      // Create index should succeed
      try {
        spark.sql(s"CREATE INDEX $indexName ON $tableName ($colName)")
        println(s"Successfully created index on $colName")
      } catch {
        case e: Exception =>
          fail(s"Failed to create index on supported column $colName: ${e.getMessage}")
      }
      // Verify query using the index returns correct results
      val query =  s"""SELECT id, $colName
                       |FROM $tableName
                       |WHERE $colName = $testValue""".stripMargin
      val result1 = spark.sql(query)
      val result = result1.collect()
      assertEquals(1, result.length, s"Query on $colName should return exactly one row")
      assertEquals(2, result(0).getInt(0), s"Query on $colName should return id=2")
      // For debugging: check if secondary index is being used
      val explainPlan = spark.sql(
        s"""
           |EXPLAIN EXTENDED
           |SELECT id, $colName
           |FROM $tableName
           |WHERE $colName = $testValue
         """.stripMargin
      ).collect().map(_.getString(0)).mkString("\n")
      println(s"Query plan for $colName: \n$explainPlan")
      // Drop the index for cleanup
      spark.sql(s"DROP INDEX $indexName ON $tableName")
    }
//    // Test creating indexes on unsupported columns
//    unsupportedColumns.foreach { colName =>
//      val indexName = s"idx_$colName"
//      // Create index should fail
//      try {
//        spark.sql(s"CREATE INDEX $indexName ON $tableName ($colName)")
//        fail(s"Expected exception when creating index on unsupported column $colName")
//      } catch {
//        case e: HoodieMetadataIndexException =>
//          // Check if the exception message indicates unsupported data type
//          assertTrue(
//            e.getMessage.contains("Not eligible for indexing") ||
//            e.getMessage.contains("data type") ||
//            e.getCause.isInstanceOf[HoodieMetadataIndexException],
//            s"Unexpected exception type for $colName: ${e.getMessage}"
//          )
//      }
//    }
    // Clean up
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }

  /**
   * Test secondary index with logical types.
   *
   * Given: A table with various logical types
   * When: Creating secondary indexes on logical type columns
   * Then: Should succeed for timestamp/date logical types and fail for others
   */
  @Test
  def testSecondaryIndexWithLogicalTypes(): Unit = {
    val tableName = "test_si_logical_types"
    // Create table with focus on timestamp logical types
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id INT,
         |  -- Different representations of timestamps (all should be supported)
         |  timestamp_millis TIMESTAMP,
         |  timestamp_micros TIMESTAMP,
         |  date_field DATE,
         |  -- String representation of UUID (regular string, should be supported)
         |  uuid_string STRING,
         |  -- Decimal types (should not be supported)
         |  decimal_field DECIMAL(10,2),
         |  -- Other fields
         |  partition_col STRING,
         |  ts BIGINT
         |) USING HUDI
         |OPTIONS (
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  'hoodie.metadata.enable' = 'true',
         |  'hoodie.metadata.record.index.enable' = 'true',
         |  'hoodie.datasource.write.partitionpath.field' = 'partition_col'
         |)
         |PARTITIONED BY (partition_col)
         |LOCATION '$basePath/$tableName'
       """.stripMargin)
    // Insert test data using SELECT
    spark.sql(
      s"""
         |INSERT INTO $tableName
         |SELECT
         |  1 as id,
         |  cast('2023-01-01 10:00:00.123' as timestamp) as timestamp_millis,
         |  cast('2023-01-01 10:00:00.123456' as timestamp) as timestamp_micros,
         |  cast('2023-01-01' as date) as date_field,
         |  '550e8400-e29b-41d4-a716-446655440000' as uuid_string,
         |  cast(123.45 as decimal(10,2)) as decimal_field,
         |  'p1' as partition_col,
         |  1 as ts
       """.stripMargin)
    spark.sql(
      s"""
         |INSERT INTO $tableName
         |SELECT
         |  2 as id,
         |  cast('2023-01-02 10:00:00.456' as timestamp) as timestamp_millis,
         |  cast('2023-01-02 10:00:00.456789' as timestamp) as timestamp_micros,
         |  cast('2023-01-02' as date) as date_field,
         |  '550e8400-e29b-41d4-a716-446655440001' as uuid_string,
         |  cast(234.56 as decimal(10,2)) as decimal_field,
         |  'p2' as partition_col,
         |  2 as ts
       """.stripMargin)
    // Test supported logical types
    val supportedLogicalTypes = Seq(
      ("timestamp_millis", "cast('2023-01-01 10:00:00.123' as timestamp)"),
      ("timestamp_micros", "cast('2023-01-01 10:00:00.123456' as timestamp)"),
      ("date_field", "cast('2023-01-01' as date)"),
      ("uuid_string", "'550e8400-e29b-41d4-a716-446655440000'") // Regular string
    )
    supportedLogicalTypes.foreach { case (colName, testValue) =>
      val indexName = s"idx_$colName"
      try {
        spark.sql(s"CREATE INDEX $indexName ON $tableName ($colName)")
        println(s"Successfully created index on logical type column $colName")
        // Verify query works
        val result = spark.sql(
          s"""
             |SELECT id, $colName
             |FROM $tableName
             |WHERE $colName = $testValue
           """.stripMargin
        ).collect()
        assertEquals(1, result.length, s"Query on $colName should return exactly one row")
        assertEquals(1, result(0).getInt(0), s"Query on $colName should return id=1")
        spark.sql(s"DROP INDEX $indexName ON $tableName")
      } catch {
        case e: Exception =>
          fail(s"Failed to create index on supported logical type $colName: ${e.getMessage}")
      }
    }
    // Test unsupported logical types
    try {
      spark.sql(s"CREATE INDEX idx_decimal ON $tableName (decimal_field)")
      fail("Expected exception when creating index on decimal field")
    } catch {
      case _: Exception =>
        println("Expected failure for creating index on decimal field")
    }
    // Clean up
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }
}
