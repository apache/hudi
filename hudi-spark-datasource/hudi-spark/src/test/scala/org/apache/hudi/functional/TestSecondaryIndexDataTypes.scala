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

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Tag

/**
 * Test secondary index creation with various data types.
 *
 * Given: A table with all Spark SQL data types
 * When: Creating secondary indexes on each column
 * Then: Should succeed for supported types (String, Int, BigInt, Long, Double, timestamp types)
 *       and fail for unsupported types
 */
@Tag("functional")
class TestSecondaryIndexDataTypes extends HoodieSparkSqlTestBase {
  /**
   * Test secondary index creation with all data types and verify query behavior.
   *
   * Given: A table with columns of all data types including basic types and logical types
   * When: Attempting to create secondary index on each column
   * Then: Should succeed for supported types and fail with exception for unsupported types
   *       Queries using secondary index should return correct results
   */
  test("test Secondary Index With All DataTypes") {
    withTempDir { tmpPath =>
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
           |  col_double DOUBLE,  -- Double is supported for secondary indexes
           |  -- Unsupported types
           |  col_boolean BOOLEAN,
           |  col_float FLOAT,
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
           |  orderingFields = 'ts',
           |  'hoodie.metadata.enable' = 'true',
           |  'hoodie.metadata.record.index.enable' = 'true',
           |  'hoodie.datasource.write.partitionpath.field' = 'partition_col'
           |)
           |PARTITIONED BY (partition_col)
           |LOCATION '$tmpPath'
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
           |  cast(1 as float) as col_float,
           |  cast(1 as double) as col_double,
           |  cast(1 as decimal(10,2)) as col_decimal,
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
           |  cast(2.2220 as float) as col_float,
           |  cast(2.222220 as double) as col_double,
           |  cast(22.20 as decimal(10,2)) as col_decimal,
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
           |  cast(3.3 as float) as col_float,
           |  cast(3.33 as double) as col_double,
           |  cast(33.33 as decimal(10,2)) as col_decimal,
           |  cast('binary3' as binary) as col_binary,
           |  array('e', 'f') as col_array,
           |  map('k3', 3) as col_map,
           |  named_struct('field1', 'f3', 'field2', 3) as col_struct,
           |  'p1' as partition_col,
           |  3 as ts
       """.stripMargin)
      // Define supported columns with multiple test values for comprehensive validation
      // Filter based on values of string, long, double, int. Spark will take care of the type cast.
      val supportedColumns = Seq(
        ("col_string", Seq(("'string3'", 3))),
        ("col_int", Seq(("'300'", 3), ("200L", 2), (100, 1), (100.00, 1), ("'100.00'", 1))),
        ("col_bigint", Seq(("'3000'", 3), ("2000L", 2), (1000, 1), (1000.00, 1), ("'1000.00'", 1))),
        ("col_long", Seq(("'3000'", 3), ("2000L", 2), (1000, 1), (1000.00, 1), ("'1000.00'", 1))),
        ("col_smallint", Seq(("'30'", 3), ("20L", 2), (10, 1), (10.00, 1), ("'10.00'", 1))),
        ("col_tinyint", Seq(("'3'", 3), ("2L", 2), (1, 1), (1.0, 1), ("'1.00'", 1))),
        ("col_timestamp", Seq(("cast('2023-01-03 10:00:00' as timestamp)", 3))),
        ("col_date", Seq(("cast('2023-01-03' as date)", 3))),
        ("col_double", Seq(("'1'", 1), ("1L", 1), (2.222220, 2), ("'3.3300'", 3)))
      )
      val unsupportedColumns = Seq(
        "col_float",
        "col_decimal",
        "col_boolean",
        "col_binary",
        "col_array",
        "col_map",
        "col_struct"
      )
      // Test creating indexes on supported columns
      supportedColumns.foreach { case (colName, _) =>
        val indexName = s"idx_$colName"
        // Create index should succeed
        try {
          spark.sql(s"CREATE INDEX $indexName ON $tableName ($colName)")
          println(s"Successfully created index on $colName")
        } catch {
          case e: Exception =>
            fail(s"Failed to create index on supported column $colName: ${e.getMessage}")
        }
      }
      // Test queries with multiple values for each supported column
      supportedColumns.foreach { case (colName, testValues) =>
        // Verify query using the index returns correct results for each test value
        testValues.foreach { case (testValue, expectedId) =>
          val query = s"""
             |SELECT id, $colName
             |FROM $tableName
             |WHERE $colName = $testValue
           """.stripMargin
          val result = spark.sql(query).collect()

          assertEquals(1, result.length, s"Query on $colName with value $testValue should return exactly one row")
          assertEquals(expectedId, result(0).getInt(0), s"Query on $colName with value $testValue should return id=$expectedId")
        }
      }
      val query = s"select key from hudi_metadata('$tmpPath') where type=7"
      checkAnswer(query)(
        Seq("string3$3"),
        Seq("string2$2"),
        Seq("string1$1"),
        Seq("1672653600000000$2"),
        Seq("1672567200000000$1"),
        Seq("1672740000000000$3"),
        Seq("1$1"),
        Seq("3$3"),
        Seq("2$2"),
        Seq("30$3"),
        Seq("10$1"),
        Seq("20$2"),
        Seq("1000$1"),
        Seq("2000$2"),
        Seq("3000$3"),
        Seq("100$1"),
        Seq("200$2"),
        Seq("300$3"),
        Seq("19358$1"),
        Seq("19360$3"),
        Seq("19359$2"),
        Seq("2000$2"),
        Seq("1000$1"),
        Seq("3000$3"),
        Seq("1.0$1"),
        Seq("2.22222$2"),
        Seq("3.33$3")
      )
      // Test creating indexes on unsupported columns
      unsupportedColumns.foreach { colName =>
        val indexName = s"idx_$colName"
        // Create index should fail
        try {
          spark.sql(s"CREATE INDEX $indexName ON $tableName ($colName)")
          fail(s"Expected exception when creating index on unsupported column $colName")
        } catch {
          case e: HoodieMetadataIndexException =>
            // Check if the exception message indicates unsupported data type
            assertTrue(
              e.getMessage.contains("Not eligible for indexing") ||
                e.getMessage.contains("data type") ||
                e.getCause.isInstanceOf[HoodieMetadataIndexException],
              s"Unexpected exception type for $colName: ${e.getMessage}"
            )
        }
      }
      // Clean up
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }
  /**
   * Test secondary index with logical types.
   *
   * Given: A table with various logical types
   * When: Creating secondary indexes on logical type columns
   * Then: Should succeed for timestamp/date logical types and fail for others
   */
  test("test Secondary Index With logical DataTypes") {
    withTempDir { tmpPath =>
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
           |  orderingFields = 'ts',
           |  'hoodie.metadata.enable' = 'true',
           |  'hoodie.metadata.record.index.enable' = 'true',
           |  'hoodie.datasource.write.partitionpath.field' = 'partition_col'
           |)
           |PARTITIONED BY (partition_col)
           |LOCATION '$tmpPath'
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
        } catch {
          case e: Exception =>
            fail(s"Failed to create index on supported logical type $colName: ${e.getMessage}")
        }
      }
      // Validate secondary index records for each column
      val query = s"select key from hudi_metadata('$tmpPath') where type=7"
      checkAnswer(query)(
        Seq("1672567200123456$1"),
        Seq("1672653600456789$2"),
        Seq("550e8400-e29b-41d4-a716-446655440001$2"),
        Seq("550e8400-e29b-41d4-a716-446655440000$1"),
        Seq("1672567200123000$1"),
        Seq("1672653600456000$2"),
        Seq("19359$2"),
        Seq("19358$1")
      )
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
}
