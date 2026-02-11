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

package org.apache.spark.sql.hudi.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * Tests for improved error handling in HoodieAnalysis when queries contain
 * unresolved columns or tables.
 */
class TestHoodieAnalysisErrorHandling extends HoodieSparkSqlTestBase {

  test("MergeInto with unresolved column in source query should provide helpful error message") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create target Hudi table
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  price DOUBLE,
           |  ts INT
           |) USING hudi
           |LOCATION '${tmp.getCanonicalPath}'
           |TBLPROPERTIES (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           """.stripMargin)

      // Insert initial data
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000)")

      // Test MERGE INTO with non-existent column in source query
      val exception = intercept[AnalysisException] {
        spark.sql(
          s"""
             |MERGE INTO $tableName AS target
             |USING (
             |  SELECT 1 AS id, 'updated' AS name, 20.0 AS price, 2000 AS ts, nonexistent_column AS extra
             |) AS source
             |ON target.id = source.id
             |WHEN MATCHED THEN UPDATE SET *
             |WHEN NOT MATCHED THEN INSERT *
             """.stripMargin)
      }

      // Verify the error message contains helpful information
      val errorMessage = exception.getMessage
      assert(errorMessage.contains("nonexistent_column") ||
        errorMessage.contains("unresolved") ||
        errorMessage.contains("cannot be resolved"),
        s"Error message should mention the unresolved column. Actual message: $errorMessage")
    }
  }

  test("MergeInto with unresolved column in ON condition should provide helpful error message") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create target Hudi table
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  price DOUBLE,
           |  ts INT
           |) USING hudi
           |LOCATION '${tmp.getCanonicalPath}'
           |TBLPROPERTIES (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           """.stripMargin)

      // Insert initial data
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000)")

      // Test MERGE INTO with non-existent column in ON condition
      val exception = intercept[AnalysisException] {
        spark.sql(
          s"""
             |MERGE INTO $tableName AS target
             |USING (
             |  SELECT 1 AS id, 'updated' AS name, 20.0 AS price, 2000 AS ts
             |) AS source
             |ON target.nonexistent_id = source.id
             |WHEN MATCHED THEN UPDATE SET *
             |WHEN NOT MATCHED THEN INSERT *
             """.stripMargin)
      }

      // Verify the error message contains helpful information
      val errorMessage = exception.getMessage
      assert(errorMessage.contains("nonexistent_id") ||
        errorMessage.contains("unresolved") ||
        errorMessage.contains("cannot be resolved"),
        s"Error message should mention the unresolved column. Actual message: $errorMessage")
    }
  }

  test("InsertInto from unresolved source table should provide helpful error message") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create target Hudi table
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  price DOUBLE,
           |  ts INT
           |) USING hudi
           |LOCATION '${tmp.getCanonicalPath}'
           |TBLPROPERTIES (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           """.stripMargin)

      // Test INSERT INTO from non-existent source table
      val exception = intercept[AnalysisException] {
        spark.sql(
          s"""
             |INSERT INTO $tableName
             |SELECT * FROM nonexistent_source_table
             """.stripMargin)
      }

      // Verify the error message contains helpful information
      val errorMessage = exception.getMessage
      assert(errorMessage.contains("typos in column or table names") &&
        errorMessage.contains("unresolved"),
        s"Error message should mention typos and unresolved references. Actual message: $errorMessage")
    }
  }

  test("MergeInto with typo in column name should provide helpful error message") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create target Hudi table
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  price DOUBLE,
           |  ts INT
           |) USING hudi
           |LOCATION '${tmp.getCanonicalPath}'
           |TBLPROPERTIES (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           """.stripMargin)

      // Insert initial data
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000)")

      // Test MERGE INTO with typo in column name - reference non-existent source.pricee
      // when source only has 'price' column
      val exception = intercept[AnalysisException] {
        spark.sql(
          s"""
             |MERGE INTO $tableName AS target
             |USING (
             |  SELECT 1 AS id, 'updated' AS name, 20.0 AS price, 2000 AS ts
             |) AS source
             |ON target.id = source.id
             |WHEN MATCHED THEN UPDATE SET
             |  id = source.id,
             |  name = source.name,
             |  price = source.pricee,
             |  ts = source.ts
             |WHEN NOT MATCHED THEN INSERT (id, name, price, ts)
             |  VALUES (source.id, source.name, source.pricee, source.ts)
             """.stripMargin)
      }

      // Verify the error message is helpful
      val errorMessage = exception.getMessage
      assert(errorMessage.contains("pricee") ||
        errorMessage.contains("unresolved") ||
        errorMessage.contains("cannot be resolved"),
        s"Error message should mention the typo column. Actual message: $errorMessage")
    }
  }
}
