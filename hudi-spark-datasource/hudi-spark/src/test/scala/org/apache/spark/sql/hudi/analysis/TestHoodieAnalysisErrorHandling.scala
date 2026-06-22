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
 * Regression tests covering how Hudi's analysis surface unresolved references in
 * Spark SQL queries. Hudi's [[ProducesHudiMetaFields]] extractor and the MERGE INTO
 * resolution path in [[HoodieSparkBaseAnalysis]] used to swallow the
 * [[org.apache.spark.sql.catalyst.analysis.UnresolvedException]] and rewrite it as a
 * generic Hudi error, which lost Spark's "did you mean" suggestions. Both sites now
 * fall through to Spark's CheckAnalysis so the user-facing error remains
 * Spark-native (e.g. `UNRESOLVED_COLUMN.WITH_SUGGESTION`).
 */
class TestHoodieAnalysisErrorHandling extends HoodieSparkSqlTestBase {

  test("MERGE INTO with unresolved column in source query surfaces Spark's native error") {
    withTempDir { tmp =>
      val tableName = generateTableName
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

      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000)")

      // Source query references a non-existent column. Spark's analyzer
      // should produce a precise error naming the missing column.
      val ex = intercept[AnalysisException] {
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
      val msg = ex.getMessage
      assertNativeUnresolvedColumn(msg, "nonexistent_column")
      assertNoHudiGenericRewrite(msg)
    }
  }

  test("INSERT INTO from non-existent source table surfaces Spark's native error") {
    withTempDir { tmp =>
      val tableName = generateTableName
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

      val ex = intercept[AnalysisException] {
        spark.sql(
          s"""
             |INSERT INTO $tableName
             |SELECT * FROM nonexistent_source_table
             """.stripMargin)
      }
      val msg = ex.getMessage
      assertNativeTableNotFound(msg, "nonexistent_source_table")
      assertNoHudiGenericRewrite(msg)
    }
  }

  test("MERGE INTO with unresolved column in ON predicate surfaces Spark's native error") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id INT, name STRING, price DOUBLE, ts INT
           |) USING hudi
           |LOCATION '${tmp.getCanonicalPath}'
           |TBLPROPERTIES (primaryKey = 'id', preCombineField = 'ts')
           """.stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000)")

      val ex = intercept[AnalysisException] {
        spark.sql(
          s"""
             |MERGE INTO $tableName AS target
             |USING (SELECT 1 AS id, 'u' AS name, 20.0 AS price, 2000 AS ts) AS source
             |ON target.nonexistent_id = source.id
             |WHEN MATCHED THEN UPDATE SET *
             |WHEN NOT MATCHED THEN INSERT *
             """.stripMargin)
      }
      val msg = ex.getMessage
      assertNativeUnresolvedColumn(msg, "nonexistent_id")
      assertNoHudiGenericRewrite(msg)
    }
  }

  /**
   * Match Spark's native "column not resolved" error. Spark 3.4+ uses the
   * `UNRESOLVED_COLUMN.WITH_SUGGESTION` error class with a phrase like
   * "A column or function parameter with name ... cannot be resolved"; older
   * Spark just says "cannot be resolved". Accept either, but also require the
   * specific column name appears so we know the right column was reported.
   */
  private def assertNativeUnresolvedColumn(msg: String, columnName: String): Unit = {
    val hasCannotResolve =
      msg.contains("cannot be resolved") ||
        msg.contains("UNRESOLVED_COLUMN")
    assert(hasCannotResolve,
      s"Expected Spark's native unresolved-column error; got: $msg")
    assert(msg.contains(columnName),
      s"Expected error to mention column '$columnName'; got: $msg")
  }

  private def assertNativeTableNotFound(msg: String, tableName: String): Unit = {
    val hasTableNotFound =
      msg.contains("TABLE_OR_VIEW_NOT_FOUND") ||
        msg.toLowerCase.contains("table or view not found") ||
        msg.contains("cannot be resolved")
    assert(hasTableNotFound,
      s"Expected Spark's native table-not-found error; got: $msg")
    assert(msg.contains(tableName),
      s"Expected error to mention table '$tableName'; got: $msg")
  }

  /**
   * The earlier version of this PR caught UnresolvedException and rewrote it as
   * "Failed to resolve query. The query contains unresolved columns or tables.
   *  Please check for: (1) typos ...". Make sure we no longer mask Spark's
   * native message with that generic Hudi wrapper.
   */
  private def assertNoHudiGenericRewrite(msg: String): Unit = {
    assert(!msg.contains("Failed to resolve query"),
      s"Hudi should not rewrap Spark's analysis error; got: $msg")
    assert(!msg.contains("Please check for: (1) typos"),
      s"Hudi should not rewrap Spark's analysis error; got: $msg")
  }
}
