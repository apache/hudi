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

import org.apache.hudi.HoodieSparkUtils

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
   * Assert the failure is Spark's native unresolved-column error and not a Hudi rewrite.
   *
   * On Spark 3.4+ we require the structured `UNRESOLVED_COLUMN` error-class token. That
   * token is produced only by Spark's own `CheckAnalysis`; the pre-PR Hudi path rewrote
   * the failure as a generic "Failed to resolve query ..." message that never carried it,
   * so requiring the token here actually distinguishes the new behavior from the old (the
   * looser "cannot be resolved" substring would have matched either way — see
   * https://github.com/apache/hudi/pull/18147#discussion_r2795763747). On Spark 3.3, which
   * predates error classes, the phrasing varies by code path — accept the legacy
   * "cannot resolve" / "cannot be resolved" forms as well as "Column '...' does not exist"
   * (what Spark 3.3 emits for the unresolved references in these queries).
   *
   * In all cases require the offending column name to appear, so we know the precise
   * column was reported rather than some unrelated resolution failure.
   */
  private def assertNativeUnresolvedColumn(msg: String, columnName: String): Unit = {
    if (HoodieSparkUtils.gteqSpark3_4) {
      assert(msg.contains("UNRESOLVED_COLUMN"),
        s"Expected Spark's structured UNRESOLVED_COLUMN error class; got: $msg")
    } else {
      assert(msg.contains("cannot resolve") || msg.contains("cannot be resolved") ||
        msg.contains("does not exist"),
        s"Expected Spark's native unresolved-column error; got: $msg")
    }
    assert(msg.contains(columnName),
      s"Expected error to mention column '$columnName'; got: $msg")
  }

  /**
   * Assert the failure is Spark's native table-not-found error. On Spark 3.4+ require the
   * structured `TABLE_OR_VIEW_NOT_FOUND` error-class token (same reasoning as
   * [[assertNativeUnresolvedColumn]]); on Spark 3.3 fall back to the legacy phrasing.
   */
  private def assertNativeTableNotFound(msg: String, tableName: String): Unit = {
    if (HoodieSparkUtils.gteqSpark3_4) {
      assert(msg.contains("TABLE_OR_VIEW_NOT_FOUND"),
        s"Expected Spark's structured TABLE_OR_VIEW_NOT_FOUND error class; got: $msg")
    } else {
      assert(msg.toLowerCase.contains("table or view not found"),
        s"Expected Spark's native table-not-found error; got: $msg")
    }
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
