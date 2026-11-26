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

package org.apache.spark.sql.hudi.ddl

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestSharedSessionTableCleanup extends HoodieSparkSqlTestBase {

  test("Test cleanup drops this suite's tables and temp views, not another suite's") {
    assume(HoodieSparkSqlTestBase.sharedSessionEnabled, "per-suite mode owns the whole catalog")
    // Stands in for a suite running at the same time: its own session on the shared catalog.
    val otherSuite = spark.newSession()
    val otherTable = "shared_session_cleanup_other_suite_tbl"
    val otherView = "shared_session_cleanup_other_suite_view"
    val ownTable = generateTableName
    val ownDerivedTable = s"${generateTableName}_pt"
    val ownView = "shared_session_cleanup_own_view"
    try {
      otherSuite.sql(s"create table $otherTable (id int) using parquet")
      otherSuite.range(1).createOrReplaceTempView(otherView)
      spark.sql(s"create table $ownTable (id int) using parquet")
      spark.sql(s"create table $ownDerivedTable (id int) using parquet")
      spark.range(1).createOrReplaceTempView(ownView)

      dropSuiteTables()

      val catalog = spark.sessionState.catalog
      assert(!catalog.tableExists(TableIdentifier(ownTable)))
      assert(!catalog.tableExists(TableIdentifier(ownDerivedTable)))
      assert(!catalog.isTempView(TableIdentifier(ownView)))
      assert(catalog.tableExists(TableIdentifier(otherTable)))
      assert(otherSuite.sessionState.catalog.isTempView(TableIdentifier(otherView)))
    } finally {
      otherSuite.sql(s"drop table if exists $otherTable")
    }
  }
}
