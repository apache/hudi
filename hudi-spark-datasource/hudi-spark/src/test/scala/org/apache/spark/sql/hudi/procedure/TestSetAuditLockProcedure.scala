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

import java.io.File

/**
 * Test suite for the SetAuditLockProcedure Spark SQL procedure.
 *
 * This class contains comprehensive tests to verify the functionality of
 * the set_audit_lock procedure, including enabling/disabling audit logging,
 * parameter validation, and error handling scenarios.
 *
 * @author Apache Hudi
 * @since 1.1.0
 */
class TestSetAuditLockProcedure extends HoodieSparkProcedureTestBase {

  override def generateTableName: String = {
    super.generateTableName.split("\\.").last
  }

  /**
   * Helper method to create a test table and return its path.
   */
  private def createTestTable(tmp: File, tableName: String): String = {
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
    // Insert data to initialize the Hudi metadata structure
    spark.sql(s"insert into $tableName select 1, 'test', 10.0, 1000")
    s"${tmp.getCanonicalPath}/$tableName"
  }

  /**
   * Test enabling audit logging using table name parameter.
   */
  test("Test Call set_audit_lock Procedure - Enable Audit with Table Name") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      val result = spark.sql(s"""call set_audit_lock(table => '$tableName', state => 'enabled')""").collect()

      assertResult(1)(result.length)
      assertResult(tableName)(result.head.get(0))
      assertResult("enabled")(result.head.get(1))
      assert(result.head.get(2).toString.contains("successfully enabled"))
    }
  }

  /**
   * Test enabling audit logging using path parameter.
   */
  test("Test Call set_audit_lock Procedure - Enable Audit with Path") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      val result = spark.sql(s"""call set_audit_lock(path => '$tablePath', state => 'enabled')""").collect()

      assertResult(1)(result.length)
      assertResult(tablePath)(result.head.get(0))
      assertResult("enabled")(result.head.get(1))
      assert(result.head.get(2).toString.contains("successfully enabled"))
    }
  }

  /**
   * Test disabling audit logging using table name parameter.
   */
  test("Test Call set_audit_lock Procedure - Disable Audit with Table Name") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      val result = spark.sql(s"""call set_audit_lock(table => '$tableName', state => 'disabled')""").collect()

      assertResult(1)(result.length)
      assertResult(tableName)(result.head.get(0))
      assertResult("disabled")(result.head.get(1))
      assert(result.head.get(2).toString.contains("successfully disabled"))
    }
  }

  /**
   * Test disabling audit logging using path parameter.
   */
  test("Test Call set_audit_lock Procedure - Disable Audit with Path") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      val result = spark.sql(s"""call set_audit_lock(path => '$tablePath', state => 'disabled')""").collect()

      assertResult(1)(result.length)
      assertResult(tablePath)(result.head.get(0))
      assertResult("disabled")(result.head.get(1))
      assert(result.head.get(2).toString.contains("successfully disabled"))
    }
  }

  /**
   * Test parameter validation by providing an invalid state parameter.
   * Verifies that the procedure rejects invalid state values and provides
   * an appropriate error message.
   */
  test("Test Call set_audit_lock Procedure - Invalid State Parameter") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createTestTable(tmp, tableName)

      // Test invalid state parameter
      checkExceptionContain(s"""call set_audit_lock(table => '$tableName', state => 'invalid')""")(
        "State parameter must be 'enabled' or 'disabled'")
    }
  }

  /**
   * Test parameter validation by omitting required arguments.
   * Verifies that the procedure properly validates required parameters
   * and provides appropriate error messages for missing arguments.
   */
  test("Test Call set_audit_lock Procedure - Missing Required Arguments") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      // Test missing both table and path parameters
      checkExceptionContain(s"""call set_audit_lock(state => 'enabled')""")(
        "Either table or path parameter must be provided")

      // Test missing state parameter
      checkExceptionContain(s"""call set_audit_lock(table => '$tableName')""")(
        "Argument: state is required")

      // Test providing both table and path parameters
      checkExceptionContain(s"""call set_audit_lock(table => '$tableName', path => '$tablePath', state => 'enabled')""")(
        "Cannot specify both table and path parameters")
    }
  }
}
