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

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.File

/**
 * Test suite for the SetAuditLockProcedure Spark SQL procedure.
 *
 * This class contains comprehensive tests to verify the functionality of
 * the set_audit_lock procedure, including enabling/disabling audit logging,
 * parameter validation, and error handling scenarios.
 * Refactored to use parameterized tests for better code reuse.
 *
 * @author Apache Hudi
 * @since 1.1.0
 */
class TestSetAuditLockProcedure extends HoodieSparkProcedureTestBase {

  /**
   * Helper method to create a test table and return its path.
   */
  private def createTestTable(tmp: File, tableName: String): String = {
    spark.sql(
      s"""
         |create table if not exists $tableName (
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
    s"${tmp.getCanonicalPath}/$tableName"
  }

  /**
   * Parameterized test for enabling audit logging.
   * Tests both table name and path parameter approaches.
   */
  @ParameterizedTest(name = "Enable audit with {0}")
  @ValueSource(strings = Array("table", "path"))
  def testEnableAudit(paramType: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      val (paramName, paramValue, expectedReturn) = paramType match {
        case "table" => ("table", tableName, tableName)
        case "path" => ("path", tablePath, tablePath)
      }

      val result = spark.sql(s"""call set_audit_lock($paramName => '$paramValue', state => 'enabled')""").collect()

      assertResult(1)(result.length)
      assertResult(expectedReturn)(result.head.get(0))
      assertResult("enabled")(result.head.get(1))
      assert(result.head.get(2).toString.contains("successfully enabled"))
    }
  }

  /**
   * Parameterized test for disabling audit logging.
   * Tests both table name and path parameter approaches.
   */
  @ParameterizedTest(name = "Disable audit with {0}")
  @ValueSource(strings = Array("table", "path"))
  def testDisableAudit(paramType: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      val (paramName, paramValue, expectedReturn) = paramType match {
        case "table" => ("table", tableName, tableName)
        case "path" => ("path", tablePath, tablePath)
      }

      val result = spark.sql(s"""call set_audit_lock($paramName => '$paramValue', state => 'disabled')""").collect()

      assertResult(1)(result.length)
      assertResult(expectedReturn)(result.head.get(0))
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
