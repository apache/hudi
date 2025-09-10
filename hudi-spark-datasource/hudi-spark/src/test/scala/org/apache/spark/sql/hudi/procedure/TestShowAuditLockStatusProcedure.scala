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
 * Test suite for the ShowAuditLockStatusProcedure Spark SQL procedure.
 *
 * This class contains comprehensive tests to verify the functionality of
 * the show_audit_lock_status procedure, including status checking with both
 * table name and path parameters, and various audit states.
 *
 * @author Apache Hudi
 * @since 1.1.0
 */
class TestShowAuditLockStatusProcedure extends HoodieSparkProcedureTestBase {

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
   * Test showing audit status when audit is disabled (default state) using table name.
   */
  test("Test Show Audit Status - Disabled with Table Name") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createTestTable(tmp, tableName)

      val result = spark.sql(s"""call show_audit_lock_status(table => '$tableName')""").collect()

      assertResult(1)(result.length)
      assertResult(tableName)(result.head.get(0)) // table name
      assertResult(false)(result.head.get(1)) // audit_enabled
      assert(result.head.get(2).toString.contains("audit_enabled.json")) // config_path
      assert(result.head.get(3).toString.contains("audit")) // audit_folder_path
    }
  }

  /**
   * Test showing audit status when audit is disabled (default state) using path.
   */
  test("Test Show Audit Status - Disabled with Path") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      val result = spark.sql(s"""call show_audit_lock_status(path => '$tablePath')""").collect()

      assertResult(1)(result.length)
      assertResult(tablePath)(result.head.get(0)) // path
      assertResult(false)(result.head.get(1)) // audit_enabled
      assert(result.head.get(2).toString.contains("audit_enabled.json")) // config_path
      assert(result.head.get(3).toString.contains("audit")) // audit_folder_path
    }
  }

  /**
   * Test showing audit status when audit is enabled using table name.
   */
  test("Test Show Audit Status - Enabled with Table Name") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createTestTable(tmp, tableName)

      // First enable audit logging
      spark.sql(s"""call set_audit_lock(table => '$tableName', state => 'enabled')""")

      // Then check the status
      val result = spark.sql(s"""call show_audit_lock_status(table => '$tableName')""").collect()

      assertResult(1)(result.length)
      assertResult(tableName)(result.head.get(0)) // table name
      assertResult(true)(result.head.get(1)) // audit_enabled
      assert(result.head.get(2).toString.contains("audit_enabled.json")) // config_path
      assert(result.head.get(3).toString.contains("audit")) // audit_folder_path
    }
  }

  /**
   * Test showing audit status when audit is enabled using path.
   */
  test("Test Show Audit Status - Enabled with Path") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      // First enable audit logging
      spark.sql(s"""call set_audit_lock(path => '$tablePath', state => 'enabled')""")

      // Then check the status
      val result = spark.sql(s"""call show_audit_lock_status(path => '$tablePath')""").collect()

      assertResult(1)(result.length)
      assertResult(tablePath)(result.head.get(0)) // path
      assertResult(true)(result.head.get(1)) // audit_enabled
      assert(result.head.get(2).toString.contains("audit_enabled.json")) // config_path
      assert(result.head.get(3).toString.contains("audit")) // audit_folder_path
    }
  }

  /**
   * Test audit status after enabling and then disabling audit.
   * Verifies that the status correctly reflects the disabled state.
   */
  test("Test Show Audit Status After Enable and Disable") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createTestTable(tmp, tableName)

      // Enable audit
      spark.sql(s"""call set_audit_lock(table => '$tableName', state => 'enabled')""")
      // Verify enabled status
      val enabledResult = spark.sql(s"""call show_audit_lock_status(table => '$tableName')""").collect()
      assertResult(true)(enabledResult.head.get(1))

      // Disable audit
      spark.sql(s"""call set_audit_lock(table => '$tableName', state => 'disabled')""")
      // Verify disabled status
      val disabledResult = spark.sql(s"""call show_audit_lock_status(table => '$tableName')""").collect()
      assertResult(false)(disabledResult.head.get(1))
    }
  }

  /**
   * Test parameter validation by omitting required arguments.
   * Verifies that the procedure properly validates required parameters.
   */
  test("Test Show Audit Status - Missing Required Arguments") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      // Test missing both table and path parameters
      checkExceptionContain(s"""call show_audit_lock_status()""")(
        "Either table or path parameter must be provided")

      // Test providing both table and path parameters
      checkExceptionContain(s"""call show_audit_lock_status(table => '$tableName', path => '$tablePath')""")(
        "Cannot specify both table and path parameters")
    }
  }

  /**
   * Test showing audit status for a table that doesn't exist.
   * Verifies graceful handling of non-existent tables.
   */
  test("Test Show Audit Status - Non-existent Table") {
    val nonExistentTable = "non_existent_table_" + System.currentTimeMillis()
    // This should not throw an exception but should handle gracefully
    checkExceptionContain(s"""call show_audit_lock_status(table => '$nonExistentTable')""")(
      "not found")
  }

  /**
   * Test output schema verification.
   * Ensures the procedure returns the correct column structure.
   */
  test("Test Show Audit Status - Output Schema") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createTestTable(tmp, tableName)

      val result = spark.sql(s"""call show_audit_lock_status(table => '$tableName')""")
      val schema = result.schema

      // Verify column count and names
      assertResult(4)(schema.fields.length)
      assertResult("table")(schema.fields(0).name)
      assertResult("audit_enabled")(schema.fields(1).name)
      assertResult("config_path")(schema.fields(2).name)
      assertResult("audit_folder_path")(schema.fields(3).name)

      // Verify column types
      assertResult("string")(schema.fields(0).dataType.typeName)
      assertResult("boolean")(schema.fields(1).dataType.typeName)
      assertResult("string")(schema.fields(2).dataType.typeName)
      assertResult("string")(schema.fields(3).dataType.typeName)
    }
  }

}
