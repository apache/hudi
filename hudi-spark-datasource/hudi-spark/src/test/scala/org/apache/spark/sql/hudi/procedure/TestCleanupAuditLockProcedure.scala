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

import org.apache.hudi.client.transaction.lock.audit.StorageLockProviderAuditService
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieClientTestUtils

import java.io.File
import java.nio.file.{Files, Paths}

/**
 * Test suite for the CleanupAuditLockProcedure Spark SQL procedure.
 *
 * This class contains comprehensive tests to verify the functionality of
 * the cleanup_audit_lock procedure, including dry run mode, age-based cleanup,
 * parameter validation, and error handling scenarios.
 *
 * @author Apache Hudi
 * @since 1.1.0
 */
class TestCleanupAuditLockProcedure extends HoodieSparkProcedureTestBase {

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
   * Helper method to create test audit files with specified ages.
   *
   * @param tablePath The base path of the table
   * @param filenames List of filenames to create
   * @param ageDaysAgo How many days ago to set the modification time
   */
  private def createTestAuditFiles(tablePath: String, filenames: List[String], ageDaysAgo: Int): Unit = {
    val auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(tablePath)
    val auditDir = Paths.get(auditFolderPath)

    // Create audit directory if it doesn't exist
    if (!Files.exists(auditDir)) {
      Files.createDirectories(auditDir)
    }

    // Create test audit files with specified modification time
    val targetTime = System.currentTimeMillis() - (ageDaysAgo * 24L * 60L * 60L * 1000L)

    filenames.foreach { filename =>
      val filePath = auditDir.resolve(filename)
      val content = s"""{"ownerId":"test","transactionStartTime":${System.currentTimeMillis()},"timestamp":${System.currentTimeMillis()},"state":"START","lockExpiration":${System.currentTimeMillis() + 60000},"lockHeld":true}"""
      Files.write(filePath, content.getBytes())
      // Set the modification time to simulate old files
      Files.setLastModifiedTime(filePath, java.nio.file.attribute.FileTime.fromMillis(targetTime))
    }
  }

  /**
   * Helper method to count audit files in the audit folder.
   */
  private def countAuditFiles(tablePath: String): Int = {
    val auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(tablePath)
    val auditDir = Paths.get(auditFolderPath)

    if (Files.exists(auditDir)) {
      Files.list(auditDir)
        .filter(_.toString.endsWith(".jsonl"))
        .count()
        .toInt
    } else {
      0
    }
  }

  /**
   * Test cleanup with table name parameter - dry run mode.
   */
  test("Test Call cleanup_audit_lock Procedure - Dry Run with Table Name") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      // Create some old audit files (10 days old)
      createTestAuditFiles(tablePath, List("old1.jsonl", "old2.jsonl"), ageDaysAgo = 10)
      // Create some recent audit files (2 days old)
      createTestAuditFiles(tablePath, List("recent1.jsonl"), ageDaysAgo = 2)

      val result = spark.sql(s"""call cleanup_audit_lock(table => '$tableName', dry_run => true, age_days => 7)""").collect()

      assertResult(1)(result.length)
      assertResult(tableName)(result.head.get(0))
      assertResult(2)(result.head.get(1)) // Should find 2 old files
      assertResult(true)(result.head.get(2)) // dry_run = true
      assertResult(7)(result.head.get(3)) // age_days = 7
      assert(result.head.get(4).toString.contains("Dry run"))

      // Verify files still exist (dry run shouldn't delete)
      assertResult(3)(countAuditFiles(tablePath))
    }
  }

  /**
   * Test cleanup with path parameter - actual deletion.
   */
  test("Test Call cleanup_audit_lock Procedure - Actual Cleanup with Path") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      // Create some old audit files (10 days old)
      createTestAuditFiles(tablePath, List("old1.jsonl", "old2.jsonl"), ageDaysAgo = 10)
      // Create some recent audit files (2 days old)
      createTestAuditFiles(tablePath, List("recent1.jsonl"), ageDaysAgo = 2)

      val result = spark.sql(s"""call cleanup_audit_lock(path => '$tablePath', dry_run => false, age_days => 7)""").collect()

      assertResult(1)(result.length)
      assertResult(tablePath)(result.head.get(0))
      assertResult(2)(result.head.get(1)) // Should delete 2 old files
      assertResult(false)(result.head.get(2)) // dry_run = false
      assertResult(7)(result.head.get(3)) // age_days = 7
      assert(result.head.get(4).toString.contains("Successfully deleted"))

      // Verify only recent file remains
      assertResult(1)(countAuditFiles(tablePath))
    }
  }

  /**
   * Test cleanup with default parameters.
   */
  test("Test Call cleanup_audit_lock Procedure - Default Parameters") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      // Create some old audit files (10 days old) - should be deleted with default 7 days
      createTestAuditFiles(tablePath, List("old1.jsonl"), ageDaysAgo = 10)
      // Create some recent audit files (3 days old) - should be kept
      createTestAuditFiles(tablePath, List("recent1.jsonl"), ageDaysAgo = 3)

      val result = spark.sql(s"""call cleanup_audit_lock(table => '$tableName')""").collect()

      assertResult(1)(result.length)
      assertResult(tableName)(result.head.get(0))
      assertResult(1)(result.head.get(1)) // Should delete 1 old file
      assertResult(false)(result.head.get(2)) // dry_run defaults to false
      assertResult(7)(result.head.get(3)) // age_days defaults to 7

      // Verify only recent file remains
      assertResult(1)(countAuditFiles(tablePath))
    }
  }

  /**
   * Test cleanup when no audit folder exists.
   */
  test("Test Call cleanup_audit_lock Procedure - No Audit Folder") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)
      // Don't create any audit files

      val result = spark.sql(s"""call cleanup_audit_lock(table => '$tableName')""").collect()

      assertResult(1)(result.length)
      assertResult(tableName)(result.head.get(0))
      assertResult(0)(result.head.get(1)) // No files to delete
      assert(result.head.get(4).toString.contains("No audit folder found"))
    }
  }

  /**
   * Test cleanup when no old files exist.
   */
  test("Test Call cleanup_audit_lock Procedure - No Old Files") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      // Create only recent audit files (2 days old)
      createTestAuditFiles(tablePath, List("recent1.jsonl", "recent2.jsonl"), ageDaysAgo = 2)

      val result = spark.sql(s"""call cleanup_audit_lock(table => '$tableName', age_days => 7)""").collect()

      assertResult(1)(result.length)
      assertResult(tableName)(result.head.get(0))
      assertResult(0)(result.head.get(1)) // No old files to delete
      assert(result.head.get(4).toString.contains("No audit files older than 7 days found"))

      // Verify all files remain
      assertResult(2)(countAuditFiles(tablePath))
    }
  }

  /**
   * Test parameter validation - negative age_days.
   */
  test("Test Call cleanup_audit_lock Procedure - Invalid Age Days") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createTestTable(tmp, tableName)

      checkExceptionContain(s"""call cleanup_audit_lock(table => '$tableName', age_days => -1)""")(
        "age_days must be a positive integer")

      checkExceptionContain(s"""call cleanup_audit_lock(table => '$tableName', age_days => 0)""")(
        "age_days must be a positive integer")
    }
  }

  /**
   * Test parameter validation - both table and path provided.
   * Since both are provided, the procedure will use the table parameter (consistent with other procedures).
   */
  test("Test Call cleanup_audit_lock Procedure - Both Table and Path Parameters") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      // Should work fine - will use table parameter when both are provided
      val result = spark.sql(s"""call cleanup_audit_lock(table => '$tableName', path => '$tablePath')""").collect()
      assertResult(1)(result.length)
      assertResult(tableName)(result.head.getString(0))
    }
  }

  /**
   * Test parameter validation - missing required arguments.
   */
  test("Test Call cleanup_audit_lock Procedure - Missing Required Arguments") {
    checkExceptionContain(s"""call cleanup_audit_lock(dry_run => true)""")(
      "Table name or table path must be given one")
  }

  /**
   * Test cleanup with non-existent table.
   */
  test("Test Call cleanup_audit_lock Procedure - Non-existent Table") {
    val nonExistentTable = "non_existent_table"

    intercept[Exception] {
      spark.sql(s"""call cleanup_audit_lock(table => '$nonExistentTable')""")
    }
  }

  /**
   * Test cleanup with invalid path.
   */
  test("Test Call cleanup_audit_lock Procedure - Invalid Path") {
    val invalidPath = "/non/existent/path"

    intercept[Exception] {
      spark.sql(s"""call cleanup_audit_lock(path => '$invalidPath')""")
    }
  }
}
