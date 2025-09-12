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
import org.apache.spark.sql.Row

import java.io.File
import java.nio.file.{Files, Paths}

/**
 * Test suite for the ValidateAuditLockProcedure Spark SQL procedure.
 *
 * This class contains comprehensive tests to verify the functionality of
 * the validate_audit_lock procedure with parameterized test scenarios.
 *
 * @author Apache Hudi
 * @since 1.1.0
 */
class TestValidateAuditLockProcedure extends HoodieSparkProcedureTestBase {

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
   * Represents a single audit record (JSON line in a .jsonl file)
   */
  case class AuditRecord(
    ownerId: String,
    transactionStartTime: Long,
    timestamp: Long,
    state: String, // START, RENEW, or END
    lockExpiration: Long,
    lockHeld: Boolean = true
  )

  /**
   * Represents a transaction scenario with its audit records
   */
  case class TransactionScenario(
    filename: String, // e.g., "1234567890_owner1.jsonl"
    records: List[AuditRecord]
  )

  /**
   * Represents expected validation results
   */
  case class ExpectedResult(
    validationResult: String, // PASSED, WARNING, FAILED, ERROR
    transactionsValidated: Int,
    issuesFound: Int,
    detailsContains: List[String] = List() // Strings that should be in details
  )

  /**
   * Test scenario definition with input and expected output
   */
  case class ValidationTestScenario(
    name: String,
    scenarioGenerator: () => List[TransactionScenario],
    expectedResultGenerator: () => ExpectedResult
  )

  /**
   * Helper method to create audit files from scenarios
   */
  private def createAuditFiles(tablePath: String, scenarios: List[TransactionScenario]): Unit = {
    val auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(tablePath)
    val auditDir = Paths.get(auditFolderPath)
    
    // Create audit directory if it doesn't exist
    if (!Files.exists(auditDir)) {
      Files.createDirectories(auditDir)
    }
    
    scenarios.foreach { scenario =>
      val filePath = auditDir.resolve(scenario.filename)
      val jsonLines = scenario.records.map { record =>
        s"""{"ownerId":"${record.ownerId}","transactionStartTime":${record.transactionStartTime},"timestamp":${record.timestamp},"state":"${record.state}","lockExpiration":${record.lockExpiration},"lockHeld":${record.lockHeld}}"""
      }.mkString("\n")
      
      Files.write(filePath, jsonLines.getBytes())
    }
  }

  /**
   * Helper method to run a parameterized validation test scenario
   */
  private def runValidationScenario(scenario: ValidationTestScenario): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)
      
      // Generate test data
      val transactionScenarios = scenario.scenarioGenerator()
      val expectedResult = scenario.expectedResultGenerator()
      
      // Create audit files
      createAuditFiles(tablePath, transactionScenarios)
      
      // Run validation
      val result = spark.sql(s"""call validate_audit_lock(table => '$tableName')""").collect()
      
      // Verify results
      assertResult(1)(result.length)
      
      val row = result.head
      assertResult(tableName)(row.getString(0))
      assertResult(expectedResult.validationResult)(row.getString(1))
      assertResult(expectedResult.transactionsValidated)(row.getInt(2))
      assertResult(expectedResult.issuesFound)(row.getInt(3))
      
      val details = row.getString(4)
      expectedResult.detailsContains.foreach { expectedSubstring =>
        assert(details.contains(expectedSubstring), 
          s"Details '$details' should contain '$expectedSubstring'")
      }
    }
  }

  // ==================== Scenario-Based Validation Tests ====================

  /**
   * Test scenario: No overlapping transactions, all transactions closed properly
   */
  test("Test Validation - No Issues (PASSED)") {
    val scenario = ValidationTestScenario(
      name = "No Issues - All Transactions Completed Properly",
      scenarioGenerator = () => {
        val baseTime = System.currentTimeMillis()
        List(
          TransactionScenario(
            filename = s"${baseTime}_owner1.jsonl",
            records = List(
              AuditRecord("owner1", baseTime, baseTime + 100, "START", baseTime + 60000),
              AuditRecord("owner1", baseTime, baseTime + 200, "RENEW", baseTime + 60000),
              AuditRecord("owner1", baseTime, baseTime + 300, "END", baseTime + 60000)
            )
          ),
          TransactionScenario(
            filename = s"${baseTime + 500}_owner2.jsonl",
            records = List(
              AuditRecord("owner2", baseTime + 500, baseTime + 600, "START", baseTime + 60000),
              AuditRecord("owner2", baseTime + 500, baseTime + 700, "END", baseTime + 60000)
            )
          )
        )
      },
      expectedResultGenerator = () => ExpectedResult(
        validationResult = "PASSED",
        transactionsValidated = 2,
        issuesFound = 0,
        detailsContains = List("successfully")
      )
    )
    
    runValidationScenario(scenario)
  }

  /**
   * Test scenario: Single unclosed transaction (should be WARNING only)
   */
  test("Test Validation - Single Unclosed Transaction (WARNING)") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)
      
      val baseTime = 1000000L
      
      // Create single audit file without END record
      val scenarios = List(
        TransactionScenario(
          filename = s"${baseTime}_owner1.jsonl",
          records = List(
            AuditRecord("owner1", baseTime, baseTime + 100, "START", baseTime + 200)
          )
        )
      )
      
      createAuditFiles(tablePath, scenarios)
      
      val result = spark.sql(s"""call validate_audit_lock(table => '$tableName')""").collect()
      val row = result.head
      
      // This should be WARNING only since there's no overlap possible with just one transaction
      assertResult("WARNING")(row.getString(1))
      assertResult(1)(row.getInt(2)) // transactions_validated
      assertResult(1)(row.getInt(3)) // issues_found
    }
  }

  /**
   * Test scenario: Transactions without proper END, with proper separation (WARNING)
   */
  test("Test Validation - Unclosed Transactions (WARNING)") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)
      
      val baseTime = 1000000L
      
      // Create two transactions: one unclosed, one complete, no overlap
      val scenarios = List(
        TransactionScenario(
          filename = s"${baseTime}_owner1.jsonl",
          records = List(
            AuditRecord("owner1", baseTime, baseTime + 100, "START", baseTime + 200)
            // No END record - effective end at expiration (baseTime + 200)
          )
        ),
        TransactionScenario(
          filename = s"${baseTime + 300}_owner2.jsonl", // Starts after owner1's expiration
          records = List(
            AuditRecord("owner2", baseTime + 300, baseTime + 400, "START", baseTime + 60000),
            AuditRecord("owner2", baseTime + 300, baseTime + 500, "END", baseTime + 60000)
          )
        )
      )
      
      createAuditFiles(tablePath, scenarios)
      
      val result = spark.sql(s"""call validate_audit_lock(table => '$tableName')""").collect()
      val row = result.head
      
      println(s"DEBUG - Result: ${row.getString(1)}, Issues: ${row.getInt(3)}, Details: ${row.getString(4)}")
      
      assertResult("WARNING")(row.getString(1))
      assertResult(2)(row.getInt(2)) // transactions_validated
      assertResult(1)(row.getInt(3)) // issues_found - only the unclosed transaction
      assert(row.getString(4).contains("[WARNING]"))
      assert(row.getString(4).contains("owner1.jsonl"))
      assert(row.getString(4).contains("did not end gracefully"))
    }
  }

  /**
   * Test scenario: Overlapping transactions with improper closure (FAILED)
   */
  test("Test Validation - Overlapping Transactions (FAILED)") {
    val scenario = ValidationTestScenario(
      name = "Overlapping Transactions",
      scenarioGenerator = () => {
        val baseTime = System.currentTimeMillis()
        List(
          TransactionScenario(
            filename = s"${baseTime}_owner1.jsonl",
            records = List(
              AuditRecord("owner1", baseTime, baseTime + 100, "START", baseTime + 60000),
              AuditRecord("owner1", baseTime, baseTime + 500, "END", baseTime + 60000) // Ends after owner2 starts
            )
          ),
          TransactionScenario(
            filename = s"${baseTime + 200}_owner2.jsonl", // Starts before owner1 ends
            records = List(
              AuditRecord("owner2", baseTime + 200, baseTime + 300, "START", baseTime + 60000),
              AuditRecord("owner2", baseTime + 200, baseTime + 400, "END", baseTime + 60000)
            )
          )
        )
      },
      expectedResultGenerator = () => ExpectedResult(
        validationResult = "FAILED",
        transactionsValidated = 2,
        issuesFound = 1,
        detailsContains = List("[ERROR]", "owner1.jsonl", "overlaps with", "owner2.jsonl")
      )
    )
    
    runValidationScenario(scenario)
  }

  /**
   * Test scenario: Mixed issues - overlaps and unclosed transactions
   */
  test("Test Validation - Mixed Issues (FAILED)") {
    val scenario = ValidationTestScenario(
      name = "Mixed Issues",
      scenarioGenerator = () => {
        val baseTime = System.currentTimeMillis()
        List(
          TransactionScenario(
            filename = s"${baseTime}_owner1.jsonl",
            records = List(
              AuditRecord("owner1", baseTime, baseTime + 100, "START", baseTime + 60000)
              // No END - unclosed
            )
          ),
          TransactionScenario(
            filename = s"${baseTime + 50}_owner2.jsonl", // Overlaps with owner1
            records = List(
              AuditRecord("owner2", baseTime + 50, baseTime + 150, "START", baseTime + 60000),
              AuditRecord("owner2", baseTime + 50, baseTime + 250, "END", baseTime + 60000)
            )
          )
        )
      },
      expectedResultGenerator = () => ExpectedResult(
        validationResult = "FAILED",
        transactionsValidated = 2,
        issuesFound = 2,
        detailsContains = List("[ERROR]", "[WARNING]", "overlaps with", "did not end gracefully")
      )
    )
    
    runValidationScenario(scenario)
  }

  /**
   * Test scenario: Out-of-order filenames but valid non-overlapping transactions (PASSED)
   */
  test("Test Validation - Out of Order Filenames but Valid Transactions (PASSED)") {
    val scenario = ValidationTestScenario(
      name = "Out of Order Filenames - Valid Transactions",
      scenarioGenerator = () => {
        val baseTime = System.currentTimeMillis()
        List(
          // File with later timestamp in name but contains earlier transaction
          TransactionScenario(
            filename = s"${baseTime + 2000}_owner2.jsonl", // Filename suggests later time
            records = List(
              AuditRecord("owner2", baseTime + 100, baseTime + 200, "START", baseTime + 60000), // Actually starts first
              AuditRecord("owner2", baseTime + 100, baseTime + 300, "END", baseTime + 60000)
            )
          ),
          // File with earlier timestamp in name but contains later transaction
          TransactionScenario(
            filename = s"${baseTime}_owner1.jsonl", // Filename suggests earlier time
            records = List(
              AuditRecord("owner1", baseTime + 500, baseTime + 600, "START", baseTime + 60000), // Actually starts second
              AuditRecord("owner1", baseTime + 500, baseTime + 700, "END", baseTime + 60000)
            )
          )
        )
      },
      expectedResultGenerator = () => ExpectedResult(
        validationResult = "PASSED",
        transactionsValidated = 2,
        issuesFound = 0,
        detailsContains = List("successfully")
      )
    )
    
    runValidationScenario(scenario)
  }

  // ==================== Parameter Validation Tests ====================

  /**
   * Test parameter validation by providing both table and path parameters.
   * Since both are provided, the procedure will use the table parameter (consistent with other procedures).
   */
  test("Test Parameter Validation - Both Table and Path Parameters") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)

      // Should work fine - will use table parameter when both are provided
      val result = spark.sql(s"""call validate_audit_lock(table => '$tableName', path => '$tablePath')""").collect()
      assertResult(1)(result.length)
      assertResult(tableName)(result.head.getString(0))
    }
  }

  /**
   * Test parameter validation by omitting both required arguments.
   */
  test("Test Parameter Validation - Missing Required Arguments") {
    checkExceptionContain(s"""call validate_audit_lock()""")(
      "Table name or table path must be given one")
  }

  /**
   * Test validation with non-existent table.
   */
  test("Test Parameter Validation - Non-existent Table") {
    val nonExistentTable = "non_existent_table"
    
    intercept[Exception] {
      spark.sql(s"""call validate_audit_lock(table => '$nonExistentTable')""")
    }
  }

  /**
   * Test validation with invalid path.
   */
  test("Test Parameter Validation - Invalid Path") {
    val invalidPath = "/non/existent/path"
    
    intercept[Exception] {
      spark.sql(s"""call validate_audit_lock(path => '$invalidPath')""")
    }
  }

  /**
   * Test validation with no audit folder.
   */
  test("Test Validation - No Audit Folder (PASSED)") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = createTestTable(tmp, tableName)
      // Don't create any audit files

      val result = spark.sql(s"""call validate_audit_lock(table => '$tableName')""").collect()

      assertResult(1)(result.length)
      val row = result.head
      assertResult(tableName)(row.getString(0))
      assertResult("PASSED")(row.getString(1))
      assertResult(0)(row.getInt(2)) // transactions_validated
      assertResult(0)(row.getInt(3)) // issues_found
      assert(row.getString(4).contains("No audit folder found"))
    }
  }
}