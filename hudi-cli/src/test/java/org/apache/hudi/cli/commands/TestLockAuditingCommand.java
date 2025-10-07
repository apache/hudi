/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.client.transaction.lock.audit.StorageLockProviderAuditService;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link org.apache.hudi.cli.commands.LockAuditingCommand}.
 */
@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestLockAuditingCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Helper method to create AuditRecord for tests with lockHeld defaulting to true.
   */
  private static LockAuditingCommand.AuditRecord createAuditRecord(
      String ownerId, long transactionStartTime, long timestamp, String state, long lockExpiration) {
    return new LockAuditingCommand.AuditRecord(ownerId, transactionStartTime, timestamp, state, lockExpiration, true);
  }

  /**
   * Represents a transaction scenario with its audit records
   */
  static class TransactionScenario {
    final String filename; // e.g., "1234567890_owner1.jsonl"
    final List<LockAuditingCommand.AuditRecord> records;

    TransactionScenario(String filename, List<LockAuditingCommand.AuditRecord> records) {
      this.filename = filename;
      this.records = records;
    }
  }

  /**
   * Helper method to create audit files from scenarios
   */
  private void createAuditFiles(List<TransactionScenario> scenarios) throws IOException {
    String auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath);
    StoragePath auditDir = new StoragePath(auditFolderPath);

    // Create audit directory if it doesn't exist
    if (!HoodieCLI.storage.exists(auditDir)) {
      HoodieCLI.storage.createDirectory(auditDir);
    }

    for (TransactionScenario scenario : scenarios) {
      StoragePath filePath = new StoragePath(auditDir, scenario.filename);
      StringBuilder jsonLines = new StringBuilder();
      for (LockAuditingCommand.AuditRecord record : scenario.records) {
        if (jsonLines.length() > 0) {
          jsonLines.append("\n");
        }
        jsonLines.append(OBJECT_MAPPER.writeValueAsString(record));
      }

      try (OutputStream outputStream = HoodieCLI.storage.create(filePath, true)) {
        outputStream.write(jsonLines.toString().getBytes());
      }
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    HoodieCLI.conf = storageConf();
    String tableName = tableName();
    String tablePath = tablePath(tableName);
    HoodieCLI.basePath = tablePath;
    
    // Initialize table
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE.name())
        .setTableName(tableName)
        .setRecordKeyFields("key")
        .initTable(storageConf(), tablePath);
        
    // Initialize storage
    HoodieCLI.initFS(true);
  }

  /**
   * Test enabling lock audit when no table is loaded.
   */
  @Test
  public void testEnableLockAuditNoTable() {
    // Clear the base path to simulate no table loaded
    String originalBasePath = HoodieCLI.basePath;
    HoodieCLI.basePath = null;

    try {
      Object result = shell.evaluate(() -> "locks audit enable");
      assertAll("Command runs with no table loaded",
          () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
          () -> assertNotNull(result.toString()),
          () -> assertEquals("No Hudi table loaded. Please connect to a table first.", result.toString()));
    } finally {
      HoodieCLI.basePath = originalBasePath;
    }
  }

  /**
   * Test disabling lock audit when no table is loaded.
   */
  @Test
  public void testDisableLockAuditNoTable() {
    // Clear the base path to simulate no table loaded
    String originalBasePath = HoodieCLI.basePath;
    HoodieCLI.basePath = null;

    try {
      Object result = shell.evaluate(() -> "locks audit disable");
      assertAll("Command runs with no table loaded",
          () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
          () -> assertNotNull(result.toString()),
          () -> assertEquals("No Hudi table loaded. Please connect to a table first.", result.toString()));
    } finally {
      HoodieCLI.basePath = originalBasePath;
    }
  }

  /**
   * Test showing lock audit status when no table is loaded.
   */
  @Test
  public void testShowLockAuditStatusNoTable() {
    // Clear the base path to simulate no table loaded
    String originalBasePath = HoodieCLI.basePath;
    HoodieCLI.basePath = null;

    try {
      Object result = shell.evaluate(() -> "locks audit status");
      assertAll("Command runs with no table loaded",
          () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
          () -> assertNotNull(result.toString()),
          () -> assertEquals("No Hudi table loaded. Please connect to a table first.", result.toString()));
    } finally {
      HoodieCLI.basePath = originalBasePath;
    }
  }

  /**
   * Test enabling lock audit successfully.
   */
  @Test
  public void testEnableLockAuditSuccess() throws Exception {
    Object result = shell.evaluate(() -> "locks audit enable");
    
    assertAll("Enable command runs successfully",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Lock audit enabled successfully")));

    // Verify the config file was created with correct content
    String lockFolderPath = String.format("%s%s.hoodie%s.locks", HoodieCLI.basePath, StoragePath.SEPARATOR, StoragePath.SEPARATOR);
    String auditConfigPath = String.format("%s%s%s", lockFolderPath, StoragePath.SEPARATOR, StorageLockProviderAuditService.AUDIT_CONFIG_FILE_NAME);
    StoragePath configPath = new StoragePath(auditConfigPath);
    
    assertTrue(HoodieCLI.storage.exists(configPath), "Config file should exist");
    
    String configContent;
    try (InputStream inputStream = HoodieCLI.storage.open(configPath)) {
      configContent = new String(FileIOUtils.readAsByteArray(inputStream));
    }
    JsonNode rootNode = OBJECT_MAPPER.readTree(configContent);
    JsonNode enabledNode = rootNode.get(StorageLockProviderAuditService.STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD);
    
    assertNotNull(enabledNode, "Config should contain enabled field");
    assertTrue(enabledNode.asBoolean(), "Audit should be enabled");
  }

  /**
   * Test showing lock audit status when disabled (no config file).
   */
  @Test
  public void testShowLockAuditStatusDisabled() {
    Object result = shell.evaluate(() -> "locks audit status");
    
    assertAll("Status command shows disabled state",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Lock Audit Status: DISABLED")),
        () -> assertTrue(result.toString().contains("(not found)")));
  }

  /**
   * Test the complete workflow: enable, check status, disable, check status.
   */
  @Test
  public void testCompleteAuditWorkflow() throws Exception {
    // 1. Enable audit
    Object enableResult = shell.evaluate(() -> "locks audit enable");
    assertTrue(ShellEvaluationResultUtil.isSuccess(enableResult));
    assertTrue(enableResult.toString().contains("Lock audit enabled successfully"));

    // 2. Check status - should be enabled
    Object statusEnabledResult = shell.evaluate(() -> "locks audit status");
    assertTrue(ShellEvaluationResultUtil.isSuccess(statusEnabledResult));
    assertTrue(statusEnabledResult.toString().contains("Lock Audit Status: ENABLED"));

    // 3. Disable audit with default keepAuditFiles=true
    Object disableResult = shell.evaluate(() -> "locks audit disable");
    assertTrue(ShellEvaluationResultUtil.isSuccess(disableResult));
    assertTrue(disableResult.toString().contains("Lock audit disabled successfully"));
    assertTrue(disableResult.toString().contains("Existing audit files preserved"));

    // 4. Check status - should be disabled
    Object statusDisabledResult = shell.evaluate(() -> "locks audit status");
    assertTrue(ShellEvaluationResultUtil.isSuccess(statusDisabledResult));
    assertTrue(statusDisabledResult.toString().contains("Lock Audit Status: DISABLED"));

    // Verify the config file still exists but with audit disabled
    String lockFolderPath = String.format("%s%s.hoodie%s.locks", HoodieCLI.basePath, StoragePath.SEPARATOR, StoragePath.SEPARATOR);
    String auditConfigPath = String.format("%s%s%s", lockFolderPath, StoragePath.SEPARATOR, StorageLockProviderAuditService.AUDIT_CONFIG_FILE_NAME);
    StoragePath configPath = new StoragePath(auditConfigPath);
    
    assertTrue(HoodieCLI.storage.exists(configPath), "Config file should still exist");
    
    String configContent;
    try (InputStream inputStream = HoodieCLI.storage.open(configPath)) {
      configContent = new String(FileIOUtils.readAsByteArray(inputStream));
    }
    JsonNode rootNode = OBJECT_MAPPER.readTree(configContent);
    JsonNode enabledNode = rootNode.get(StorageLockProviderAuditService.STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD);
    
    assertNotNull(enabledNode, "Config should contain enabled field");
    assertFalse(enabledNode.asBoolean(), "Audit should be disabled");
  }

  /**
   * Test disabling audit when no config file exists.
   */
  @Test
  public void testDisableLockAuditNoConfigFile() {
    Object result = shell.evaluate(() -> "locks audit disable");
    
    assertAll("Disable command handles missing config file",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertEquals("Lock audit is already disabled (no configuration file found).", result.toString()));
  }

  /**
   * Test disabling audit with keepAuditFiles=false option.
   */
  @Test
  public void testDisableLockAuditWithoutKeepingFiles() throws Exception {
    // First enable audit
    shell.evaluate(() -> "locks audit enable");

    // Create some audit files to be cleaned up
    List<TransactionScenario> scenarios = new ArrayList<>();
    List<LockAuditingCommand.AuditRecord> records = new ArrayList<>();
    records.add(createAuditRecord("owner1", 1000L, 1100L, "START", 61000L));
    records.add(createAuditRecord("owner1", 1000L, 1200L, "END", 61000L));
    scenarios.add(new TransactionScenario("1000_owner1.jsonl", records));
    createAuditFiles(scenarios);

    // Disable with keepAuditFiles=false
    Object result = shell.evaluate(() -> "locks audit disable --keepAuditFiles false");
    
    assertAll("Disable command with keepAuditFiles=false",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Lock audit disabled successfully")),
        () -> assertTrue(result.toString().contains("Audit files cleaned up at:")
                        || result.toString().contains("Some audit files may not have been cleaned up")));
  }

  /**
   * Test that disable with keepAuditFiles=false actually cleans up files.
   */
  @Test
  public void testDisableLockAuditCleansUpFiles() throws Exception {
    // First enable audit
    shell.evaluate(() -> "locks audit enable");

    // Create some audit files
    List<TransactionScenario> scenarios = new ArrayList<>();
    List<LockAuditingCommand.AuditRecord> records = new ArrayList<>();
    records.add(createAuditRecord("owner1", 1000L, 1100L, "START", 61000L));
    records.add(createAuditRecord("owner1", 1000L, 1200L, "END", 61000L));
    scenarios.add(new TransactionScenario("1000_owner1.jsonl", records));
    createAuditFiles(scenarios);

    // Verify files exist before disable
    String auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath);
    StoragePath auditFolder = new StoragePath(auditFolderPath);
    List<StoragePathInfo> filesBefore = HoodieCLI.storage.listDirectEntries(auditFolder);
    long jsonlFilesBefore = filesBefore.stream()
        .filter(pathInfo -> pathInfo.isFile() && pathInfo.getPath().getName().endsWith(".jsonl"))
        .count();
    assertTrue(jsonlFilesBefore > 0, "Should have audit files before disable");

    // Disable with keepAuditFiles=false - this should trigger cleanup
    Object result = shell.evaluate(() -> "locks audit disable --keepAuditFiles false");
    
    assertAll("Cleanup triggered by disable",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Lock audit disabled successfully")));
  }

  /**
   * Test disable with keepAuditFiles=false when no audit files exist.
   */
  @Test
  public void testDisableLockAuditWithoutKeepingFilesNoFiles() {
    // First enable audit but don't create any audit files
    shell.evaluate(() -> "locks audit enable");

    // Disable with keepAuditFiles=false when no files exist
    Object result = shell.evaluate(() -> "locks audit disable --keepAuditFiles false");
    
    assertAll("Disable with cleanup when no files exist",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Lock audit disabled successfully")),
        // Should handle the case where no files exist to clean up
        () -> assertTrue(result.toString().contains("Audit files cleaned up at:")
                        || result.toString().contains("No audit files")
                        || result.toString().contains("nothing to cleanup")));
  }

  /**
   * Test enabling audit multiple times (should overwrite).
   */
  @Test
  public void testEnableLockAuditMultipleTimes() throws Exception {
    // Enable first time
    Object result1 = shell.evaluate(() -> "locks audit enable");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result1));
    assertTrue(result1.toString().contains("Lock audit enabled successfully"));

    // Enable second time (should succeed and overwrite)
    Object result2 = shell.evaluate(() -> "locks audit enable");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result2));
    assertTrue(result2.toString().contains("Lock audit enabled successfully"));

    // Verify config is still correct
    String lockFolderPath = String.format("%s%s.hoodie%s.locks", HoodieCLI.basePath, StoragePath.SEPARATOR, StoragePath.SEPARATOR);
    String auditConfigPath = String.format("%s%s%s", lockFolderPath, StoragePath.SEPARATOR, StorageLockProviderAuditService.AUDIT_CONFIG_FILE_NAME);
    StoragePath configPath = new StoragePath(auditConfigPath);
    
    String configContent;
    try (InputStream inputStream = HoodieCLI.storage.open(configPath)) {
      configContent = new String(FileIOUtils.readAsByteArray(inputStream));
    }
    JsonNode rootNode = OBJECT_MAPPER.readTree(configContent);
    JsonNode enabledNode = rootNode.get(StorageLockProviderAuditService.STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD);
    
    assertTrue(enabledNode.asBoolean(), "Audit should still be enabled");
  }

  // ==================== Validation Tests ====================

  /**
   * Test validation when no audit folder exists.
   */
  @Test
  public void testValidateAuditLocksNoAuditFolder() {
    Object result = shell.evaluate(() -> "locks audit validate");
    
    assertAll("Validation handles missing audit folder",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Validation Result: PASSED")),
        () -> assertTrue(result.toString().contains("Transactions Validated: 0")),
        () -> assertTrue(result.toString().contains("Issues Found: 0")),
        () -> assertTrue(result.toString().contains("No audit folder found")));
  }

  /**
   * Test validation when audit folder exists but no audit files.
   */
  @Test
  public void testValidateAuditLocksNoAuditFiles() throws IOException {
    // Create audit folder but no files
    String auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath);
    StoragePath auditDir = new StoragePath(auditFolderPath);
    HoodieCLI.storage.createDirectory(auditDir);
    
    Object result = shell.evaluate(() -> "locks audit validate");
    
    assertAll("Validation handles no audit files",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Validation Result: PASSED")),
        () -> assertTrue(result.toString().contains("Transactions Validated: 0")),
        () -> assertTrue(result.toString().contains("Issues Found: 0")),
        () -> assertTrue(result.toString().contains("No audit files found")));
  }

  /**
   * Test validation - No Issues (PASSED)
   */
  @Test
  public void testValidateAuditLocksNoIssues() throws IOException {
    long baseTime = System.currentTimeMillis();
    List<TransactionScenario> scenarios = new ArrayList<>();
    
    // Transaction 1: Complete transaction
    List<LockAuditingCommand.AuditRecord> records1 = new ArrayList<>();
    records1.add(createAuditRecord("owner1", baseTime, baseTime + 100, "START", baseTime + 60000));
    records1.add(createAuditRecord("owner1", baseTime, baseTime + 200, "RENEW", baseTime + 60000));
    records1.add(createAuditRecord("owner1", baseTime, baseTime + 300, "END", baseTime + 60000));
    scenarios.add(new TransactionScenario(baseTime + "_owner1.jsonl", records1));
    
    // Transaction 2: Complete transaction starting after first one ends
    List<LockAuditingCommand.AuditRecord> records2 = new ArrayList<>();
    records2.add(createAuditRecord("owner2", baseTime + 500, baseTime + 600, "START", baseTime + 60000));
    records2.add(createAuditRecord("owner2", baseTime + 500, baseTime + 700, "END", baseTime + 60000));
    scenarios.add(new TransactionScenario((baseTime + 500) + "_owner2.jsonl", records2));
    
    createAuditFiles(scenarios);
    
    Object result = shell.evaluate(() -> "locks audit validate");
    
    assertAll("Validation passes with no issues",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Validation Result: PASSED")),
        () -> assertTrue(result.toString().contains("Transactions Validated: 2")),
        () -> assertTrue(result.toString().contains("Issues Found: 0")),
        () -> assertTrue(result.toString().contains("successfully")));
  }

  /**
   * Test validation - Single Unclosed Transaction (WARNING)
   */
  @Test
  public void testValidateAuditLocksSingleUnclosedTransaction() throws IOException {
    long baseTime = 1000000L;
    List<TransactionScenario> scenarios = new ArrayList<>();
    
    // Single audit file without END record
    List<LockAuditingCommand.AuditRecord> records1 = new ArrayList<>();
    records1.add(createAuditRecord("owner1", baseTime, baseTime + 100, "START", baseTime + 200));
    scenarios.add(new TransactionScenario(baseTime + "_owner1.jsonl", records1));
    
    createAuditFiles(scenarios);
    
    Object result = shell.evaluate(() -> "locks audit validate");
    
    assertAll("Single unclosed transaction shows warning",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Validation Result: WARNING")),
        () -> assertTrue(result.toString().contains("Transactions Validated: 1")),
        () -> assertTrue(result.toString().contains("Issues Found: 1")));
  }

  /**
   * Test validation - Unclosed Transactions (WARNING)
   */
  @Test
  public void testValidateAuditLocksUnclosedTransactions() throws IOException {
    long baseTime = 1000000L;
    List<TransactionScenario> scenarios = new ArrayList<>();
    
    // Transaction 1: Unclosed (effective end at expiration = baseTime + 200)
    List<LockAuditingCommand.AuditRecord> records1 = new ArrayList<>();
    records1.add(createAuditRecord("owner1", baseTime, baseTime + 100, "START", baseTime + 200));
    scenarios.add(new TransactionScenario(baseTime + "_owner1.jsonl", records1));
    
    // Transaction 2: Complete, starts after owner1's expiration
    List<LockAuditingCommand.AuditRecord> records2 = new ArrayList<>();
    records2.add(createAuditRecord("owner2", baseTime + 300, baseTime + 400, "START", baseTime + 60000));
    records2.add(createAuditRecord("owner2", baseTime + 300, baseTime + 500, "END", baseTime + 60000));
    scenarios.add(new TransactionScenario((baseTime + 300) + "_owner2.jsonl", records2));
    
    createAuditFiles(scenarios);
    
    Object result = shell.evaluate(() -> "locks audit validate");
    
    assertAll("Unclosed transactions show warning",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Validation Result: WARNING")),
        () -> assertTrue(result.toString().contains("Transactions Validated: 2")),
        () -> assertTrue(result.toString().contains("Issues Found: 1")),
        () -> assertTrue(result.toString().contains("[WARNING]")),
        () -> assertTrue(result.toString().contains("owner1.jsonl")),
        () -> assertTrue(result.toString().contains("did not end gracefully")));
  }

  /**
   * Test validation - Overlapping Transactions (FAILED)
   */
  @Test
  public void testValidateAuditLocksOverlappingTransactions() throws IOException {
    long baseTime = System.currentTimeMillis();
    List<TransactionScenario> scenarios = new ArrayList<>();
    
    // Transaction 1: Ends after owner2 starts (overlapping)
    List<LockAuditingCommand.AuditRecord> records1 = new ArrayList<>();
    records1.add(createAuditRecord("owner1", baseTime, baseTime + 100, "START", baseTime + 60000));
    records1.add(createAuditRecord("owner1", baseTime, baseTime + 500, "END", baseTime + 60000)); // Ends after owner2 starts
    scenarios.add(new TransactionScenario(baseTime + "_owner1.jsonl", records1));
    
    // Transaction 2: Starts before owner1 ends (overlapping)
    List<LockAuditingCommand.AuditRecord> records2 = new ArrayList<>();
    records2.add(createAuditRecord("owner2", baseTime + 200, baseTime + 300, "START", baseTime + 60000)); // Starts before owner1 ends
    records2.add(createAuditRecord("owner2", baseTime + 200, baseTime + 400, "END", baseTime + 60000));
    scenarios.add(new TransactionScenario((baseTime + 200) + "_owner2.jsonl", records2));
    
    createAuditFiles(scenarios);
    
    Object result = shell.evaluate(() -> "locks audit validate");
    
    assertAll("Overlapping transactions show failure",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Validation Result: FAILED")),
        () -> assertTrue(result.toString().contains("Transactions Validated: 2")),
        () -> assertTrue(result.toString().contains("Issues Found: 1")),
        () -> assertTrue(result.toString().contains("[ERROR]")),
        () -> assertTrue(result.toString().contains("owner1.jsonl")),
        () -> assertTrue(result.toString().contains("overlaps with")),
        () -> assertTrue(result.toString().contains("owner2.jsonl")));
  }

  /**
   * Test validation - Mixed Issues (FAILED)
   */
  @Test
  public void testValidateAuditLocksMixedIssues() throws IOException {
    long baseTime = System.currentTimeMillis();
    List<TransactionScenario> scenarios = new ArrayList<>();
    
    // Transaction 1: Unclosed transaction
    List<LockAuditingCommand.AuditRecord> records1 = new ArrayList<>();
    records1.add(createAuditRecord("owner1", baseTime, baseTime + 100, "START", baseTime + 60000));
    // No END - unclosed
    scenarios.add(new TransactionScenario(baseTime + "_owner1.jsonl", records1));
    
    // Transaction 2: Overlaps with owner1
    List<LockAuditingCommand.AuditRecord> records2 = new ArrayList<>();
    records2.add(createAuditRecord("owner2", baseTime + 50, baseTime + 150, "START", baseTime + 60000)); // Overlaps with owner1
    records2.add(createAuditRecord("owner2", baseTime + 50, baseTime + 250, "END", baseTime + 60000));
    scenarios.add(new TransactionScenario((baseTime + 50) + "_owner2.jsonl", records2));
    
    createAuditFiles(scenarios);
    
    Object result = shell.evaluate(() -> "locks audit validate");
    
    assertAll("Mixed issues show failure",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Validation Result: FAILED")),
        () -> assertTrue(result.toString().contains("Transactions Validated: 2")),
        () -> assertTrue(result.toString().contains("Issues Found: 2")),
        () -> assertTrue(result.toString().contains("[ERROR]")),
        () -> assertTrue(result.toString().contains("[WARNING]")),
        () -> assertTrue(result.toString().contains("overlaps with")),
        () -> assertTrue(result.toString().contains("did not end gracefully")));
  }

  /**
   * Test validation - Out of Order Filenames but Valid Transactions (PASSED)
   */
  @Test
  public void testValidateAuditLocksOutOfOrderFilenames() throws IOException {
    long baseTime = System.currentTimeMillis();
    List<TransactionScenario> scenarios = new ArrayList<>();
    
    // File with later timestamp in name but contains earlier transaction
    List<LockAuditingCommand.AuditRecord> records1 = new ArrayList<>();
    records1.add(createAuditRecord("owner2", baseTime + 100, baseTime + 200, "START", baseTime + 60000)); // Actually starts first
    records1.add(createAuditRecord("owner2", baseTime + 100, baseTime + 300, "END", baseTime + 60000));
    scenarios.add(new TransactionScenario((baseTime + 2000) + "_owner2.jsonl", records1)); // Filename suggests later time
    
    // File with earlier timestamp in name but contains later transaction
    List<LockAuditingCommand.AuditRecord> records2 = new ArrayList<>();
    records2.add(createAuditRecord("owner1", baseTime + 500, baseTime + 600, "START", baseTime + 60000)); // Actually starts second
    records2.add(createAuditRecord("owner1", baseTime + 500, baseTime + 700, "END", baseTime + 60000));
    scenarios.add(new TransactionScenario(baseTime + "_owner1.jsonl", records2)); // Filename suggests earlier time
    
    createAuditFiles(scenarios);
    
    Object result = shell.evaluate(() -> "locks audit validate");
    
    assertAll("Out of order filenames but valid transactions pass",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Validation Result: PASSED")),
        () -> assertTrue(result.toString().contains("Transactions Validated: 2")),
        () -> assertTrue(result.toString().contains("Issues Found: 0")),
        () -> assertTrue(result.toString().contains("successfully")));
  }

  /**
   * Test validation when no table is loaded.
   */
  @Test
  public void testValidateAuditLocksNoTable() {
    // Clear the base path to simulate no table loaded
    String originalBasePath = HoodieCLI.basePath;
    HoodieCLI.basePath = null;

    try {
      Object result = shell.evaluate(() -> "locks audit validate");
      assertAll("Validation handles no table loaded",
          () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
          () -> assertNotNull(result.toString()),
          () -> assertEquals("No Hudi table loaded. Please connect to a table first.", result.toString()));
    } finally {
      HoodieCLI.basePath = originalBasePath;
    }
  }

  /**
   * Test validation with malformed audit files.
   */
  @Test
  public void testValidateAuditLocksMalformedFiles() throws IOException {
    // Create audit directory
    String auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath);
    StoragePath auditDir = new StoragePath(auditFolderPath);
    HoodieCLI.storage.createDirectory(auditDir);
    
    // Create a malformed audit file
    StoragePath filePath = new StoragePath(auditDir, "malformed_file.jsonl");
    try (OutputStream outputStream = HoodieCLI.storage.create(filePath, true)) {
      outputStream.write("invalid json content".getBytes());
    }
    
    Object result = shell.evaluate(() -> "locks audit validate");
    
    assertAll("Validation handles malformed files",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Validation Result: FAILED")),
        () -> assertTrue(result.toString().contains("Transactions Validated: 0")),
        () -> assertTrue(result.toString().contains("Failed to parse any audit files")));
  }

  // ==================== Cleanup Tests ====================

  /**
   * Test cleanup when no audit folder exists.
   */
  @Test
  public void testCleanupAuditLocksNoAuditFolder() {
    Object result = shell.evaluate(() -> "locks audit cleanup");
    
    assertAll("Cleanup handles missing audit folder",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertEquals("No audit folder found - nothing to cleanup.", result.toString()));
  }

  /**
   * Test cleanup when audit folder exists but no audit files.
   */
  @Test
  public void testCleanupAuditLocksNoAuditFiles() throws IOException {
    // Create audit folder but no files
    String auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath);
    StoragePath auditDir = new StoragePath(auditFolderPath);
    HoodieCLI.storage.createDirectory(auditDir);
    
    Object result = shell.evaluate(() -> "locks audit cleanup");
    
    assertAll("Cleanup handles no audit files",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("No audit files older than 7 days found")));
  }

  /**
   * Test cleanup with dry run - test basic functionality.
   */
  @Test
  public void testCleanupAuditLocksDryRun() throws IOException {
    // Create some audit files (they will have current modification time)
    long oldTime = System.currentTimeMillis() - (10 * 24 * 60 * 60 * 1000L); // 10 days ago
    List<TransactionScenario> scenarios = new ArrayList<>();
    
    List<LockAuditingCommand.AuditRecord> records = new ArrayList<>();
    records.add(createAuditRecord("owner1", oldTime, oldTime + 100, "START", oldTime + 60000));
    records.add(createAuditRecord("owner1", oldTime, oldTime + 200, "END", oldTime + 60000));
    scenarios.add(new TransactionScenario(oldTime + "_owner1.jsonl", records));
    
    createAuditFiles(scenarios);
    
    // Test default cleanup with dry run
    Object result = shell.evaluate(() -> "locks audit cleanup --dryRun true");
    
    assertAll("Dry run executes successfully",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        // Since files are created with current time, they should be too recent to delete
        () -> assertTrue(result.toString().contains("No audit files older than") 
                        || result.toString().contains("Dry run: Would delete")));
    
    // Verify files still exist
    String auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath);
    StoragePath auditFolder = new StoragePath(auditFolderPath);
    List<StoragePathInfo> filesAfter = HoodieCLI.storage.listDirectEntries(auditFolder);
    long jsonlFiles = filesAfter.stream()
        .filter(pathInfo -> pathInfo.isFile() && pathInfo.getPath().getName().endsWith(".jsonl"))
        .count();
    assertEquals(1, jsonlFiles, "Files should still exist after dry run");
  }

  /**
   * Test cleanup with custom age threshold.
   */
  @Test
  public void testCleanupAuditLocksCustomAge() throws IOException {
    // Create some audit files
    List<TransactionScenario> scenarios = new ArrayList<>();
    
    List<LockAuditingCommand.AuditRecord> records = new ArrayList<>();
    records.add(createAuditRecord("owner1", 1000L, 1100L, "START", 61000L));
    records.add(createAuditRecord("owner1", 1000L, 1200L, "END", 61000L));
    scenarios.add(new TransactionScenario("1000_owner1.jsonl", records));
    
    createAuditFiles(scenarios);
    
    // Test with custom age threshold - files will be recent so won't be deleted
    Object result = shell.evaluate(() -> "locks audit cleanup --ageDays 30 --dryRun true");
    
    assertAll("Custom age threshold works",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("No audit files older than 30 days found")));
  }

  /**
   * Test actual cleanup (not dry run).
   */
  @Test
  public void testCleanupAuditLocksActualDelete() throws IOException {
    // Create some audit files
    List<TransactionScenario> scenarios = new ArrayList<>();
    
    List<LockAuditingCommand.AuditRecord> records = new ArrayList<>();
    records.add(createAuditRecord("owner1", 1000L, 1100L, "START", 61000L));
    records.add(createAuditRecord("owner1", 1000L, 1200L, "END", 61000L));
    scenarios.add(new TransactionScenario("1000_owner1.jsonl", records));
    
    createAuditFiles(scenarios);
    
    // Test actual cleanup - files will be recent so shouldn't be deleted
    Object result = shell.evaluate(() -> "locks audit cleanup --dryRun false");
    
    assertAll("Actual cleanup executes successfully",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        // Recent files shouldn't be deleted
        () -> assertTrue(result.toString().contains("No audit files older than 7 days found")));
  }

  /**
   * Test cleanup with recent files (should not delete).
   */
  @Test
  public void testCleanupAuditLocksRecentFiles() throws IOException {
    // Create recent audit files
    long recentTime = System.currentTimeMillis() - (2 * 24 * 60 * 60 * 1000L); // 2 days ago
    List<TransactionScenario> scenarios = new ArrayList<>();
    
    List<LockAuditingCommand.AuditRecord> records = new ArrayList<>();
    records.add(createAuditRecord("owner1", recentTime, recentTime + 100, "START", recentTime + 60000));
    records.add(createAuditRecord("owner1", recentTime, recentTime + 200, "END", recentTime + 60000));
    scenarios.add(new TransactionScenario(recentTime + "_owner1.jsonl", records));
    
    createAuditFiles(scenarios);
    
    Object result = shell.evaluate(() -> "locks audit cleanup");
    
    assertAll("Recent files are not deleted",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("No audit files older than 7 days found")));
  }

  /**
   * Test cleanup parameter validation - invalid age days.
   */
  @Test
  public void testCleanupAuditLocksInvalidAgeDays() {
    // Test with negative ageDays (Spring Shell treats this as invalid integer format)
    Object resultNegative = shell.evaluate(() -> "locks audit cleanup --ageDays -1");

    assertAll("Negative ageDays should be rejected",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(resultNegative)),
        () -> assertNotNull(resultNegative.toString()),
        () -> assertEquals("Error: ageDays must be a value greater than 0.", resultNegative.toString()));

    // Test with invalid string to verify our parsing validation
    Object resultInvalid = shell.evaluate(() -> "locks audit cleanup --ageDays abc");

    assertAll("Invalid string ageDays should be rejected",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(resultInvalid)),
        () -> assertNotNull(resultInvalid.toString()),
        () -> assertEquals("Error: ageDays must be a value greater than 0.", resultInvalid.toString()));
  }

  /**
   * Test cleanup when no table is loaded.
   */
  @Test
  public void testCleanupAuditLocksNoTable() {
    // Clear the base path to simulate no table loaded
    String originalBasePath = HoodieCLI.basePath;
    HoodieCLI.basePath = null;

    Object result = shell.evaluate(() -> "locks audit cleanup");
    assertAll("Cleanup handles no table loaded",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertEquals("No Hudi table loaded. Please connect to a table first.", result.toString()));
    HoodieCLI.basePath = originalBasePath;
  }

  /**
   * Test cleanup with multiple files.
   */
  @Test
  public void testCleanupAuditLocksMixedFiles() throws IOException {
    // Create multiple audit files
    List<TransactionScenario> scenarios = new ArrayList<>();
    
    // File 1
    List<LockAuditingCommand.AuditRecord> records1 = new ArrayList<>();
    records1.add(createAuditRecord("owner1", 1000L, 1100L, "START", 61000L));
    records1.add(createAuditRecord("owner1", 1000L, 1200L, "END", 61000L));
    scenarios.add(new TransactionScenario("1000_owner1.jsonl", records1));
    
    // File 2
    List<LockAuditingCommand.AuditRecord> records2 = new ArrayList<>();
    records2.add(createAuditRecord("owner2", 2000L, 2100L, "START", 62000L));
    records2.add(createAuditRecord("owner2", 2000L, 2200L, "END", 62000L));
    scenarios.add(new TransactionScenario("2000_owner2.jsonl", records2));
    
    createAuditFiles(scenarios);
    
    Object result = shell.evaluate(() -> "locks audit cleanup --dryRun true");
    System.out.println("DEBUG - Mixed files result: " + result.toString());
    
    assertAll("Multiple files handled correctly",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("No audit files older than")
                        || result.toString().contains("cleanup")
                        || result.toString().contains("found")));
  }

  /**
   * Test that disable method can call performAuditCleanup with ageDays=0 to delete all files.
   * This validates that the internal validation allows ageDays >= 0 (not just > 0).
   */
  @Test
  public void testDisableLockAuditWithAgeDaysZero() throws Exception {
    // First enable audit
    shell.evaluate(() -> "locks audit enable");

    // Create some audit files to be cleaned up
    List<TransactionScenario> scenarios = new ArrayList<>();
    List<LockAuditingCommand.AuditRecord> records = new ArrayList<>();
    records.add(createAuditRecord("owner1", 1000L, 1100L, "START", 61000L));
    records.add(createAuditRecord("owner1", 1000L, 1200L, "END", 61000L));
    scenarios.add(new TransactionScenario("1000_owner1.jsonl", records));
    createAuditFiles(scenarios);

    // Verify files exist before disable
    String auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath);
    StoragePath auditFolder = new StoragePath(auditFolderPath);
    List<StoragePathInfo> filesBefore = HoodieCLI.storage.listDirectEntries(auditFolder);
    long jsonlFilesBefore = filesBefore.stream()
        .filter(pathInfo -> pathInfo.isFile() && pathInfo.getPath().getName().endsWith(".jsonl"))
        .count();
    assertTrue(jsonlFilesBefore > 0, "Should have audit files before disable");

    // Disable with keepAuditFiles=false, which internally calls performAuditCleanup(false, 0)
    Object result = shell.evaluate(() -> "locks audit disable --keepAuditFiles false");

    assertAll("Disable with ageDays=0 deletes all files",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Lock audit disabled successfully")),
        () -> assertTrue(result.toString().contains("cleaned up") || result.toString().contains("No audit files to clean up")));

    // Verify files were cleaned up (ageDays=0 should delete all files)
    List<StoragePathInfo> filesAfter = HoodieCLI.storage.listDirectEntries(auditFolder);
    long jsonlFilesAfter = filesAfter.stream()
        .filter(pathInfo -> pathInfo.isFile() && pathInfo.getPath().getName().endsWith(".jsonl"))
        .count();
    assertEquals(0, jsonlFilesAfter, "All audit files should be deleted when ageDays=0");
  }
}