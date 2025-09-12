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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;

import java.io.InputStream;
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

    // Disable with keepAuditFiles=false
    Object result = shell.evaluate(() -> "locks audit disable --keepAuditFiles false");
    
    assertAll("Disable command with keepAuditFiles=false",
        () -> assertTrue(ShellEvaluationResultUtil.isSuccess(result)),
        () -> assertNotNull(result.toString()),
        () -> assertTrue(result.toString().contains("Lock audit disabled successfully")),
        () -> assertTrue(result.toString().contains("Audit files cleaned up at:")));
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
}