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
import org.apache.hudi.client.transaction.lock.audit.StorageLockProviderAuditService;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * CLI commands for managing Hudi table lock auditing functionality.
 */
@ShellComponent
public class LockAuditingCommand {

  private static final Logger LOG = LoggerFactory.getLogger(LockAuditingCommand.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Represents a transaction window with start time, end time, and metadata.
   */
  static class TransactionWindow {
    final String ownerId;
    final long transactionStartTime;
    final long startTimestamp;
    final Option<Long> endTimestamp;
    final Option<Long> lastExpirationTime;
    final String filename;

    TransactionWindow(String ownerId, long transactionStartTime, long startTimestamp,
                      Option<Long> endTimestamp, Option<Long> lastExpirationTime, String filename) {
      this.ownerId = ownerId;
      this.transactionStartTime = transactionStartTime;
      this.startTimestamp = startTimestamp;
      this.endTimestamp = endTimestamp;
      this.lastExpirationTime = lastExpirationTime;
      this.filename = filename;
    }

    long getEffectiveEndTime() {
      return endTimestamp.orElse(lastExpirationTime.orElse(startTimestamp));
    }
  }

  /**
   * Holds validation results.
   */
  static class ValidationResults {
    final List<String> errors;
    final List<String> warnings;

    ValidationResults(List<String> errors, List<String> warnings) {
      this.errors = errors;
      this.warnings = warnings;
    }
  }

  /**
   * Holds cleanup operation results.
   */
  static class CleanupResult {
    final boolean success;
    final int deletedCount;
    final int failedCount;
    final int totalFiles;
    final String message;
    final boolean isDryRun;

    CleanupResult(boolean success, int deletedCount, int failedCount, int totalFiles, String message, boolean isDryRun) {
      this.success = success;
      this.deletedCount = deletedCount;
      this.failedCount = failedCount;
      this.totalFiles = totalFiles;
      this.message = message;
      this.isDryRun = isDryRun;
    }

    boolean hasErrors() {
      return failedCount > 0;
    }

    boolean hasFiles() {
      return totalFiles > 0;
    }

    @Override
    public String toString() {
      return message;
    }
  }

  /**
   * Enables lock audit logging for the currently connected Hudi table.
   * This command creates or updates the audit configuration file to enable
   * audit logging for storage lock operations.
   * 
   * @return Status message indicating success or failure
   */
  @ShellMethod(key = "locks audit enable", value = "Enable storage lock audit service for the current table")
  public String enableLockAudit() {
    
    if (HoodieCLI.basePath == null) {
      return "No Hudi table loaded. Please connect to a table first.";
    }

    try {
      // Create the audit config file path using utility method
      String auditConfigPath = StorageLockProviderAuditService.getAuditConfigPath(HoodieCLI.basePath);
      
      // Create the JSON content
      ObjectNode configJson = OBJECT_MAPPER.createObjectNode();
      configJson.put(StorageLockProviderAuditService.STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD, true);
      String jsonContent = OBJECT_MAPPER.writeValueAsString(configJson);
      
      // Write the config file using HoodieStorage
      StoragePath configPath = new StoragePath(auditConfigPath);
      try (OutputStream outputStream = HoodieCLI.storage.create(configPath, true)) {
        outputStream.write(jsonContent.getBytes());
      }
      
      return String.format("Lock audit enabled successfully.\nAudit config written to: %s\n"
          + "Audit files will be stored at: %s", auditConfigPath, StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath));
      
    } catch (Exception e) {
      LOG.error("Error enabling lock audit", e);
      return String.format("Failed to enable lock audit: %s", e.getMessage());
    }
  }

  /**
   * Disables lock audit logging for the currently connected Hudi table.
   * This command updates the audit configuration file to disable audit logging.
   * 
   * @param keepAuditFiles Whether to preserve existing audit files when disabling
   * @return Status message indicating success or failure
   */
  @ShellMethod(key = "locks audit disable", value = "Disable storage lock audit service for the current table")
  public String disableLockAudit(
      @ShellOption(value = {"--keepAuditFiles"}, defaultValue = "true",
          help = "Keep existing audit files when disabling") final boolean keepAuditFiles) {
    
    if (HoodieCLI.basePath == null) {
      return "No Hudi table loaded. Please connect to a table first.";
    }

    try {
      // Create the audit config file path
      String auditConfigPath = StorageLockProviderAuditService.getAuditConfigPath(HoodieCLI.basePath);
      
      // Check if config file exists
      StoragePath configPath = new StoragePath(auditConfigPath);
      if (!HoodieCLI.storage.exists(configPath)) {
        return "Lock audit is already disabled (no configuration file found).";
      }
      
      // Create the JSON content with audit disabled
      ObjectNode configJson = OBJECT_MAPPER.createObjectNode();
      configJson.put(StorageLockProviderAuditService.STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD, false);
      String jsonContent = OBJECT_MAPPER.writeValueAsString(configJson);
      
      // Write the config file
      try (OutputStream outputStream = HoodieCLI.storage.create(configPath, true)) {
        outputStream.write(jsonContent.getBytes());
      }
      
      String message = String.format("Lock audit disabled successfully.\nAudit config updated at: %s", auditConfigPath);
      
      if (keepAuditFiles) {
        message += String.format("\nExisting audit files preserved at: %s", StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath));
      } else {
        // Call the cleanup method to prune all old files
        CleanupResult cleanupResult = performAuditCleanup(false, 0);
        if (cleanupResult.success && cleanupResult.deletedCount > 0) {
          message += String.format("\nAudit files cleaned up at: %s", StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath));
        } else if (cleanupResult.success && cleanupResult.deletedCount == 0) {
          message += String.format("\nNo audit files to clean up at: %s", StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath));
        } else {
          message += String.format("\nWarning: Some audit files may not have been cleaned up: %s", cleanupResult.message);
        }
      }
      
      return message;
      
    } catch (Exception e) {
      LOG.error("Error disabling lock audit", e);
      return String.format("Failed to disable lock audit: %s", e.getMessage());
    }
  }

  /**
   * Shows the current status of lock audit logging for the connected table.
   * This command checks the audit configuration file and reports whether
   * auditing is currently enabled or disabled.
   * 
   * @return Status information about the current audit configuration
   */
  @ShellMethod(key = "locks audit status", value = "Show the current status of lock audit service")
  public String showLockAuditStatus() {
    
    if (HoodieCLI.basePath == null) {
      return "No Hudi table loaded. Please connect to a table first.";
    }

    try {
      // Create the audit config file path
      String auditConfigPath = StorageLockProviderAuditService.getAuditConfigPath(HoodieCLI.basePath);
      
      // Check if config file exists
      StoragePath configPath = new StoragePath(auditConfigPath);
      if (!HoodieCLI.storage.exists(configPath)) {
        return String.format("Lock Audit Status: DISABLED\n"
            + "Table: %s\n"
            + "Config file: %s (not found)\n"
            + "Use 'locks audit enable' to enable audit logging.", 
            HoodieCLI.basePath, auditConfigPath);
      }
      
      // Read and parse the configuration
      String configContent;
      try (InputStream inputStream = HoodieCLI.storage.open(configPath)) {
        configContent = new String(FileIOUtils.readAsByteArray(inputStream));
      }
      JsonNode rootNode = OBJECT_MAPPER.readTree(configContent);
      JsonNode enabledNode = rootNode.get(StorageLockProviderAuditService.STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD);
      boolean isEnabled = enabledNode != null && enabledNode.asBoolean(false);
      
      String status = isEnabled ? "ENABLED" : "DISABLED";
      
      return String.format("Lock Audit Status: %s\n"
          + "Table: %s\n"
          + "Config file: %s\n"
          + "Audit files location: %s",
          status, HoodieCLI.basePath, auditConfigPath, StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath));
      
    } catch (Exception e) {
      LOG.error("Error checking lock audit status", e);
      return String.format("Failed to check lock audit status: %s", e.getMessage());
    }
  }

  /**
   * Validates the audit lock files for consistency and integrity.
   * This command checks for issues such as corrupted files, invalid format,
   * incorrect state transitions, and orphaned locks.
   * 
   * @return Validation results including any issues found
   */
  @ShellMethod(key = "locks audit validate", value = "Validate audit lock files for consistency and integrity")
  public String validateAuditLocks() {
    
    if (HoodieCLI.basePath == null) {
      return "No Hudi table loaded. Please connect to a table first.";
    }

    try {
      String auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath);
      StoragePath auditFolder = new StoragePath(auditFolderPath);
      
      // Check if audit folder exists
      if (!HoodieCLI.storage.exists(auditFolder)) {
        return "Validation Result: PASSED\n"
            + "Transactions Validated: 0\n"
            + "Issues Found: 0\n"
            + "Details: No audit folder found - nothing to validate";
      }
      
      // Get all audit files
      List<StoragePathInfo> allFiles = HoodieCLI.storage.listDirectEntries(auditFolder);
      List<StoragePathInfo> auditFiles = new ArrayList<>();
      for (StoragePathInfo pathInfo : allFiles) {
        if (pathInfo.isFile() && pathInfo.getPath().getName().endsWith(".jsonl")) {
          auditFiles.add(pathInfo);
        }
      }
      
      if (auditFiles.isEmpty()) {
        return "Validation Result: PASSED\n"
            + "Transactions Validated: 0\n"
            + "Issues Found: 0\n"
            + "Details: No audit files found - nothing to validate";
      }
      
      // Parse all audit files into transaction windows
      List<TransactionWindow> windows = new ArrayList<>();
      for (StoragePathInfo pathInfo : auditFiles) {
        Option<TransactionWindow> window = parseAuditFile(pathInfo);
        if (window.isPresent()) {
          windows.add(window.get());
        }
      }
      
      if (windows.isEmpty()) {
        return String.format("Validation Result: FAILED\n"
            + "Transactions Validated: 0\n"
            + "Issues Found: %d\n"
            + "Details: Failed to parse any audit files", auditFiles.size());
      }
      
      // Validate transactions
      ValidationResults validationResults = validateTransactionWindows(windows);
      
      // Generate result
      int totalIssues = validationResults.errors.size() + validationResults.warnings.size();
      String result;
      String details;
      
      if (totalIssues == 0) {
        result = "PASSED";
        details = "All audit lock transactions validated successfully";
      } else {
        result = validationResults.errors.isEmpty() ? "WARNING" : "FAILED";
        List<String> allIssues = new ArrayList<>();
        allIssues.addAll(validationResults.errors);
        allIssues.addAll(validationResults.warnings);
        details = String.join(", ", allIssues);
      }
      
      return String.format("Validation Result: %s\n"
          + "Transactions Validated: %d\n"
          + "Issues Found: %d\n"
          + "Details: %s", result, windows.size(), totalIssues, details);
      
    } catch (Exception e) {
      LOG.error("Error validating audit locks", e);
      return String.format("Validation Result: ERROR\n"
          + "Transactions Validated: 0\n"
          + "Issues Found: -1\n"
          + "Details: Validation failed: %s", e.getMessage());
    }
  }

  /**
   * Cleans up old audit lock files based on age threshold.
   * This command removes audit files that are older than the specified number of days.
   * 
   * @param dryRun Whether to perform a dry run (preview changes without deletion)
   * @param ageDays Number of days to keep audit files (default 7)
   * @return Status message indicating files cleaned or to be cleaned
   */
  @ShellMethod(key = "locks audit cleanup", value = "Clean up old audit lock files")
  public String cleanupAuditLocks(
      @ShellOption(value = {"--dryRun"}, defaultValue = "false",
          help = "Preview changes without actually deleting files") final boolean dryRun,
      @ShellOption(value = {"--ageDays"}, defaultValue = "7",
          help = "Delete audit files older than this many days") final int ageDays) {
    
    if (HoodieCLI.basePath == null) {
      return "No Hudi table loaded. Please connect to a table first.";
    }

    if (ageDays <= 0) {
      return "Error: ageDays must be a positive integer.";
    }

    return performAuditCleanup(dryRun, ageDays).toString();
  }

  /**
   * Internal method to perform audit cleanup. Used by both the CLI command and disable method.
   *
   * @param dryRun Whether to perform a dry run (preview changes without deletion)
   * @param ageDays Number of days to keep audit files (0 means delete all)
   * @return CleanupResult containing cleanup operation details
   */
  private CleanupResult performAuditCleanup(boolean dryRun, int ageDays) {
    try {
      String auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath);
      StoragePath auditFolder = new StoragePath(auditFolderPath);
      
      // Check if audit folder exists
      if (!HoodieCLI.storage.exists(auditFolder)) {
        String message = "No audit folder found - nothing to cleanup.";
        return new CleanupResult(true, 0, 0, 0, message, dryRun);
      }
      
      // Calculate cutoff timestamp (ageDays ago)
      long cutoffTime = System.currentTimeMillis() - (ageDays * 24L * 60L * 60L * 1000L);
      
      // List all files in audit folder and filter by modification time
      List<StoragePathInfo> allFiles = HoodieCLI.storage.listDirectEntries(auditFolder);
      List<StoragePathInfo> auditFiles = new ArrayList<>();
      List<StoragePathInfo> oldFiles = new ArrayList<>();
      
      // Filter to get only .jsonl files
      for (org.apache.hudi.storage.StoragePathInfo pathInfo : allFiles) {
        if (pathInfo.isFile() && pathInfo.getPath().getName().endsWith(".jsonl")) {
          auditFiles.add(pathInfo);
          if (pathInfo.getModificationTime() < cutoffTime) {
            oldFiles.add(pathInfo);
          }
        }
      }
      
      if (oldFiles.isEmpty()) {
        String message = String.format("No audit files older than %d days found.", ageDays);
        return new CleanupResult(true, 0, 0, auditFiles.size(), message, dryRun);
      }
      
      int fileCount = oldFiles.size();
      
      if (dryRun) {
        String message = String.format("Dry run: Would delete %d audit files older than %d days.", fileCount, ageDays);
        return new CleanupResult(true, 0, 0, fileCount, message, dryRun);
      } else {
        // Actually delete the files
        int deletedCount = 0;
        int failedCount = 0;
        
        for (org.apache.hudi.storage.StoragePathInfo pathInfo : oldFiles) {
          try {
            HoodieCLI.storage.deleteFile(pathInfo.getPath());
            deletedCount++;
          } catch (Exception e) {
            failedCount++;
            LOG.warn("Failed to delete audit file: " + pathInfo.getPath(), e);
          }
        }
        
        if (failedCount == 0) {
          String message = String.format("Successfully deleted %d audit files older than %d days.", deletedCount, ageDays);
          return new CleanupResult(true, deletedCount, failedCount, fileCount, message, dryRun);
        } else {
          String message = String.format("Deleted %d audit files, failed to delete %d files.", deletedCount, failedCount);
          return new CleanupResult(false, deletedCount, failedCount, fileCount, message, dryRun);
        }
      }
      
    } catch (Exception e) {
      LOG.error("Error cleaning up audit locks", e);
      String message = String.format("Failed to cleanup audit locks: %s", e.getMessage());
      return new CleanupResult(false, 0, 0, 0, message, dryRun);
    }
  }

  /**
   * Parses an audit file and extracts transaction window information.
   */
  private Option<TransactionWindow> parseAuditFile(StoragePathInfo pathInfo) {
    String filename = pathInfo.getPath().getName();
    
    try {
      // Read file content using Hudi storage API
      String content;
      try (InputStream inputStream = HoodieCLI.storage.open(pathInfo.getPath());
           BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line).append("\n");
        }
        content = sb.toString();
      }
      
      // Parse JSONL content
      String[] lines = content.split("\n");
      List<JsonNode> jsonObjects = new ArrayList<>();
      for (String line : lines) {
        if (line.trim().isEmpty()) {
          continue;
        }
        try {
          jsonObjects.add(OBJECT_MAPPER.readTree(line));
        } catch (Exception e) {
          LOG.warn("Failed to parse JSON line in file " + filename + ": " + line, e);
        }
      }
      
      if (jsonObjects.isEmpty()) {
        return Option.empty();
      }
      
      // Extract transaction metadata
      JsonNode firstObject = jsonObjects.get(0);
      String ownerId = firstObject.has("ownerId") ? firstObject.get("ownerId").asText() : "unknown";
      long transactionStartTime = firstObject.has("transactionStartTime") 
          ? firstObject.get("transactionStartTime").asLong() : 0L;
      
      // Find first START timestamp
      long startTimestamp = transactionStartTime; // default to transaction start time
      for (JsonNode obj : jsonObjects) {
        if (obj.has("state") && "START".equals(obj.get("state").asText())) {
          startTimestamp = obj.has("timestamp") ? obj.get("timestamp").asLong() : transactionStartTime;
          break;
        }
      }
      
      // Find last END timestamp
      Option<Long> endTimestamp = Option.empty();
      for (int i = jsonObjects.size() - 1; i >= 0; i--) {
        JsonNode obj = jsonObjects.get(i);
        if (obj.has("state") && "END".equals(obj.get("state").asText())) {
          endTimestamp = Option.of(obj.has("timestamp") ? obj.get("timestamp").asLong() : 0L);
          break;
        }
      }
      
      // Find last expiration time as fallback
      Option<Long> lastExpirationTime = Option.empty();
      if (!jsonObjects.isEmpty()) {
        JsonNode lastObject = jsonObjects.get(jsonObjects.size() - 1);
        if (lastObject.has("lockExpiration")) {
          lastExpirationTime = Option.of(lastObject.get("lockExpiration").asLong());
        }
      }
      
      return Option.of(new TransactionWindow(
          ownerId,
          transactionStartTime,
          startTimestamp,
          endTimestamp,
          lastExpirationTime,
          filename
      ));
    } catch (Exception e) {
      LOG.warn("Failed to parse audit file: " + filename, e);
      return Option.empty();
    }
  }

  /**
   * Validates transaction windows for overlaps and proper closure.
   */
  private ValidationResults validateTransactionWindows(List<TransactionWindow> windows) {
    List<String> errors = new ArrayList<>();
    List<String> warnings = new ArrayList<>();
    
    // Check for transactions without proper END
    for (TransactionWindow window : windows) {
      if (!window.endTimestamp.isPresent()) {
        warnings.add(String.format("[WARNING] %s => transaction did not end gracefully. This could be due to driver OOM or non-graceful shutdown.", 
            window.filename));
      }
    }
    
    // Sort windows by start time for overlap detection
    List<TransactionWindow> sortedWindows = new ArrayList<>(windows);
    sortedWindows.sort(Comparator.comparingLong(w -> w.startTimestamp));
    
    // Check for overlaps
    for (int i = 0; i < sortedWindows.size(); i++) {
      TransactionWindow currentWindow = sortedWindows.get(i);
      long currentEnd = currentWindow.getEffectiveEndTime();
      
      // Check all subsequent windows for overlaps
      for (int j = i + 1; j < sortedWindows.size(); j++) {
        TransactionWindow otherWindow = sortedWindows.get(j);
        long otherStart = otherWindow.startTimestamp;
        
        // Check if windows overlap and current transaction didn't end gracefully
        if (otherStart < currentEnd && !currentWindow.endTimestamp.isPresent()) {
          errors.add(String.format("[ERROR] %s => overlaps with %s",
              currentWindow.filename, otherWindow.filename));
        }
      }
    }
    
    return new ValidationResults(errors, warnings);
  }
}