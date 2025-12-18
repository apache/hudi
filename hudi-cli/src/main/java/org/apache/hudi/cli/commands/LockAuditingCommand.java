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
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
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
@Slf4j
public class LockAuditingCommand {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Represents a single audit log entry in the JSONL file.
   * Maps to the structure written by StorageLockProviderAuditService.
   */
  @Getter
  public static class AuditRecord {
    private final String ownerId;
    private final long transactionStartTime;
    private final long timestamp;
    private final String state;
    private final long lockExpiration;
    private final boolean lockHeld;

    @JsonCreator
    public AuditRecord(
        @JsonProperty("ownerId") String ownerId,
        @JsonProperty("transactionStartTime") long transactionStartTime,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("state") String state,
        @JsonProperty("lockExpiration") long lockExpiration,
        @JsonProperty("lockHeld") boolean lockHeld) {
      this.ownerId = ownerId;
      this.transactionStartTime = transactionStartTime;
      this.timestamp = timestamp;
      this.state = state;
      this.lockExpiration = lockExpiration;
      this.lockHeld = lockHeld;
    }
  }

  /**
   * Represents a transaction window with start time, end time, and metadata.
   */
  static class TransactionWindow {
    final String ownerId;
    final long transactionCreationTime;
    final long lockAcquisitionTime;
    final Option<Long> lockReleaseTime;
    final Option<Long> lockExpirationTime;
    final String filename;

    TransactionWindow(String ownerId, long transactionCreationTime, long lockAcquisitionTime,
                      Option<Long> lockReleaseTime, Option<Long> lockExpirationTime, String filename) {
      this.ownerId = ownerId;
      this.transactionCreationTime = transactionCreationTime;
      this.lockAcquisitionTime = lockAcquisitionTime;
      this.lockReleaseTime = lockReleaseTime;
      this.lockExpirationTime = lockExpirationTime;
      this.filename = filename;
    }

    long getEffectiveEndTime() {
      return lockReleaseTime.orElse(lockExpirationTime.orElse(lockAcquisitionTime));
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
      log.error("Error enabling lock audit", e);
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
      StoragePath configPath = new StoragePath(auditConfigPath);

      // Check if config file exists by attempting to get its info
      try {
        HoodieCLI.storage.getPathInfo(configPath);
      } catch (FileNotFoundException e) {
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
      log.error("Error disabling lock audit", e);
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
      StoragePath configPath = new StoragePath(auditConfigPath);

      // Read and parse the configuration
      String configContent;
      try (InputStream inputStream = HoodieCLI.storage.open(configPath)) {
        configContent = new String(FileIOUtils.readAsByteArray(inputStream));
      } catch (FileNotFoundException e) {
        return String.format("Lock Audit Status: DISABLED\n"
            + "Table: %s\n"
            + "Config file: %s (not found)\n"
            + "Use 'locks audit enable' to enable audit logging.",
            HoodieCLI.basePath, auditConfigPath);
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
      log.error("Error checking lock audit status", e);
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

      // Get all audit files
      List<StoragePathInfo> allFiles;
      try {
        allFiles = HoodieCLI.storage.listDirectEntries(auditFolder);
      } catch (FileNotFoundException e) {
        return "Validation Result: PASSED\n"
            + "Transactions Validated: 0\n"
            + "Issues Found: 0\n"
            + "Details: No audit folder found - nothing to validate";
      }
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
      List<String> parseErrors = new ArrayList<>();
      for (StoragePathInfo pathInfo : auditFiles) {
        Option<TransactionWindow> window = parseAuditFile(pathInfo);
        if (window.isPresent()) {
          windows.add(window.get());
        } else {
          parseErrors.add(String.format("[ERROR] Failed to parse audit file: %s", pathInfo.getPath().getName()));
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

      // Add parse errors to the validation results
      validationResults.errors.addAll(parseErrors);
      
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
          + "Audit Files: %d total, %d parsed successfully, %d failed to parse\n"
          + "Transactions Validated: %d\n"
          + "Issues Found: %d\n"
          + "Details: %s", result, auditFiles.size(), windows.size(), parseErrors.size(), windows.size(), totalIssues, details);
      
    } catch (Exception e) {
      log.error("Error validating audit locks", e);
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
          help = "Delete audit files older than this many days") final String ageDaysStr) {

    try {
      if (HoodieCLI.basePath == null) {
        return "No Hudi table loaded. Please connect to a table first.";
      }

      // Parse ageDays manually to handle validation properly
      int ageDays;
      try {
        ageDays = Integer.parseInt(ageDaysStr);
      } catch (NumberFormatException e) {
        return "Error: ageDays must be a value greater than 0.";
      }

      if (ageDays < 0) {
        return "Error: ageDays must be non-negative (>= 0).";
      }

      return performAuditCleanup(dryRun, ageDays).toString();
    } catch (Exception e) {
      log.error("Error during audit cleanup", e);
      return String.format("Error during cleanup: %s", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
    }
  }

  /**
   * Internal method to perform audit cleanup. Used by both the CLI command and disable method.
   *
   * @param dryRun Whether to perform a dry run (preview changes without deletion)
   * @param ageDays Number of days to keep audit files (0 means delete all, must be non-negative)
   * @return CleanupResult containing cleanup operation details
   */
  private CleanupResult performAuditCleanup(boolean dryRun, int ageDays) {
    try {
      if (HoodieCLI.storage == null) {
        String message = "Storage not initialized.";
        return new CleanupResult(false, 0, 0, 0, message, dryRun);
      }

      String auditFolderPath = StorageLockProviderAuditService.getAuditFolderPath(HoodieCLI.basePath);
      StoragePath auditFolder = new StoragePath(auditFolderPath);

      // Calculate cutoff timestamp (ageDays ago)
      long cutoffTime = System.currentTimeMillis() - (ageDays * 24L * 60L * 60L * 1000L);

      // List all files in audit folder and filter by modification time
      List<StoragePathInfo> allFiles;
      try {
        allFiles = HoodieCLI.storage.listDirectEntries(auditFolder);
      } catch (FileNotFoundException e) {
        String message = "No audit folder found - nothing to cleanup.";
        return new CleanupResult(true, 0, 0, 0, message, dryRun);
      }
      List<StoragePathInfo> auditFiles = new ArrayList<>();
      List<StoragePathInfo> oldFiles = new ArrayList<>();
      
      // Filter to get only .jsonl files
      for (StoragePathInfo pathInfo : allFiles) {
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

        for (StoragePathInfo pathInfo : oldFiles) {
          try {
            HoodieCLI.storage.deleteFile(pathInfo.getPath());
            deletedCount++;
          } catch (Exception e) {
            failedCount++;
            log.warn("Failed to delete audit file: " + pathInfo.getPath(), e);
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
      log.error("Error cleaning up audit locks", e);
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
      // Read and parse JSONL content
      List<AuditRecord> entries = new ArrayList<>();
      try (InputStream inputStream = HoodieCLI.storage.open(pathInfo.getPath());
           BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.trim().isEmpty()) {
            continue;
          }
          try {
            AuditRecord entry = OBJECT_MAPPER.readValue(line, AuditRecord.class);
            entries.add(entry);
          } catch (Exception e) {
            log.warn("Failed to parse JSON line in file " + filename + ": " + line, e);
          }
        }
      }

      if (entries.isEmpty()) {
        return Option.empty();
      }

      // Extract transaction metadata from first entry
      AuditRecord firstEntry = entries.get(0);
      String ownerId = firstEntry.getOwnerId();
      long transactionCreationTime = firstEntry.getTransactionStartTime();

      // Find first START timestamp
      long lockAcquisitionTime = transactionCreationTime; // default to transaction creation time
      for (AuditRecord entry : entries) {
        if ("START".equals(entry.getState())) {
          lockAcquisitionTime = entry.getTimestamp();
          break;
        }
      }

      // Find last END timestamp
      Option<Long> lockReleaseTime = Option.empty();
      for (int i = entries.size() - 1; i >= 0; i--) {
        AuditRecord entry = entries.get(i);
        if ("END".equals(entry.getState())) {
          lockReleaseTime = Option.of(entry.getTimestamp());
          break;
        }
      }

      // Find last expiration time as fallback
      Option<Long> lockExpirationTime = Option.empty();
      if (!entries.isEmpty()) {
        AuditRecord lastEntry = entries.get(entries.size() - 1);
        lockExpirationTime = Option.of(lastEntry.getLockExpiration());
      }

      return Option.of(new TransactionWindow(
          ownerId,
          transactionCreationTime,
          lockAcquisitionTime,
          lockReleaseTime,
          lockExpirationTime,
          filename
      ));
    } catch (Exception e) {
      log.warn("Failed to parse audit file: " + filename, e);
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
      if (!window.lockReleaseTime.isPresent()) {
        warnings.add(String.format("[WARNING] %s => transaction did not end gracefully. This could be due to driver OOM or non-graceful shutdown.",
            window.filename));
      }
    }

    // Sort windows by start time for overlap detection
    List<TransactionWindow> sortedWindows = new ArrayList<>(windows);
    sortedWindows.sort(Comparator.comparingLong(w -> w.lockAcquisitionTime));

    // Check for overlaps
    for (int i = 0; i < sortedWindows.size(); i++) {
      TransactionWindow currentWindow = sortedWindows.get(i);
      long currentEnd = currentWindow.getEffectiveEndTime();

      // Check all subsequent windows for overlaps
      for (int j = i + 1; j < sortedWindows.size(); j++) {
        TransactionWindow otherWindow = sortedWindows.get(j);
        long otherStart = otherWindow.lockAcquisitionTime;

        // Check if windows overlap
        if (otherStart < currentEnd) {
          errors.add(String.format("[ERROR] %s => overlaps with %s",
              currentWindow.filename, otherWindow.filename));
        }
      }
    }
    
    return new ValidationResults(errors, warnings);
  }
}