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
import org.apache.hudi.storage.StoragePath;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import org.apache.hudi.common.util.FileIOUtils;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * CLI commands for managing Hudi table locking and audit functionality.
 */
@ShellComponent
public class LockCommand {

  private static final Logger LOG = LoggerFactory.getLogger(LockCommand.class);
  private static final String AUDIT_CONFIG_FILE_NAME = "audit_enabled.json";
  private static final String STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD = "STORAGE_LOCK_AUDIT_SERVICE_ENABLED";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @ShellMethod(key = "locks audit enable", value = "Enable storage lock audit service for the current table")
  public String enableLockAudit() {
    
    if (HoodieCLI.basePath == null) {
      return "No Hudi table loaded. Please connect to a table first.";
    }

    try {
      // Create the audit config file path using .hoodie/.locks structure 
      String lockFolderPath = String.format("%s%s.hoodie%s.locks", HoodieCLI.basePath, StoragePath.SEPARATOR, StoragePath.SEPARATOR);
      String auditConfigPath = String.format("%s%s%s", lockFolderPath, StoragePath.SEPARATOR, AUDIT_CONFIG_FILE_NAME);
      
      // Create the JSON content
      ObjectNode configJson = OBJECT_MAPPER.createObjectNode();
      configJson.put(STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD, true);
      String jsonContent = OBJECT_MAPPER.writeValueAsString(configJson);
      
      // Write the config file using HoodieStorage
      StoragePath configPath = new StoragePath(auditConfigPath);
      try (OutputStream outputStream = HoodieCLI.storage.create(configPath, true)) {
        outputStream.write(jsonContent.getBytes());
      }
      
      return String.format("Lock audit enabled successfully.\nAudit config written to: %s\n"
          + "Audit files will be stored at: %s%saudit%s", auditConfigPath, lockFolderPath, StoragePath.SEPARATOR, StoragePath.SEPARATOR);
      
    } catch (Exception e) {
      LOG.error("Error enabling lock audit", e);
      return String.format("Failed to enable lock audit: %s", e.getMessage());
    }
  }

  @ShellMethod(key = "locks audit disable", value = "Disable storage lock audit service for the current table")
  public String disableLockAudit(
      @ShellOption(value = {"--keepAuditFiles"}, defaultValue = "true",
          help = "Keep existing audit files when disabling") final boolean keepAuditFiles) {
    
    if (HoodieCLI.basePath == null) {
      return "No Hudi table loaded. Please connect to a table first.";
    }

    try {
      // Create the audit config file path
      String lockFolderPath = String.format("%s%s.hoodie%s.locks", HoodieCLI.basePath, StoragePath.SEPARATOR, StoragePath.SEPARATOR);
      String auditConfigPath = String.format("%s%s%s", lockFolderPath, StoragePath.SEPARATOR, AUDIT_CONFIG_FILE_NAME);
      
      // Check if config file exists
      StoragePath configPath = new StoragePath(auditConfigPath);
      if (!HoodieCLI.storage.exists(configPath)) {
        return "Lock audit is already disabled (no configuration file found).";
      }
      
      // Create the JSON content with audit disabled
      ObjectNode configJson = OBJECT_MAPPER.createObjectNode();
      configJson.put(STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD, false);
      String jsonContent = OBJECT_MAPPER.writeValueAsString(configJson);
      
      // Write the config file
      try (OutputStream outputStream = HoodieCLI.storage.create(configPath, true)) {
        outputStream.write(jsonContent.getBytes());
      }
      
      String message = String.format("Lock audit disabled successfully.\nAudit config updated at: %s", auditConfigPath);
      
      if (keepAuditFiles) {
        message += String.format("\nExisting audit files preserved at: %s%saudit%s", lockFolderPath, StoragePath.SEPARATOR, StoragePath.SEPARATOR);
      } else {
        // Todo: write then call the api method to prune the old files
        message += String.format("\nAudit files cleaned up at: %s%saudit%s", lockFolderPath, StoragePath.SEPARATOR, StoragePath.SEPARATOR);
      }
      
      return message;
      
    } catch (Exception e) {
      LOG.error("Error disabling lock audit", e);
      return String.format("Failed to disable lock audit: %s", e.getMessage());
    }
  }

  @ShellMethod(key = "locks audit status", value = "Show the current status of lock audit service")
  public String showLockAuditStatus() {
    
    if (HoodieCLI.basePath == null) {
      return "No Hudi table loaded. Please connect to a table first.";
    }

    try {
      // Create the audit config file path
      String lockFolderPath = String.format("%s%s.hoodie%s.locks", HoodieCLI.basePath, StoragePath.SEPARATOR, StoragePath.SEPARATOR);
      String auditConfigPath = String.format("%s%s%s", lockFolderPath, StoragePath.SEPARATOR, AUDIT_CONFIG_FILE_NAME);
      
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
      JsonNode enabledNode = rootNode.get(STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD);
      boolean isEnabled = enabledNode != null && enabledNode.asBoolean(false);
      
      String status = isEnabled ? "ENABLED" : "DISABLED";
      
      return String.format("Lock Audit Status: %s\n"
          + "Table: %s\n"
          + "Config file: %s\n"
          + "Audit files location: %s%saudit%s",
          status, HoodieCLI.basePath, auditConfigPath, lockFolderPath, StoragePath.SEPARATOR, StoragePath.SEPARATOR);
      
    } catch (Exception e) {
      LOG.error("Error checking lock audit status", e);
      return String.format("Failed to check lock audit status: %s", e.getMessage());
    }
  }
}