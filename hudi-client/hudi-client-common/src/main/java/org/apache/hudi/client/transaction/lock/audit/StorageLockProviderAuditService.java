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

package org.apache.hudi.client.transaction.lock.audit;

import org.apache.hudi.client.transaction.lock.StorageLockClient;
import org.apache.hudi.storage.StoragePath;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Storage-based audit service implementation for lock provider operations.
 * Writes audit records to a single JSONL file per transaction to track lock lifecycle events.
 */
public class StorageLockProviderAuditService implements AuditService {
  
  private static final Logger LOG = LoggerFactory.getLogger(StorageLockProviderAuditService.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  
  // Audit configuration constants
  public static final String AUDIT_FOLDER_NAME = "audit";
  public static final String AUDIT_CONFIG_FILE_NAME = "audit_enabled.json";
  public static final String STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD = "STORAGE_LOCK_AUDIT_SERVICE_ENABLED";
  
  /**
   * Constructs the full path to the audit configuration file for a given table.
   *
   * @param basePath The base path of the Hudi table
   * @return The full path to the audit_enabled.json configuration file
   */
  public static String getAuditConfigPath(String basePath) {
    String lockFolderPath = StorageLockClient.getLockFolderPath(basePath);
    return String.format("%s%s%s", lockFolderPath, StoragePath.SEPARATOR, AUDIT_CONFIG_FILE_NAME);
  }
  
  /**
   * Constructs the full path to the audit folder for a given table.
   *
   * @param basePath The base path of the Hudi table
   * @return The full path to the audit folder where audit files are stored
   */
  public static String getAuditFolderPath(String basePath) {
    String lockFolderPath = StorageLockClient.getLockFolderPath(basePath);
    return String.format("%s%s%s", lockFolderPath, StoragePath.SEPARATOR, AUDIT_FOLDER_NAME);
  }
  
  private final String ownerId;
  private final long transactionStartTime;
  private final String auditFilePath;
  private final StorageLockClient storageLockClient;
  private final Function<Long, Long> lockExpirationFunction;
  private final Supplier<Boolean> lockHeldSupplier;
  private final StringBuilder auditBuffer;
  
  /**
   * Creates a new StorageLockProviderAuditService instance.
   * 
   * @param basePath The base path where audit files will be written
   * @param ownerId The full owner ID for the lock
   * @param transactionStartTime The timestamp when the transaction started (lock acquired)
   * @param storageLockClient The storage client for writing audit files
   * @param lockExpirationFunction Function that takes a timestamp and returns the lock expiration time
   * @param lockHeldSupplier Supplier that provides whether the lock is currently held
   */
  public StorageLockProviderAuditService(
      String basePath,
      String ownerId,
      long transactionStartTime,
      StorageLockClient storageLockClient,
      Function<Long, Long> lockExpirationFunction,
      Supplier<Boolean> lockHeldSupplier) {
    this.ownerId = ownerId;
    this.transactionStartTime = transactionStartTime;
    this.storageLockClient = storageLockClient;
    this.lockExpirationFunction = lockExpirationFunction;
    this.lockHeldSupplier = lockHeldSupplier;
    this.auditBuffer = new StringBuilder();
    
    // Generate audit file path: <txn-start>_<full-owner-id>.jsonl
    String filename = String.format("%d_%s.jsonl", transactionStartTime, ownerId);
    this.auditFilePath = String.format("%s%s%s",
        getAuditFolderPath(basePath),
        StoragePath.SEPARATOR,
        filename);
    
    LOG.debug("Initialized audit service for transaction starting at {} with file: {}", 
        transactionStartTime, auditFilePath);
  }
  
  /**
   * Records an audit operation with the current timestamp and state.
   * 
   * @param state The audit operation state to record
   * @param timestamp The timestamp when the operation occurred
   * @throws Exception if there's an error creating or writing the audit record
   */
  @Override
  public synchronized void recordOperation(AuditOperationState state, long timestamp) throws Exception {
    // Create audit record
    Map<String, Object> auditRecord = new HashMap<>();
    auditRecord.put("ownerId", ownerId);
    auditRecord.put("transactionStartTime", transactionStartTime);
    auditRecord.put("timestamp", timestamp);
    auditRecord.put("state", state.name());
    auditRecord.put("lockExpiration", lockExpirationFunction.apply(timestamp));
    auditRecord.put("lockHeld", lockHeldSupplier.get());
    
    // Convert to JSON and append newline for JSONL format
    String jsonLine = OBJECT_MAPPER.writeValueAsString(auditRecord) + "\n";
    
    // Append to buffer
    auditBuffer.append(jsonLine);
    
    // Write the accumulated audit records to file
    writeAuditFile();
    
    LOG.debug("Recorded audit operation: state={}, timestamp={}, file={}", 
        state, timestamp, auditFilePath);
  }
  
  /**
   * Writes the accumulated audit records to the audit file.
   * This method is called after each recordOperation to persist the audit log.
   * Failures to write audit records are logged but do not throw exceptions
   * to avoid breaking lock operations.
   */
  private void writeAuditFile() {
    try {
      // Write the entire buffer content to the audit file
      // This overwrites the file with all accumulated records
      String content = auditBuffer.toString();
      boolean success = storageLockClient.writeObject(auditFilePath, content);
      if (success) {
        LOG.debug("Successfully wrote audit records to: {}", auditFilePath);
      } else {
        LOG.error("Failed to write audit records to: {}", auditFilePath);
      }
    } catch (Exception e) {
      LOG.error("Failed to write audit records to: {}", auditFilePath, e);
      // Don't throw exception - audit failures should not break lock operations
    }
  }
  
  /**
   * Closes the audit service. Since all audit records are written after each operation,
   * no additional cleanup is required during close.
   * 
   * @throws Exception if there's an error during cleanup (not expected in current implementation)
   */
  @Override
  public synchronized void close() throws Exception {
    // All audit records are already written after each recordOperation()
    // No additional writes needed during close
    LOG.debug("Closed StorageLockProviderAuditService for transaction: {}, owner: {}", 
        transactionStartTime, ownerId);
  }
}
