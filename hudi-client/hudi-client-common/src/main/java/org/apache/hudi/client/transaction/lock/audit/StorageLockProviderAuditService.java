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
import java.util.function.Supplier;

/**
 * Storage-based audit service implementation for lock provider operations.
 * Writes audit records as individual JSON files to track lock lifecycle events.
 */
public class StorageLockProviderAuditService implements AuditService {
  
  private static final Logger LOG = LoggerFactory.getLogger(StorageLockProviderAuditService.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  
  private final String basePath;
  private final String ownerId;
  private final String ownerShortUuid;
  private final StorageLockClient storageLockClient;
  private final Supplier<Long> lockExpirationSupplier;
  private final Supplier<Boolean> lockHeldSupplier;
  
  /**
   * Constructor for StorageLockProviderAuditService.
   * 
   * @param basePath The base path where audit files will be written
   * @param ownerId The full owner ID
   * @param storageLockClient The storage client for writing audit files
   * @param lockExpirationSupplier Supplier that provides the lock expiration time
   * @param lockHeldSupplier Supplier that provides whether the lock is currently held
   */
  public StorageLockProviderAuditService(
      String basePath,
      String ownerId,
      StorageLockClient storageLockClient,
      Supplier<Long> lockExpirationSupplier,
      Supplier<Boolean> lockHeldSupplier) {
    this.basePath = basePath;
    this.ownerId = ownerId;
    // Extract short UUID from owner ID (first 8 characters of UUID portion)
    this.ownerShortUuid = extractShortUuid(ownerId);
    this.storageLockClient = storageLockClient;
    this.lockExpirationSupplier = lockExpirationSupplier;
    this.lockHeldSupplier = lockHeldSupplier;
  }
  
  @Override
  public void recordOperation(AuditOperationState state, long timestamp) throws Exception {
    // Generate unique filename: <timestamp>-<owner-short-uuid>-<state>.json
    String filename = String.format("%d-%s-%s.json", 
        timestamp, 
        ownerShortUuid, 
        state.name().toLowerCase());
    
    // Build the full path for the audit file
    String auditFilePath = String.format("%s%s.hoodie%s.locks%saudit%s%s",
        basePath,
        StoragePath.SEPARATOR,
        StoragePath.SEPARATOR,
        StoragePath.SEPARATOR,
        StoragePath.SEPARATOR,
        filename);
    
    // Create audit record
    Map<String, Object> auditRecord = new HashMap<>();
    auditRecord.put("ownerId", ownerId);
    auditRecord.put("timestamp", timestamp);
    auditRecord.put("state", state.name());
    auditRecord.put("lockExpiration", lockExpirationSupplier.get());
    auditRecord.put("lockHeld", lockHeldSupplier.get());
    
    // Convert to JSON
    String jsonContent = OBJECT_MAPPER.writeValueAsString(auditRecord);
    
    // Write the audit file
    writeAuditFile(auditFilePath, jsonContent);
    
    LOG.debug("Recorded audit operation: state={}, timestamp={}, path={}", 
        state, timestamp, auditFilePath);
  }
  
  private void writeAuditFile(String filePath, String content) {
    try {
      // Use the storage client's writeObject method to write the audit file
      boolean success = storageLockClient.writeObject(filePath, content);
      if (success) {
        LOG.debug("Successfully wrote audit file to: {}", filePath);
      } else {
        LOG.warn("Failed to write audit file to: {}", filePath);
      }
    } catch (Exception e) {
      LOG.warn("Failed to write audit file to: {}", filePath, e);
      // Don't throw exception - audit failures should not break lock operations
    }
  }
  
  private String extractShortUuid(String fullOwnerId) {
    // Owner ID typically contains UUID, extract first 8 characters for short form
    if (fullOwnerId != null && fullOwnerId.length() >= 8) {
      // Otherwise, just use first 8 characters
      return fullOwnerId.substring(0, 8);
    }
    return fullOwnerId;
  }
  
  @Override
  public void close() throws Exception {
    // No resources to clean up
    LOG.debug("Closing StorageLockProviderAuditService for owner: {}", ownerId);
  }
}
