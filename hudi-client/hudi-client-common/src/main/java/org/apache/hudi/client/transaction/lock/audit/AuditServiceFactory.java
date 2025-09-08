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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Generic factory for creating audit services.
 * This factory determines whether auditing is enabled by checking configuration files.
 */
public class AuditServiceFactory {

  private static final Logger LOG = LoggerFactory.getLogger(AuditServiceFactory.class);
  private static final String AUDIT_CONFIG_FILE_NAME = "audit_enabled.json";
  private static final String STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD = "STORAGE_LOCK_AUDIT_SERVICE_ENABLED";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Creates a lock provider audit service instance by checking the audit configuration file.
   *
   * @param ownerId The owner ID for the lock provider
   * @param basePath The base path of the Hudi table
   * @param storageLockClient The storage lock client to use for reading configuration
   * @param transactionStartTime The timestamp when the transaction started (lock acquired)
   * @param lockExpirationFunction Function that takes a timestamp and returns the lock expiration time
   * @param lockHeldSupplier Supplier that provides whether the lock is currently held
   * @return An Option containing the audit service if enabled, Option.empty() otherwise
   */
  public static Option<AuditService> createLockProviderAuditService(
          String ownerId,
          String basePath,
          StorageLockClient storageLockClient,
          long transactionStartTime,
          java.util.function.Function<Long, Long> lockExpirationFunction,
          Supplier<Boolean> lockHeldSupplier) {

    if (!isAuditEnabled(basePath, storageLockClient)) {
      return Option.empty();
    }

    // Create and return the audit service
    AuditService auditService = new StorageLockProviderAuditService(
        basePath,
        ownerId, 
        transactionStartTime,
        storageLockClient,
        lockExpirationFunction,
        lockHeldSupplier);
    
    return Option.of(auditService);
  }

  /**
   * Checks if audit is enabled by reading the audit configuration file.
   *
   * @param basePath The base path of the Hudi table
   * @param storageLockClient The storage lock client to use for reading configuration
   * @return true if audit is enabled, false otherwise
   */
  private static boolean isAuditEnabled(String basePath, StorageLockClient storageLockClient) {
    try {
      // Construct the audit config path using the same lock folder as the lock file
      String lockFolderPath = StorageLockClient.getLockFolderPath(basePath);
      String auditConfigPath = String.format("%s%s%s", lockFolderPath, StoragePath.SEPARATOR, AUDIT_CONFIG_FILE_NAME);

      LOG.debug("Checking for audit configuration at: {}", auditConfigPath);

      // Use the readObject method to read the config file
      // Pass true for checkExistsFirst since audit config is rarely present (99% miss rate)
      Option<String> jsonContent = storageLockClient.readObject(auditConfigPath, true);

      if (jsonContent.isPresent()) {
        LOG.debug("Audit configuration file found, parsing content");
        JsonNode rootNode = OBJECT_MAPPER.readTree(jsonContent.get());
        JsonNode enabledNode = rootNode.get(STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD);

        boolean isEnabled = enabledNode != null && enabledNode.asBoolean(false);

        if (isEnabled) {
          LOG.info("Audit logging is ENABLED for lock operations at base path: {}", basePath);
        } else {
          LOG.info("Audit configuration present but audit logging is DISABLED");
        }

        return isEnabled;
      }

      LOG.debug("No audit configuration file found at: {}", auditConfigPath);
      return false;
    } catch (Exception e) {
      LOG.error("Error reading audit configuration from base path: {}. Audit will be disabled.", basePath, e);
      return false;
    }
  }
}
