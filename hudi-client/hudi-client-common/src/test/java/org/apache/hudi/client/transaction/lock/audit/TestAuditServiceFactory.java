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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class for AuditServiceFactory
 */
public class TestAuditServiceFactory {
  
  private StorageLockClient mockStorageLockClient;
  private final String ownerId = "test-owner";
  private final String basePath = "s3://bucket/path/to/table";
  
  @BeforeEach
  void setUp() {
    mockStorageLockClient = mock(StorageLockClient.class);
  }
  
  @Test
  void testCreateAuditServiceWhenConfigNotFound() {
    // Config file doesn't exist
    String expectedPath = basePath + "/.hoodie/.locks/audit_enabled.json";
    when(mockStorageLockClient.readObject(eq(expectedPath), eq(true)))
        .thenReturn(Option.empty());
    
    Option<AuditService> result = AuditServiceFactory.createLockProviderAuditService(
        ownerId, basePath, mockStorageLockClient);
    
    // Should return empty when config not found
    assertTrue(result.isEmpty());
    verify(mockStorageLockClient).readObject(expectedPath, true);
  }
  
  @Test
  void testCreateAuditServiceWhenDisabledInConfig() {
    // Config file exists but audit is disabled
    String expectedPath = basePath + "/.hoodie/.locks/audit_enabled.json";
    String configJson = "{\"STORAGE_LOCK_AUDIT_SERVICE_ENABLED\": false}";
    when(mockStorageLockClient.readObject(eq(expectedPath), eq(true)))
        .thenReturn(Option.of(configJson));
    
    Option<AuditService> result = AuditServiceFactory.createLockProviderAuditService(
        ownerId, basePath, mockStorageLockClient);
    
    // Should return empty when audit is disabled
    assertTrue(result.isEmpty());
    verify(mockStorageLockClient).readObject(expectedPath, true);
  }
  
  @Test
  void testCreateAuditServiceWhenEnabledInConfig() {
    // Config file exists and audit is enabled
    String expectedPath = basePath + "/.hoodie/.locks/audit_enabled.json";
    String configJson = "{\"STORAGE_LOCK_AUDIT_SERVICE_ENABLED\": true}";
    when(mockStorageLockClient.readObject(eq(expectedPath), eq(true)))
        .thenReturn(Option.of(configJson));
    
    Option<AuditService> result = AuditServiceFactory.createLockProviderAuditService(
        ownerId, basePath, mockStorageLockClient);
    
    // Should return empty for now (no concrete implementation yet)
    // When implementation is added, this should return a present Option
    assertTrue(result.isEmpty());
    verify(mockStorageLockClient).readObject(expectedPath, true);
  }
  
  @Test
  void testCreateAuditServiceWithMalformedJson() {
    // Config file exists but contains invalid JSON
    String expectedPath = basePath + "/.hoodie/.locks/audit_enabled.json";
    String malformedJson = "{invalid json}";
    when(mockStorageLockClient.readObject(eq(expectedPath), eq(true)))
        .thenReturn(Option.of(malformedJson));
    
    Option<AuditService> result = AuditServiceFactory.createLockProviderAuditService(
        ownerId, basePath, mockStorageLockClient);
    
    // Should return empty when JSON is malformed
    assertTrue(result.isEmpty());
    verify(mockStorageLockClient).readObject(expectedPath, true);
  }
  
  @Test
  void testCreateAuditServiceWithEmptyConfig() {
    // Config file exists but doesn't contain the expected field
    String expectedPath = basePath + "/.hoodie/.locks/audit_enabled.json";
    String configJson = "{\"some_other_field\": true}";
    when(mockStorageLockClient.readObject(eq(expectedPath), eq(true)))
        .thenReturn(Option.of(configJson));
    
    Option<AuditService> result = AuditServiceFactory.createLockProviderAuditService(
        ownerId, basePath, mockStorageLockClient);
    
    // Should return empty when field is missing (defaults to false)
    assertTrue(result.isEmpty());
    verify(mockStorageLockClient).readObject(expectedPath, true);
  }
  
  @Test
  void testCreateAuditServiceUsesCheckExistsFirst() {
    // Verify that the factory passes true for checkExistsFirst
    String expectedPath = basePath + "/.hoodie/.locks/audit_enabled.json";
    when(mockStorageLockClient.readObject(eq(expectedPath), eq(true)))
        .thenReturn(Option.empty());
    
    AuditServiceFactory.createLockProviderAuditService(
        ownerId, basePath, mockStorageLockClient);
    
    // Should pass true for checkExistsFirst since audit config is rarely present
    verify(mockStorageLockClient).readObject(expectedPath, true);
  }
  
}
