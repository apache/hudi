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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for StorageLockProviderAuditService.
 */
public class TestStorageLockProviderAuditService {

  private static final String BASE_PATH = "s3://bucket/table";
  private static final String OWNER_ID = "writer-12345678-9abc-def0-1234-567890abcdef";
  private static final String SHORT_UUID = "writer-1";  // First 8 chars of OWNER_ID
  private static final long LOCK_EXPIRATION = 1000000L;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Mock
  private StorageLockClient storageLockClient;
  
  @Mock
  private Supplier<Long> lockExpirationSupplier;
  
  @Mock
  private Supplier<Boolean> lockHeldSupplier;

  private StorageLockProviderAuditService auditService;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    when(lockExpirationSupplier.get()).thenReturn(LOCK_EXPIRATION);
    when(lockHeldSupplier.get()).thenReturn(true);
    
    auditService = new StorageLockProviderAuditService(
        BASE_PATH,
        OWNER_ID,
        storageLockClient,
        lockExpirationSupplier,
        lockHeldSupplier);
  }

  @Test
  void testRecordOperationStartSuccess() throws Exception {
    long timestamp = System.currentTimeMillis();
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);

    auditService.recordOperation(AuditOperationState.START, timestamp);

    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient, times(1)).writeObject(pathCaptor.capture(), contentCaptor.capture());

    String expectedPath = String.format("%s%s.hoodie%s.locks%saudit%s%d-%s-start.json",
        BASE_PATH,
        StoragePath.SEPARATOR,
        StoragePath.SEPARATOR,
        StoragePath.SEPARATOR,
        StoragePath.SEPARATOR,
        timestamp,
        SHORT_UUID);
    assertEquals(expectedPath, pathCaptor.getValue());

    Map<String, Object> auditRecord = OBJECT_MAPPER.readValue(contentCaptor.getValue(), Map.class);
    assertEquals(OWNER_ID, auditRecord.get("ownerId"));
    assertEquals(timestamp, ((Number) auditRecord.get("timestamp")).longValue());
    assertEquals("START", auditRecord.get("state"));
    assertEquals(LOCK_EXPIRATION, ((Number) auditRecord.get("lockExpiration")).longValue());
    assertTrue((Boolean) auditRecord.get("lockHeld"));
  }

  @Test
  void testRecordOperationRenewSuccess() throws Exception {
    long timestamp = System.currentTimeMillis();
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    when(lockHeldSupplier.get()).thenReturn(true);

    auditService.recordOperation(AuditOperationState.RENEW, timestamp);

    verify(storageLockClient, times(1)).writeObject(
        contains(String.format("%d-%s-renew.json", timestamp, SHORT_UUID)),
        anyString());
  }

  @Test
  void testRecordOperationEndSuccess() throws Exception {
    long timestamp = System.currentTimeMillis();
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    when(lockHeldSupplier.get()).thenReturn(false);
    when(lockExpirationSupplier.get()).thenReturn(0L);

    auditService.recordOperation(AuditOperationState.END, timestamp);

    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient, times(1)).writeObject(
        contains(String.format("%d-%s-end.json", timestamp, SHORT_UUID)),
        contentCaptor.capture());

    Map<String, Object> auditRecord = OBJECT_MAPPER.readValue(contentCaptor.getValue(), Map.class);
    assertEquals("END", auditRecord.get("state"));
    assertFalse((Boolean) auditRecord.get("lockHeld"));
    assertEquals(0L, ((Number) auditRecord.get("lockExpiration")).longValue());
  }

  @Test
  void testRecordOperationWriteFailure() throws Exception {
    long timestamp = System.currentTimeMillis();
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(false);

    auditService.recordOperation(AuditOperationState.START, timestamp);

    verify(storageLockClient, times(1)).writeObject(anyString(), anyString());
  }

  @Test
  void testRecordOperationWriteException() throws Exception {
    long timestamp = System.currentTimeMillis();
    when(storageLockClient.writeObject(anyString(), anyString()))
        .thenThrow(new RuntimeException("Storage error"));

    auditService.recordOperation(AuditOperationState.START, timestamp);

    verify(storageLockClient, times(1)).writeObject(anyString(), anyString());
  }

  @Test
  void testExtractShortUuidFromFullUuid() throws Exception {
    String ownerIdWithFullUuid = "12345678-9abc-def0-1234-567890abcdef";
    StorageLockProviderAuditService service = new StorageLockProviderAuditService(
        BASE_PATH,
        ownerIdWithFullUuid,
        storageLockClient,
        lockExpirationSupplier,
        lockHeldSupplier);

    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    service.recordOperation(AuditOperationState.START, System.currentTimeMillis());

    verify(storageLockClient, times(1)).writeObject(
        contains("-12345678-start.json"),
        anyString());
  }

  @Test
  void testExtractShortUuidFromShortString() throws Exception {
    String shortOwnerId = "abc123";
    StorageLockProviderAuditService service = new StorageLockProviderAuditService(
        BASE_PATH,
        shortOwnerId,
        storageLockClient,
        lockExpirationSupplier,
        lockHeldSupplier);

    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    service.recordOperation(AuditOperationState.START, System.currentTimeMillis());

    verify(storageLockClient, times(1)).writeObject(
        contains("-abc123-start.json"),
        anyString());
  }

  @Test
  void testExtractShortUuidFromRegularString() throws Exception {
    String regularOwnerId = "regular-owner-id-without-uuid";
    StorageLockProviderAuditService service = new StorageLockProviderAuditService(
        BASE_PATH,
        regularOwnerId,
        storageLockClient,
        lockExpirationSupplier,
        lockHeldSupplier);

    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    service.recordOperation(AuditOperationState.START, System.currentTimeMillis());

    verify(storageLockClient, times(1)).writeObject(
        contains("-regular--start.json"),
        anyString());
  }

  @Test
  void testMultipleOperationsInSequence() throws Exception {
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);

    long startTime = System.currentTimeMillis();
    long renewTime = startTime + 1000;
    long endTime = startTime + 2000;

    when(lockHeldSupplier.get()).thenReturn(true, true, false);
    when(lockExpirationSupplier.get()).thenReturn(LOCK_EXPIRATION, LOCK_EXPIRATION + 1000, 0L);

    auditService.recordOperation(AuditOperationState.START, startTime);
    auditService.recordOperation(AuditOperationState.RENEW, renewTime);
    auditService.recordOperation(AuditOperationState.END, endTime);

    verify(storageLockClient, times(3)).writeObject(anyString(), anyString());
    verify(storageLockClient).writeObject(contains("start.json"), anyString());
    verify(storageLockClient).writeObject(contains("renew.json"), anyString());
    verify(storageLockClient).writeObject(contains("end.json"), anyString());
  }

  @Test
  void testAuditRecordContentStructure() throws Exception {
    long timestamp = System.currentTimeMillis();
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    when(lockExpirationSupplier.get()).thenReturn(999999L);
    when(lockHeldSupplier.get()).thenReturn(true);

    auditService.recordOperation(AuditOperationState.RENEW, timestamp);

    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient).writeObject(anyString(), contentCaptor.capture());

    Map<String, Object> auditRecord = OBJECT_MAPPER.readValue(contentCaptor.getValue(), Map.class);
    
    assertNotNull(auditRecord.get("ownerId"));
    assertNotNull(auditRecord.get("timestamp"));
    assertNotNull(auditRecord.get("state"));
    assertNotNull(auditRecord.get("lockExpiration"));
    assertNotNull(auditRecord.get("lockHeld"));
    
    assertEquals(5, auditRecord.size(), "Audit record should contain exactly 5 fields");
  }

  @Test
  void testCloseMethod() throws Exception {
    auditService.close();
    
    verify(storageLockClient, never()).writeObject(anyString(), anyString());
  }

  @Test
  void testDynamicSupplierValues() throws Exception {
    long timestamp = System.currentTimeMillis();
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    
    when(lockHeldSupplier.get()).thenReturn(true, false);
    when(lockExpirationSupplier.get()).thenReturn(1000L, 2000L);

    auditService.recordOperation(AuditOperationState.START, timestamp);
    auditService.recordOperation(AuditOperationState.END, timestamp + 1000);

    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient, times(2)).writeObject(anyString(), contentCaptor.capture());

    Map<String, Object> firstRecord = OBJECT_MAPPER.readValue(contentCaptor.getAllValues().get(0), Map.class);
    Map<String, Object> secondRecord = OBJECT_MAPPER.readValue(contentCaptor.getAllValues().get(1), Map.class);

    assertTrue((Boolean) firstRecord.get("lockHeld"));
    assertEquals(1000L, ((Number) firstRecord.get("lockExpiration")).longValue());
    
    assertFalse((Boolean) secondRecord.get("lockHeld"));
    assertEquals(2000L, ((Number) secondRecord.get("lockExpiration")).longValue());
  }

  @Test
  void testFilePathGeneration() throws Exception {
    long timestamp = 1234567890123L;
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);

    auditService.recordOperation(AuditOperationState.START, timestamp);

    String expectedFilename = String.format("%d-%s-start.json", timestamp, SHORT_UUID);
    String expectedPath = String.format("%s/.hoodie/.locks/audit/%s", BASE_PATH, expectedFilename);
    
    verify(storageLockClient).writeObject(eq(expectedPath), anyString());
  }

  @Test
  void testNullSupplierReturns() throws Exception {
    long timestamp = System.currentTimeMillis();
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    when(lockExpirationSupplier.get()).thenReturn(null);
    when(lockHeldSupplier.get()).thenReturn(null);

    auditService.recordOperation(AuditOperationState.START, timestamp);

    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient).writeObject(anyString(), contentCaptor.capture());

    Map<String, Object> auditRecord = OBJECT_MAPPER.readValue(contentCaptor.getValue(), Map.class);
    assertTrue(auditRecord.containsKey("lockExpiration"));
    assertTrue(auditRecord.containsKey("lockHeld"));
  }
}