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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for StorageLockProviderAuditService.
 */
public class TestStorageLockProviderAuditService {

  private static final String BASE_PATH = "s3://bucket/table";
  private static final String OWNER_ID = "writer-12345678-9abc-def0-1234-567890abcdef";
  private static final long TRANSACTION_START_TIME = 1234567890000L;
  private static final long LOCK_EXPIRATION = 1000000L;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private StorageLockClient storageLockClient;
  private Supplier<Boolean> lockHeldSupplier;
  private StorageLockProviderAuditService auditService;

  @BeforeEach
  void setUp() {
    storageLockClient = mock(StorageLockClient.class);
    lockHeldSupplier = (Supplier<Boolean>) mock(Supplier.class);
    
    when(lockHeldSupplier.get()).thenReturn(true);
    
    auditService = new StorageLockProviderAuditService(
        BASE_PATH,
        OWNER_ID,
        TRANSACTION_START_TIME,
        storageLockClient,
        timestamp -> LOCK_EXPIRATION,
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

    String expectedPath = String.format("%s%s.hoodie%s.locks%saudit%s%d_%s.jsonl",
        BASE_PATH,
        StoragePath.SEPARATOR,
        StoragePath.SEPARATOR,
        StoragePath.SEPARATOR,
        StoragePath.SEPARATOR,
        TRANSACTION_START_TIME,
        OWNER_ID);
    assertEquals(expectedPath, pathCaptor.getValue());

    // Parse JSONL content (should be a single line)
    String[] lines = contentCaptor.getValue().trim().split("\n");
    assertEquals(1, lines.length, "Should have one JSON line");
    
    Map<String, Object> auditRecord = OBJECT_MAPPER.readValue(lines[0], Map.class);
    assertEquals(OWNER_ID, auditRecord.get("ownerId"));
    assertEquals(TRANSACTION_START_TIME, ((Number) auditRecord.get("transactionStartTime")).longValue());
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

    // First record START
    auditService.recordOperation(AuditOperationState.START, timestamp - 1000);
    // Then record RENEW
    auditService.recordOperation(AuditOperationState.RENEW, timestamp);

    // Verify the file is written twice (once for START, once after RENEW)
    verify(storageLockClient, times(2)).writeObject(
        contains(String.format("%d_%s.jsonl", TRANSACTION_START_TIME, OWNER_ID)),
        anyString());
  }

  @Test
  void testRecordOperationEndSuccess() throws Exception {
    long timestamp = System.currentTimeMillis();
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    when(lockHeldSupplier.get()).thenReturn(false);

    auditService.recordOperation(AuditOperationState.END, timestamp);

    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient, times(1)).writeObject(
        contains(String.format("%d_%s.jsonl", TRANSACTION_START_TIME, OWNER_ID)),
        contentCaptor.capture());

    String[] lines = contentCaptor.getValue().trim().split("\n");
    Map<String, Object> auditRecord = OBJECT_MAPPER.readValue(lines[0], Map.class);
    assertEquals("END", auditRecord.get("state"));
    assertFalse((Boolean) auditRecord.get("lockHeld"));
    // In real usage, lock expiration is still calculated normally even when ending
    assertEquals(LOCK_EXPIRATION, ((Number) auditRecord.get("lockExpiration")).longValue());
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
  void testFileNameWithFullUuid() throws Exception {
    String ownerIdWithFullUuid = "12345678-9abc-def0-1234-567890abcdef";
    long txnStartTime = System.currentTimeMillis();
    StorageLockProviderAuditService service = new StorageLockProviderAuditService(
        BASE_PATH,
        ownerIdWithFullUuid,
        txnStartTime,
        storageLockClient,
        timestamp -> LOCK_EXPIRATION,
        lockHeldSupplier);

    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    service.recordOperation(AuditOperationState.START, System.currentTimeMillis());

    verify(storageLockClient, times(1)).writeObject(
        contains(String.format("%d_%s.jsonl", txnStartTime, ownerIdWithFullUuid)),
        anyString());
  }

  @Test
  void testFileNameWithShortOwnerId() throws Exception {
    String shortOwnerId = "abc123";
    long txnStartTime = System.currentTimeMillis();
    StorageLockProviderAuditService service = new StorageLockProviderAuditService(
        BASE_PATH,
        shortOwnerId,
        txnStartTime,
        storageLockClient,
        timestamp -> LOCK_EXPIRATION,
        lockHeldSupplier);

    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    service.recordOperation(AuditOperationState.START, System.currentTimeMillis());

    verify(storageLockClient, times(1)).writeObject(
        contains(String.format("%d_%s.jsonl", txnStartTime, shortOwnerId)),
        anyString());
  }

  @Test
  void testFileNameWithRegularOwnerId() throws Exception {
    String regularOwnerId = "regular-owner-id-without-uuid";
    long txnStartTime = System.currentTimeMillis();
    StorageLockProviderAuditService service = new StorageLockProviderAuditService(
        BASE_PATH,
        regularOwnerId,
        txnStartTime,
        storageLockClient,
        timestamp -> LOCK_EXPIRATION,
        lockHeldSupplier);

    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    service.recordOperation(AuditOperationState.START, System.currentTimeMillis());

    verify(storageLockClient, times(1)).writeObject(
        contains(String.format("%d_%s.jsonl", txnStartTime, regularOwnerId)),
        anyString());
  }

  @Test
  void testMultipleOperationsInSequence() throws Exception {
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);

    long startTime = System.currentTimeMillis();
    long renewTime = startTime + 1000;
    long endTime = startTime + 2000;

    when(lockHeldSupplier.get()).thenReturn(true, true, false);

    auditService.recordOperation(AuditOperationState.START, startTime);
    auditService.recordOperation(AuditOperationState.RENEW, renewTime);
    auditService.recordOperation(AuditOperationState.END, endTime);

    // All operations write to the same JSONL file
    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient, times(3)).writeObject(
        contains(String.format("%d_%s.jsonl", TRANSACTION_START_TIME, OWNER_ID)), 
        contentCaptor.capture());
    
    // The last write should contain all three records
    String lastContent = contentCaptor.getValue();
    String[] lines = lastContent.trim().split("\n");
    assertEquals(3, lines.length, "Should have three JSON lines");
    
    Map<String, Object> startRecord = OBJECT_MAPPER.readValue(lines[0], Map.class);
    Map<String, Object> renewRecord = OBJECT_MAPPER.readValue(lines[1], Map.class);
    Map<String, Object> endRecord = OBJECT_MAPPER.readValue(lines[2], Map.class);
    
    assertEquals("START", startRecord.get("state"));
    assertEquals("RENEW", renewRecord.get("state"));
    assertEquals("END", endRecord.get("state"));
  }

  @Test
  void testAuditRecordContentStructure() throws Exception {
    long timestamp = System.currentTimeMillis();
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    when(lockHeldSupplier.get()).thenReturn(true);

    auditService.recordOperation(AuditOperationState.RENEW, timestamp);

    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient).writeObject(anyString(), contentCaptor.capture());

    String[] lines = contentCaptor.getValue().trim().split("\n");
    Map<String, Object> auditRecord = OBJECT_MAPPER.readValue(lines[0], Map.class);
    
    assertNotNull(auditRecord.get("ownerId"));
    assertNotNull(auditRecord.get("transactionStartTime"));
    assertNotNull(auditRecord.get("timestamp"));
    assertNotNull(auditRecord.get("state"));
    assertNotNull(auditRecord.get("lockExpiration"));
    assertNotNull(auditRecord.get("lockHeld"));
    
    assertEquals(6, auditRecord.size(), "Audit record should contain exactly 6 fields");
  }

  @Test
  void testCloseMethodWithBufferedData() throws Exception {
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    
    // Record an operation to buffer some data
    auditService.recordOperation(AuditOperationState.START, System.currentTimeMillis());
    
    // Close should write any buffered data
    auditService.close();
    
    // Verify write was called once for the START and no additional calls during close
    // (since writeAuditFile is called immediately after each recordOperation)
    verify(storageLockClient, times(1)).writeObject(anyString(), anyString());
  }

  @Test
  void testDynamicFunctionValues() throws Exception {
    long timestamp = System.currentTimeMillis();
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    
    // Create a service that calculates different expiration times based on the timestamp
    StorageLockProviderAuditService dynamicService = new StorageLockProviderAuditService(
        BASE_PATH,
        OWNER_ID,
        TRANSACTION_START_TIME,
        storageLockClient,
        ts -> ts + 1000L, // Different function: adds 1000ms to timestamp
        lockHeldSupplier);
    
    when(lockHeldSupplier.get()).thenReturn(true, false);

    dynamicService.recordOperation(AuditOperationState.START, timestamp);
    dynamicService.recordOperation(AuditOperationState.END, timestamp + 1000);

    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient, times(2)).writeObject(anyString(), contentCaptor.capture());

    // First write contains only START record
    String[] firstLines = contentCaptor.getAllValues().get(0).trim().split("\n");
    Map<String, Object> firstRecord = OBJECT_MAPPER.readValue(firstLines[0], Map.class);
    
    // Second write contains both START and END records
    String[] secondLines = contentCaptor.getAllValues().get(1).trim().split("\n");
    Map<String, Object> secondRecord = OBJECT_MAPPER.readValue(secondLines[1], Map.class);

    assertTrue((Boolean) firstRecord.get("lockHeld"));
    assertEquals(timestamp + 1000L, ((Number) firstRecord.get("lockExpiration")).longValue());
    
    assertFalse((Boolean) secondRecord.get("lockHeld"));
    assertEquals(timestamp + 1000L + 1000L, ((Number) secondRecord.get("lockExpiration")).longValue());
  }

  @Test
  void testFilePathGeneration() throws Exception {
    long timestamp = 1234567890123L;
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);

    auditService.recordOperation(AuditOperationState.START, timestamp);

    String expectedFilename = String.format("%d_%s.jsonl", TRANSACTION_START_TIME, OWNER_ID);
    String expectedPath = String.format("%s/.hoodie/.locks/audit/%s", BASE_PATH, expectedFilename);
    
    verify(storageLockClient).writeObject(eq(expectedPath), anyString());
  }

  @Test
  void testNullSupplierReturns() throws Exception {
    long timestamp = System.currentTimeMillis();
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    when(lockHeldSupplier.get()).thenReturn(null);

    auditService.recordOperation(AuditOperationState.START, timestamp);

    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient).writeObject(anyString(), contentCaptor.capture());

    String[] lines = contentCaptor.getValue().trim().split("\n");
    Map<String, Object> auditRecord = OBJECT_MAPPER.readValue(lines[0], Map.class);
    assertTrue(auditRecord.containsKey("lockExpiration"));
    assertTrue(auditRecord.containsKey("lockHeld"));
  }

  @Test
  void testConcurrentRenewAndEndOperations() throws Exception {
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    when(lockHeldSupplier.get()).thenReturn(true, false);
    
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(2);
    
    long baseTime = System.currentTimeMillis();
    
    // Thread 1: RENEW operation (simulating heartbeat thread)
    executor.submit(() -> {
      try {
        startLatch.await(); // Wait for signal to start simultaneously
        auditService.recordOperation(AuditOperationState.RENEW, baseTime + 1000);
      } catch (Exception e) {
        // Test should handle any exceptions gracefully
      } finally {
        completionLatch.countDown();
      }
    });
    
    // Thread 2: END operation (simulating main thread unlocking)
    executor.submit(() -> {
      try {
        startLatch.await(); // Wait for signal to start simultaneously
        auditService.recordOperation(AuditOperationState.END, baseTime + 1001);
      } catch (Exception e) {
        // Test should handle any exceptions gracefully
      } finally {
        completionLatch.countDown();
      }
    });
    
    // Signal both threads to start simultaneously
    startLatch.countDown();
    
    // Wait for both operations to complete
    assertTrue(completionLatch.await(5, TimeUnit.SECONDS), "Both operations should complete within 5 seconds");
    
    // Verify that both operations were recorded (order may vary due to concurrency)
    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient, times(2)).writeObject(anyString(), contentCaptor.capture());
    
    // Parse the final content to verify both RENEW and END are present
    String finalContent = contentCaptor.getValue();
    String[] lines = finalContent.trim().split("\\n");
    assertEquals(2, lines.length, "Should have exactly 2 audit records");
    
    // Parse both records and verify they contain both RENEW and END states
    Map<String, Object> record1 = OBJECT_MAPPER.readValue(lines[0], Map.class);
    Map<String, Object> record2 = OBJECT_MAPPER.readValue(lines[1], Map.class);
    
    // Verify we have both RENEW and END states (order may vary)
    String state1 = (String) record1.get("state");
    String state2 = (String) record2.get("state");
    assertTrue((state1.equals("RENEW") && state2.equals("END"))
               || (state1.equals("END") && state2.equals("RENEW")),
               "Should contain both RENEW and END operations");
    
    // Verify timestamps are correct
    long timestamp1 = ((Number) record1.get("timestamp")).longValue();
    long timestamp2 = ((Number) record2.get("timestamp")).longValue();
    assertTrue(timestamp1 >= baseTime + 1000 && timestamp1 <= baseTime + 1001);
    assertTrue(timestamp2 >= baseTime + 1000 && timestamp2 <= baseTime + 1001);
    
    executor.shutdown();
    assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
  }
}