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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
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
    lockHeldSupplier = mock(Supplier.class);

    when(lockHeldSupplier.get()).thenReturn(true);

    auditService = new StorageLockProviderAuditService(
        BASE_PATH,
        OWNER_ID,
        TRANSACTION_START_TIME,
        storageLockClient,
        timestamp -> LOCK_EXPIRATION,
        lockHeldSupplier);
  }

  /**
   * Helper method to validate audit record structure and content.
   * 
   * @param auditRecord The audit record to validate
   * @param expectedState Expected operation state
   * @param expectedTimestamp Expected timestamp
   * @param expectedLockHeld Expected lock held status
   */
  private void validateAuditRecord(Map<String, Object> auditRecord, AuditOperationState expectedState, 
                                  long expectedTimestamp, boolean expectedLockHeld) {
    assertNotNull(auditRecord, "Audit record should not be null");
    assertEquals(6, auditRecord.size(), "Audit record should contain exactly 6 fields");
    
    // Validate all required fields are present
    assertNotNull(auditRecord.get("ownerId"), "ownerId should be present");
    assertNotNull(auditRecord.get("transactionStartTime"), "transactionStartTime should be present");
    assertNotNull(auditRecord.get("timestamp"), "timestamp should be present");
    assertNotNull(auditRecord.get("state"), "state should be present");
    assertNotNull(auditRecord.get("lockExpiration"), "lockExpiration should be present");
    assertNotNull(auditRecord.get("lockHeld"), "lockHeld should be present");
    
    // Validate field values
    assertEquals(OWNER_ID, auditRecord.get("ownerId"));
    assertEquals(TRANSACTION_START_TIME, ((Number) auditRecord.get("transactionStartTime")).longValue());
    assertEquals(expectedTimestamp, ((Number) auditRecord.get("timestamp")).longValue());
    assertEquals(expectedState.name(), auditRecord.get("state"));
    assertEquals(LOCK_EXPIRATION, ((Number) auditRecord.get("lockExpiration")).longValue());
    assertEquals(expectedLockHeld, auditRecord.get("lockHeld"));
  }

  /**
   * Helper method to verify expected file path generation.
   * 
   * @param actualPath The actual file path captured
   * @param ownerId The owner ID used
   * @param transactionStartTime The transaction start time used
   */
  private void validateFilePath(String actualPath, String ownerId, long transactionStartTime) {
    String expectedPath = String.format("%s%s.hoodie%s.locks%saudit%s%d_%s.jsonl",
        BASE_PATH,
        StoragePath.SEPARATOR,
        StoragePath.SEPARATOR,
        StoragePath.SEPARATOR,
        StoragePath.SEPARATOR,
        transactionStartTime,
        ownerId);
    assertEquals(expectedPath, actualPath);
  }

  /**
   * Comprehensive test for complete audit lifecycle: START -> RENEW -> END.
   * This consolidates individual operation tests into a single comprehensive test
   * that validates the entire audit flow.
   */
  @Test
  void testCompleteAuditLifecycle() throws Exception {
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);

    long startTime = System.currentTimeMillis();
    long renewTime = startTime + 1000;
    long endTime = startTime + 2000;

    // Configure lock held status for each operation
    when(lockHeldSupplier.get()).thenReturn(true, true, false);

    // Execute complete lifecycle
    auditService.recordOperation(AuditOperationState.START, startTime);
    auditService.recordOperation(AuditOperationState.RENEW, renewTime);
    auditService.recordOperation(AuditOperationState.END, endTime);

    // Capture all write calls
    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient, times(3)).writeObject(pathCaptor.capture(), contentCaptor.capture());

    // Validate file path (should be consistent across all operations)
    validateFilePath(pathCaptor.getValue(), OWNER_ID, TRANSACTION_START_TIME);

    // Parse final content - should contain all three records
    String finalContent = contentCaptor.getValue();
    String[] lines = finalContent.trim().split("\n");
    assertEquals(3, lines.length, "Should have three JSON lines");

    // Validate each audit record
    @SuppressWarnings("unchecked")
    Map<String, Object> startRecord = OBJECT_MAPPER.readValue(lines[0], Map.class);
    @SuppressWarnings("unchecked")
    Map<String, Object> renewRecord = OBJECT_MAPPER.readValue(lines[1], Map.class);
    @SuppressWarnings("unchecked")
    Map<String, Object> endRecord = OBJECT_MAPPER.readValue(lines[2], Map.class);

    validateAuditRecord(startRecord, AuditOperationState.START, startTime, true);
    validateAuditRecord(renewRecord, AuditOperationState.RENEW, renewTime, true);
    validateAuditRecord(endRecord, AuditOperationState.END, endTime, false);
  }

  /**
   * Test data provider for different owner ID formats.
   * 
   * @return Stream of owner ID test cases
   */
  static java.util.stream.Stream<Arguments> ownerIdTestCases() {
    return java.util.stream.Stream.of(
        Arguments.of("12345678-9abc-def0-1234-567890abcdef", "Full UUID format"),
        Arguments.of("abc123", "Short owner ID"),
        Arguments.of("regular-owner-id-without-uuid", "Regular owner ID"),
        Arguments.of("writer-12345678-9abc-def0-1234-567890abcdef", "Writer with UUID")
    );
  }

  /**
   * Test audit file naming with different owner ID formats.
   * Consolidated test that verifies file path generation for various owner ID formats.
   * 
   * @param ownerId The owner ID to test
   * @param description Description of the test case
   */
  @ParameterizedTest(name = "{1}: {0}")
  @MethodSource("ownerIdTestCases")
  void testFileNameWithDifferentOwnerIds(String ownerId, String description) throws Exception {
    long txnStartTime = System.currentTimeMillis();
    StorageLockProviderAuditService service = new StorageLockProviderAuditService(
        BASE_PATH,
        ownerId,
        txnStartTime,
        storageLockClient,
        timestamp -> LOCK_EXPIRATION,
        lockHeldSupplier);

    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    service.recordOperation(AuditOperationState.START, System.currentTimeMillis());

    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient, times(1)).writeObject(pathCaptor.capture(), anyString());
    
    validateFilePath(pathCaptor.getValue(), ownerId, txnStartTime);
  }

  /**
   * Test data provider for write failure scenarios.
   * 
   * @return Stream of write failure test cases
   */
  static Stream<Arguments> writeFailureTestCases() {
    return Stream.of(
        Arguments.of(false, "Write returns false"),
        Arguments.of(new RuntimeException("Storage error"), "Write throws exception")
    );
  }

  /**
   * Test handling of write failures using parameterized test.
   * Consolidates multiple write failure scenarios into a single parameterized test.
   * 
   * @param failureCondition The failure condition (Boolean false or Exception)
   * @param description Description of the test case
   */
  @ParameterizedTest(name = "{1}")
  @MethodSource("writeFailureTestCases")
  void testWriteFailureHandling(Object failureCondition, String description) throws Exception {
    long timestamp = System.currentTimeMillis();
    
    if (failureCondition instanceof Boolean) {
      when(storageLockClient.writeObject(anyString(), anyString())).thenReturn((Boolean) failureCondition);
    } else if (failureCondition instanceof Exception) {
      when(storageLockClient.writeObject(anyString(), anyString())).thenThrow((Exception) failureCondition);
    }

    // Should not throw exception - audit failures should be handled gracefully
    auditService.recordOperation(AuditOperationState.START, timestamp);

    verify(storageLockClient, times(1)).writeObject(anyString(), anyString());
  }

  @Test
  void testDynamicFunctionValues() throws Exception {
    long timestamp = System.currentTimeMillis();
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);

    // Create a service with dynamic expiration calculation
    StorageLockProviderAuditService dynamicService = new StorageLockProviderAuditService(
        BASE_PATH,
        OWNER_ID,
        TRANSACTION_START_TIME,
        storageLockClient,
        ts -> ts + 1000L, // Adds 1000ms to timestamp
        lockHeldSupplier);

    when(lockHeldSupplier.get()).thenReturn(true, false);

    dynamicService.recordOperation(AuditOperationState.START, timestamp);
    dynamicService.recordOperation(AuditOperationState.END, timestamp + 1000);

    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient, times(2)).writeObject(anyString(), contentCaptor.capture());

    // Validate dynamic expiration calculation
    String[] firstLines = contentCaptor.getAllValues().get(0).trim().split("\n");
    @SuppressWarnings("unchecked")
    Map<String, Object> firstRecord = OBJECT_MAPPER.readValue(firstLines[0], Map.class);

    String[] secondLines = contentCaptor.getAllValues().get(1).trim().split("\n");
    @SuppressWarnings("unchecked")
    Map<String, Object> secondRecord = OBJECT_MAPPER.readValue(secondLines[1], Map.class);

    assertTrue((Boolean) firstRecord.get("lockHeld"));
    assertEquals(timestamp + 1000L, ((Number) firstRecord.get("lockExpiration")).longValue());

    assertFalse((Boolean) secondRecord.get("lockHeld"));
    assertEquals(timestamp + 1000L + 1000L, ((Number) secondRecord.get("lockExpiration")).longValue());
  }

  @Test
  void testFilePathGeneration() throws Exception {
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);

    auditService.recordOperation(AuditOperationState.START, System.currentTimeMillis());

    String expectedPath = String.format("%s/.hoodie/.locks/audit/%d_%s.jsonl", 
        BASE_PATH, TRANSACTION_START_TIME, OWNER_ID);

    verify(storageLockClient).writeObject(eq(expectedPath), anyString());
  }

  @Test
  void testCloseMethodWithBufferedData() throws Exception {
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);

    // Record an operation to buffer data
    auditService.recordOperation(AuditOperationState.START, System.currentTimeMillis());

    // Close should not write additional data (data is written immediately after each operation)
    auditService.close();

    // Verify write was called only once for the START operation
    verify(storageLockClient, times(1)).writeObject(anyString(), anyString());
  }

  @Test
  void testConcurrentOperations() throws Exception {
    when(storageLockClient.writeObject(anyString(), anyString())).thenReturn(true);
    when(lockHeldSupplier.get()).thenReturn(true, false);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(2);

    long baseTime = System.currentTimeMillis();

    // Concurrent RENEW and END operations
    executor.submit(() -> {
      try {
        startLatch.await();
        auditService.recordOperation(AuditOperationState.RENEW, baseTime + 1000);
      } catch (Exception e) {
        // Handle gracefully
      } finally {
        completionLatch.countDown();
      }
    });

    executor.submit(() -> {
      try {
        startLatch.await();
        auditService.recordOperation(AuditOperationState.END, baseTime + 1001);
      } catch (Exception e) {
        // Handle gracefully
      } finally {
        completionLatch.countDown();
      }
    });

    startLatch.countDown();
    assertTrue(completionLatch.await(5, TimeUnit.SECONDS));

    // Verify both operations were recorded
    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storageLockClient, times(2)).writeObject(anyString(), contentCaptor.capture());

    String finalContent = contentCaptor.getValue();
    String[] lines = finalContent.trim().split("\\n");
    assertEquals(2, lines.length);

    // Verify both RENEW and END states are present
    @SuppressWarnings("unchecked")
    Map<String, Object> record1 = OBJECT_MAPPER.readValue(lines[0], Map.class);
    @SuppressWarnings("unchecked")
    Map<String, Object> record2 = OBJECT_MAPPER.readValue(lines[1], Map.class);

    String state1 = (String) record1.get("state");
    String state2 = (String) record2.get("state");
    assertTrue((state1.equals("RENEW") && state2.equals("END"))
        || (state1.equals("END") && state2.equals("RENEW")));

    executor.shutdown();
    assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
  }
}
