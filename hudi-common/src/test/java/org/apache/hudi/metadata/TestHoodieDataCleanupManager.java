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

package org.apache.hudi.metadata;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for HoodieDataCleanupManager to ensure proper tracking and cleanup of persisted data.
 */
public class TestHoodieDataCleanupManager {

  private HoodieDataCleanupManager cleanupManager;
  private ConcurrentHashMap<Long, List<Object>> threadPersistedData;

  @BeforeEach
  public void setUp() {
    cleanupManager = new HoodieDataCleanupManager();
    threadPersistedData = cleanupManager.getThreadPersistedData();
    // Clear any existing data
    threadPersistedData.clear();
  }
  
  @AfterEach
  void tearDown() {
    // Clear thread tracking
    threadPersistedData.clear();
  }

  /**
   * Test that HoodiePairData objects are tracked and cleaned up on exception.
   */
  @Test
  public void testTrackPersistedDataAndCleanupOnException_HoodiePairData() {
    long threadId = Thread.currentThread().getId();
    AtomicInteger unpersistCount = new AtomicInteger(0);
    
    // Create mock HoodiePairData
    HoodiePairData<String, String> mockPairData = mock(HoodiePairData.class);
    doAnswer(invocation -> {
      unpersistCount.incrementAndGet();
      return null;
    }).when(mockPairData).unpersistWithDependencies();
    
    // Track the data
    cleanupManager.trackPersistedData(mockPairData);
    
    // Verify it's tracked
    assertNotNull(threadPersistedData.get(threadId));
    assertEquals(1, threadPersistedData.get(threadId).size());
    assertTrue(threadPersistedData.get(threadId).contains(mockPairData));
    
    // Execute with exception
    HoodieException exception = assertThrows(HoodieException.class, () -> {
      cleanupManager.ensureDataCleanupOnException(v -> {
        throw new HoodieException("Test exception");
      });
    });
    
    // Verify cleanup was called
    assertEquals("Test exception", exception.getMessage());
    assertEquals(1, unpersistCount.get());
    verify(mockPairData, times(1)).unpersistWithDependencies();
    
    // Verify thread tracking is cleared
    assertFalse(threadPersistedData.containsKey(threadId));
  }

  /**
   * Test that HoodieData objects are tracked and cleaned up on exception.
   * Tests the HoodieData cleanup branch in cleanupPersistedData.
   */
  @Test
  void testTrackPersistedDataAndCleanupOnException_HoodieData() {
    long threadId = Thread.currentThread().getId();
    AtomicInteger unpersistCount = new AtomicInteger(0);
    
    // Create mock HoodieData
    HoodieData<String> mockData = mock(HoodieData.class);
    doAnswer(invocation -> {
      unpersistCount.incrementAndGet();
      return null;
    }).when(mockData).unpersistWithDependencies();
    
    // Track HoodieData
    cleanupManager.trackPersistedData(mockData);
    
    // Execute with exception
    RuntimeException exception = assertThrows(RuntimeException.class, () -> {
      cleanupManager.ensureDataCleanupOnException(v -> {
        throw new RuntimeException("Test runtime exception");
      });
    });
    
    // Verify cleanup was called
    assertEquals("Test runtime exception", exception.getMessage());
    assertEquals(1, unpersistCount.get());
    verify(mockData, times(1)).unpersistWithDependencies();
    
    // Verify thread tracking is cleared
    assertFalse(threadPersistedData.containsKey(threadId));
  }

  /**
   * Test successful execution without exception - no cleanup should occur.
   */
  @Test
  void testEnsureDataCleanupOnException_SuccessfulExecution() {
    long threadId = Thread.currentThread().getId();
    
    // Create mock data
    HoodiePairData<String, String> mockPairData = mock(HoodiePairData.class);
    
    // Track the data
    cleanupManager.trackPersistedData(mockPairData);
    
    // Execute successfully
    String result = cleanupManager.ensureDataCleanupOnException(v -> "Success");
    
    // Verify result
    assertEquals("Success", result);
    
    // Verify no cleanup was called
    verify(mockPairData, never()).unpersistWithDependencies();
    
    // Verify thread tracking is cleared
    assertFalse(threadPersistedData.containsKey(threadId));
  }

  /**
   * Test cleanup with multiple persisted data objects.
   */
  @Test
  void testCleanupMultiplePersistedDataObjects() {
    long threadId = Thread.currentThread().getId();
    AtomicInteger unpersistCount = new AtomicInteger(0);
    
    // Create multiple mock objects
    HoodiePairData<String, String> mockPairData1 = mock(HoodiePairData.class);
    HoodiePairData<String, String> mockPairData2 = mock(HoodiePairData.class);
    HoodieData<String> mockData = mock(HoodieData.class);
    
    // Setup unpersist tracking
    doAnswer(invocation -> {
      unpersistCount.incrementAndGet();
      return null;
    }).when(mockPairData1).unpersistWithDependencies();
    
    doAnswer(invocation -> {
      unpersistCount.incrementAndGet();
      return null;
    }).when(mockPairData2).unpersistWithDependencies();
    
    doAnswer(invocation -> {
      unpersistCount.incrementAndGet();
      return null;
    }).when(mockData).unpersistWithDependencies();
    
    // Track all data
    cleanupManager.trackPersistedData(mockPairData1);
    cleanupManager.trackPersistedData(mockPairData2);
    cleanupManager.trackPersistedData(mockData);
    
    // Execute with exception
    assertThrows(HoodieException.class, () -> {
      cleanupManager.ensureDataCleanupOnException(v -> {
        throw new HoodieException("Test exception");
      });
    });
    
    // Verify all objects were cleaned up
    assertEquals(3, unpersistCount.get());
    verify(mockPairData1, times(1)).unpersistWithDependencies();
    verify(mockPairData2, times(1)).unpersistWithDependencies();
    verify(mockData, times(1)).unpersistWithDependencies();
    
    // Verify thread tracking is cleared
    assertFalse(threadPersistedData.containsKey(threadId));
  }

  /**
   * Test cleanup when unpersist throws an exception - should continue with other cleanups.
   */
  @Test
  void testCleanupContinuesWhenUnpersistFails() {
    long threadId = Thread.currentThread().getId();
    AtomicInteger unpersistCount = new AtomicInteger(0);
    
    // Create mock objects
    HoodiePairData<String, String> failingPairData = mock(HoodiePairData.class);
    HoodiePairData<String, String> successfulPairData = mock(HoodiePairData.class);
    
    // First one throws exception
    doThrow(new RuntimeException("Unpersist failed")).when(failingPairData).unpersistWithDependencies();
    
    // Second one succeeds
    doAnswer(invocation -> {
      unpersistCount.incrementAndGet();
      return null;
    }).when(successfulPairData).unpersistWithDependencies();
    
    // Track both
    cleanupManager.trackPersistedData(failingPairData);
    cleanupManager.trackPersistedData(successfulPairData);
    
    // Execute with exception
    assertThrows(HoodieException.class, () -> {
      cleanupManager.ensureDataCleanupOnException(v -> {
        throw new HoodieException("Test exception");
      });
    });
    
    // Verify both were attempted
    verify(failingPairData, times(1)).unpersistWithDependencies();
    verify(successfulPairData, times(1)).unpersistWithDependencies();
    assertEquals(1, unpersistCount.get()); // Only successful one counted
    
    // Verify thread tracking is cleared
    assertFalse(threadPersistedData.containsKey(threadId));
  }

  /**
   * Test thread isolation - each thread tracks its own data.
   */
  @Test
  public void testThreadIsolation() throws Exception {
    int numThreads = 3;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(numThreads);
    AtomicInteger[] unpersistCounts = new AtomicInteger[numThreads];
    
    for (int i = 0; i < numThreads; i++) {
      unpersistCounts[i] = new AtomicInteger(0);
    }
    
    // Submit tasks to different threads
    for (int i = 0; i < numThreads; i++) {
      final int threadIndex = i;
      
      executor.submit(() -> {
        try {
          startLatch.await();
          
          // Create thread-specific mock
          HoodiePairData<String, String> mockPairData = mock(HoodiePairData.class);
          doAnswer(invocation -> {
            unpersistCounts[threadIndex].incrementAndGet();
            return null;
          }).when(mockPairData).unpersistWithDependencies();
          
          // Track data
          cleanupManager.trackPersistedData(mockPairData);
          
          // Execute with exception
          assertThrows(HoodieException.class, () -> {
            cleanupManager.ensureDataCleanupOnException(v -> {
              throw new HoodieException("Thread " + threadIndex + " exception");
            });
          });
          
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          completionLatch.countDown();
        }
      });
    }
    
    // Start all threads
    startLatch.countDown();
    completionLatch.await();
    executor.shutdown();
    
    // Verify each thread cleaned up exactly once
    for (int i = 0; i < numThreads; i++) {
      assertEquals(1, unpersistCounts[i].get(), "Thread " + i + " should have cleaned up exactly once");
    }
    
    // Verify no thread data remains
    assertTrue(threadPersistedData.isEmpty());
  }

  /**
   * Test that objects of unsupported types are not cleaned up.
   */
  @Test
  void testUnsupportedObjectTypeInCleanup() {
    long threadId = Thread.currentThread().getId();
    
    // Add an unsupported object type directly
    Object unsupportedObject = new Object();
    threadPersistedData.computeIfAbsent(threadId, k -> new java.util.ArrayList<>()).add(unsupportedObject);
    
    // Add a valid mock too
    HoodiePairData<String, String> mockPairData = mock(HoodiePairData.class);
    AtomicInteger unpersistCount = new AtomicInteger(0);
    doAnswer(invocation -> {
      unpersistCount.incrementAndGet();
      return null;
    }).when(mockPairData).unpersistWithDependencies();
    
    cleanupManager.trackPersistedData(mockPairData);
    
    // Execute with exception
    assertThrows(HoodieException.class, () -> {
      cleanupManager.ensureDataCleanupOnException(v -> {
        throw new HoodieException("Test exception");
      });
    });
    
    // Verify only the valid object was cleaned up
    assertEquals(1, unpersistCount.get());
    verify(mockPairData, times(1)).unpersistWithDependencies();
    
    // Verify thread tracking is cleared
    assertFalse(threadPersistedData.containsKey(threadId));
  }

  /**
   * Test no persisted data scenario.
   */
  @Test
  void testNoPersistentDataScenario() {
    long threadId = Thread.currentThread().getId();
    
    // Execute with exception without tracking any data
    assertThrows(HoodieException.class, () -> {
      cleanupManager.ensureDataCleanupOnException(v -> {
        throw new HoodieException("Test exception");
      });
    });
    
    // Verify thread tracking is cleared (even though it was never populated)
    assertFalse(threadPersistedData.containsKey(threadId));
  }

  /**
   * Test null data handling in cleanup.
   */
  @Test 
  void testNullDataInCleanup() {
    long threadId = Thread.currentThread().getId();
    
    // Add null to the persisted data list
    threadPersistedData.computeIfAbsent(threadId, k -> new java.util.ArrayList<>()).add(null);
    
    // Add a valid mock
    HoodiePairData<String, String> mockPairData = mock(HoodiePairData.class);
    AtomicInteger unpersistCount = new AtomicInteger(0);
    doAnswer(invocation -> {
      unpersistCount.incrementAndGet();
      return null;
    }).when(mockPairData).unpersistWithDependencies();
    
    cleanupManager.trackPersistedData(mockPairData);
    
    // Execute with exception
    assertThrows(HoodieException.class, () -> {
      cleanupManager.ensureDataCleanupOnException(v -> {
        throw new HoodieException("Test exception");
      });
    });
    
    // Verify only the valid object was cleaned up (null should be ignored)
    assertEquals(1, unpersistCount.get());
    verify(mockPairData, times(1)).unpersistWithDependencies();
    
    // Verify thread tracking is cleared
    assertFalse(threadPersistedData.containsKey(threadId));
  }
}