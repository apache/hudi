/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link PreemptiveMemorySegmentPool}. */
class TestPreemptiveMemorySegmentPool {

  private static final int PAGE_SIZE = 32 * 1024;

  @Test
  void testReclaimsPageForCurrentOwner() {
    HeapMemorySegmentPool delegate = new HeapMemorySegmentPool(PAGE_SIZE, PAGE_SIZE);
    AtomicReference<String> excludedOwner = new AtomicReference<>();
    AtomicReference<PreemptiveMemorySegmentPool> poolReference = new AtomicReference<>();
    MemorySegment heldSegment = delegate.nextSegment();

    PreemptiveMemorySegmentPool pool = new PreemptiveMemorySegmentPool(delegate, ownerId -> {
      excludedOwner.set(ownerId);
      poolReference.get().returnAll(Collections.singletonList(heldSegment));
      return true;
    });
    poolReference.set(pool);

    pool.setCurrentOwner("bucket-0");
    MemorySegment reclaimedSegment;
    try {
      reclaimedSegment = pool.nextSegment();
    } finally {
      pool.clearCurrentOwner();
    }

    assertEquals("bucket-0", excludedOwner.get());
    assertSame(heldSegment, reclaimedSegment);
    pool.returnAll(Collections.singletonList(reclaimedSegment));
    assertEquals(1, pool.freePages());
  }

  @Test
  void testDoesNotPreemptWithoutCurrentOwner() {
    HeapMemorySegmentPool delegate = new HeapMemorySegmentPool(PAGE_SIZE, PAGE_SIZE);
    delegate.nextSegment();
    AtomicInteger preemptionCount = new AtomicInteger();
    PreemptiveMemorySegmentPool pool = new PreemptiveMemorySegmentPool(delegate, ownerId -> {
      preemptionCount.incrementAndGet();
      return true;
    });

    assertNull(pool.nextSegment());
    assertEquals(0, preemptionCount.get());
  }

  @Test
  void testRetriesAllocationOnceWithoutNestedPreemption() {
    HeapMemorySegmentPool delegate = new HeapMemorySegmentPool(PAGE_SIZE, PAGE_SIZE);
    delegate.nextSegment();
    AtomicInteger preemptionCount = new AtomicInteger();
    AtomicReference<PreemptiveMemorySegmentPool> poolReference = new AtomicReference<>();
    PreemptiveMemorySegmentPool pool = new PreemptiveMemorySegmentPool(delegate, ownerId -> {
      preemptionCount.incrementAndGet();
      assertNull(poolReference.get().nextSegment(), "nested allocation must not trigger preemption");
      return true;
    });
    poolReference.set(pool);

    pool.setCurrentOwner("bucket-0");
    try {
      assertNull(pool.nextSegment(), "allocation should fail when the callback returns no pages");
    } finally {
      pool.clearCurrentOwner();
    }
    assertEquals(1, preemptionCount.get());
  }

  @Test
  void testResetsPreemptionStateAfterCallbackFailure() {
    HeapMemorySegmentPool delegate = new HeapMemorySegmentPool(PAGE_SIZE, PAGE_SIZE);
    delegate.nextSegment();
    AtomicInteger preemptionCount = new AtomicInteger();
    PreemptiveMemorySegmentPool pool = new PreemptiveMemorySegmentPool(delegate, ownerId -> {
      if (preemptionCount.incrementAndGet() == 1) {
        throw new IllegalStateException("reclamation failed");
      }
      return false;
    });

    pool.setCurrentOwner("bucket-0");
    try {
      assertThrows(IllegalStateException.class, pool::nextSegment);
      assertNull(pool.nextSegment(), "a later allocation should be allowed to invoke the callback again");
    } finally {
      pool.clearCurrentOwner();
    }
    assertEquals(2, preemptionCount.get());
  }

  @Test
  void testCloseReleasesManagedMemoryDelegate() throws Exception {
    int numPages = 3;
    MemoryManager memoryManager = MemoryManager.create((long) PAGE_SIZE * numPages, PAGE_SIZE);
    LazyMemorySegmentPool delegate =
        new LazyMemorySegmentPool(new Object(), memoryManager, numPages);
    PreemptiveMemorySegmentPool pool = new PreemptiveMemorySegmentPool(delegate, ownerId -> false);
    List<MemorySegment> allocatedSegments = new ArrayList<>();

    try {
      try {
        for (int i = 0; i < numPages; i++) {
          MemorySegment segment = pool.nextSegment();
          assertNotNull(segment);
          allocatedSegments.add(segment);
        }
        assertEquals(0, pool.freePages());
        pool.returnAll(allocatedSegments);
      } finally {
        pool.close();
      }
      assertTrue(memoryManager.verifyEmpty(), "closing the wrapper should release managed pages");
    } finally {
      memoryManager.shutdown();
    }
  }
}
