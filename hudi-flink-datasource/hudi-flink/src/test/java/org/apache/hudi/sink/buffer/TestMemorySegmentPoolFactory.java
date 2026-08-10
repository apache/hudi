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

import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link MemorySegmentPoolFactory}.
 */
public class TestMemorySegmentPoolFactory {

  @Test
  public void testCreateHeapMemoryPool() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.WRITE_MEMORY_SEGMENT_PAGE_SIZE, 32 * 1024);

    MemorySegmentPoolFactory factory = new MemorySegmentPoolFactory(null, null, 0);
    MemorySegmentPool pool = factory.createMemorySegmentPool(conf, 1024 * 1024L);

    assertNotNull(pool);
    assertTrue(pool instanceof HeapMemorySegmentPool);
  }

  @Test
  public void testCreateMultipleHeapMemoryPools() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.WRITE_MEMORY_SEGMENT_PAGE_SIZE, 32 * 1024);

    MemorySegmentPoolFactory factory = new MemorySegmentPoolFactory(null, null, 0);
    MemorySegmentPool[] pools = factory.createMemorySegmentPools(conf, 4, 4 * 1024 * 1024L);

    assertNotNull(pools);
    assertEquals(4, pools.length);
    for (MemorySegmentPool pool : pools) {
      assertNotNull(pool);
      assertTrue(pool instanceof HeapMemorySegmentPool);
    }
  }

  @Test
  public void testCreateManagedMemoryPool() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.WRITE_MEMORY_SEGMENT_PAGE_SIZE, 32 * 1024);

    MemoryManager memoryManager = mock(MemoryManager.class);
    when(memoryManager.getPageSize()).thenReturn(32 * 1024);

    Object owner = new Object();
    long managedMemorySize = 2 * 1024 * 1024L;

    MemorySegmentPoolFactory factory = new MemorySegmentPoolFactory(owner, memoryManager, managedMemorySize);
    MemorySegmentPool pool = factory.createMemorySegmentPool(conf, 1024 * 1024L);

    assertNotNull(pool);
    assertTrue(pool instanceof LazyMemorySegmentPool);
  }

  @Test
  public void testCreateMultipleManagedMemoryPools() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.WRITE_MEMORY_SEGMENT_PAGE_SIZE, 32 * 1024);

    MemoryManager memoryManager = mock(MemoryManager.class);
    when(memoryManager.getPageSize()).thenReturn(32 * 1024);

    Object owner = new Object();
    long managedMemorySize = 4 * 1024 * 1024L;

    MemorySegmentPoolFactory factory = new MemorySegmentPoolFactory(owner, memoryManager, managedMemorySize);
    MemorySegmentPool[] pools = factory.createMemorySegmentPools(conf, 4, 1024 * 1024L);

    assertNotNull(pools);
    assertEquals(4, pools.length);
    for (MemorySegmentPool pool : pools) {
      assertNotNull(pool);
      assertTrue(pool instanceof LazyMemorySegmentPool);
    }
  }
}

