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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.apache.flink.table.runtime.util.MemorySegmentPool;

import javax.annotation.Nullable;

/**
 * Factory to create {@code MemorySegmentPool} instances.
 * <p>
 * Supports two types of memory pools:
 * <ul>
 *   <li>Heap-based memory pool ({@code HeapMemorySegmentPool}) - uses JVM heap memory</li>
 *   <li>Flink managed memory pool ({@code LazyMemorySegmentPool}) - uses Flink's managed memory</li>
 * </ul>
 */
public class MemorySegmentPoolFactory {
  private final Option<Object> owner;
  private final Option<MemoryManager> memoryManager;
  private final long managedMemorySize;

  public MemorySegmentPoolFactory(@Nullable Object owner, @Nullable MemoryManager memoryManager, long managedMemorySize) {
    this.owner = Option.ofNullable(owner);
    this.memoryManager = Option.ofNullable(memoryManager);
    this.managedMemorySize = managedMemorySize;
  }

  public MemorySegmentPool createMemorySegmentPool(Configuration conf, long heapMemorySize) {
    return createMemorySegmentPools(conf, 1, heapMemorySize)[0];
  }

  /**
   * Creates multiple memory segment pools that share the total configured memory budget.
   *
   * @param conf the configuration
   * @param numPools number of pools to create
   * @return array of memory segment pools, each with size = totalSize / numPools
   */
  public MemorySegmentPool[] createMemorySegmentPools(Configuration conf, int numPools, long heapMemorySize) {
    if (memoryManager.isEmpty()) {
      return createHeapMemoryPools(conf, numPools, heapMemorySize);
    } else {
      return createManagedMemoryPools(numPools);
    }
  }

  private MemorySegmentPool[] createHeapMemoryPools(Configuration conf, int numPools, long heapMemorySize) {
    ValidationUtils.checkArgument(numPools > 0, "numPools must be positive");
    ValidationUtils.checkArgument(heapMemorySize > 0, "Buffer size must be positive");
    int pageSize = conf.get(FlinkOptions.WRITE_MEMORY_SEGMENT_PAGE_SIZE);
    long poolSize = heapMemorySize / numPools;
    MemorySegmentPool[] pools = new MemorySegmentPool[numPools];
    ValidationUtils.checkArgument(poolSize >= pageSize,
        String.format("The total size %s of memory pool should not be less than page size %s.", poolSize, pageSize));
    for (int i = 0; i < numPools; i++) {
      pools[i] = new HeapMemorySegmentPool(pageSize, poolSize);
    }
    return pools;
  }

  private MemorySegmentPool[] createManagedMemoryPools(int numPools) {
    long poolSize = managedMemorySize / numPools;
    MemorySegmentPool[] pools = new MemorySegmentPool[numPools];
    int pageSize = memoryManager.get().getPageSize();
    ValidationUtils.checkArgument(poolSize >= pageSize,
        String.format("The total size %s of memory pool should not be less than page size %s.", poolSize, pageSize));
    for (int i = 0; i < numPools; i++) {
      pools[i] = new LazyMemorySegmentPool(owner.get(), memoryManager.get(), (int) (poolSize / pageSize));
    }
    return pools;
  }
}
