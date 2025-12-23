/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * Manages Arrow BufferAllocator lifecycle for Arrow-based file format operations.
 *
 * <p>Following Arrow best practices:
 * <ul>
 *   <li>Single RootAllocator per application</li>
 *   <li>Named child allocators for debugging and isolation</li>
 *   <li>Caller-specified memory limits per child allocator</li>
 * </ul>
 *
 * <p>The root allocator is hardcoded to Long.MAX_VALUE and acts as a
 * bookkeeper. Memory limits are enforced at the child allocator level, with each
 * caller specifying an appropriate limit for their use case.
 *
 * @see <a href="https://arrow.apache.org/docs/java/memory.html">Arrow Memory Management</a>
 */
public class HoodieArrowAllocator {

  private HoodieArrowAllocator() {
    // Utility class
  }

  /**
   * Initialization-on-demand holder idiom for thread-safe lazy initialization.
   * The root allocator is created when first accessed and has a maximum size of Long.MAX_VALUE.
   */
  private static class RootAllocatorHolder {
    static final BufferAllocator INSTANCE = new RootAllocator(Long.MAX_VALUE);

    static {
      Runtime.getRuntime().addShutdownHook(new Thread(
              INSTANCE::close, "hudi-arrow-root-allocator-shutdown"));
    }
  }

  /**
   * Get the shared root allocator.
   * Thread-safe lazy initialization using the holder idiom.
   *
   * @return The singleton root allocator instance
   */
  private static BufferAllocator getRootAllocator() {
    return RootAllocatorHolder.INSTANCE;
  }

  /**
   * Create a named child allocator for Arrow operations.
   * Caller is responsible for closing the returned allocator.
   *
   * @param name Descriptive name for debugging (e.g., "HoodieSparkLanceReader-data-file.lance")
   * @param childSizeBytes Maximum memory size in bytes for this child allocator
   * @return A new child allocator with the specified size limit
   */
  public static BufferAllocator newChildAllocator(String name, long childSizeBytes) {
    return getRootAllocator().newChildAllocator("hudi-arrow-" + name, 0, childSizeBytes);
  }
}