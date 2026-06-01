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

package org.apache.hudi.common.config;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_LOG_APPEND;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MIN_MEMORY_FOR_LOG_APPEND_BUFFER_IN_BYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HoodieMemoryConfig} — focused on the log-append fraction config added for
 * dynamic per-task memory capping of {@code HoodieAppendHandle}'s in-memory buffer.
 */
class TestHoodieMemoryConfig {

  /** Default fraction matches the merge/compaction defaults so out-of-the-box behavior is consistent. */
  @Test
  void testLogAppendFractionDefault() {
    HoodieMemoryConfig memoryConfig = HoodieMemoryConfig.newBuilder()
        .fromProperties(new Properties())
        .build();

    assertEquals(0.6, memoryConfig.getDouble(MAX_MEMORY_FRACTION_FOR_LOG_APPEND),
        "default log-append fraction is 0.6 — matches merge/compaction defaults");
  }

  /** User-supplied fraction via Properties overrides the default. */
  @Test
  void testLogAppendFractionOverrideViaProperties() {
    Properties props = new Properties();
    props.setProperty(MAX_MEMORY_FRACTION_FOR_LOG_APPEND.key(), "0.42");
    HoodieMemoryConfig memoryConfig = HoodieMemoryConfig.newBuilder()
        .fromProperties(props)
        .build();

    assertEquals(0.42, memoryConfig.getDouble(MAX_MEMORY_FRACTION_FOR_LOG_APPEND),
        "user-supplied fraction overrides the default");
  }

  /**
   * Pins the 16MB floor that bounds the dynamic per-task ceiling on very small executors —
   * smaller than the 100MB spillable-map floor because many append handles can be active
   * concurrently in a single task and we tolerate small blocks on tight executors over OOM.
   */
  @Test
  void testLogAppendBufferFloorIs16MB() {
    assertEquals(16 * 1024 * 1024L, MIN_MEMORY_FOR_LOG_APPEND_BUFFER_IN_BYTES,
        "log-append buffer floor pinned at 16MB — see HoodieAppendHandle effectiveBlockSize derivation");
  }
}
