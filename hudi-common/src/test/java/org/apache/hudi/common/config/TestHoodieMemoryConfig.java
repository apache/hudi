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

package org.apache.hudi.common.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_COMPACTION;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_COMPACTION;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.WRITESTATUS_FAILURE_FRACTION;

class TestHoodieMemoryConfig {
  private static final double MAX_MEMORY_FRACTION_FOR_MERGE_TEST = 0.23;
  private static final long MAX_MEMORY_FOR_MERGE_TEST = 0;
  private static final long MAX_MEMORY_FOR_COMPACTION_TEST = 1;
  private static final double MAX_MEMORY_FRACTION_FOR_COMPACTION_TEST = 0.35;

  private static final int MAX_DFS_STREAM_BUFFER_SIZE_TEST = 20;
  private static final double WRITESTATUS_FAILURE_FRACTION_TEST = 0.5;

  @Test
  void testBuilder() {
    HoodieMemoryConfig memoryConfig = HoodieMemoryConfig.newBuilder()
        .fromProperties(new Properties())
        .withMaxMemoryFractionPerPartitionMerge(MAX_MEMORY_FRACTION_FOR_MERGE_TEST)
        .withMaxMemoryMaxSize(MAX_MEMORY_FOR_MERGE_TEST, MAX_MEMORY_FOR_COMPACTION_TEST)
        .withMaxMemoryFractionPerCompaction(MAX_MEMORY_FRACTION_FOR_COMPACTION_TEST)
        .withMaxDFSStreamBufferSize(MAX_DFS_STREAM_BUFFER_SIZE_TEST)
        .withWriteStatusFailureFraction(WRITESTATUS_FAILURE_FRACTION_TEST)
        .build();

    Assertions.assertEquals(MAX_MEMORY_FRACTION_FOR_MERGE_TEST, memoryConfig.getDouble(MAX_MEMORY_FRACTION_FOR_MERGE));
    Assertions.assertEquals(MAX_MEMORY_FOR_MERGE_TEST, memoryConfig.getLong(MAX_MEMORY_FOR_MERGE));
    Assertions.assertEquals(MAX_MEMORY_FOR_COMPACTION_TEST, memoryConfig.getDouble(MAX_MEMORY_FOR_COMPACTION));
    Assertions.assertEquals(MAX_MEMORY_FRACTION_FOR_COMPACTION_TEST, memoryConfig.getDouble(MAX_MEMORY_FRACTION_FOR_COMPACTION));
    Assertions.assertEquals(MAX_DFS_STREAM_BUFFER_SIZE_TEST, memoryConfig.getInt(MAX_DFS_STREAM_BUFFER_SIZE));
    Assertions.assertEquals(WRITESTATUS_FAILURE_FRACTION_TEST, memoryConfig.getDouble(WRITESTATUS_FAILURE_FRACTION));
  }
}