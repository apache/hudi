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

package org.apache.hudi.client.utils;

import org.apache.hudi.config.HoodieMemoryConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.apache.hudi.config.HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FRACTION_FOR_COMPACTION;
import static org.apache.hudi.config.HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FRACTION_FOR_MERGE;
import static org.apache.hudi.config.HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP;
import static org.apache.hudi.config.HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_MERGE_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSparkConfigUtils {
  @TempDir
  public java.nio.file.Path basePath;

  @Test
  public void testMaxMemoryPerPartitionMergeWithMaxSizeDefined() {
    String path = basePath.toString();

    long mergeMaxSize = 1000;
    long compactionMaxSize = 1000;

    HoodieMemoryConfig memoryConfig = HoodieMemoryConfig.newBuilder().withMaxMemoryMaxSize(mergeMaxSize, compactionMaxSize).build();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(path).withMemoryConfig(memoryConfig).build();

    assertEquals(mergeMaxSize, SparkConfigUtils.getMaxMemoryPerPartitionMerge(config.getProps()));
    assertEquals(compactionMaxSize, SparkConfigUtils.getMaxMemoryPerCompaction(config.getProps()));
  }

  @Test
  public void testMaxMemoryPerPartitionMergeInDefault() {
    String path = basePath.toString();

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(path).build();

    String compactionFraction = config.getProps().getProperty(MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP, DEFAULT_MAX_MEMORY_FRACTION_FOR_COMPACTION);
    long compactionMaxSize = SparkConfigUtils.getMaxMemoryAllowedForMerge(compactionFraction);

    String mergeFraction = config.getProps().getProperty(MAX_MEMORY_FRACTION_FOR_MERGE_PROP, DEFAULT_MAX_MEMORY_FRACTION_FOR_MERGE);
    long mergeMaxSize = SparkConfigUtils.getMaxMemoryAllowedForMerge(mergeFraction);

    assertEquals(mergeMaxSize, SparkConfigUtils.getMaxMemoryPerPartitionMerge(config.getProps()));
    assertEquals(compactionMaxSize, SparkConfigUtils.getMaxMemoryPerCompaction(config.getProps()));
  }
}
