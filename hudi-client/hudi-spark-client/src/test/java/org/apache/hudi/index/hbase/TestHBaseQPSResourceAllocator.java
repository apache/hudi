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

package org.apache.hudi.index.hbase;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieHBaseIndexConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHBaseQPSResourceAllocator {

  @Test
  public void testsDefaultQPSResourceAllocator() {
    HoodieWriteConfig config = getConfig(Option.empty());
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
    HBaseIndexQPSResourceAllocator hBaseIndexQPSResourceAllocator = index.createQPSResourceAllocator(config);
    assertEquals(hBaseIndexQPSResourceAllocator.getClass().getName(),
        DefaultHBaseQPSResourceAllocator.class.getName());
    assertEquals(config.getHbaseIndexQPSFraction(),
        hBaseIndexQPSResourceAllocator.acquireQPSResources(config.getHbaseIndexQPSFraction(), 100), 0.0f);
  }

  @Test
  public void testsExplicitDefaultQPSResourceAllocator() {
    HoodieWriteConfig config = getConfig(Option.of(HoodieHBaseIndexConfig.QPS_ALLOCATOR_CLASS_NAME.defaultValue()));
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
    HBaseIndexQPSResourceAllocator hBaseIndexQPSResourceAllocator = index.createQPSResourceAllocator(config);
    assertEquals(hBaseIndexQPSResourceAllocator.getClass().getName(),
        DefaultHBaseQPSResourceAllocator.class.getName());
    assertEquals(config.getHbaseIndexQPSFraction(),
        hBaseIndexQPSResourceAllocator.acquireQPSResources(config.getHbaseIndexQPSFraction(), 100), 0.0f);
  }

  @Test
  public void testsInvalidQPSResourceAllocator() {
    HoodieWriteConfig config = getConfig(Option.of("InvalidResourceAllocatorClassName"));
    SparkHoodieHBaseIndex index = new SparkHoodieHBaseIndex(config);
    HBaseIndexQPSResourceAllocator hBaseIndexQPSResourceAllocator = index.createQPSResourceAllocator(config);
    assertEquals(hBaseIndexQPSResourceAllocator.getClass().getName(),
        DefaultHBaseQPSResourceAllocator.class.getName());
    assertEquals(config.getHbaseIndexQPSFraction(),
        hBaseIndexQPSResourceAllocator.acquireQPSResources(config.getHbaseIndexQPSFraction(), 100), 0.0f);
  }

  private HoodieWriteConfig getConfig(Option<String> resourceAllocatorClass) {
    HoodieHBaseIndexConfig hoodieHBaseIndexConfig = getConfigWithResourceAllocator(resourceAllocatorClass);
    return getConfigBuilder(hoodieHBaseIndexConfig).build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder(HoodieHBaseIndexConfig hoodieHBaseIndexConfig) {
    return HoodieWriteConfig.newBuilder().withPath("/foo").withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(1, 1)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024)
            .withInlineCompaction(false).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .hfileMaxFileSize(1000 * 1024).parquetMaxFileSize(1024 * 1024).build())
        .forTable("test-trip-table").withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.HBASE).withHBaseIndexConfig(hoodieHBaseIndexConfig).build());
  }

  private HoodieHBaseIndexConfig getConfigWithResourceAllocator(Option<String> resourceAllocatorClass) {
    HoodieHBaseIndexConfig.Builder builder = new HoodieHBaseIndexConfig.Builder()
        .hbaseZkPort(0)
        .hbaseZkQuorum("localhost")
        .hbaseTableName("foobar")
        .hbaseIndexGetBatchSize(100);
    if (resourceAllocatorClass.isPresent()) {
      builder.withQPSResourceAllocatorType(resourceAllocatorClass.get());
    }
    return builder.build();
  }
}
