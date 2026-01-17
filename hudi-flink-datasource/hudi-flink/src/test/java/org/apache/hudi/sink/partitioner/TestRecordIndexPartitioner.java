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

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link RecordIndexPartitioner}.
 */
public class TestRecordIndexPartitioner {

  private static RecordIndexPartitioner partitioner;

  @TempDir
  static File tempFile;

  @BeforeAll
  public static void beforeAll() throws Exception {
    final String basePath = tempFile.getAbsolutePath();
    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    conf.setString(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    partitioner = new RecordIndexPartitioner(conf);
  }

  @Test
  void testPartitionMethod() {
    // Test partitioning with different record keys
    HoodieKey key1 = new HoodieKey("record_key_1", "partition_path");
    HoodieKey key2 = new HoodieKey("record_key_2", "partition_path");
    HoodieKey key3 = new HoodieKey("another_record_key", "partition_path");
    
    int numPartitions = 10;
    
    // Test that partitioning works consistently
    int partition1 = partitioner.partition(key1, numPartitions);
    int partition2 = partitioner.partition(key2, numPartitions);
    int partition3 = partitioner.partition(key3, numPartitions);
    
    // Each partition should be within the range [0, numPartitions)
    assertTrue(partition1 >= 0 && partition1 < numPartitions);
    assertTrue(partition2 >= 0 && partition2 < numPartitions);
    assertTrue(partition3 >= 0 && partition3 < numPartitions);
    
    // Same key should always map to the same partition
    assertEquals(partitioner.partition(key1, numPartitions), partitioner.partition(key1, numPartitions));
  }

  @Test
  void testPartitionConsistency() {
    HoodieKey key = new HoodieKey("consistent_test_key", "partition_path");
    int numPartitions = 5;
    
    // Test that the same key always maps to the same partition
    int expectedPartition = partitioner.partition(key, numPartitions);
    for (int i = 0; i < 10; i++) {
      assertEquals(expectedPartition, partitioner.partition(key, numPartitions));
    }
  }

  @Test
  void testEdgeCaseSinglePartition() {
    HoodieKey key = new HoodieKey("any_key", "partition_path");
    int numPartitions = 1; // Single partition
    
    // With single partition, everything should go to partition 0
    assertEquals(0, partitioner.partition(key, numPartitions));
  }

  @Test
  void testLargeNumberOfPartitions() {
    HoodieKey key = new HoodieKey("test_key_for_large_partition", "partition_path");
    int numPartitions = 100; // Large number of partitions
    
    int partition = partitioner.partition(key, numPartitions);
    assertTrue(partition >= 0 && partition < numPartitions);
  }
}