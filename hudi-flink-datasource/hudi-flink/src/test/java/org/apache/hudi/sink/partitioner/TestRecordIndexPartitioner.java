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
import org.apache.hudi.common.util.hash.BucketIndexUtil;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for partitioned record level index routing in {@link RecordIndexPartitioner}.
 */
public class TestRecordIndexPartitioner {
  private static final int FILE_GROUP_COUNT = 3;
  private static final int NUM_PARTITIONS = 4;

  @TempDir
  File tempFile;

  @Test
  void testNewPartitionUsesMinFileGroupCount() throws Exception {
    RecordIndexPartitioner partitioner = newPartitioner();
    String recordKey = "record_key";

    assertEquals(
        expectedPartition(recordKey, "par1"),
        partitioner.partition(new HoodieKey(recordKey, "par1"), NUM_PARTITIONS));
    assertEquals(
        expectedPartition(recordKey, "par2"),
        partitioner.partition(new HoodieKey(recordKey, "par2"), NUM_PARTITIONS));
  }

  @Test
  void testPartitionConsistency() throws Exception {
    RecordIndexPartitioner partitioner = newPartitioner();
    HoodieKey key = new HoodieKey("consistent_test_key", "partition_path");

    int expectedPartition = partitioner.partition(key, NUM_PARTITIONS);
    for (int i = 0; i < 10; i++) {
      assertEquals(expectedPartition, partitioner.partition(key, NUM_PARTITIONS));
    }
  }

  @Test
  void testPartitionWithinRange() throws Exception {
    RecordIndexPartitioner partitioner = newPartitioner();

    int partition1 = partitioner.partition(new HoodieKey("record_key", "par1"), NUM_PARTITIONS);
    int partition2 = partitioner.partition(new HoodieKey("record_key", "par2"), NUM_PARTITIONS);
    int partition3 = partitioner.partition(new HoodieKey("record_key", "par3"), NUM_PARTITIONS);

    assertTrue(partition1 >= 0 && partition1 < NUM_PARTITIONS);
    assertTrue(partition2 >= 0 && partition2 < NUM_PARTITIONS);
    assertTrue(partition3 >= 0 && partition3 < NUM_PARTITIONS);

    assertNotEquals(partition1, partition2);
    assertNotEquals(partition1, partition3);
  }

  @Test
  void testSinglePartition() throws Exception {
    RecordIndexPartitioner partitioner = newPartitioner();

    assertEquals(0, partitioner.partition(new HoodieKey("any_key", "par1"), 1));
  }

  private RecordIndexPartitioner newPartitioner() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.RECORD_LEVEL_INDEX.name());
    conf.setString(HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), String.valueOf(FILE_GROUP_COUNT));
    StreamerUtil.initTableIfNotExists(conf);
    return new RecordIndexPartitioner(conf);
  }

  private int expectedPartition(String recordKey, String partitionPath) {
    int recordIndexFileGroup = HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(recordKey, FILE_GROUP_COUNT);
    return BucketIndexUtil.getPartitionIndexFunc(NUM_PARTITIONS).apply(FILE_GROUP_COUNT, partitionPath, recordIndexFileGroup);
  }
}
