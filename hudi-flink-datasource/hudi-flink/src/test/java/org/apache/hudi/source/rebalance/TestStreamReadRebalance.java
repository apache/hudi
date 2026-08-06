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

package org.apache.hudi.source.rebalance;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.source.rebalance.partitioner.StreamReadAppendPartitioner;
import org.apache.hudi.source.rebalance.partitioner.StreamReadBucketIndexPartitioner;
import org.apache.hudi.source.rebalance.selector.StreamReadAppendKeySelector;
import org.apache.hudi.source.rebalance.selector.StreamReadBucketIndexKeySelector;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestStreamReadRebalance {

  @Test
  void testAppendSelectorAndPartitionerUseSplitNumber() throws Exception {
    MergeOnReadInputSplit split = newSplit(7, "partition", "00000003-file");

    Integer key = new StreamReadAppendKeySelector().getKey(split);
    assertEquals(7, key);
    assertEquals(3, new StreamReadAppendPartitioner(4).partition(key, 128));
  }

  @Test
  void testBucketSelectorAndPartitionerUsePartitionAndFileId() throws Exception {
    String partition = "partition=par1";
    String fileId = BucketIdentifier.newBucketFileIdPrefix(3);
    MergeOnReadInputSplit split = newSplit(1, partition, fileId);
    Pair<String, String> key = new StreamReadBucketIndexKeySelector().getKey(split);

    assertEquals(Pair.of(partition, fileId), key);

    Configuration conf = new Configuration();
    conf.set(FlinkOptions.READ_TASKS, 4);
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 8);
    StreamReadBucketIndexPartitioner partitioner = new StreamReadBucketIndexPartitioner(conf);
    int first = partitioner.partition(key, 128);
    int second = partitioner.partition(key, 128);

    assertEquals(first, second);
    assertTrue(first >= 0 && first < 4);
  }

  private static MergeOnReadInputSplit newSplit(int splitNumber, String partition, String fileId) {
    return new MergeOnReadInputSplit(
        splitNumber,
        null,
        Option.empty(),
        "001",
        "/tmp/table",
        1024,
        "payload_combine",
        null,
        fileId,
        partition);
  }
}
