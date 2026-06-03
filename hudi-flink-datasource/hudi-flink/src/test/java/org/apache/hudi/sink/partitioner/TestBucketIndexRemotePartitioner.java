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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.RemotePartitionHelper;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link BucketIndexRemotePartitioner}.
 */
class TestBucketIndexRemotePartitioner {

  @Test
  void testGetRemotePartitionDelegatesWithNormalizedPartitionPath() throws Exception {
    RemotePartitionHelper remotePartitionHelper = mock(RemotePartitionHelper.class);
    NumBucketsFunction numBucketsFunction = new NumBucketsFunction(8);

    when(remotePartitionHelper.getPartition(8, "", 2, 10)).thenReturn(5);

    int partition = BucketIndexRemotePartitioner.getRemotePartition(
        remotePartitionHelper, numBucketsFunction, null, 2, 10);

    assertEquals(5, partition);
    verify(remotePartitionHelper).getPartition(8, "", 2, 10);
  }

  @Test
  void testGetRemotePartitionWrapsRemoteFailure() throws Exception {
    RemotePartitionHelper remotePartitionHelper = mock(RemotePartitionHelper.class);
    NumBucketsFunction numBucketsFunction = new NumBucketsFunction(4);

    when(remotePartitionHelper.getPartition(4, "par1", 1, 6)).thenThrow(new Exception("remote unavailable"));

    HoodieException error = assertThrows(HoodieException.class, () ->
        BucketIndexRemotePartitioner.getRemotePartition(remotePartitionHelper, numBucketsFunction, "par1", 1, 6));

    assertEquals("Get remote partition failed.", error.getMessage());
    assertEquals("remote unavailable", error.getCause().getMessage());
  }

  @Test
  void testGetRemotePartitionRejectsNegativePartition() throws Exception {
    RemotePartitionHelper remotePartitionHelper = mock(RemotePartitionHelper.class);
    NumBucketsFunction numBucketsFunction = new NumBucketsFunction(4);

    when(remotePartitionHelper.getPartition(4, "par1", 1, 6)).thenReturn(-1);

    HoodieException error = assertThrows(HoodieException.class, () ->
        BucketIndexRemotePartitioner.getRemotePartition(remotePartitionHelper, numBucketsFunction, "par1", 1, 6));

    assertEquals("Get remote partition succeeded, but the subtask id is negative: -1", error.getMessage());
  }

  @Test
  void testPartitionUsesRemotePartition() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 8);
    RemotePartitionHelper remotePartitionHelper = mock(RemotePartitionHelper.class);
    HoodieKey key = new HoodieKey("id1", null);
    int currentBucket = BucketIdentifier.getBucketId(key.getRecordKey(), "id", 8);

    when(remotePartitionHelper.getPartition(8, "", currentBucket, 16)).thenReturn(11);

    BucketIndexRemotePartitioner<HoodieKey> partitioner = new BucketIndexRemotePartitioner<>(conf, "id");
    setRemotePartitionHelper(partitioner, remotePartitionHelper);

    assertEquals(11, partitioner.partition(key, 16));
  }

  private void setRemotePartitionHelper(
      BucketIndexRemotePartitioner<HoodieKey> partitioner,
      RemotePartitionHelper remotePartitionHelper) throws Exception {
    Field field = BucketIndexRemotePartitioner.class.getDeclaredField("remotePartitionHelper");
    field.setAccessible(true);
    field.set(partitioner, remotePartitionHelper);
  }
}
