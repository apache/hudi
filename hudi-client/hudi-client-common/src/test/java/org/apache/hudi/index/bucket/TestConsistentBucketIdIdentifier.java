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

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.model.HoodieConsistentHashingMetadata.HASH_VALUE_MASK;

/**
 * Unit test of consistent bucket identifier
 */
public class TestConsistentBucketIdIdentifier {

  @Test
  public void testGetBucket() {
    List<ConsistentHashingNode> nodes = Arrays.asList(
        new ConsistentHashingNode(100, "0"),
        new ConsistentHashingNode(0x2fffffff, "1"),
        new ConsistentHashingNode(0x4fffffff, "2"));
    HoodieConsistentHashingMetadata meta = new HoodieConsistentHashingMetadata((short) 0, "", "", 3, 0, nodes);
    ConsistentBucketIdentifier identifier = new ConsistentBucketIdentifier(meta);

    Assertions.assertEquals(3, identifier.getNumBuckets());

    // Get bucket by hash keys
    Assertions.assertEquals(nodes.get(2), identifier.getBucket(Arrays.asList("Hudi")));
    Assertions.assertEquals(nodes.get(1), identifier.getBucket(Arrays.asList("bucket_index")));
    Assertions.assertEquals(nodes.get(1), identifier.getBucket(Arrays.asList("consistent_hashing")));
    Assertions.assertEquals(nodes.get(1), identifier.getBucket(Arrays.asList("bucket_index", "consistent_hashing")));
    int[] ref1 = {2, 2, 1, 1, 0, 1, 1, 1, 0, 1};
    int[] ref2 = {1, 0, 1, 0, 1, 1, 1, 0, 1, 2};
    for (int i = 0; i < 10; ++i) {
      Assertions.assertEquals(nodes.get(ref1[i]), identifier.getBucket(Arrays.asList(Integer.toString(i))));
      Assertions.assertEquals(nodes.get(ref2[i]), identifier.getBucket(Arrays.asList(Integer.toString(i), Integer.toString(i + 1))));
    }

    // Get bucket by hash value
    Assertions.assertEquals(nodes.get(0), identifier.getBucket(0));
    Assertions.assertEquals(nodes.get(0), identifier.getBucket(50));
    Assertions.assertEquals(nodes.get(0), identifier.getBucket(100));
    Assertions.assertEquals(nodes.get(1), identifier.getBucket(101));
    Assertions.assertEquals(nodes.get(1), identifier.getBucket(0x1fffffff));
    Assertions.assertEquals(nodes.get(1), identifier.getBucket(0x2fffffff));
    Assertions.assertEquals(nodes.get(2), identifier.getBucket(0x40000000));
    Assertions.assertEquals(nodes.get(2), identifier.getBucket(0x40000001));
    Assertions.assertEquals(nodes.get(2), identifier.getBucket(0x4fffffff));
    Assertions.assertEquals(nodes.get(0), identifier.getBucket(0x50000000));
    Assertions.assertEquals(nodes.get(0), identifier.getBucket(HASH_VALUE_MASK));

    // Get bucket by file id
    Assertions.assertEquals(nodes.get(0), identifier.getBucketByFileId(FSUtils.createNewFileId("0", 0)));
    Assertions.assertEquals(nodes.get(1), identifier.getBucketByFileId(FSUtils.createNewFileId("1", 0)));
    Assertions.assertEquals(nodes.get(2), identifier.getBucketByFileId(FSUtils.createNewFileId("2", 0)));
  }
}
