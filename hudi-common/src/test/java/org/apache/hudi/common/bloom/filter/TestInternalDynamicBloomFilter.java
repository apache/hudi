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

package org.apache.hudi.common.bloom.filter;

import org.apache.hadoop.util.hash.Hash;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Unit tests {@link InternalDynamicBloomFilter} for size bounding.
 */
public class TestInternalDynamicBloomFilter {

  @Test
  public void testBoundedSize() {

    int[] batchSizes = {1000, 10000, 10000, 100000, 100000, 10000};
    int indexForMaxGrowth = 3;
    int maxSize = batchSizes[0] * 100;
    BloomFilter filter = new HoodieDynamicBoundedBloomFilter(batchSizes[0], 0.000001, Hash.MURMUR_HASH, maxSize);
    int index = 0;
    int lastKnownBloomSize = 0;
    while (index < batchSizes.length) {
      for (int i = 0; i < batchSizes[index]; i++) {
        String key = UUID.randomUUID().toString();
        filter.add(key);
      }

      String serString = filter.serializeToString();
      if (index != 0) {
        int curLength = serString.length();
        if (index > indexForMaxGrowth) {
          Assert.assertEquals("Length should not increase after hitting max entries", curLength, lastKnownBloomSize);
        } else {
          Assert.assertTrue("Length should increase until max entries are reached", curLength > lastKnownBloomSize);
        }
      }
      lastKnownBloomSize = serString.length();
      index++;
    }
  }
}
