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

package org.apache.hudi.common.bloom;

import org.apache.hudi.common.util.hash.Hash;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
          assertEquals(curLength, lastKnownBloomSize, "Length should not increase after hitting max entries");
        } else {
          assertTrue(curLength > lastKnownBloomSize, "Length should increase until max entries are reached");
        }
      }
      lastKnownBloomSize = serString.length();
      index++;
    }
  }

  @Test
  public void testInternalDynamicBloomFilterRescale() {
    HoodieDynamicBoundedBloomFilter filter = new HoodieDynamicBoundedBloomFilter(1000,
        0.000001, Hash.MURMUR_HASH, 10000);
    assertEquals(1, filter.getMatrixLength());
    HoodieDynamicBoundedBloomFilter rescaledToSize2Filter = filter.rescaleFromTarget(2);
    assertEquals(2, rescaledToSize2Filter.getMatrixLength());
    HoodieDynamicBoundedBloomFilter rescaledToSize4Filter = rescaledToSize2Filter.rescaleFromTarget(4);
    assertEquals(4, rescaledToSize4Filter.getMatrixLength());
  }
}
