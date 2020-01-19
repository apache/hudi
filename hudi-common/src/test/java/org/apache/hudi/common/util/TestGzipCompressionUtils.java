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

package org.apache.hudi.common.util;

import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.bloom.filter.BloomFilter;
import org.apache.hudi.common.bloom.filter.SimpleBloomFilter;

import org.apache.hadoop.util.hash.Hash;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class TestGzipCompressionUtils {

  @Test
  public void testCompressDeCompressString() {
    String expected = HoodieCommonTestHarness.generateRandomString(2048);
    String compressed = GzipCompressionUtils.compress(expected);
    String decompressed = GzipCompressionUtils.decompress(compressed);
    Assert.assertEquals(expected, decompressed);
  }

  @Test
  public void testCompressDeCompressBloomFilter() {
    String bloomFilterString = generateBloomIndexString();
    String compressed = GzipCompressionUtils.compress(bloomFilterString);
    String decompressed = GzipCompressionUtils.decompress(compressed);
    Assert.assertEquals(bloomFilterString.length(), decompressed.length());
    // Ensure Bloom Filter can be formed with decompressed value
    BloomFilter bloomFilter = new SimpleBloomFilter(decompressed);
    Assert.assertEquals(bloomFilterString, decompressed);
  }

  private static String generateBloomIndexString() {
    int maxNumEntries = 100000;
    BloomFilter bloomFilter = new SimpleBloomFilter(60000, 0.000000001, Hash.MURMUR_HASH);
    for (int i = 0; i < (int)(maxNumEntries * 0.25); i++) {
      bloomFilter.add(UUID.randomUUID().toString());
    }
    return bloomFilter.serializeToString();
  }
}
