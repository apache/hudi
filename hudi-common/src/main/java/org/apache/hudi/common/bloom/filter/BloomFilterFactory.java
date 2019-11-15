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

/**
 * A Factory class to generate different versions of {@link BloomFilter}
 */
public class BloomFilterFactory {

  /**
   * Creates a new {@link BloomFilter} with the given args
   *
   * @param numEntries total number of entries
   * @param errorRate max allowed error rate
   * @param bloomFilterTypeCode bloom filter type code
   * @return the {@link BloomFilter} thus created
   */
  public static BloomFilter createBloomFilter(int numEntries, double errorRate, String bloomFilterTypeCode) {
    return createBloomFilter(numEntries, errorRate, Hash.MURMUR_HASH, bloomFilterTypeCode);
  }

  /**
   * Creates a new {@link BloomFilter} with the given args
   *
   * @param numEntries total number of entries
   * @param errorRate max allowed error rate
   * @param hashType type of the hashing function (see {@link org.apache.hadoop.util.hash.Hash}).
   * @param bloomFilterTypeCode bloom filter type code
   * @return the {@link BloomFilter} thus created
   */
  public static BloomFilter createBloomFilter(int numEntries, double errorRate, int hashType, String bloomFilterTypeCode) {
    if (bloomFilterTypeCode.equals(SimpleBloomFilter.TYPE_CODE)) {
      return new SimpleBloomFilter(numEntries, errorRate, hashType);
    } else if (bloomFilterTypeCode.contains(HoodieDynamicBloomFilter.TYPE_CODE_PREFIX)) {
      return new HoodieDynamicBloomFilter(numEntries, errorRate, hashType);
    } else {
      throw new IllegalArgumentException("Bloom Filter type code not recognizable " + bloomFilterTypeCode);
    }
  }

  /**
   * Generate {@link BloomFilter} from serialized String
   *
   * @param serString the serialized string of the {@link BloomFilter}
   * @param bloomFilterTypeCode bloom filter type code
   * @return the {@link BloomFilter} thus generated from the passed in serialized string
   */
  public static BloomFilter fromString(String serString, String bloomFilterTypeCode) {
    if (bloomFilterTypeCode.equals(SimpleBloomFilter.TYPE_CODE)) {
      return new SimpleBloomFilter(serString);
    } else if (bloomFilterTypeCode.contains(HoodieDynamicBloomFilter.TYPE_CODE_PREFIX)) {
      return new HoodieDynamicBloomFilter(serString, bloomFilterTypeCode);
    } else {
      throw new IllegalArgumentException("Bloom Filter type code not recognizable " + bloomFilterTypeCode);
    }
  }
}
