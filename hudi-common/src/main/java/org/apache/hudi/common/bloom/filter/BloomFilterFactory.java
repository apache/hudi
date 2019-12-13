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
 * A Factory class to generate different versions of {@link BloomFilter}.
 */
public class BloomFilterFactory {

  /**
   * Creates a new {@link BloomFilter} with the given args.
   *
   * @param numEntries                 total number of entries
   * @param errorRate                  max allowed error rate
   * @param bloomFilterTypeCodeOrdinal bloom filter type code ordinal as string
   * @return the {@link BloomFilter} thus created
   */
  public static BloomFilter createBloomFilter(int numEntries, double errorRate, int maxNumberOfEntries,
                                              String bloomFilterTypeCodeOrdinal) {
    int ordinal = Integer.parseInt(bloomFilterTypeCodeOrdinal);
    if (ordinal < BloomFilterTypeCode.values().length) {
      BloomFilterTypeCode typeCode = BloomFilterTypeCode.values()[ordinal];
      if (typeCode == BloomFilterTypeCode.SIMPLE) {
        return new SimpleBloomFilter(numEntries, errorRate, Hash.MURMUR_HASH);
      } else if (typeCode == BloomFilterTypeCode.DYNAMIC_V0) {
        return new HoodieDynamicBoundedBloomFilter(numEntries, errorRate, Hash.MURMUR_HASH, maxNumberOfEntries);
      } else {
        throw new IllegalArgumentException("Bloom Filter type code not recognizable " + typeCode);
      }
    } else {
      throw new IllegalArgumentException("Bloom Filter type code not recognizable " + bloomFilterTypeCodeOrdinal);
    }
  }

  /**
   * Generate {@link BloomFilter} from serialized String.
   *
   * @param serString                  the serialized string of the {@link BloomFilter}
   * @param bloomFilterTypeCodeOrdinal bloom filter type code ordinal as string
   * @return the {@link BloomFilter} thus generated from the passed in serialized string
   */
  public static BloomFilter fromString(String serString, String bloomFilterTypeCodeOrdinal) {
    int ordinal = Integer.parseInt(bloomFilterTypeCodeOrdinal);
    if (ordinal < BloomFilterTypeCode.values().length) {
      BloomFilterTypeCode typeCode = BloomFilterTypeCode.values()[ordinal];
      if (typeCode == BloomFilterTypeCode.SIMPLE) {
        return new SimpleBloomFilter(serString);
      } else if (typeCode == BloomFilterTypeCode.DYNAMIC_V0) {
        return new HoodieDynamicBoundedBloomFilter(serString, typeCode);
      } else {
        throw new IllegalArgumentException("Bloom Filter type code not recognizable " + typeCode);
      }
    } else {
      throw new IllegalArgumentException("Bloom Filter type code not recognizable " + bloomFilterTypeCodeOrdinal);
    }
  }
}
