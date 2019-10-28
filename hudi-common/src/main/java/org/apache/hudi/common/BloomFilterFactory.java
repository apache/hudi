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
package org.apache.hudi.common;

import org.apache.hadoop.util.hash.Hash;

/**
 * A Factory class to generate different versions of {@link BloomFilter}
 */
public class BloomFilterFactory {

  /**
   * Creates a new {@link BloomFilter} with the given args
   * @param numEntries total number of entries
   * @param errorRate max allowed error rate
   * @param bloomFilterVersion bloom filter version
   * @return the {@link BloomFilter} thus created
   */
  public static BloomFilter createBloomFilter(int numEntries, double errorRate, int bloomFilterVersion) {
        return createBloomFilter(numEntries, errorRate, Hash.MURMUR_HASH, bloomFilterVersion);
  }

  /**
   * Creates a new {@link BloomFilter} with the given args
   * @param numEntries total number of entries
   * @param errorRate max allowed error rate
   * @param hashType type of the hashing function (see
   * {@link org.apache.hadoop.util.hash.Hash}).
   * @param bloomFilterVersion bloom filter version
   * @return the {@link BloomFilter} thus created
   */
  public static BloomFilter createBloomFilter(int numEntries, double errorRate, int hashType, int bloomFilterVersion) {
    if(bloomFilterVersion == SimpleBloomFilter.VERSION){
      return new SimpleBloomFilter(numEntries, errorRate, hashType);
    } else if(bloomFilterVersion == HudiDynamicBloomFilter.VERSION){
      return new HudiDynamicBloomFilter(numEntries, errorRate, hashType);
    } else{
      throw new IllegalArgumentException("Bloom Filter version not recognizable "+ bloomFilterVersion);
    }
  }

  /**
   * Generate {@link BloomFilter} from serialized String
   * @param serString the serialized string of the {@link BloomFilter}
   * @param bloomFilterVersion bloom filter version
   * @return the {@link BloomFilter} thus generated from the passed in serialized string
   */
  public static BloomFilter getBloomFilterFromSerializedString(String serString, int bloomFilterVersion){
    if(bloomFilterVersion == SimpleBloomFilter.VERSION){
      return new SimpleBloomFilter(serString);
    } else if(bloomFilterVersion == HudiDynamicBloomFilter.VERSION){
      return new HudiDynamicBloomFilter(serString);
    } else{
      throw new IllegalArgumentException("Bloom Filter version not recognizable "+ bloomFilterVersion);
    }
  }
}
