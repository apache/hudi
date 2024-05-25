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

/**
 * Bloom filter utils.
 */
class BloomFilterUtils {

  /**
   * Used in computing the optimal Bloom filter size. This approximately equals 0.480453.
   */
  private static final double LOG2_SQUARED = Math.log(2) * Math.log(2);

  /**
   * @return the bitsize given the total number of entries and error rate.
   */
  static int getBitSize(int numEntries, double errorRate) {
    return (int) Math.ceil(numEntries * (-Math.log(errorRate) / LOG2_SQUARED));
  }

  /**
   * @return the number of hashes given the bitsize and total number of entries.
   */
  static int getNumHashes(int bitSize, int numEntries) {
    // Number of the hash functions
    return (int) Math.ceil(Math.log(2) * bitSize / numEntries);
  }
}
