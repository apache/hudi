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
 * A Bloom filter interface.
 */
public interface BloomFilter {

  /**
   * Add a key represented by a {@link String} to the {@link BloomFilter}.
   *
   * @param key the key to the added to the {@link BloomFilter}
   */
  void add(String key);

  /**
   * Add a key's bytes, representing UTF8-encoded string, to the {@link BloomFilter}.
   *
   * @param key the key bytes to the added to the {@link BloomFilter}
   */
  void add(byte[] key);

  /**
   * Tests for key membership.
   *
   * @param key the key to be checked for membership
   * @return {@code true} if key may be found, {@code false} if key is not found for sure.
   */
  boolean mightContain(String key);

  /**
   * Serialize the bloom filter as a string.
   */
  String serializeToString();

  /**
   * @return the bloom index type code.
   **/
  BloomFilterTypeCode getBloomFilterTypeCode();

  /**
   * Performs a logical OR operations with other BloomFilter.
   * @param other
   */
  void or(BloomFilter other);
}
