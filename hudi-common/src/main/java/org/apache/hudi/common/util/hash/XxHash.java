/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.util.hash;

/**
 * Pure Java implementation of xxHash64, a very fast non-cryptographic hash function.
 * <p>
 * xxHash is designed for speed, exploiting instruction-level parallelism.
 * It is typically 2-4x faster than MurmurHash while maintaining excellent
 * distribution properties.
 * <p>
 * This implementation is based on the xxHash specification and reference implementation
 * by Yann Collet. xxHash is released under BSD 2-Clause license.
 *
 * @see <a href="https://xxhash.com/">xxHash</a>
 * @see <a href="https://github.com/Cyan4973/xxHash">xxHash Reference Implementation</a>
 */
public class XxHash extends Hash {
  private static final XxHash INSTANCE = new XxHash();

  // xxHash64 primes
  private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
  private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
  private static final long PRIME64_3 = 0x165667B19E3779F9L;
  private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
  private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

  public static Hash getInstance() {
    return INSTANCE;
  }

  @Override
  public int hash(byte[] data, int length, int seed) {
    // Use the seed as a long for xxHash64
    long hash64 = hash64(data, 0, length, seed & 0xFFFFFFFFL);
    // Return lower 32 bits for compatibility with the Hash interface
    return (int) hash64;
  }

  /**
   * Computes the 64-bit xxHash of the given data.
   *
   * @param data   input byte array
   * @param offset starting offset in the array
   * @param length number of bytes to hash
   * @param seed   seed value
   * @return 64-bit hash value
   */
  public static long hash64(byte[] data, int offset, int length, long seed) {
    long hash;
    int remaining = length;
    int idx = offset;

    if (length >= 32) {
      // Initialize accumulators
      long acc1 = seed + PRIME64_1 + PRIME64_2;
      long acc2 = seed + PRIME64_2;
      long acc3 = seed;
      long acc4 = seed - PRIME64_1;

      // Process 32-byte blocks
      while (remaining >= 32) {
        acc1 = round(acc1, getLong(data, idx));
        acc2 = round(acc2, getLong(data, idx + 8));
        acc3 = round(acc3, getLong(data, idx + 16));
        acc4 = round(acc4, getLong(data, idx + 24));
        idx += 32;
        remaining -= 32;
      }

      // Merge accumulators
      hash = Long.rotateLeft(acc1, 1)
          + Long.rotateLeft(acc2, 7)
          + Long.rotateLeft(acc3, 12)
          + Long.rotateLeft(acc4, 18);

      hash = mergeAccumulator(hash, acc1);
      hash = mergeAccumulator(hash, acc2);
      hash = mergeAccumulator(hash, acc3);
      hash = mergeAccumulator(hash, acc4);
    } else {
      hash = seed + PRIME64_5;
    }

    hash += length;

    // Process remaining 8-byte blocks
    while (remaining >= 8) {
      long k1 = getLong(data, idx);
      k1 *= PRIME64_2;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= PRIME64_1;
      hash ^= k1;
      hash = Long.rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
      idx += 8;
      remaining -= 8;
    }

    // Process remaining 4-byte block
    if (remaining >= 4) {
      hash ^= (getInt(data, idx) & 0xFFFFFFFFL) * PRIME64_1;
      hash = Long.rotateLeft(hash, 23) * PRIME64_2 + PRIME64_3;
      idx += 4;
      remaining -= 4;
    }

    // Process remaining bytes
    while (remaining > 0) {
      hash ^= (data[idx] & 0xFF) * PRIME64_5;
      hash = Long.rotateLeft(hash, 11) * PRIME64_1;
      idx++;
      remaining--;
    }

    // Final avalanche
    return avalanche(hash);
  }

  private static long round(long acc, long input) {
    acc += input * PRIME64_2;
    acc = Long.rotateLeft(acc, 31);
    acc *= PRIME64_1;
    return acc;
  }

  private static long mergeAccumulator(long hash, long acc) {
    acc = round(0, acc);
    hash ^= acc;
    hash = hash * PRIME64_1 + PRIME64_4;
    return hash;
  }

  private static long avalanche(long hash) {
    hash ^= hash >>> 33;
    hash *= PRIME64_2;
    hash ^= hash >>> 29;
    hash *= PRIME64_3;
    hash ^= hash >>> 32;
    return hash;
  }

  /**
   * Reads a little-endian 64-bit long from the byte array.
   */
  private static long getLong(byte[] data, int index) {
    return (data[index] & 0xFFL)
        | ((data[index + 1] & 0xFFL) << 8)
        | ((data[index + 2] & 0xFFL) << 16)
        | ((data[index + 3] & 0xFFL) << 24)
        | ((data[index + 4] & 0xFFL) << 32)
        | ((data[index + 5] & 0xFFL) << 40)
        | ((data[index + 6] & 0xFFL) << 48)
        | ((data[index + 7] & 0xFFL) << 56);
  }

  /**
   * Reads a little-endian 32-bit int from the byte array.
   */
  private static int getInt(byte[] data, int index) {
    return (data[index] & 0xFF)
        | ((data[index + 1] & 0xFF) << 8)
        | ((data[index + 2] & 0xFF) << 16)
        | ((data[index + 3] & 0xFF) << 24);
  }
}