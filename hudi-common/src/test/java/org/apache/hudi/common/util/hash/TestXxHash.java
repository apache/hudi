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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests {@link XxHash}.
 */
public class TestXxHash {

  @Test
  public void testGetInstance() {
    Hash hash = XxHash.getInstance();
    assertNotNull(hash);
    assertEquals(hash, XxHash.getInstance()); // Singleton
  }

  @Test
  public void testHashConsistency() {
    XxHash hash = (XxHash) XxHash.getInstance();
    byte[] data = "test data for hashing".getBytes(StandardCharsets.UTF_8);

    int hash1 = hash.hash(data, data.length, 0);
    int hash2 = hash.hash(data, data.length, 0);

    assertEquals(hash1, hash2, "Same input should produce same hash");
  }

  @Test
  public void testDifferentSeedsProduceDifferentHashes() {
    XxHash hash = (XxHash) XxHash.getInstance();
    byte[] data = "test data".getBytes(StandardCharsets.UTF_8);

    int hash1 = hash.hash(data, data.length, 0);
    int hash2 = hash.hash(data, data.length, 42);

    assertNotEquals(hash1, hash2, "Different seeds should produce different hashes");
  }

  @Test
  public void testDifferentDataProducesDifferentHashes() {
    XxHash hash = (XxHash) XxHash.getInstance();
    byte[] data1 = "hello".getBytes(StandardCharsets.UTF_8);
    byte[] data2 = "world".getBytes(StandardCharsets.UTF_8);

    int hash1 = hash.hash(data1, data1.length, 0);
    int hash2 = hash.hash(data2, data2.length, 0);

    assertNotEquals(hash1, hash2, "Different data should produce different hashes");
  }

  @Test
  public void testPartialDataHashing() {
    XxHash hash = (XxHash) XxHash.getInstance();
    byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);

    int fullHash = hash.hash(data, data.length, 0);
    int partialHash = hash.hash(data, 5, 0); // Only hash "hello"

    assertNotEquals(fullHash, partialHash, "Partial data should produce different hash");
  }

  @Test
  public void testHashTypeRegistration() {
    assertEquals(Hash.XX_HASH, 2, "XX_HASH constant should be 2");
    assertEquals(Hash.XX_HASH, Hash.parseHashType("xxhash"));
    assertEquals(Hash.XX_HASH, Hash.parseHashType("XXHASH"));
    assertEquals(Hash.XX_HASH, Hash.parseHashType("XxHash"));

    Hash hash = Hash.getInstance(Hash.XX_HASH);
    assertNotNull(hash);
    assertEquals(XxHash.class, hash.getClass());
  }

  @Test
  public void testEmptyData() {
    // Test hash64 for empty data with seed 0
    // Reference: xxHash64 of empty string with seed 0 = 0xEF46DB3751D8E999
    long hash64 = XxHash.hash64(new byte[0], 0, 0, 0);
    assertEquals(0xEF46DB3751D8E999L, hash64, "Empty string hash with seed 0 should match reference");
  }

  @Test
  public void testKnownTestVectors() {
    // Test vectors from xxHash reference implementation
    // These are standard test vectors to verify correctness

    // Test with seed 0
    byte[] testData = new byte[256];
    for (int i = 0; i < testData.length; i++) {
      testData[i] = (byte) i;
    }

    // Reference values for xxHash64 with seed 0:
    // Length 0: 0xEF46DB3751D8E999
    // Length 1 (byte 0): 0xE934A84ADB052768
    // Length 4: 0x9136A0DCA57457EE
    assertEquals(0xEF46DB3751D8E999L, XxHash.hash64(testData, 0, 0, 0), "Length 0 hash mismatch");
    assertEquals(0xE934A84ADB052768L, XxHash.hash64(testData, 0, 1, 0), "Length 1 hash mismatch");
    assertEquals(0x9136A0DCA57457EEL, XxHash.hash64(testData, 0, 4, 0), "Length 4 hash mismatch");
  }

  @Test
  public void testLargeData() {
    XxHash hash = (XxHash) XxHash.getInstance();
    byte[] data = new byte[10000];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 256);
    }

    int hash1 = hash.hash(data, data.length, 0);
    int hash2 = hash.hash(data, data.length, 0);

    assertEquals(hash1, hash2, "Large data hashing should be consistent");
  }

  @Test
  public void testHashDistribution() {
    // Test that xxHash produces well-distributed hash values
    XxHash hash = (XxHash) XxHash.getInstance();
    int numSamples = 1000;
    int[] hashValues = new int[numSamples];

    for (int i = 0; i < numSamples; i++) {
      byte[] data = String.format("key_%d", i).getBytes(StandardCharsets.UTF_8);
      hashValues[i] = hash.hash(data, data.length, 0);
    }

    // Check for collisions - with good distribution, we should have few collisions
    Set<Integer> uniqueHashes = new HashSet<>();
    for (int h : hashValues) {
      uniqueHashes.add(h);
    }

    // Expect very high uniqueness with xxHash
    double uniquenessRatio = (double) uniqueHashes.size() / numSamples;
    assertEquals(1.0, uniquenessRatio, 0.01, "xxHash should have near-perfect uniqueness for 1000 distinct keys");
  }

  @Test
  public void testHash64Direct() {
    // Test the 64-bit hash method directly
    byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
    long hash64 = XxHash.hash64(data, 0, data.length, 0);

    // Should be consistent
    long hash64Again = XxHash.hash64(data, 0, data.length, 0);
    assertEquals(hash64, hash64Again, "hash64 should be consistent");

    // Different seed should give different result
    long hash64WithSeed = XxHash.hash64(data, 0, data.length, 12345);
    assertNotEquals(hash64, hash64WithSeed, "Different seeds should produce different hashes");
  }

  @Test
  public void testBlockProcessing() {
    // Test data that exercises the 32-byte block processing path
    byte[] largeData = new byte[100];
    for (int i = 0; i < largeData.length; i++) {
      largeData[i] = (byte) (i * 17);  // Some non-trivial pattern
    }

    long hash = XxHash.hash64(largeData, 0, largeData.length, 0);
    long hashAgain = XxHash.hash64(largeData, 0, largeData.length, 0);
    assertEquals(hash, hashAgain, "Block processing should be consistent");
  }

  @Test
  public void testVariousLengths() {
    // Test various data lengths to exercise all code paths
    byte[] data = new byte[64];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    // Test lengths that exercise different paths
    int[] lengths = {0, 1, 3, 4, 7, 8, 15, 16, 31, 32, 33, 64};
    for (int len : lengths) {
      long hash1 = XxHash.hash64(data, 0, len, 0);
      long hash2 = XxHash.hash64(data, 0, len, 0);
      assertEquals(hash1, hash2, "Length " + len + " should produce consistent hashes");
    }
  }
}