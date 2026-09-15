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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link InternalBloomFilter}, pinning the serialized byte layout (bit {@code i} at
 * byte {@code i >> 3} under mask {@code 1 << (i & 7)}, the format shared with Hadoop's
 * {@code BloomFilter}) against a {@link BitSet}-based oracle and pre-captured golden strings.
 */
public class TestInternalBloomFilter {

  private static final String SIMPLE_GOLDEN = "/////wAAAAoBAAACz+cFPAf5vcr6feFygnZHoFAOwLY7/WznVO6WN9QF7fMmM1m+zXrzC9ICIvydFz8bNEUfQN/L"
      + "vtxjs9bkOxNklqSWK6H6aacyEc1SNA0+iZW9Ae0xTLgQp2k6dg==";
  private static final String DYNAMIC_GOLDEN = "/////wAAAAoBAAABIAAAABQAAAA8AAAAA/////8AAAAKAQAAASBSsn9fX63/7M15X+Rmc+75/96eez/Tdme7//nv"
      + "1JuP6WZX/tv/////AAAACgEAAAEg6fC+36p6fdz/Lr98Ppl+/QvlV2rfp5+9/Pm358cj1x/f/Hes/////wAAAAoB"
      + "AAABINvt/Ha3Ped32/7//3Wtp+3rLH2o/08eqTXZa/u/j77vpvzdvg==";

  @Test
  public void testSerializedBitsMatchBitSetOracle() throws IOException {
    int[][] configs = {{63, 3}, {64, 3}, {65, 3}, {127, 5}, {128, 5}, {1000, 7}, {43133, 30}};
    for (int[] config : configs) {
      int vectorSize = config[0];
      int nbHash = config[1];
      InternalBloomFilter filter = new InternalBloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);
      HashFunction hashFunction = new HashFunction(vectorSize, nbHash, Hash.MURMUR_HASH);
      BitSet oracle = new BitSet(vectorSize);
      Random random = new Random(vectorSize);
      for (int i = 0; i < 200; i++) {
        Key key = randomKey(random);
        filter.add(key);
        for (int pos : hashFunction.hash(key)) {
          oracle.set(pos);
        }
        assertTrue(filter.membershipTest(key));
      }
      assertArrayEquals(oracleBits(vectorSize, oracle), serializedBits(filter),
          "Serialized bits mismatch for vectorSize=" + vectorSize);
      for (int i = 0; i < 200; i++) {
        Key key = randomKey(random);
        boolean oracleContains = Arrays.stream(hashFunction.hash(key)).allMatch(oracle::get);
        assertEquals(oracleContains, filter.membershipTest(key),
            "Membership mismatch for vectorSize=" + vectorSize);
      }
    }
  }

  @Test
  public void testWriteReadFieldsRoundTrip() throws IOException {
    for (int vectorSize : new int[] {63, 64, 65, 127, 128, 1000}) {
      InternalBloomFilter filter = new InternalBloomFilter(vectorSize, 3, Hash.MURMUR_HASH);
      Random random = new Random(vectorSize);
      Key[] keys = new Key[100];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = randomKey(random);
        filter.add(keys[i]);
      }
      byte[] serialized = serialize(filter);
      InternalBloomFilter deserialized = new InternalBloomFilter();
      deserialized.readFields(new DataInputStream(new ByteArrayInputStream(serialized)));
      for (Key key : keys) {
        assertTrue(deserialized.membershipTest(key));
      }
      assertArrayEquals(serialized, serialize(deserialized),
          "Round-trip bytes mismatch for vectorSize=" + vectorSize);
    }
  }

  @Test
  public void testReadFieldsIgnoresUnusedTrailingBits() throws IOException {
    int vectorSize = 61;
    InternalBloomFilter filter = new InternalBloomFilter(vectorSize, 3, Hash.MURMUR_HASH);
    Random random = new Random(vectorSize);
    Key[] keys = new Key[50];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = randomKey(random);
      filter.add(keys[i]);
    }
    byte[] serialized = serialize(filter);
    byte[] mutated = Arrays.copyOf(serialized, serialized.length);
    // The last byte carries bits 56..60; bits 61..63 are beyond vectorSize and must be ignored.
    mutated[mutated.length - 1] |= (byte) 0xE0;
    InternalBloomFilter deserialized = new InternalBloomFilter();
    deserialized.readFields(new DataInputStream(new ByteArrayInputStream(mutated)));
    for (Key key : keys) {
      assertTrue(deserialized.membershipTest(key));
    }
    assertArrayEquals(serialized, serialize(deserialized), "Unused trailing bits must not survive a round trip");
  }

  @Test
  public void testBitwiseOpsMatchBitSetOracle() throws IOException {
    int vectorSize = 127;
    int nbHash = 5;
    HashFunction hashFunction = new HashFunction(vectorSize, nbHash, Hash.MURMUR_HASH);
    Random random = new Random(42);
    InternalBloomFilter first = new InternalBloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);
    InternalBloomFilter second = new InternalBloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);
    BitSet firstOracle = new BitSet(vectorSize);
    BitSet secondOracle = new BitSet(vectorSize);
    for (int i = 0; i < 100; i++) {
      Key key = randomKey(random);
      first.add(key);
      for (int pos : hashFunction.hash(key)) {
        firstOracle.set(pos);
      }
      key = randomKey(random);
      second.add(key);
      for (int pos : hashFunction.hash(key)) {
        secondOracle.set(pos);
      }
    }

    InternalBloomFilter orFilter = copy(first);
    orFilter.or(second);
    BitSet orOracle = (BitSet) firstOracle.clone();
    orOracle.or(secondOracle);
    assertArrayEquals(oracleBits(vectorSize, orOracle), serializedBits(orFilter));

    InternalBloomFilter andFilter = copy(first);
    andFilter.and(second);
    BitSet andOracle = (BitSet) firstOracle.clone();
    andOracle.and(secondOracle);
    assertArrayEquals(oracleBits(vectorSize, andOracle), serializedBits(andFilter));

    InternalBloomFilter xorFilter = copy(first);
    xorFilter.xor(second);
    BitSet xorOracle = (BitSet) firstOracle.clone();
    xorOracle.xor(secondOracle);
    assertArrayEquals(oracleBits(vectorSize, xorOracle), serializedBits(xorFilter));

    InternalBloomFilter notFilter = copy(first);
    notFilter.not();
    BitSet notOracle = (BitSet) firstOracle.clone();
    notOracle.flip(0, vectorSize);
    assertArrayEquals(oracleBits(vectorSize, notOracle), serializedBits(notFilter));
  }

  @Test
  public void testSerializedStringGoldens() {
    BloomFilter simple = BloomFilterFactory.createBloomFilter(
        50, 0.001, -1, BloomFilterTypeCode.SIMPLE.name());
    for (int i = 0; i < 50; i++) {
      simple.add(goldenKey(i));
    }
    assertEquals(SIMPLE_GOLDEN, simple.serializeToString());
    BloomFilter simpleFromGolden = BloomFilterFactory.fromString(SIMPLE_GOLDEN, BloomFilterTypeCode.SIMPLE.name());
    for (int i = 0; i < 50; i++) {
      assertTrue(simpleFromGolden.mightContain(goldenKey(i)));
    }

    BloomFilter dynamic = BloomFilterFactory.createBloomFilter(
        20, 0.001, 60, BloomFilterTypeCode.DYNAMIC_V0.name());
    for (int i = 0; i < 100; i++) {
      dynamic.add(goldenKey(i));
    }
    assertEquals(DYNAMIC_GOLDEN, dynamic.serializeToString());
    BloomFilter dynamicFromGolden = BloomFilterFactory.fromString(DYNAMIC_GOLDEN, BloomFilterTypeCode.DYNAMIC_V0.name());
    for (int i = 0; i < 100; i++) {
      assertTrue(dynamicFromGolden.mightContain(goldenKey(i)));
    }
  }

  private static String goldenKey(int i) {
    return String.format("key-%03d", i);
  }

  private static Key randomKey(Random random) {
    byte[] keyBytes = new byte[1 + random.nextInt(40)];
    random.nextBytes(keyBytes);
    return new Key(keyBytes);
  }

  /** Serializes the filter and strips the 13-byte header (version, nbHash, hashType, vectorSize). */
  private static byte[] serializedBits(InternalBloomFilter filter) throws IOException {
    byte[] serialized = serialize(filter);
    return Arrays.copyOfRange(serialized, 13, serialized.length);
  }

  private static byte[] serialize(InternalBloomFilter filter) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    filter.write(new DataOutputStream(baos));
    return baos.toByteArray();
  }

  /** Packs the oracle bits with the Hadoop BloomFilter byte layout: bit i at byte i >> 3, mask 1 << (i & 7). */
  private static byte[] oracleBits(int vectorSize, BitSet bits) {
    byte[] bytes = new byte[(vectorSize + 7) / 8];
    for (int i = 0; i < vectorSize; i++) {
      if (bits.get(i)) {
        bytes[i >> 3] |= (byte) (1 << (i & 7));
      }
    }
    return bytes;
  }

  private static InternalBloomFilter copy(InternalBloomFilter filter) throws IOException {
    InternalBloomFilter copied = new InternalBloomFilter();
    copied.readFields(new DataInputStream(new ByteArrayInputStream(serialize(filter))));
    return copied;
  }
}
