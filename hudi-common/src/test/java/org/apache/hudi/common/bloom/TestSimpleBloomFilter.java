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

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link SimpleBloomFilter}.
 */
public class TestSimpleBloomFilter {

  @Test
  public void testAddAndMightContain() {
    SimpleBloomFilter filter = new SimpleBloomFilter(1000, 0.000001, Hash.MURMUR_HASH);
    filter.add("key1");
    filter.add("key2");

    assertTrue(filter.mightContain("key1"));
    assertTrue(filter.mightContain("key2"));
  }

  @Test
  public void testMightContainReturnsFalseForKeyNeverAdded() {
    SimpleBloomFilter filter = new SimpleBloomFilter(1000, 0.000001, Hash.MURMUR_HASH);
    filter.add("key1");
    // With a low error rate and small key set, an unrelated key should not be reported as present.
    assertFalse(filter.mightContain("totally-different-key-xyz"));
  }

  @Test
  public void testAddNullBytesThrows() {
    SimpleBloomFilter filter = new SimpleBloomFilter(1000, 0.000001, Hash.MURMUR_HASH);
    assertThrows(NullPointerException.class, () -> filter.add((byte[]) null));
  }

  @Test
  public void testMightContainNullThrows() {
    SimpleBloomFilter filter = new SimpleBloomFilter(1000, 0.000001, Hash.MURMUR_HASH);
    assertThrows(NullPointerException.class, () -> filter.mightContain(null));
  }

  @Test
  public void testSerializeToStringRoundTrip() {
    SimpleBloomFilter filter = new SimpleBloomFilter(1000, 0.000001, Hash.MURMUR_HASH);
    filter.add("key1");
    filter.add("key2");

    String serialized = filter.serializeToString();
    SimpleBloomFilter deserialized = new SimpleBloomFilter(serialized);

    assertTrue(deserialized.mightContain("key1"));
    assertTrue(deserialized.mightContain("key2"));
  }

  @Test
  public void testByteBufferConstructorRoundTrip() {
    SimpleBloomFilter filter = new SimpleBloomFilter(1000, 0.000001, Hash.MURMUR_HASH);
    filter.add("key1");

    String serialized = filter.serializeToString();
    ByteBuffer byteBuffer = ByteBuffer.wrap(serialized.getBytes());
    SimpleBloomFilter deserialized = new SimpleBloomFilter(byteBuffer);

    assertTrue(deserialized.mightContain("key1"));
  }

  @Test
  public void testGetBloomFilterTypeCode() {
    SimpleBloomFilter filter = new SimpleBloomFilter(1000, 0.000001, Hash.MURMUR_HASH);
    assertEquals(BloomFilterTypeCode.SIMPLE, filter.getBloomFilterTypeCode());
  }

  @Test
  public void testOrMergesTwoFilters() {
    SimpleBloomFilter filter1 = new SimpleBloomFilter(1000, 0.000001, Hash.MURMUR_HASH);
    filter1.add("key1");
    SimpleBloomFilter filter2 = new SimpleBloomFilter(1000, 0.000001, Hash.MURMUR_HASH);
    filter2.add("key2");

    filter1.or(filter2);

    assertTrue(filter1.mightContain("key1"));
    assertTrue(filter1.mightContain("key2"));
  }

  @Test
  public void testOrWithNullFilterIsNoOp() {
    SimpleBloomFilter filter1 = new SimpleBloomFilter(1000, 0.000001, Hash.MURMUR_HASH);
    filter1.add("key1");
    filter1.or(null);
    assertTrue(filter1.mightContain("key1"));
  }

  @Test
  public void testOrWithIncompatibleFilterTypeThrows() {
    SimpleBloomFilter filter1 = new SimpleBloomFilter(1000, 0.000001, Hash.MURMUR_HASH);
    BloomFilter dynamicFilter = new HoodieDynamicBoundedBloomFilter(1000, 0.000001, Hash.MURMUR_HASH, 10000);

    assertThrows(IllegalArgumentException.class, () -> filter1.or(dynamicFilter));
  }
}
