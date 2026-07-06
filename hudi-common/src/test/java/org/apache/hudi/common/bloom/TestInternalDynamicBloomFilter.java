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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link InternalDynamicBloomFilter} for size bounding.
 */
public class TestInternalDynamicBloomFilter {

  @Test
  public void testBoundedSize() {

    int[] batchSizes = {1000, 10000, 10000, 100000, 100000, 10000};
    int indexForMaxGrowth = 3;
    int maxSize = batchSizes[0] * 100;
    BloomFilter filter = new HoodieDynamicBoundedBloomFilter(batchSizes[0], 0.000001, Hash.MURMUR_HASH, maxSize);
    int index = 0;
    int lastKnownBloomSize = 0;
    while (index < batchSizes.length) {
      for (int i = 0; i < batchSizes[index]; i++) {
        String key = UUID.randomUUID().toString();
        filter.add(key);
      }

      String serString = filter.serializeToString();
      if (index != 0) {
        int curLength = serString.length();
        if (index > indexForMaxGrowth) {
          assertEquals(curLength, lastKnownBloomSize, "Length should not increase after hitting max entries");
        } else {
          assertTrue(curLength > lastKnownBloomSize, "Length should increase until max entries are reached");
        }
      }
      lastKnownBloomSize = serString.length();
      index++;
    }
  }

  @Test
  public void testInternalDynamicBloomFilterRescale() {
    HoodieDynamicBoundedBloomFilter filter = new HoodieDynamicBoundedBloomFilter(1000,
        0.000001, Hash.MURMUR_HASH, 10000);
    assertEquals(1, filter.getMatrixLength());
    HoodieDynamicBoundedBloomFilter rescaledToSize2Filter = filter.rescaleFromTarget(2);
    assertEquals(2, rescaledToSize2Filter.getMatrixLength());
    HoodieDynamicBoundedBloomFilter rescaledToSize4Filter = rescaledToSize2Filter.rescaleFromTarget(4);
    assertEquals(4, rescaledToSize4Filter.getMatrixLength());
  }

  @Test
  public void testAddNullKeyThrows() {
    InternalDynamicBloomFilter filter = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    assertThrows(NullPointerException.class, () -> filter.add((Key) null));
  }

  @Test
  public void testMembershipTestNullKeyReturnsTrue() {
    InternalDynamicBloomFilter filter = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    assertTrue(filter.membershipTest(null));
  }

  @Test
  public void testAddGrowsMatrixAcrossNumberOfKeys() {
    int keysPerRow = 10;
    InternalDynamicBloomFilter filter = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, keysPerRow, 1000);
    assertEquals(1, filter.getMatrixLength());

    for (int i = 0; i < keysPerRow; i++) {
      filter.add(new Key(("key" + i).getBytes()));
    }
    // The first row is now full but growth is deferred until the next add.
    assertEquals(1, filter.getMatrixLength());

    filter.add(new Key("one-more-key".getBytes()));
    assertEquals(2, filter.getMatrixLength());
  }

  @Test
  public void testMembershipTestFindsAddedKeyAcrossRows() {
    InternalDynamicBloomFilter filter = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 2, 1000);
    for (int i = 0; i < 20; i++) {
      filter.add(new Key(("key" + i).getBytes()));
    }
    assertTrue(filter.getMatrixLength() > 1);
    for (int i = 0; i < 20; i++) {
      assertTrue(filter.membershipTest(new Key(("key" + i).getBytes())));
    }
  }

  @Test
  public void testAndThrowsForIncompatibleFilter() {
    InternalDynamicBloomFilter filter = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    InternalDynamicBloomFilter differentVectorSize = new InternalDynamicBloomFilter(500, 5, Hash.MURMUR_HASH, 10, 1000);
    assertThrows(IllegalArgumentException.class, () -> filter.and(differentVectorSize));
  }

  @Test
  public void testAndKeepsCommonKey() {
    InternalDynamicBloomFilter filter1 = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    InternalDynamicBloomFilter filter2 = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    Key commonKey = new Key("common-key".getBytes());
    filter1.add(commonKey);
    filter2.add(commonKey);

    filter1.and(filter2);

    assertTrue(filter1.membershipTest(commonKey));
  }

  @Test
  public void testOrThrowsForIncompatibleFilter() {
    InternalDynamicBloomFilter filter = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    InternalDynamicBloomFilter differentNbHash = new InternalDynamicBloomFilter(1000, 6, Hash.MURMUR_HASH, 10, 1000);
    assertThrows(IllegalArgumentException.class, () -> filter.or(differentNbHash));
  }

  @Test
  public void testOrMergesKeysFromBothFilters() {
    InternalDynamicBloomFilter filter1 = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    InternalDynamicBloomFilter filter2 = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    Key key1 = new Key("key1".getBytes());
    Key key2 = new Key("key2".getBytes());
    filter1.add(key1);
    filter2.add(key2);

    filter1.or(filter2);

    assertTrue(filter1.membershipTest(key1));
    assertTrue(filter1.membershipTest(key2));
  }

  @Test
  public void testXorThrowsForIncompatibleFilter() {
    InternalDynamicBloomFilter filter = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    InternalDynamicBloomFilter differentNr = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 20, 1000);
    assertThrows(IllegalArgumentException.class, () -> filter.xor(differentNr));
  }

  @Test
  public void testXorWithSelfClearsAllBits() {
    InternalDynamicBloomFilter filter = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    Key key = new Key("key1".getBytes());
    filter.add(key);
    assertTrue(filter.membershipTest(key));

    filter.xor(filter);

    assertFalse(filter.membershipTest(key));
  }

  @Test
  public void testNotInvertsEmptyFilterToAlwaysContain() {
    InternalDynamicBloomFilter filter = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    Key key = new Key("never-added".getBytes());
    assertFalse(filter.membershipTest(key));

    filter.not();

    assertTrue(filter.membershipTest(key));
  }

  @Test
  public void testAddRowsWithNonPositiveSizeIsNoOp() {
    InternalDynamicBloomFilter filter = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    filter.addRows(0);
    assertEquals(1, filter.getMatrixLength());
    filter.addRows(-1);
    assertEquals(1, filter.getMatrixLength());
  }

  @Test
  public void testAddRowsWithMultipleRows() {
    InternalDynamicBloomFilter filter = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    filter.addRows(3);
    assertEquals(4, filter.getMatrixLength());
  }

  @Test
  public void testToStringContainsOneEntryPerRow() {
    InternalDynamicBloomFilter filter = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 10, 1000);
    filter.addRows(2);
    assertEquals(3, filter.getMatrixLength());

    // Each row's underlying InternalBloomFilter renders its bit set as a "{...}" block,
    // so the number of opening braces reflects the number of rows toString() iterated over.
    String str = filter.toString();
    long rowBlockCount = str.chars().filter(c -> c == '{').count();
    assertEquals(3, rowBlockCount);
  }

  @Test
  public void testWriteAndReadFieldsRoundTrip() throws IOException {
    InternalDynamicBloomFilter original = new InternalDynamicBloomFilter(1000, 5, Hash.MURMUR_HASH, 2, 1000);
    for (int i = 0; i < 10; i++) {
      original.add(new Key(("key" + i).getBytes()));
    }
    assertTrue(original.getMatrixLength() > 1);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    original.write(new DataOutputStream(baos));

    InternalDynamicBloomFilter deserialized = new InternalDynamicBloomFilter();
    deserialized.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

    assertEquals(original.getMatrixLength(), deserialized.getMatrixLength());
    for (int i = 0; i < 10; i++) {
      assertTrue(deserialized.membershipTest(new Key(("key" + i).getBytes())));
    }
  }
}
