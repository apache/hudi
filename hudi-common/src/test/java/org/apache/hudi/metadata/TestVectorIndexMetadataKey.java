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

package org.apache.hudi.metadata;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestVectorIndexMetadataKey {

  @Test
  void testBlockReservedAndDeltaRangesSortInSinglePrefixScan() {
    assertOrdered(
        VectorIndexMetadataKey.postingBlock(1, 7, 0, 0x00000000L),
        VectorIndexMetadataKey.postingBlock(1, 7, 0, 0x00000001L),
        VectorIndexMetadataKey.postingBlock(1, 7, 0, VectorIndexMetadataKey.MAX_PACKED_BLOCK_ID),
        VectorIndexMetadataKey.postingBlock(1, 7, 0, VectorIndexMetadataKey.FIRST_RESERVED_BLOCK_ID),
        VectorIndexMetadataKey.postingBlock(1, 7, 0, VectorIndexMetadataKey.LAST_RESERVED_BLOCK_ID),
        VectorIndexMetadataKey.postingDelta(1, 7, 0, "any"));
  }

  @Test
  void testUnsignedComponentBoundaries() {
    assertOrdered(
        VectorIndexMetadataKey.postingBlock(1, 0x7FFFFFFF, 0, 0),
        VectorIndexMetadataKey.postingBlock(1, 0x80000000, 0, 0),
        VectorIndexMetadataKey.postingBlock(1, 0xFFFFFFFF, 0, 0));
    assertOrdered(
        VectorIndexMetadataKey.postingBlock(1, 7, 0x7FFF, 0),
        VectorIndexMetadataKey.postingBlock(1, 7, 0x8000, 0));
    assertOrdered(
        VectorIndexMetadataKey.postingBlock(1, 7, 0, 0x7FFFFFFFL),
        VectorIndexMetadataKey.postingBlock(1, 7, 0, 0x80000000L));
  }

  @Test
  void testBigEndianCarryBoundaries() {
    assertOrdered(
        VectorIndexMetadataKey.postingBlock(1, 0x000000FF, 0xFFFF, VectorIndexMetadataKey.DELTA_BLOCK_ID),
        VectorIndexMetadataKey.postingBlock(1, 0x00000100, 0x0000, 0));
    assertOrdered(
        VectorIndexMetadataKey.postingBlock(1, 7, 0x00FF, VectorIndexMetadataKey.DELTA_BLOCK_ID),
        VectorIndexMetadataKey.postingBlock(1, 7, 0x0100, 0));
  }

  @Test
  void testDeltaKeyTailUtf8OrderingAndDecode() {
    String a = VectorIndexMetadataKey.postingDelta(1, 7, 0, "a");
    String aa = VectorIndexMetadataKey.postingDelta(1, 7, 0, "aa");
    String b = VectorIndexMetadataKey.postingDelta(1, 7, 0, "b");
    String eAcute = VectorIndexMetadataKey.postingDelta(1, 7, 0, "é");

    assertOrdered(a, aa, b, eAcute);
    assertEquals("é", VectorIndexMetadataKey.postingRecordKey(eAcute));
    assertEquals(7, VectorIndexMetadataKey.postingClusterId(eAcute));
    assertEquals(0, VectorIndexMetadataKey.postingShard(eAcute));
  }

  @Test
  void testFamilyMajorOrdering() {
    assertOrdered(
        VectorIndexMetadataKey.activeManifest(),
        VectorIndexMetadataKey.manifest(1),
        VectorIndexMetadataKey.manifest(2),
        VectorIndexMetadataKey.quantizer(1, 0),
        VectorIndexMetadataKey.centroids(1, 0),
        VectorIndexMetadataKey.clusterStats(1, 7),
        VectorIndexMetadataKey.postingBlock(1, 7, 0, 0),
        VectorIndexMetadataKey.sourceInstantMarker(1, "20260724010101"));
  }

  @Test
  void testSourceInstantMarkersSortWithinGenerationAndDecode() {
    String prefix = VectorIndexMetadataKey.sourceInstantMarkerPrefix(3);
    String first = VectorIndexMetadataKey.sourceInstantMarker(3, "20260724010101");
    String second = VectorIndexMetadataKey.sourceInstantMarker(3, "20260724010202");
    String nextGeneration = VectorIndexMetadataKey.sourceInstantMarker(4, "20260724000000");

    assertOrdered(prefix, first, second, nextGeneration);
    assertEquals("20260724010101", VectorIndexMetadataKey.sourceInstant(first));
    assertNull(VectorIndexMetadataKey.sourceInstant(VectorIndexMetadataKey.manifest(3)));
    assertNull(VectorIndexMetadataKey.sourceInstant(prefix));
    assertThrows(IllegalArgumentException.class,
        () -> VectorIndexMetadataKey.sourceInstantMarker(3, ""));
    assertThrows(IllegalArgumentException.class,
        () -> VectorIndexMetadataKey.sourceInstantMarker(3, null));
    assertTrue(VectorIndexMetadataKey.compareUnsigned(
        second, VectorIndexMetadataKey.exclusiveEnd(prefix)) < 0);
  }

  @Test
  void testPrefixScanExclusiveEnd() {
    String prefix = VectorIndexMetadataKey.postingPrefix(1, 7, 0);
    String end = VectorIndexMetadataKey.exclusiveEnd(prefix);

    assertTrue(VectorIndexMetadataKey.compareUnsigned(prefix, end) < 0);
    assertTrue(VectorIndexMetadataKey.compareUnsigned(
        VectorIndexMetadataKey.postingDelta(1, 7, 0, "z"), end) < 0);
    assertTrue(VectorIndexMetadataKey.compareUnsigned(
        VectorIndexMetadataKey.postingBlock(1, 7, 1, 0), end) >= 0);
    assertNull(VectorIndexMetadataKey.exclusiveEnd(new String(new byte[] {(byte) 0xFF}, StandardCharsets.ISO_8859_1)));
  }

  private static void assertOrdered(String... keys) {
    for (int i = 1; i < keys.length; i++) {
      assertTrue(VectorIndexMetadataKey.compareUnsigned(keys[i - 1], keys[i]) < 0,
          "key " + (i - 1) + " should sort before key " + i);
    }
  }
}
