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

package org.apache.hudi.metadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestFullTextIndexUtils {

  private static String repeat(char c, int n) {
    char[] chars = new char[n];
    Arrays.fill(chars, c);
    return new String(chars);
  }

  static Stream<Arguments> goldenTokens() {
    return Stream.of(
        Arguments.of(null, Collections.emptyList()),
        Arguments.of("", Collections.emptyList()),
        Arguments.of("   \t\n", Collections.emptyList()),
        Arguments.of("disk full on node-7", Arrays.asList("disk", "full", "on", "node", "7")),
        Arguments.of("Disk DISK disk", Collections.singletonList("disk")),
        Arguments.of("a,b;c.d!e?f(g)h", Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h")),
        Arguments.of("price$10\\off", Arrays.asList("price", "10", "off")),
        Arguments.of("Ünïcödé Straße 東京 42", Arrays.asList("ünïcödé", "straße", "東京", "42")),
        Arguments.of("ab_cd-ef", Arrays.asList("ab", "cd", "ef")),
        Arguments.of(repeat('x', 64) + " keep", Arrays.asList(repeat('x', 64), "keep")),
        Arguments.of(repeat('y', 65) + " keep", Collections.singletonList("keep")),
        // 22 three-byte characters = 66 UTF-8 bytes: dropped by byte length, not char count.
        Arguments.of(repeat('東', 22) + " keep", Collections.singletonList("keep")));
  }

  @ParameterizedTest
  @MethodSource("goldenTokens")
  void testTokenizeGolden(String input, List<String> expected) {
    assertEquals(expected, new ArrayList<>(FullTextIndexUtils.tokenize(input)));
  }

  @Test
  void testKeysSortApartAndRoundTrip() {
    String unit = FullTextIndexUtils.baseUnit("20260924063417000");
    String fileId = "1f0e2d3c-4b5a-6978-8f9e-0a1b2c3d4e5f-0";
    String presence = FullTextIndexUtils.presenceKey("disk", fileId, unit);
    String positions = FullTextIndexUtils.positionsKey("disk", fileId, unit);
    assertEquals("P$disk$" + fileId + "$b:20260924063417000", presence);
    assertTrue(presence.startsWith(FullTextIndexUtils.presencePrefix("disk")));
    assertFalse(presence.startsWith(FullTextIndexUtils.presencePrefix("dis")));
    assertFalse(positions.startsWith(FullTextIndexUtils.presencePrefix("disk")));
    assertArrayEquals(new String[] {"disk", fileId, unit}, FullTextIndexUtils.parseKey(presence));
    assertArrayEquals(new String[] {"disk", fileId, unit}, FullTextIndexUtils.parseKey(positions));
    assertThrows(IllegalArgumentException.class, () -> FullTextIndexUtils.parseKey("disk$x"));
    String marker = FullTextIndexUtils.markerKey(fileId, unit);
    assertEquals("U$" + fileId + "$b:20260924063417000", marker);
    assertFalse(marker.startsWith("P$") || marker.startsWith("B$"));
  }

  @Test
  void testBitmapRoundTrip() {
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(0, 3, 70000, 1_000_000);
    assertEquals(bitmap, FullTextIndexUtils.deserialize(FullTextIndexUtils.serialize(bitmap)));
    assertEquals(new RoaringBitmap(), FullTextIndexUtils.deserialize(FullTextIndexUtils.serialize(new RoaringBitmap())));
  }
}
