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

package org.apache.hudi.common.table.log;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestTable.readLastLineFromResourceFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for {@link LogReaderUtils}
 */
public class TestLogReaderUtils {
  @Test
  public void testEncodeAndDecodePositions() throws IOException {
    Set<Long> positions = generatePositions();
    String content = LogReaderUtils.encodePositions(positions);
    Roaring64NavigableMap roaring64NavigableMap = LogReaderUtils.decodeRecordPositionsHeader(content);
    assertPositionEquals(positions, roaring64NavigableMap);
  }

  @Test
  public void testEncodeBitmapAndDecodePositions() throws IOException {
    Roaring64NavigableMap positionBitmap = new Roaring64NavigableMap();
    Set<Long> positions = generatePositions();
    positions.forEach(positionBitmap::add);
    String content = LogReaderUtils.encodePositions(positionBitmap);
    Roaring64NavigableMap roaring64NavigableMap = LogReaderUtils.decodeRecordPositionsHeader(content);
    assertPositionEquals(positions, roaring64NavigableMap);
  }

  @Test
  public void testCompatibilityOfDecodingPositions() throws IOException {
    Set<Long> expectedPositions = Arrays.stream(
            readLastLineFromResourceFile("/format/expected_record_positions.data").split(","))
        .map(Long::parseLong).collect(Collectors.toSet());
    String content = readLastLineFromResourceFile("/format/record_positions_header_v3.data");
    Roaring64NavigableMap roaring64NavigableMap = LogReaderUtils.decodeRecordPositionsHeader(content);
    assertPositionEquals(expectedPositions, roaring64NavigableMap);
  }

  public static Set<Long> generatePositions() {
    Random random = new Random(0x2023);
    Set<Long> positions = new HashSet<>();
    while (positions.size() < 1000) {
      long pos = Math.abs(random.nextLong() % 1_000_000_000_000L);
      positions.add(pos);
    }
    return positions;
  }

  public static void assertPositionEquals(Set<Long> expectedPositions,
                                          Roaring64NavigableMap roaring64NavigableMap) {
    List<Long> sortedExpectedPositions =
        expectedPositions.stream().sorted().collect(Collectors.toList());
    Iterator<Long> expectedIterator = sortedExpectedPositions.iterator();
    Iterator<Long> iterator = roaring64NavigableMap.iterator();
    while (expectedIterator.hasNext() && iterator.hasNext()) {
      assertEquals(expectedIterator.next(), iterator.next());
    }
    assertFalse(expectedIterator.hasNext());
    assertFalse(iterator.hasNext());
  }
}
