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

package org.apache.hudi.source.split;

import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieSourceSplitSerializer}.
 */
public class TestHoodieSourceSplitSerializer {

  private final HoodieSourceSplitSerializer serializer = new HoodieSourceSplitSerializer();

  @Test
  public void testSerializeAndDeserializeBasicSplit() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        1,
        "base-path",
        Option.empty(),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-123",
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    assertNotNull(serialized);
    assertTrue(serialized.length > 0);

    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertEquals(original.getSplitNum(), deserialized.getSplitNum());
    assertEquals(original.getBasePath(), deserialized.getBasePath());
    assertEquals(original.getLogPaths(), deserialized.getLogPaths());
    assertEquals(original.getTablePath(), deserialized.getTablePath());
    assertEquals(original.getPartitionPath(), deserialized.getPartitionPath());
    assertEquals(original.getMergeType(), deserialized.getMergeType());
    assertEquals(original.getFileId(), deserialized.getFileId());
    assertEquals(original.getConsumed(), deserialized.getConsumed());
    assertEquals(original.getFileOffset(), deserialized.getFileOffset());
  }

  @Test
  public void testSerializeAndDeserializeSplitWithNullBasePath() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        2,
        null,
        Option.of(Arrays.asList("log1", "log2")),
        "/table/path",
        "/partition/path",
        "payload_combine",
        "19700101000000000",
        "file-456",
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertFalse(deserialized.getBasePath().isPresent());
    assertEquals(original.getLogPaths(), deserialized.getLogPaths());
    assertEquals(2, deserialized.getLogPaths().get().size());
  }

  @Test
  public void testSerializeAndDeserializeSplitWithLogPaths() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        3,
        "base-path",
        Option.of(Arrays.asList("log1.parquet", "log2.parquet", "log3.parquet")),
        "/table/path",
        "/partition/path",
        "payload_combine",
        "19700101000000000",
        "file-789",
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getLogPaths().isPresent());
    assertEquals(3, deserialized.getLogPaths().get().size());
    assertEquals("log1.parquet", deserialized.getLogPaths().get().get(0));
    assertEquals("log2.parquet", deserialized.getLogPaths().get().get(1));
    assertEquals("log3.parquet", deserialized.getLogPaths().get().get(2));
  }

  @Test
  public void testSerializeAndDeserializeSplitWithEmptyLogPaths() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        4,
        "base-path",
        Option.of(Collections.emptyList()),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-000",
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getLogPaths().isPresent());
    assertEquals(0, deserialized.getLogPaths().get().size());
  }

  @Test
  public void testSerializeAndDeserializeSplitWithConsumedState() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        5,
        "base-path",
        Option.empty(),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-111",
        Option.empty()
    );

    // Update position to simulate consumed state
    original.updatePosition(3, 100L);

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertEquals(3, deserialized.getFileOffset());
    assertEquals(100L, deserialized.getConsumed());
    assertTrue(deserialized.isConsumed());
  }

  @Test
  public void testSerializeAndDeserializeSplitAfterConsume() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        6,
        "base-path",
        Option.empty(),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-222",
        Option.empty()
    );

    // Consume multiple times
    original.consume();
    original.consume();
    original.consume();

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertEquals(3L, deserialized.getConsumed());
    assertTrue(deserialized.isConsumed());
  }

  @Test
  public void testSerializeAndDeserializeComplexSplit() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        100,
        "/base/path/to/file.parquet",
        Option.of(Arrays.asList(
            "/log/path1/file.log",
            "/log/path2/file.log",
            "/log/path3/file.log",
            "/log/path4/file.log"
        )),
        "/very/long/table/path/with/multiple/segments",
        "/partition/year=2024/month=01/day=22",
        "payload_combine",
        "19700101000000000",
        "complex-file-id-with-uuid-12345678",
        Option.empty()
    );

    original.updatePosition(10, 5000L);

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertEquals(original.getSplitNum(), deserialized.getSplitNum());
    assertEquals(original.getBasePath().get(), deserialized.getBasePath().get());
    assertEquals(original.getLogPaths().get().size(), deserialized.getLogPaths().get().size());
    assertEquals(original.getTablePath(), deserialized.getTablePath());
    assertEquals(original.getPartitionPath(), deserialized.getPartitionPath());
    assertEquals(original.getMergeType(), deserialized.getMergeType());
    assertEquals(original.getFileId(), deserialized.getFileId());
    assertEquals(10, deserialized.getFileOffset());
    assertEquals(5000L, deserialized.getConsumed());
  }

  @Test
  public void testGetVersion() {
    assertEquals(1, serializer.getVersion());
  }

  @Test
  public void testSerializationIdempotency() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        7,
        "base-path",
        Option.of(Arrays.asList("log1")),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-333",
        Option.empty()
    );

    byte[] serialized1 = serializer.serialize(original);
    byte[] serialized2 = serializer.serialize(original);

    // Serializing the same object twice should produce identical results
    assertEquals(serialized1.length, serialized2.length);
  }

  @Test
  public void testDeserializationProducesEquivalentSplit() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        8,
        "base-path",
        Option.of(Arrays.asList("log1", "log2")),
        "/table/path",
        "/partition/path",
        "payload_combine",
        "19700101000000000",
        "file-444",
        Option.empty()
    );

    original.updatePosition(5, 200L);

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    // The deserialized split should be equivalent to the original
    assertEquals(original, deserialized);
  }

  @Test
  public void testSerializeMultipleSplitsWithDifferentStates() throws IOException {
    HoodieSourceSplit split1 = new HoodieSourceSplit(1, "base1", Option.empty(), "/t1", "/p1", "read_optimized", "19700101000000000","f1", Option.empty());
    HoodieSourceSplit split2 = new HoodieSourceSplit(2, "base2", Option.of(Arrays.asList("log1")), "/t2", "/p2", "payload_combine", "19700101000000000","f2", Option.empty());
    HoodieSourceSplit split3 = new HoodieSourceSplit(3, null, Option.of(Arrays.asList("log1", "log2", "log3")), "/t3", "/p3", "read_optimized", "19700101000000000","f3", Option.empty());

    split1.updatePosition(1, 10L);
    split2.consume();
    split3.updatePosition(5, 1000L);

    byte[] serialized1 = serializer.serialize(split1);
    byte[] serialized2 = serializer.serialize(split2);
    byte[] serialized3 = serializer.serialize(split3);

    HoodieSourceSplit deserialized1 = serializer.deserialize(serializer.getVersion(), serialized1);
    HoodieSourceSplit deserialized2 = serializer.deserialize(serializer.getVersion(), serialized2);
    HoodieSourceSplit deserialized3 = serializer.deserialize(serializer.getVersion(), serialized3);

    assertEquals(split1, deserialized1);
    assertEquals(split2, deserialized2);
    assertEquals(split3, deserialized3);
  }

  @Test
  public void testSerializeWithVeryLargeConsumedValue() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        999,
        "base-path",
        Option.empty(),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-large",
        Option.empty()
    );

    original.updatePosition(Integer.MAX_VALUE, Long.MAX_VALUE);

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertEquals(Integer.MAX_VALUE, deserialized.getFileOffset());
    assertEquals(Long.MAX_VALUE, deserialized.getConsumed());
  }

  @Test
  public void testSerializeWithZeroValues() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        0,
        "base-path",
        Option.empty(),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-zero",
        Option.empty()
    );

    original.updatePosition(0, 0L);

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertEquals(0, deserialized.getSplitNum());
    assertEquals(0, deserialized.getFileOffset());
    assertEquals(0L, deserialized.getConsumed());
  }

  @Test
  public void testSerializeWithNegativeSplitNum() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        -1,
        "base-path",
        Option.empty(),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-negative",
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertEquals(-1, deserialized.getSplitNum());
  }

  @Test
  public void testSerializeWithVeryLongStrings() throws IOException {
    StringBuilder longString = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      longString.append("very-long-path-segment-");
    }

    HoodieSourceSplit original = new HoodieSourceSplit(
        1,
        longString.toString(),
        Option.of(Arrays.asList(longString.toString(), longString.toString())),
        longString.toString(),
        longString.toString(),
        "read_optimized",
        "19700101000000000",
        longString.toString(),
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertEquals(original.getBasePath(), deserialized.getBasePath());
    assertEquals(original.getTablePath(), deserialized.getTablePath());
    assertEquals(original.getPartitionPath(), deserialized.getPartitionPath());
    assertEquals(original.getFileId(), deserialized.getFileId());
  }

  @Test
  public void testSerializeWithSpecialCharactersInStrings() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        1,
        "base/path/with/特殊字符/and/émojis/\u0000/null",
        Option.of(Arrays.asList("log/with/special/字符/path")),
        "/table/path/with/\t/tabs/and/\n/newlines",
        "/partition/with/\r\n/carriage/return",
        "read_optimized",
        "19700101000000000",
        "file-id-with-unicode-字符-émojis-🎉",
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertEquals(original.getBasePath(), deserialized.getBasePath());
    assertEquals(original.getLogPaths(), deserialized.getLogPaths());
    assertEquals(original.getTablePath(), deserialized.getTablePath());
    assertEquals(original.getPartitionPath(), deserialized.getPartitionPath());
    assertEquals(original.getFileId(), deserialized.getFileId());
  }

  @Test
  public void testSerializeWithManyLogPaths() throws IOException {
    List<String> manyLogPaths = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      manyLogPaths.add("/log/path/" + i + ".log");
    }

    HoodieSourceSplit original = new HoodieSourceSplit(
        1,
        "base-path",
        Option.of(manyLogPaths),
        "/table/path",
        "/partition/path",
        "payload_combine",
        "19700101000000000",
        "file-many-logs",
        Option.empty()
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertEquals(1000, deserialized.getLogPaths().get().size());
    assertEquals(original.getLogPaths(), deserialized.getLogPaths());
  }

  @Test
  public void testRoundTripSerializationMultipleTimes() throws IOException {
    HoodieSourceSplit original = new HoodieSourceSplit(
        1,
        "base-path",
        Option.of(Arrays.asList("log1", "log2")),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-roundtrip",
        Option.empty()
    );

    original.updatePosition(5, 100L);

    // Serialize and deserialize multiple times
    HoodieSourceSplit current = original;
    for (int i = 0; i < 10; i++) {
      byte[] serialized = serializer.serialize(current);
      current = serializer.deserialize(serializer.getVersion(), serialized);
    }

    assertEquals(original, current);
  }

  @Test
  public void testSerializeWithInstantRangeStartAndEnd() throws IOException {
    InstantRange instantRange = InstantRange.builder()
            .startInstant("20230101000000000")
            .endInstant("20230131235959999")
            .rangeType(InstantRange.RangeType.OPEN_CLOSED)
            .build();

    HoodieSourceSplit original = new HoodieSourceSplit(
        1,
        "base-path",
        Option.empty(),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-123",
        Option.of(instantRange)
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getInstantRange().isPresent());
    assertTrue(deserialized.getInstantRange().get().getStartInstant().isPresent());
    assertTrue(deserialized.getInstantRange().get().getEndInstant().isPresent());
    assertEquals("20230101000000000", deserialized.getInstantRange().get().getStartInstant().get());
    assertEquals("20230131235959999", deserialized.getInstantRange().get().getEndInstant().get());
  }

  @Test
  public void testSerializeWithInstantRangeOnlyStart() throws IOException {
    InstantRange instantRange = InstantRange.builder()
            .startInstant("20230101000000000")
            .rangeType(InstantRange.RangeType.OPEN_CLOSED)
            .nullableBoundary(true)
            .build();

    HoodieSourceSplit original = new HoodieSourceSplit(
        2,
        "base-path",
        Option.of(Arrays.asList("log1")),
        "/table/path",
        "/partition/path",
        "payload_combine",
        "19700101000000000",
        "file-456",
        Option.of(instantRange)
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getInstantRange().isPresent());
    assertTrue(deserialized.getInstantRange().get().getStartInstant().isPresent());
    assertFalse(deserialized.getInstantRange().get().getEndInstant().isPresent());
    assertEquals("20230101000000000", deserialized.getInstantRange().get().getStartInstant().get());
  }

  @Test
  public void testSerializeWithClosedClosedInstantRange() throws IOException {
    InstantRange instantRange = InstantRange.builder()
            .startInstant("20230101000000000")
            .endInstant("20230131235959999")
            .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
            .build();

    HoodieSourceSplit original = new HoodieSourceSplit(
        4,
        "base-path",
        Option.of(Arrays.asList("log1", "log2", "log3")),
        "/table/path",
        "/partition/path",
        "payload_combine",
        "19700101000000000",
        "file-range",
        Option.of(instantRange)
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getInstantRange().isPresent());
    assertTrue(deserialized.getInstantRange().get().getStartInstant().isPresent());
    assertTrue(deserialized.getInstantRange().get().getEndInstant().isPresent());
    assertEquals("20230101000000000", deserialized.getInstantRange().get().getStartInstant().get());
    assertEquals("20230131235959999", deserialized.getInstantRange().get().getEndInstant().get());
  }

  @Test
  public void testSerializeWithInstantRangeAndConsumedState() throws IOException {
    InstantRange instantRange = InstantRange.builder()
            .startInstant("20230101000000000")
            .endInstant("20230131235959999")
            .rangeType(InstantRange.RangeType.OPEN_CLOSED)
            .build();

    HoodieSourceSplit original = new HoodieSourceSplit(
        5,
        "base-path",
        Option.empty(),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-consumed",
        Option.of(instantRange)
    );

    original.updatePosition(10, 500L);

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getInstantRange().isPresent());
    assertEquals(10, deserialized.getFileOffset());
    assertEquals(500L, deserialized.getConsumed());
    assertEquals("20230101000000000", deserialized.getInstantRange().get().getStartInstant().get());
    assertEquals("20230131235959999", deserialized.getInstantRange().get().getEndInstant().get());
  }

  @Test
  public void testSerializeMultipleSplitsWithInstantRange() throws IOException {
    InstantRange range1 = InstantRange.builder()
            .startInstant("20230101000000000")
            .endInstant("20230131235959999")
            .rangeType(InstantRange.RangeType.OPEN_CLOSED)
            .build();

    InstantRange range2 = InstantRange.builder()
            .startInstant("20230201000000000")
            .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
            .nullableBoundary(true)
            .build();

    HoodieSourceSplit split1 = new HoodieSourceSplit(1, "base1", Option.empty(), "/t1", "/p1", "read_optimized", "19700101000000000", "f1", Option.of(range1));
    HoodieSourceSplit split2 = new HoodieSourceSplit(2, "base2", Option.of(Arrays.asList("log1")), "/t2", "/p2", "payload_combine", "19700101000000000", "f2", Option.of(range2));
    HoodieSourceSplit split3 = new HoodieSourceSplit(3, null, Option.of(Arrays.asList("log1", "log2")), "/t3", "/p3", "read_optimized", "19700101000000000", "f3", Option.empty());

    byte[] serialized1 = serializer.serialize(split1);
    byte[] serialized2 = serializer.serialize(split2);
    byte[] serialized3 = serializer.serialize(split3);

    HoodieSourceSplit deserialized1 = serializer.deserialize(serializer.getVersion(), serialized1);
    HoodieSourceSplit deserialized2 = serializer.deserialize(serializer.getVersion(), serialized2);
    HoodieSourceSplit deserialized3 = serializer.deserialize(serializer.getVersion(), serialized3);

    // Verify split1
    assertTrue(deserialized1.getInstantRange().isPresent());
    assertEquals("20230101000000000", deserialized1.getInstantRange().get().getStartInstant().get());
    assertEquals("20230131235959999", deserialized1.getInstantRange().get().getEndInstant().get());

    // Verify split2
    assertTrue(deserialized2.getInstantRange().isPresent());
    assertEquals("20230201000000000", deserialized2.getInstantRange().get().getStartInstant().get());
    assertFalse(deserialized2.getInstantRange().get().getEndInstant().isPresent());

    // Verify split3
    assertFalse(deserialized3.getInstantRange().isPresent());
  }

  @Test
  public void testSerializeWithExactMatchInstantRangeSingleInstant() throws IOException {
    Set<String> instants = new HashSet<>();
    instants.add("20230115120000000");

    InstantRange instantRange = InstantRange.builder()
            .explicitInstants(instants)
            .rangeType(InstantRange.RangeType.EXACT_MATCH)
            .build();

    HoodieSourceSplit original = new HoodieSourceSplit(
        1,
        "base-path",
        Option.empty(),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-exact-match",
        Option.of(instantRange)
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getInstantRange().isPresent());
    assertEquals(InstantRange.RangeType.EXACT_MATCH,
        deserialized.getInstantRange().get().getRangeType());

    // Verify the instant is in range
    assertTrue(deserialized.getInstantRange().get().isInRange("20230115120000000"));
    assertFalse(deserialized.getInstantRange().get().isInRange("20230115120000001"));
    assertFalse(deserialized.getInstantRange().get().isInRange("20230101000000000"));
  }

  @Test
  public void testSerializeWithExactMatchInstantRangeMultipleInstants() throws IOException {
    Set<String> instants = new HashSet<>();
    instants.add("20230101000000000");
    instants.add("20230115120000000");
    instants.add("20230131235959999");

    InstantRange instantRange = InstantRange.builder()
            .explicitInstants(instants)
            .rangeType(InstantRange.RangeType.EXACT_MATCH)
            .build();

    HoodieSourceSplit original = new HoodieSourceSplit(
        2,
        "base-path",
        Option.of(Arrays.asList("log1.parquet")),
        "/table/path",
        "/partition/path",
        "payload_combine",
        "19700101000000000",
        "file-exact-match-multi",
        Option.of(instantRange)
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getInstantRange().isPresent());
    assertEquals(InstantRange.RangeType.EXACT_MATCH,
        deserialized.getInstantRange().get().getRangeType());

    // Verify all instants are in range
    assertTrue(deserialized.getInstantRange().get().isInRange("20230101000000000"));
    assertTrue(deserialized.getInstantRange().get().isInRange("20230115120000000"));
    assertTrue(deserialized.getInstantRange().get().isInRange("20230131235959999"));

    // Verify instants not in the set are not in range
    assertFalse(deserialized.getInstantRange().get().isInRange("20230101000000001"));
    assertFalse(deserialized.getInstantRange().get().isInRange("20230115000000000"));
    assertFalse(deserialized.getInstantRange().get().isInRange("20230201000000000"));
  }

  @Test
  public void testSerializeWithExactMatchInstantRangeManyInstants() throws IOException {
    Set<String> instants = new HashSet<>();
    // Add 100 instants
    for (int i = 1; i <= 100; i++) {
      instants.add(String.format("2023%02d01000000000", i % 12 + 1));
    }

    InstantRange instantRange = InstantRange.builder()
            .explicitInstants(instants)
            .rangeType(InstantRange.RangeType.EXACT_MATCH)
            .build();

    HoodieSourceSplit original = new HoodieSourceSplit(
        3,
        "base-path",
        Option.of(Arrays.asList("log1.parquet", "log2.parquet")),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-exact-match-many",
        Option.of(instantRange)
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getInstantRange().isPresent());
    assertEquals(InstantRange.RangeType.EXACT_MATCH,
        deserialized.getInstantRange().get().getRangeType());

    // Verify a sample of instants are in range
    assertTrue(deserialized.getInstantRange().get().isInRange("20230101000000000"));
    assertTrue(deserialized.getInstantRange().get().isInRange("20230601000000000"));
    assertTrue(deserialized.getInstantRange().get().isInRange("20231201000000000"));

    // Verify instant not in the set is not in range
    assertFalse(deserialized.getInstantRange().get().isInRange("20230101000000001"));
  }

  @Test
  public void testSerializeWithExactMatchInstantRangeAndConsumedState() throws IOException {
    Set<String> instants = new HashSet<>();
    instants.add("20230101000000000");
    instants.add("20230201000000000");

    InstantRange instantRange = InstantRange.builder()
            .explicitInstants(instants)
            .rangeType(InstantRange.RangeType.EXACT_MATCH)
            .build();

    HoodieSourceSplit original = new HoodieSourceSplit(
        4,
        "base-path",
        Option.empty(),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-exact-consumed",
        Option.of(instantRange)
    );

    // Update position to simulate consumed state
    original.updatePosition(15, 750L);

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getInstantRange().isPresent());
    assertEquals(InstantRange.RangeType.EXACT_MATCH,
        deserialized.getInstantRange().get().getRangeType());

    // Verify consumed state
    assertEquals(15, deserialized.getFileOffset());
    assertEquals(750L, deserialized.getConsumed());

    // Verify instants are in range
    assertTrue(deserialized.getInstantRange().get().isInRange("20230101000000000"));
    assertTrue(deserialized.getInstantRange().get().isInRange("20230201000000000"));
    assertFalse(deserialized.getInstantRange().get().isInRange("20230301000000000"));
  }

  @Test
  public void testSerializeMultipleSplitsWithDifferentRangeTypes() throws IOException {
    // Split with EXACT_MATCH range
    Set<String> exactInstants = new HashSet<>();
    exactInstants.add("20230101000000000");
    exactInstants.add("20230115000000000");
    InstantRange exactMatchRange = InstantRange.builder()
            .explicitInstants(exactInstants)
            .rangeType(org.apache.hudi.common.table.log.InstantRange.RangeType.EXACT_MATCH)
            .build();

    // Split with OPEN_CLOSED range
    InstantRange openClosedRange = InstantRange.builder()
            .startInstant("20230201000000000")
            .endInstant("20230228235959999")
            .rangeType(org.apache.hudi.common.table.log.InstantRange.RangeType.OPEN_CLOSED)
            .build();

    // Split with CLOSED_CLOSED range
    InstantRange closedClosedRange = InstantRange.builder()
            .startInstant("20230301000000000")
            .endInstant("20230331235959999")
            .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
            .build();

    HoodieSourceSplit split1 = new HoodieSourceSplit(1, "base1", Option.empty(), "/t1", "/p1", "read_optimized", "19700101000000000", "f1", Option.of(exactMatchRange));
    HoodieSourceSplit split2 = new HoodieSourceSplit(2, "base2", Option.of(Arrays.asList("log1")), "/t2", "/p2", "payload_combine", "19700101000000000", "f2", Option.of(openClosedRange));
    HoodieSourceSplit split3 = new HoodieSourceSplit(3, "base3", Option.of(Arrays.asList("log1", "log2")), "/t3", "/p3", "read_optimized", "19700101000000000", "f3", Option.of(closedClosedRange));

    byte[] serialized1 = serializer.serialize(split1);
    byte[] serialized2 = serializer.serialize(split2);
    byte[] serialized3 = serializer.serialize(split3);

    HoodieSourceSplit deserialized1 = serializer.deserialize(serializer.getVersion(), serialized1);
    HoodieSourceSplit deserialized2 = serializer.deserialize(serializer.getVersion(), serialized2);
    HoodieSourceSplit deserialized3 = serializer.deserialize(serializer.getVersion(), serialized3);

    // Verify split1 (EXACT_MATCH)
    assertTrue(deserialized1.getInstantRange().isPresent());
    assertEquals(InstantRange.RangeType.EXACT_MATCH,
        deserialized1.getInstantRange().get().getRangeType());
    assertTrue(deserialized1.getInstantRange().get().isInRange("20230101000000000"));
    assertTrue(deserialized1.getInstantRange().get().isInRange("20230115000000000"));
    assertFalse(deserialized1.getInstantRange().get().isInRange("20230110000000000"));

    // Verify split2 (OPEN_CLOSED)
    assertTrue(deserialized2.getInstantRange().isPresent());
    assertEquals(InstantRange.RangeType.OPEN_CLOSED,
        deserialized2.getInstantRange().get().getRangeType());
    assertEquals("20230201000000000", deserialized2.getInstantRange().get().getStartInstant().get());
    assertEquals("20230228235959999", deserialized2.getInstantRange().get().getEndInstant().get());

    // Verify split3 (CLOSED_CLOSED)
    assertTrue(deserialized3.getInstantRange().isPresent());
    assertEquals(InstantRange.RangeType.CLOSED_CLOSED,
        deserialized3.getInstantRange().get().getRangeType());
    assertEquals("20230301000000000", deserialized3.getInstantRange().get().getStartInstant().get());
    assertEquals("20230331235959999", deserialized3.getInstantRange().get().getEndInstant().get());
  }

  @Test
  public void testRoundTripSerializationWithExactMatchRange() throws IOException {
    Set<String> instants = new HashSet<>();
    instants.add("20230101000000000");
    instants.add("20230201000000000");
    instants.add("20230301000000000");

    InstantRange instantRange =
        InstantRange.builder()
            .explicitInstants(instants)
            .rangeType(InstantRange.RangeType.EXACT_MATCH)
            .build();

    HoodieSourceSplit original = new HoodieSourceSplit(
        5,
        "base-path",
        Option.of(Arrays.asList("log1", "log2")),
        "/table/path",
        "/partition/path",
        "payload_combine",
        "19700101000000000",
        "file-roundtrip-exact",
        Option.of(instantRange)
    );

    original.updatePosition(5, 100L);

    // Serialize and deserialize multiple times
    HoodieSourceSplit current = original;
    for (int i = 0; i < 10; i++) {
      byte[] serialized = serializer.serialize(current);
      current = serializer.deserialize(serializer.getVersion(), serialized);
    }

    // Verify all fields remain intact
    assertEquals(original.getSplitNum(), current.getSplitNum());
    assertEquals(original.getBasePath(), current.getBasePath());
    assertEquals(original.getLogPaths(), current.getLogPaths());
    assertEquals(original.getTablePath(), current.getTablePath());
    assertEquals(original.getPartitionPath(), current.getPartitionPath());
    assertEquals(original.getMergeType(), current.getMergeType());
    assertEquals(original.getFileId(), current.getFileId());

    // Verify instant range functionality is preserved
    assertTrue(current.getInstantRange().isPresent());
    assertEquals(InstantRange.RangeType.EXACT_MATCH,
        current.getInstantRange().get().getRangeType());
    assertTrue(current.getInstantRange().get().isInRange("20230101000000000"));
    assertTrue(current.getInstantRange().get().isInRange("20230201000000000"));
    assertTrue(current.getInstantRange().get().isInRange("20230301000000000"));
    assertFalse(current.getInstantRange().get().isInRange("20230401000000000"));
  }

  @Test
  public void testSerializeWithClosedClosedRangeOnlyStart() throws IOException {
    InstantRange instantRange = InstantRange.builder()
            .startInstant("20230101000000000")
            .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
            .nullableBoundary(true)
            .build();

    HoodieSourceSplit original = new HoodieSourceSplit(
        6,
        "base-path",
        Option.of(Arrays.asList("log1", "log2")),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-closed-start-only",
        Option.of(instantRange)
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getInstantRange().isPresent());
    assertEquals(InstantRange.RangeType.CLOSED_CLOSED,
        deserialized.getInstantRange().get().getRangeType());
    assertTrue(deserialized.getInstantRange().get().getStartInstant().isPresent());
    assertFalse(deserialized.getInstantRange().get().getEndInstant().isPresent());
    assertEquals("20230101000000000", deserialized.getInstantRange().get().getStartInstant().get());

    // Verify range behavior - start is inclusive, no end boundary
    assertTrue(deserialized.getInstantRange().get().isInRange("20230101000000000")); // start inclusive
    assertTrue(deserialized.getInstantRange().get().isInRange("20230101000000001"));
    assertTrue(deserialized.getInstantRange().get().isInRange("20231231235959999"));
    assertFalse(deserialized.getInstantRange().get().isInRange("20221231235959999")); // before start
  }

  @Test
  public void testSerializeWithClosedClosedRangeOnlyEnd() throws IOException {
    InstantRange instantRange = InstantRange.builder()
            .endInstant("20230131235959999")
            .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
            .nullableBoundary(true)
            .build();

    HoodieSourceSplit original = new HoodieSourceSplit(
        7,
        "base-path",
        Option.empty(),
        "/table/path",
        "/partition/path",
        "payload_combine",
        "19700101000000000",
        "file-closed-end-only",
        Option.of(instantRange)
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getInstantRange().isPresent());
    assertEquals(InstantRange.RangeType.CLOSED_CLOSED,
        deserialized.getInstantRange().get().getRangeType());
    assertFalse(deserialized.getInstantRange().get().getStartInstant().isPresent());
    assertTrue(deserialized.getInstantRange().get().getEndInstant().isPresent());
    assertEquals("20230131235959999", deserialized.getInstantRange().get().getEndInstant().get());

    // Verify range behavior - no start boundary, end is inclusive
    assertTrue(deserialized.getInstantRange().get().isInRange("19700101000000000"));
    assertTrue(deserialized.getInstantRange().get().isInRange("20230131235959999")); // end inclusive
    assertFalse(deserialized.getInstantRange().get().isInRange("20230201000000000")); // after end
  }

  @Test
  public void testSerializeWithOpenClosedRangeOnlyEnd() throws IOException {
    InstantRange instantRange = InstantRange.builder()
            .endInstant("20230131235959999")
            .rangeType(InstantRange.RangeType.OPEN_CLOSED)
            .nullableBoundary(true)
            .build();

    HoodieSourceSplit original = new HoodieSourceSplit(
        8,
        "base-path",
        Option.of(Arrays.asList("log1")),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "19700101000000000",
        "file-open-end-only",
        Option.of(instantRange)
    );

    byte[] serialized = serializer.serialize(original);
    HoodieSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertNotNull(deserialized);
    assertTrue(deserialized.getInstantRange().isPresent());
    assertEquals(InstantRange.RangeType.OPEN_CLOSED,
        deserialized.getInstantRange().get().getRangeType());
    assertFalse(deserialized.getInstantRange().get().getStartInstant().isPresent());
    assertTrue(deserialized.getInstantRange().get().getEndInstant().isPresent());
    assertEquals("20230131235959999", deserialized.getInstantRange().get().getEndInstant().get());

    // Verify range behavior - no start boundary, end is inclusive
    assertTrue(deserialized.getInstantRange().get().isInRange("19700101000000000"));
    assertTrue(deserialized.getInstantRange().get().isInRange("20230131235959999")); // end inclusive
    assertFalse(deserialized.getInstantRange().get().isInRange("20230201000000000")); // after end
  }

  @Test
  public void testSerializeWithAllRangeTypesAndNullableBoundaries() throws IOException {
    // OPEN_CLOSED with only start
    InstantRange openStart = InstantRange.builder()
            .startInstant("20230101000000000")
            .rangeType(InstantRange.RangeType.OPEN_CLOSED)
            .nullableBoundary(true)
            .build();

    // OPEN_CLOSED with only end
    InstantRange openEnd = InstantRange.builder()
            .endInstant("20230131235959999")
            .rangeType(InstantRange.RangeType.OPEN_CLOSED)
            .nullableBoundary(true)
            .build();

    // CLOSED_CLOSED with only start
    InstantRange closedStart = InstantRange.builder()
            .startInstant("20230201000000000")
            .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
            .nullableBoundary(true)
            .build();

    // CLOSED_CLOSED with only end
    InstantRange closedEnd = InstantRange.builder()
            .endInstant("20230228235959999")
            .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
            .nullableBoundary(true)
            .build();

    HoodieSourceSplit split1 = new HoodieSourceSplit(1, "base1", Option.empty(), "/t1", "/p1", "read_optimized", "19700101000000000", "f1", Option.of(openStart));
    HoodieSourceSplit split2 = new HoodieSourceSplit(2, "base2", Option.empty(), "/t2", "/p2", "read_optimized", "19700101000000000", "f2", Option.of(openEnd));
    HoodieSourceSplit split3 = new HoodieSourceSplit(3, "base3", Option.empty(), "/t3", "/p3", "read_optimized", "19700101000000000", "f3", Option.of(closedStart));
    HoodieSourceSplit split4 = new HoodieSourceSplit(4, "base4", Option.empty(), "/t4", "/p4", "read_optimized", "19700101000000000", "f4", Option.of(closedEnd));

    // Serialize all
    byte[] serialized1 = serializer.serialize(split1);
    byte[] serialized2 = serializer.serialize(split2);
    byte[] serialized3 = serializer.serialize(split3);
    byte[] serialized4 = serializer.serialize(split4);

    // Deserialize all
    HoodieSourceSplit deserialized1 = serializer.deserialize(serializer.getVersion(), serialized1);
    HoodieSourceSplit deserialized2 = serializer.deserialize(serializer.getVersion(), serialized2);
    HoodieSourceSplit deserialized3 = serializer.deserialize(serializer.getVersion(), serialized3);
    HoodieSourceSplit deserialized4 = serializer.deserialize(serializer.getVersion(), serialized4);

    // Verify OPEN_CLOSED with only start
    assertTrue(deserialized1.getInstantRange().get().getStartInstant().isPresent());
    assertFalse(deserialized1.getInstantRange().get().getEndInstant().isPresent());
    assertEquals(InstantRange.RangeType.OPEN_CLOSED, deserialized1.getInstantRange().get().getRangeType());

    // Verify OPEN_CLOSED with only end
    assertFalse(deserialized2.getInstantRange().get().getStartInstant().isPresent());
    assertTrue(deserialized2.getInstantRange().get().getEndInstant().isPresent());
    assertEquals(InstantRange.RangeType.OPEN_CLOSED, deserialized2.getInstantRange().get().getRangeType());

    // Verify CLOSED_CLOSED with only start
    assertTrue(deserialized3.getInstantRange().get().getStartInstant().isPresent());
    assertFalse(deserialized3.getInstantRange().get().getEndInstant().isPresent());
    assertEquals(InstantRange.RangeType.CLOSED_CLOSED, deserialized3.getInstantRange().get().getRangeType());

    // Verify CLOSED_CLOSED with only end
    assertFalse(deserialized4.getInstantRange().get().getStartInstant().isPresent());
    assertTrue(deserialized4.getInstantRange().get().getEndInstant().isPresent());
    assertEquals(InstantRange.RangeType.CLOSED_CLOSED, deserialized4.getInstantRange().get().getRangeType());
  }

  @Test
  public void testSerializeThrowsExceptionForCompositionRange() {
    // Create a composition range
    InstantRange range1 = InstantRange.builder()
        .startInstant("20230101000000000")
        .endInstant("20230115235959999")
        .rangeType(InstantRange.RangeType.OPEN_CLOSED)
        .build();

    InstantRange range2 = InstantRange.builder()
        .startInstant("20230201000000000")
        .endInstant("20230215235959999")
        .rangeType(InstantRange.RangeType.OPEN_CLOSED)
        .build();

    InstantRange compositionRange = new InstantRange.CompositionRange(Arrays.asList(range1, range2));

    HoodieSourceSplit split = new HoodieSourceSplit(
        1,
        "base-path",
        Option.empty(),
        "/table/path",
        "/partition/path",
        "read_optimized",
        "20240101000000000",
        "file1",
        Option.of(compositionRange)
    );

    UnsupportedOperationException exception = assertThrows(
        UnsupportedOperationException.class,
        () -> serializer.serialize(split)
    );

    assertEquals("Composition Range is not supported.", exception.getMessage());
  }

  @Test
  public void testDeserializeThrowsExceptionForCompositionRangeType() throws IOException {
    // Manually create a byte array with COMPOSITION range type
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    // Serialize all fields except instant range
    out.writeInt(1); // splitNum
    out.writeBoolean(true); // basePath present
    out.writeUTF("base-path");
    out.writeBoolean(false); // logPaths not present
    out.writeUTF("/table/path"); // tablePath
    out.writeUTF("/partition/path"); // partitionPath
    out.writeUTF("read_optimized"); // mergeType
    out.writeUTF("20240101000000000"); // latestCommit
    out.writeUTF("file1"); // fileId
    out.writeLong(0L); // consumed
    out.writeInt(0); // fileOffset

    // Write instant range with COMPOSITION type
    out.writeBoolean(true); // instantRange present
    out.writeUTF("COMPOSITION"); // rangeType - this will trigger the exception

    out.flush();
    byte[] serialized = baos.toByteArray();
    out.close();

    UnsupportedOperationException exception = assertThrows(
        UnsupportedOperationException.class,
        () -> serializer.deserialize(serializer.getVersion(), serialized)
    );

    assertEquals("Composition Range is not supported.", exception.getMessage());
  }
}

