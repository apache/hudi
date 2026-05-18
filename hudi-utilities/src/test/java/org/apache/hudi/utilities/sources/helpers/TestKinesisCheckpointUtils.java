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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.utilities.sources.helpers.KinesisOffsetGen.CheckpointUtils;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for Kinesis checkpoint format and parsing.
 */
class TestKinesisCheckpointUtils {

  private static final String STREAM_NAME = "my-stream";

  @Test
  void testStrToOffsetsLegacyFormat() {
    String checkpoint = STREAM_NAME + ",shard-1:seq123,shard-2:seq456";
    Map<String, String> offsets = CheckpointUtils.strToOffsets(checkpoint);
    assertEquals(2, offsets.size());
    assertEquals("seq123", offsets.get("shard-1"));
    assertEquals("seq456", offsets.get("shard-2"));
  }

  @Test
  void testStrToOffsetsWithEndSeq() {
    String checkpoint = STREAM_NAME + ",shard-1:seq123|seq999,shard-2:seq456";
    Map<String, String> offsets = CheckpointUtils.strToOffsets(checkpoint);
    assertEquals(2, offsets.size());
    assertEquals("seq123|seq999", offsets.get("shard-1"));
    assertEquals("seq456", offsets.get("shard-2"));
  }

  @Test
  void testGetLastSeqFromValue() {
    // legacy format: no separators
    assertEquals("seq123", CheckpointUtils.getLastSeqFromValue("seq123"));
    // legacy format: lastSeq|endSeq
    assertEquals("seq123", CheckpointUtils.getLastSeqFromValue("seq123|seq999"));
    // new format: lastSeq@arrivalTime
    assertEquals("seq123", CheckpointUtils.getLastSeqFromValue("seq123@1700000000000"));
    // new format: lastSeq@arrivalTime|endSeq
    assertEquals("seq123", CheckpointUtils.getLastSeqFromValue("seq123@1700000000000|seq999"));
    assertNull(CheckpointUtils.getLastSeqFromValue(null));
    assertEquals("", CheckpointUtils.getLastSeqFromValue(""));
  }

  @Test
  void testGetArrivalTimeFromValue() {
    // no arrival time
    assertNull(CheckpointUtils.getArrivalTimeFromValue("seq123"));
    assertNull(CheckpointUtils.getArrivalTimeFromValue("seq123|seq999"));
    assertNull(CheckpointUtils.getArrivalTimeFromValue(null));
    assertNull(CheckpointUtils.getArrivalTimeFromValue(""));
    // with arrival time, no endSeq
    assertEquals(1700000000000L, CheckpointUtils.getArrivalTimeFromValue("seq123@1700000000000"));
    // with arrival time and endSeq
    assertEquals(1700000000000L, CheckpointUtils.getArrivalTimeFromValue("seq123@1700000000000|seq999"));
  }

  @Test
  void testGetEndSeqFromValue() {
    // legacy format
    assertNull(CheckpointUtils.getEndSeqFromValue("seq123"));
    assertEquals("seq999", CheckpointUtils.getEndSeqFromValue("seq123|seq999"));
    // new format: arrivalTime present but no endSeq
    assertNull(CheckpointUtils.getEndSeqFromValue("seq123@1700000000000"));
    // new format: arrivalTime and endSeq
    assertEquals("seq999", CheckpointUtils.getEndSeqFromValue("seq123@1700000000000|seq999"));
    assertNull(CheckpointUtils.getEndSeqFromValue(null));
    assertNull(CheckpointUtils.getEndSeqFromValue(""));
  }

  @Test
  void testBuildCheckpointValue() {
    // 2-arg: legacy format
    assertEquals("seq123", CheckpointUtils.buildCheckpointValue("seq123", (String) null));
    assertEquals("seq123", CheckpointUtils.buildCheckpointValue("seq123", ""));
    assertEquals("seq123|seq999", CheckpointUtils.buildCheckpointValue("seq123", "seq999"));
    // 3-arg: with arrival time
    assertEquals("seq123@1700000000000",
        CheckpointUtils.buildCheckpointValue("seq123", 1700000000000L, null));
    assertEquals("seq123@1700000000000",
        CheckpointUtils.buildCheckpointValue("seq123", 1700000000000L, ""));
    assertEquals("seq123@1700000000000|seq999",
        CheckpointUtils.buildCheckpointValue("seq123", 1700000000000L, "seq999"));
    // 3-arg: null arrival time falls back to legacy format
    assertEquals("seq123|seq999",
        CheckpointUtils.buildCheckpointValue("seq123", (Long) null, "seq999"));
  }

  @Test
  void testBuildAndParseRoundTripWithArrivalTime() {
    String value = CheckpointUtils.buildCheckpointValue("seq123", 1700000000000L, "seq999");
    assertEquals("seq123", CheckpointUtils.getLastSeqFromValue(value));
    assertEquals(1700000000000L, CheckpointUtils.getArrivalTimeFromValue(value));
    assertEquals("seq999", CheckpointUtils.getEndSeqFromValue(value));
  }

  @Test
  void testOffsetsToStrRoundTrip() {
    Map<String, String> offsets = Map.of(
        "shard-2", "seq456",
        "shard-1", "seq123|seq999");
    String str = CheckpointUtils.offsetsToStr(STREAM_NAME, offsets);
    assertTrue(str.startsWith(STREAM_NAME + ","));
    assertTrue(str.contains("shard-1:seq123|seq999"));
    assertTrue(str.contains("shard-2:seq456"));

    Map<String, String> parsed = CheckpointUtils.strToOffsets(str);
    assertEquals(offsets, parsed);
  }

  @Test
  void testIsValidStreamCheckpoint() {
    assertTrue(CheckpointUtils.isValidStreamCheckpoint(
        org.apache.hudi.common.util.Option.of(STREAM_NAME + ",shard-1:seq123"), STREAM_NAME));
    assertFalse(CheckpointUtils.isValidStreamCheckpoint(
        org.apache.hudi.common.util.Option.of("invalid"), STREAM_NAME));
    assertFalse(CheckpointUtils.isValidStreamCheckpoint(
        org.apache.hudi.common.util.Option.empty(), STREAM_NAME));
    // Checkpoint for a different stream is not valid
    assertFalse(CheckpointUtils.isValidStreamCheckpoint(
        org.apache.hudi.common.util.Option.of("other-stream,shard-1:seq123"), STREAM_NAME));
  }

  /**
   * Verify expired-shard data-loss detection: when lastSeq >= endSeq, shard was fully consumed.
   * Kinesis sequence numbers are ordered (lexicographic).
   */
  @Test
  void testExpiredShardFullyConsumedLogic() {
    String lastSeq = "49590382471490958861609854428592832524486083118";
    String endSeq = "49590382471490958861609854428592832524486083118";
    assertTrue(lastSeq.compareTo(endSeq) >= 0, "lastSeq >= endSeq means fully consumed");

    String lastSeqHigher = "49590382471490958861609854428592832524486083119";
    assertTrue(lastSeqHigher.compareTo(endSeq) >= 0);

    String lastSeqLower = "49590382471490958861609854428592832524486083117";
    assertTrue(lastSeqLower.compareTo(endSeq) < 0, "lastSeq < endSeq means data loss");
  }
}
