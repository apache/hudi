/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.sources.helpers.KinesisOffsetGen.KinesisShardRange;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for KinesisShardRange.hasUnreadRecords.
 */
class TestKinesisShardRange {

  private static final String LOCALSTACK_SENTINEL = "9223372036854775807";
  private static final String SEQ_LOW = "100";
  private static final String SEQ_MID = "200";
  private static final String SEQ_HIGH = "300";

  // 1. Open shard: always has unread records (may have new data)
  @Test
  void testOpenShardHasUnreadRecords() {
    KinesisShardRange openShard = KinesisShardRange.of("shard-0", Option.of(SEQ_MID), Option.empty());
    assertTrue(openShard.hasUnreadRecords(), "Open shard may have new records");
    assertTrue(openShard.hasUnreadRecords(true), "Open shard may have new records (LATEST)");
    assertTrue(openShard.hasUnreadRecords(false), "Open shard may have new records (TRIM_HORIZON)");

    KinesisShardRange openShardNoCheckpoint = KinesisShardRange.of("shard-0", Option.empty(), Option.empty());
    assertTrue(openShardNoCheckpoint.hasUnreadRecords(), "Open shard with no checkpoint may have records");
  }

  // 2. Closed shard, lastSeq >= endSeq: fully consumed, no unread records
  @Test
  void testClosedShardFullyConsumedLastSeqEqualsEndSeq() {
    KinesisShardRange range = KinesisShardRange.of("shard-0", Option.of(SEQ_MID), Option.of(SEQ_MID));
    assertFalse(range.hasUnreadRecords(), "lastSeq == endSeq: fully consumed");
    assertFalse(range.hasUnreadRecords(true));
    assertFalse(range.hasUnreadRecords(false));
  }

  @Test
  void testClosedShardFullyConsumedLastSeqGreaterThanEndSeq() {
    KinesisShardRange range = KinesisShardRange.of("shard-0", Option.of(SEQ_HIGH), Option.of(SEQ_MID));
    assertFalse(range.hasUnreadRecords(), "lastSeq > endSeq: fully consumed");
    assertFalse(range.hasUnreadRecords(true));
    assertFalse(range.hasUnreadRecords(false));
  }

  // 3. Closed shard, lastSeq < endSeq: may have unread records
  @Test
  void testClosedShardWithUnreadRecords() {
    KinesisShardRange range = KinesisShardRange.of("shard-0", Option.of(SEQ_LOW), Option.of(SEQ_HIGH));
    assertTrue(range.hasUnreadRecords(), "lastSeq < endSeq: may have unread records");
    assertTrue(range.hasUnreadRecords(true));
    assertTrue(range.hasUnreadRecords(false));
  }

  // 4. Closed shard, no checkpoint (empty startSeq), useLatest=true: at tip, no records
  @Test
  void testClosedShardNoCheckpointUseLatest() {
    KinesisShardRange range = KinesisShardRange.of("shard-0", Option.empty(), Option.of(SEQ_HIGH));
    assertFalse(range.hasUnreadRecords(true), "Closed shard, no checkpoint, LATEST: at tip, no records");
  }

  // 5. Closed shard, no checkpoint (empty startSeq), useLatest=false: TRIM_HORIZON, may have records
  @Test
  void testClosedShardNoCheckpointUseTrimHorizon() {
    KinesisShardRange range = KinesisShardRange.of("shard-0", Option.empty(), Option.of(SEQ_HIGH));
    assertTrue(range.hasUnreadRecords(false), "Closed shard, no checkpoint, TRIM_HORIZON: may have records");
  }

  // 6. Default hasUnreadRecords() is conservative (useLatest=false)
  @Test
  void testClosedShardNoCheckpointDefaultConservative() {
    KinesisShardRange range = KinesisShardRange.of("shard-0", Option.empty(), Option.of(SEQ_HIGH));
    assertTrue(range.hasUnreadRecords(), "Default: conservative, assumes TRIM_HORIZON");
  }

  // 7. LocalStack sentinel: lastSeq == sentinel means fully consumed
  @Test
  void testClosedShardLocalStackSentinelFullyConsumed() {
    KinesisShardRange range = KinesisShardRange.of("shard-0",
        Option.of(LOCALSTACK_SENTINEL), Option.of(LOCALSTACK_SENTINEL));
    assertFalse(range.hasUnreadRecords(), "LocalStack sentinel, lastSeq == endSeq: fully consumed");
  }

  // 8. LocalStack sentinel: lastSeq < sentinel means may have unread (conservative)
  @Test
  void testClosedShardLocalStackSentinelMayHaveUnread() {
    KinesisShardRange range = KinesisShardRange.of("shard-0", Option.of(SEQ_MID), Option.of(LOCALSTACK_SENTINEL));
    assertTrue(range.hasUnreadRecords(), "LocalStack sentinel, lastSeq < endSeq: conservative, may have records");
  }

  // 9. Empty lastSeq with non-empty endSeq (edge case)
  @Test
  void testClosedShardEmptyLastSeq() {
    KinesisShardRange range = KinesisShardRange.of("shard-0", Option.of(""), Option.of(SEQ_HIGH));
    assertTrue(range.hasUnreadRecords(false), "Empty lastSeq, TRIM_HORIZON: may have records");
    assertFalse(range.hasUnreadRecords(true), "Empty lastSeq, LATEST: no records");
  }

  // 10. Lexicographic comparison (Kinesis seq numbers are ordered)
  @Test
  void testClosedShardLexicographicComparison() {
    String seqA = "49590382471490958861609854428592832524486083117";
    String seqB = "49590382471490958861609854428592832524486083118";
    String seqC = "49590382471490958861609854428592832524486083119";

    assertFalse(KinesisShardRange.of("s", Option.of(seqB), Option.of(seqA)).hasUnreadRecords(),
        "lastSeq > endSeq (lex): fully consumed");
    assertTrue(KinesisShardRange.of("s", Option.of(seqA), Option.of(seqC)).hasUnreadRecords(),
        "lastSeq < endSeq (lex): may have unread");
  }
}
