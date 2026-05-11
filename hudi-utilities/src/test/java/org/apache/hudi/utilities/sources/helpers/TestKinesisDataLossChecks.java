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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.config.KinesisSourceConfig;
import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for KinesisOffsetGen data-loss detection:
 * {@code checkDataLossOnExpiredShards} and {@code checkDataLossOnAvailableShards}.
 *   checkDataLossOnExpiredShards — 8 cases:
 *
 *   ┌──────────────────────────────────────────────────┬─────────────────────────────────────┐
 *   │                       Case                       │               Outcome               │
 *   ├──────────────────────────────────────────────────┼─────────────────────────────────────┤
 *   │ lastSeq == endSeq                                │ No data loss (fully consumed)       │
 *   ├──────────────────────────────────────────────────┼─────────────────────────────────────┤
 *   │ lastSeq > endSeq                                 │ No data loss (fully consumed)       │
 *   ├──────────────────────────────────────────────────┼─────────────────────────────────────┤
 *   │ lastSeq < endSeq, failOnDataLoss=true            │ Throws                              │
 *   ├──────────────────────────────────────────────────┼─────────────────────────────────────┤
 *   │ lastSeq < endSeq, failOnDataLoss=false           │ Warns only                          │
 *   ├──────────────────────────────────────────────────┼─────────────────────────────────────┤
 *   │ Was open shard (no | in value, no endSeq)        │ Data loss — conservative assumption │
 *   ├──────────────────────────────────────────────────┼─────────────────────────────────────┤
 *   │ "|endSeq" format (empty lastSeq, endSeq present) │ Data loss — "" < endSeq             │
 *   ├──────────────────────────────────────────────────┼─────────────────────────────────────┤
 *   │ Null checkpoint value (corrupt entry)            │ Data loss — endSeq absent           │
 *   ├──────────────────────────────────────────────────┼─────────────────────────────────────┤
 *   │ Mixed: one consumed + one not                    │ Throws on the unconsumed one        │
 *   └──────────────────────────────────────────────────┴─────────────────────────────────────┘
 *
 *   checkDataLossOnAvailableShards — 11 cases:
 *
 *   ┌──────────────────────────────────────────────────────────────────┬──────────────────────────────────────────────────────┐
 *   │                               Case                               │                       Outcome                        │
 *   ├──────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
 *   │ lastSeq < shardStartSeq, failOnDataLoss=true                     │ Throws                                               │
 *   ├──────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
 *   │ lastSeq < shardStartSeq, failOnDataLoss=false                    │ Warns only                                           │
 *   ├──────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
 *   │ lastSeq == shardStartSeq (boundary)                              │ No data loss — records after lastSeq still available │
 *   ├──────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
 *   │ lastSeq > shardStartSeq                                          │ No data loss                                         │
 *   ├──────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
 *   │ No checkpoint entry for shard                                    │ Skipped                                              │
 *   ├──────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
 *   │ shardStartSeq is null                                            │ Skipped                                              │
 *   ├──────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
 *   │ sequenceNumberRange is null                                      │ Skipped                                              │
 *   ├──────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
 *   │ Closed shard, lastSeq < shardStartSeq                            │ Data loss                                            │
 *   ├──────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
 *   │ Closed shard fully consumed (lastSeq >= endSeq >= shardStartSeq) │ No false positive                                    │
 *   ├──────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
 *   │ Multiple shards: one trimmed, one healthy                        │ Throws only on the trimmed one                       │
 *   ├──────────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
 *   │ All shards within retention                                      │ No throw                                             │
 *   └──────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────┘
 */
class TestKinesisDataLossChecks {

  private static final String STREAM = "test-stream";
  private static final String REGION = "us-east-1";
  // Realistic 56-char Kinesis sequence numbers (lexicographic ordering).
  private static final String SEQ_100 = "49590000000000000000000000000000000000000000000000000100";
  private static final String SEQ_200 = "49590000000000000000000000000000000000000000000000000200";
  private static final String SEQ_300 = "49590000000000000000000000000000000000000000000000000300";

  // ──────────────────────────────────────────────────────────────────────────
  // Helpers
  // ──────────────────────────────────────────────────────────────────────────

  private KinesisOffsetGen offsetGen(boolean failOnDataLoss) {
    TypedProperties props = new TypedProperties();
    props.setProperty(KinesisSourceConfig.KINESIS_STREAM_NAME.key(), STREAM);
    props.setProperty(KinesisSourceConfig.KINESIS_REGION.key(), REGION);
    props.setProperty(KinesisSourceConfig.KINESIS_STARTING_POSITION.key(), "LATEST");
    props.setProperty(KinesisSourceConfig.ENABLE_FAIL_ON_DATA_LOSS.key(), String.valueOf(failOnDataLoss));
    return new KinesisOffsetGen(props);
  }

  /** Build an open shard (no endingSequenceNumber). */
  private static Shard openShard(String shardId, String startSeq) {
    return Shard.builder()
        .shardId(shardId)
        .sequenceNumberRange(SequenceNumberRange.builder()
            .startingSequenceNumber(startSeq)
            .build())
        .build();
  }

  /** Build a closed shard (has endingSequenceNumber). */
  private static Shard closedShard(String shardId, String startSeq, String endSeq) {
    return Shard.builder()
        .shardId(shardId)
        .sequenceNumberRange(SequenceNumberRange.builder()
            .startingSequenceNumber(startSeq)
            .endingSequenceNumber(endSeq)
            .build())
        .build();
  }

  // ──────────────────────────────────────────────────────────────────────────
  // checkDataLossOnExpiredShards
  // ──────────────────────────────────────────────────────────────────────────

  /** Shard fully consumed: lastSeq == endSeq. No data loss. */
  @Test
  void testExpiredShard_fullyConsumed_lastSeqEqualsEndSeq() {
    KinesisOffsetGen gen = offsetGen(true);
    Map<String, String> offsets = new HashMap<>();
    // Checkpoint value stores lastSeq|endSeq for closed shards.
    offsets.put("shard-0", SEQ_200 + "|" + SEQ_200);
    assertDoesNotThrow(() -> gen.checkDataLossOnExpiredShards(
        Collections.singletonList("shard-0"), offsets));
  }

  /** Shard fully consumed: lastSeq > endSeq. No data loss. */
  @Test
  void testExpiredShard_fullyConsumed_lastSeqBeyondEndSeq() {
    KinesisOffsetGen gen = offsetGen(true);
    Map<String, String> offsets = new HashMap<>();
    offsets.put("shard-0", SEQ_300 + "|" + SEQ_200);
    assertDoesNotThrow(() -> gen.checkDataLossOnExpiredShards(
        Collections.singletonList("shard-0"), offsets));
  }

  /** Shard not consumed: lastSeq < endSeq, failOnDataLoss=true → throws. */
  @Test
  void testExpiredShard_notConsumed_failOnDataLossTrue_throws() {
    KinesisOffsetGen gen = offsetGen(true);
    Map<String, String> offsets = new HashMap<>();
    offsets.put("shard-0", SEQ_100 + "|" + SEQ_200);
    assertThrows(HoodieReadFromSourceException.class,
        () -> gen.checkDataLossOnExpiredShards(Collections.singletonList("shard-0"), offsets));
  }

  /** Shard not consumed: lastSeq < endSeq, failOnDataLoss=false → warns, does not throw. */
  @Test
  void testExpiredShard_notConsumed_failOnDataLossFalse_noThrow() {
    KinesisOffsetGen gen = offsetGen(false);
    Map<String, String> offsets = new HashMap<>();
    offsets.put("shard-0", SEQ_100 + "|" + SEQ_200);
    assertDoesNotThrow(() -> gen.checkDataLossOnExpiredShards(
        Collections.singletonList("shard-0"), offsets));
  }

  /**
   * Expired shard that was open when last checkpointed (value has no '|', no endSeq stored).
   * We conservatively treat it as not fully consumed → data loss.
   */
  @Test
  void testExpiredShard_wasOpenShard_noEndSeqStored_dataLoss() {
    KinesisOffsetGen gen = offsetGen(true);
    Map<String, String> offsets = new HashMap<>();
    offsets.put("shard-0", SEQ_100);  // legacy / open-shard format: just lastSeq
    assertThrows(HoodieReadFromSourceException.class,
        () -> gen.checkDataLossOnExpiredShards(Collections.singletonList("shard-0"), offsets));
  }

  /**
   * Edge case: checkpoint value stored empty lastSeq with endSeq ("|endSeq" format).
   * lastSeq="" < endSeq → not fully consumed → data loss.
   */
  @Test
  void testExpiredShard_emptyLastSeqWithEndSeq_dataLoss() {
    KinesisOffsetGen gen = offsetGen(true);
    Map<String, String> offsets = new HashMap<>();
    offsets.put("shard-0", "|" + SEQ_200);
    assertThrows(HoodieReadFromSourceException.class,
        () -> gen.checkDataLossOnExpiredShards(Collections.singletonList("shard-0"), offsets));
  }

  /**
   * Edge case: checkpoint value is null (corrupt checkpoint entry).
   * endSeq absent → not fully consumed → data loss.
   */
  @Test
  void testExpiredShard_nullCheckpointValue_dataLoss() {
    KinesisOffsetGen gen = offsetGen(true);
    Map<String, String> offsets = new HashMap<>();
    offsets.put("shard-0", null);
    assertThrows(HoodieReadFromSourceException.class,
        () -> gen.checkDataLossOnExpiredShards(Collections.singletonList("shard-0"), offsets));
  }

  /**
   * Multiple expired shards: one fully consumed, one not.
   * failOnDataLoss=true: first fully-consumed shard passes, then throws on the not-consumed one.
   */
  @Test
  void testExpiredShards_mixedConsumedAndNot_throwsOnUnconsumed() {
    KinesisOffsetGen gen = offsetGen(true);
    Map<String, String> offsets = new HashMap<>();
    offsets.put("shard-0", SEQ_200 + "|" + SEQ_200); // fully consumed
    offsets.put("shard-1", SEQ_100 + "|" + SEQ_200); // not consumed
    // Order is not guaranteed so we just verify an exception is thrown somewhere.
    assertThrows(HoodieReadFromSourceException.class,
        () -> gen.checkDataLossOnExpiredShards(Arrays.asList("shard-0", "shard-1"), offsets));
  }

  /** Multiple expired shards all fully consumed → no exception even with failOnDataLoss=true. */
  @Test
  void testExpiredShards_allFullyConsumed_noThrow() {
    KinesisOffsetGen gen = offsetGen(true);
    Map<String, String> offsets = new HashMap<>();
    offsets.put("shard-0", SEQ_200 + "|" + SEQ_200);
    offsets.put("shard-1", SEQ_300 + "|" + SEQ_300);
    assertDoesNotThrow(() -> gen.checkDataLossOnExpiredShards(
        Arrays.asList("shard-0", "shard-1"), offsets));
  }

  // ──────────────────────────────────────────────────────────────────────────
  // checkDataLossOnAvailableShards
  // ──────────────────────────────────────────────────────────────────────────

  /** lastSeq is before trim horizon → data loss, failOnDataLoss=true → throws. */
  @Test
  void testAvailableShard_lastSeqBeforeTrimHorizon_failOnDataLossTrue_throws() {
    KinesisOffsetGen gen = offsetGen(true);
    List<Shard> shards = Collections.singletonList(openShard("shard-0", SEQ_200));
    Map<String, String> fromSeq = Collections.singletonMap("shard-0", SEQ_100);
    assertThrows(HoodieReadFromSourceException.class,
        () -> gen.checkDataLossOnAvailableShards(shards, fromSeq));
  }

  /** lastSeq is before trim horizon → data loss, failOnDataLoss=false → warns, no throw. */
  @Test
  void testAvailableShard_lastSeqBeforeTrimHorizon_failOnDataLossFalse_noThrow() {
    KinesisOffsetGen gen = offsetGen(false);
    List<Shard> shards = Collections.singletonList(openShard("shard-0", SEQ_200));
    Map<String, String> fromSeq = Collections.singletonMap("shard-0", SEQ_100);
    assertDoesNotThrow(() -> gen.checkDataLossOnAvailableShards(shards, fromSeq));
  }

  /** lastSeq equals trim horizon boundary → no data loss (records after lastSeq still available). */
  @Test
  void testAvailableShard_lastSeqEqualsShardStartSeq_noDataLoss() {
    KinesisOffsetGen gen = offsetGen(true);
    List<Shard> shards = Collections.singletonList(openShard("shard-0", SEQ_200));
    Map<String, String> fromSeq = Collections.singletonMap("shard-0", SEQ_200);
    assertDoesNotThrow(() -> gen.checkDataLossOnAvailableShards(shards, fromSeq));
  }

  /** lastSeq is well within retention window → no data loss. */
  @Test
  void testAvailableShard_lastSeqAfterTrimHorizon_noDataLoss() {
    KinesisOffsetGen gen = offsetGen(true);
    List<Shard> shards = Collections.singletonList(openShard("shard-0", SEQ_100));
    Map<String, String> fromSeq = Collections.singletonMap("shard-0", SEQ_200);
    assertDoesNotThrow(() -> gen.checkDataLossOnAvailableShards(shards, fromSeq));
  }

  /** Shard has no entry in fromSequenceNumbers (no prior checkpoint) → skipped, no data loss. */
  @Test
  void testAvailableShard_noCheckpointEntry_skipped() {
    KinesisOffsetGen gen = offsetGen(true);
    List<Shard> shards = Collections.singletonList(openShard("shard-0", SEQ_200));
    assertDoesNotThrow(() -> gen.checkDataLossOnAvailableShards(shards, Collections.emptyMap()));
  }

  /** shardStartSeq is null (malformed shard metadata) → skipped, no data loss. */
  @Test
  void testAvailableShard_nullShardStartSeq_skipped() {
    KinesisOffsetGen gen = offsetGen(true);
    Shard shard = Shard.builder()
        .shardId("shard-0")
        .sequenceNumberRange(SequenceNumberRange.builder().build()) // startingSequenceNumber not set
        .build();
    Map<String, String> fromSeq = Collections.singletonMap("shard-0", SEQ_100);
    assertDoesNotThrow(() -> gen.checkDataLossOnAvailableShards(
        Collections.singletonList(shard), fromSeq));
  }

  /** sequenceNumberRange is null entirely → skipped, no data loss. */
  @Test
  void testAvailableShard_nullSequenceNumberRange_skipped() {
    KinesisOffsetGen gen = offsetGen(true);
    Shard shard = Shard.builder().shardId("shard-0").build();
    Map<String, String> fromSeq = Collections.singletonMap("shard-0", SEQ_100);
    assertDoesNotThrow(() -> gen.checkDataLossOnAvailableShards(
        Collections.singletonList(shard), fromSeq));
  }

  /**
   * Closed shard: lastSeq < shardStartSeq → data loss regardless of endSeq.
   * (Records before shardStartSeq were trimmed, even on a closed shard.)
   */
  @Test
  void testAvailableShard_closedShard_lastSeqBeforeTrimHorizon_dataLoss() {
    KinesisOffsetGen gen = offsetGen(true);
    // Closed shard: records from SEQ_200 to SEQ_300, but checkpoint has SEQ_100.
    List<Shard> shards = Collections.singletonList(closedShard("shard-0", SEQ_200, SEQ_300));
    Map<String, String> fromSeq = Collections.singletonMap("shard-0", SEQ_100);
    assertThrows(HoodieReadFromSourceException.class,
        () -> gen.checkDataLossOnAvailableShards(shards, fromSeq));
  }

  /**
   * Closed shard fully consumed (lastSeq >= endSeq >= shardStartSeq) → no data loss.
   * This verifies no false positive: once consumed, lastSeq is always >= shardStartSeq.
   */
  @Test
  void testAvailableShard_closedShard_fullyConsumed_noDataLoss() {
    KinesisOffsetGen gen = offsetGen(true);
    // shardStartSeq=SEQ_100, endSeq=SEQ_200, lastSeq=SEQ_200 (fully consumed).
    List<Shard> shards = Collections.singletonList(closedShard("shard-0", SEQ_100, SEQ_200));
    Map<String, String> fromSeq = Collections.singletonMap("shard-0", SEQ_200);
    assertDoesNotThrow(() -> gen.checkDataLossOnAvailableShards(shards, fromSeq));
  }

  /**
   * Multiple shards: one with lastSeq behind trim horizon, one healthy.
   * Only the trimmed one triggers data loss.
   */
  @Test
  void testAvailableShards_mixedTrimmed_throwsOnlyForTrimmedShard() {
    KinesisOffsetGen gen = offsetGen(true);
    List<Shard> shards = Arrays.asList(
        openShard("shard-0", SEQ_200),  // trim horizon advanced; shard-0 is trimmed
        openShard("shard-1", SEQ_100)); // shard-1 is fine
    Map<String, String> fromSeq = new HashMap<>();
    fromSeq.put("shard-0", SEQ_100);  // behind trim horizon → data loss
    fromSeq.put("shard-1", SEQ_200);  // ahead of trim horizon → OK
    assertThrows(HoodieReadFromSourceException.class,
        () -> gen.checkDataLossOnAvailableShards(shards, fromSeq));
  }

  /** All available shards within retention window → no data loss. */
  @Test
  void testAvailableShards_allWithinRetentionWindow_noThrow() {
    KinesisOffsetGen gen = offsetGen(true);
    List<Shard> shards = Arrays.asList(
        openShard("shard-0", SEQ_100),
        openShard("shard-1", SEQ_100));
    Map<String, String> fromSeq = new HashMap<>();
    fromSeq.put("shard-0", SEQ_200);
    fromSeq.put("shard-1", SEQ_300);
    assertDoesNotThrow(() -> gen.checkDataLossOnAvailableShards(shards, fromSeq));
  }
}
