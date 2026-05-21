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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link StreamSync#buildUpstreamWatermarkPreCommitFunc(Map)} — the BiConsumer
 * that folds upstream-propagated per-partition (min, max) event-time into the downstream
 * commit's per-partition write stats.
 */
class TestStreamSyncUpstreamWatermarkPropagation {

  @Test
  void emptyUpstreamWatermarksReturnsEmptyOption() {
    assertFalse(StreamSync.buildUpstreamWatermarkPreCommitFunc(Collections.emptyMap()).isPresent());
  }

  @Test
  void nullUpstreamWatermarksReturnsEmptyOption() {
    assertFalse(StreamSync.buildUpstreamWatermarkPreCommitFunc(null).isPresent());
  }

  @Test
  void propagatesIntoMatchingPartitionStatsWhenPerRecordValuesAbsent() {
    Map<String, Pair<Long, Long>> upstream = new HashMap<>();
    upstream.put("dt=2026-05-20", Pair.of(100L, 200L));
    upstream.put("dt=2026-05-21", Pair.of(500L, 600L));

    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    HoodieWriteStat statMay20 = new HoodieWriteStat();
    statMay20.setPartitionPath("dt=2026-05-20");
    metadata.addWriteStat("dt=2026-05-20", statMay20);
    HoodieWriteStat statMay21 = new HoodieWriteStat();
    statMay21.setPartitionPath("dt=2026-05-21");
    metadata.addWriteStat("dt=2026-05-21", statMay21);

    Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> hook =
        StreamSync.buildUpstreamWatermarkPreCommitFunc(upstream);
    assertTrue(hook.isPresent());
    hook.get().accept(mock(HoodieTableMetaClient.class), metadata);

    assertEquals(Long.valueOf(100L), statMay20.getMinEventTime());
    assertEquals(Long.valueOf(200L), statMay20.getMaxEventTime());
    assertEquals(Long.valueOf(500L), statMay21.getMinEventTime());
    assertEquals(Long.valueOf(600L), statMay21.getMaxEventTime());
  }

  @Test
  void perRecordExtremeValuesAreNotRegressedByPropagation() {
    // Per-record path already produced a min lower than upstream and a max higher than upstream.
    // The Math.min / Math.max fold semantics on setMinEventTime / setMaxEventTime must keep the
    // more extreme per-record values.
    Map<String, Pair<Long, Long>> upstream = Collections.singletonMap(
        "dt=2026-05-20", Pair.of(100L, 200L));

    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath("dt=2026-05-20");
    stat.setMinEventTime(50L);  // lower than upstream 100 — should be preserved
    stat.setMaxEventTime(300L); // higher than upstream 200 — should be preserved
    metadata.addWriteStat("dt=2026-05-20", stat);

    StreamSync.buildUpstreamWatermarkPreCommitFunc(upstream).get()
        .accept(mock(HoodieTableMetaClient.class), metadata);

    assertEquals(Long.valueOf(50L), stat.getMinEventTime());
    assertEquals(Long.valueOf(300L), stat.getMaxEventTime());
  }

  @Test
  void upstreamPartitionsWithNoDownstreamMatchAreIgnored() {
    Map<String, Pair<Long, Long>> upstream = new HashMap<>();
    upstream.put("dt=2026-05-20", Pair.of(100L, 200L));
    upstream.put("dt=2026-05-99", Pair.of(999L, 999L)); // no matching downstream stat

    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath("dt=2026-05-20");
    metadata.addWriteStat("dt=2026-05-20", stat);

    StreamSync.buildUpstreamWatermarkPreCommitFunc(upstream).get()
        .accept(mock(HoodieTableMetaClient.class), metadata);

    assertEquals(Long.valueOf(100L), stat.getMinEventTime());
    assertEquals(Long.valueOf(200L), stat.getMaxEventTime());
    // Only the original partition stat exists in metadata; no synthetic stat created for the
    // unmatched upstream partition.
    assertEquals(1, metadata.getPartitionToWriteStats().size());
  }

  @Test
  void downstreamPartitionsWithNoUpstreamMatchAreLeftUntouched() {
    Map<String, Pair<Long, Long>> upstream = Collections.singletonMap(
        "dt=2026-05-20", Pair.of(100L, 200L));

    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    HoodieWriteStat untouchedStat = new HoodieWriteStat();
    untouchedStat.setPartitionPath("dt=2026-05-21");
    metadata.addWriteStat("dt=2026-05-21", untouchedStat);

    StreamSync.buildUpstreamWatermarkPreCommitFunc(upstream).get()
        .accept(mock(HoodieTableMetaClient.class), metadata);

    assertNull(untouchedStat.getMinEventTime());
    assertNull(untouchedStat.getMaxEventTime());
  }

  @Test
  void multipleStatsInSamePartitionAllReceivePropagation() {
    // A partition may have multiple write stats (e.g., one per file group). Each should get the
    // propagated values folded in.
    Map<String, Pair<Long, Long>> upstream = Collections.singletonMap(
        "dt=2026-05-20", Pair.of(100L, 200L));

    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    HoodieWriteStat stat1 = new HoodieWriteStat();
    stat1.setPartitionPath("dt=2026-05-20");
    HoodieWriteStat stat2 = new HoodieWriteStat();
    stat2.setPartitionPath("dt=2026-05-20");
    metadata.addWriteStat("dt=2026-05-20", stat1);
    metadata.addWriteStat("dt=2026-05-20", stat2);

    StreamSync.buildUpstreamWatermarkPreCommitFunc(upstream).get()
        .accept(mock(HoodieTableMetaClient.class), metadata);

    assertEquals(Long.valueOf(100L), stat1.getMinEventTime());
    assertEquals(Long.valueOf(200L), stat1.getMaxEventTime());
    assertEquals(Long.valueOf(100L), stat2.getMinEventTime());
    assertEquals(Long.valueOf(200L), stat2.getMaxEventTime());
  }
}
