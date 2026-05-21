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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestUpstreamEventTimeWatermarkExtractor {

  @Test
  void emptyInstantCollectionReturnsEmptyMap() throws IOException {
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    Map<String, Pair<Long, Long>> result =
        UpstreamEventTimeWatermarkExtractor.extractPerPartitionWatermarks(timeline, Collections.emptyList());
    assertTrue(result.isEmpty());
  }

  @Test
  void nullInstantCollectionReturnsEmptyMap() throws IOException {
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    Map<String, Pair<Long, Long>> result =
        UpstreamEventTimeWatermarkExtractor.extractPerPartitionWatermarks(timeline, null);
    assertTrue(result.isEmpty());
  }

  @Test
  void singleCommitWithEventTimePerPartitionRollsUp() throws IOException {
    HoodieCommitMetadata commit = new HoodieCommitMetadata();
    commit.addWriteStat("dt=2026-05-20", statWithEventTime(100L, 200L));
    commit.addWriteStat("dt=2026-05-20", statWithEventTime(50L, 250L));
    commit.addWriteStat("dt=2026-05-21", statWithEventTime(500L, 600L));

    HoodieInstant instant = mock(HoodieInstant.class);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(timeline.readCommitMetadata(any(HoodieInstant.class))).thenReturn(commit);

    Map<String, Pair<Long, Long>> result =
        UpstreamEventTimeWatermarkExtractor.extractPerPartitionWatermarks(timeline, Collections.singletonList(instant));

    assertEquals(2, result.size());
    assertEquals(Pair.of(50L, 250L), result.get("dt=2026-05-20"));
    assertEquals(Pair.of(500L, 600L), result.get("dt=2026-05-21"));
  }

  @Test
  void multipleCommitsAreFoldedAcross() throws IOException {
    HoodieCommitMetadata commit1 = new HoodieCommitMetadata();
    commit1.addWriteStat("dt=2026-05-20", statWithEventTime(100L, 200L));
    HoodieCommitMetadata commit2 = new HoodieCommitMetadata();
    commit2.addWriteStat("dt=2026-05-20", statWithEventTime(50L, 250L));
    commit2.addWriteStat("dt=2026-05-21", statWithEventTime(500L, 600L));

    HoodieInstant instant1 = mock(HoodieInstant.class);
    HoodieInstant instant2 = mock(HoodieInstant.class);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(timeline.readCommitMetadata(instant1)).thenReturn(commit1);
    when(timeline.readCommitMetadata(instant2)).thenReturn(commit2);

    Map<String, Pair<Long, Long>> result =
        UpstreamEventTimeWatermarkExtractor.extractPerPartitionWatermarks(timeline, Arrays.asList(instant1, instant2));

    assertEquals(Pair.of(50L, 250L), result.get("dt=2026-05-20"));
    assertEquals(Pair.of(500L, 600L), result.get("dt=2026-05-21"));
  }

  @Test
  void commitWithoutEventTimeIsSkipped() throws IOException {
    HoodieCommitMetadata commitWithoutWatermark = new HoodieCommitMetadata();
    commitWithoutWatermark.addWriteStat("dt=2026-05-20", new HoodieWriteStat());
    HoodieCommitMetadata commitWithWatermark = new HoodieCommitMetadata();
    commitWithWatermark.addWriteStat("dt=2026-05-20", statWithEventTime(100L, 200L));

    HoodieInstant instant1 = mock(HoodieInstant.class);
    HoodieInstant instant2 = mock(HoodieInstant.class);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(timeline.readCommitMetadata(instant1)).thenReturn(commitWithoutWatermark);
    when(timeline.readCommitMetadata(instant2)).thenReturn(commitWithWatermark);

    Map<String, Pair<Long, Long>> result =
        UpstreamEventTimeWatermarkExtractor.extractPerPartitionWatermarks(timeline, Arrays.asList(instant1, instant2));

    assertEquals(1, result.size());
    assertEquals(Pair.of(100L, 200L), result.get("dt=2026-05-20"));
  }

  @Test
  void partialMinOnlyAndMaxOnlyAreFolded() throws IOException {
    HoodieCommitMetadata commit1 = new HoodieCommitMetadata();
    HoodieWriteStat minOnly = new HoodieWriteStat();
    minOnly.setMinEventTime(100L);
    commit1.addWriteStat("dt=2026-05-20", minOnly);

    HoodieCommitMetadata commit2 = new HoodieCommitMetadata();
    HoodieWriteStat maxOnly = new HoodieWriteStat();
    maxOnly.setMaxEventTime(500L);
    commit2.addWriteStat("dt=2026-05-20", maxOnly);

    HoodieInstant instant1 = mock(HoodieInstant.class);
    HoodieInstant instant2 = mock(HoodieInstant.class);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(timeline.readCommitMetadata(instant1)).thenReturn(commit1);
    when(timeline.readCommitMetadata(instant2)).thenReturn(commit2);

    Map<String, Pair<Long, Long>> result =
        UpstreamEventTimeWatermarkExtractor.extractPerPartitionWatermarks(timeline, Arrays.asList(instant1, instant2));

    Pair<Long, Long> folded = result.get("dt=2026-05-20");
    assertNotNull(folded);
    assertEquals(Long.valueOf(100L), folded.getLeft());
    assertEquals(Long.valueOf(500L), folded.getRight());
  }

  @Test
  void readFailureSkipsCommitButContinues() throws IOException {
    HoodieCommitMetadata goodCommit = new HoodieCommitMetadata();
    goodCommit.addWriteStat("dt=2026-05-20", statWithEventTime(100L, 200L));

    HoodieInstant failingInstant = mock(HoodieInstant.class);
    HoodieInstant goodInstant = mock(HoodieInstant.class);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(timeline.readCommitMetadata(failingInstant)).thenThrow(new IOException("simulated"));
    when(timeline.readCommitMetadata(goodInstant)).thenReturn(goodCommit);

    Map<String, Pair<Long, Long>> result =
        UpstreamEventTimeWatermarkExtractor.extractPerPartitionWatermarks(timeline, Arrays.asList(failingInstant, goodInstant));

    assertEquals(1, result.size());
    assertEquals(Pair.of(100L, 200L), result.get("dt=2026-05-20"));
  }

  @Test
  void emptyCommitMetadataYieldsEmptyResult() throws IOException {
    HoodieCommitMetadata empty = new HoodieCommitMetadata();
    HoodieInstant instant = mock(HoodieInstant.class);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(timeline.readCommitMetadata(any(HoodieInstant.class))).thenReturn(empty);

    Map<String, Pair<Long, Long>> result =
        UpstreamEventTimeWatermarkExtractor.extractPerPartitionWatermarks(timeline, Collections.singletonList(instant));
    assertTrue(result.isEmpty());
  }

  private static HoodieWriteStat statWithEventTime(long min, long max) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setMinEventTime(min);
    stat.setMaxEventTime(max);
    return stat;
  }
}
