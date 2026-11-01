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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.ArchivedTimelineLoader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.CompletionTimeQueryViewV2;
import org.apache.hudi.common.testutils.MockHoodieTimeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TestIncrementalQueryAnalyzer {

  @Test
  void testQueryContextRangeEdges() {
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    HoodieInstant active = mock(HoodieInstant.class);
    when(active.getCompletionTime()).thenReturn("20240102000000");
    IncrementalQueryAnalyzer.QueryContext earliestToLatest = IncrementalQueryAnalyzer.QueryContext.create(
        null, null, Arrays.asList("001", "002"), Collections.emptyList(), Collections.singletonList(active), timeline, null);

    assertFalse(earliestToLatest.isEmpty());
    assertEquals("002", earliestToLatest.getLastInstant());
    assertTrue(earliestToLatest.isConsumingFromEarliest());
    assertTrue(earliestToLatest.isConsumingToLatest());
    assertTrue(earliestToLatest.getInstantRange().isEmpty());
    assertEquals("20240102000000", earliestToLatest.getMaxCompletionTime());
    assertEquals(Collections.singletonList(active), earliestToLatest.getInstants());

    IncrementalQueryAnalyzer.QueryContext boundedEarliest = IncrementalQueryAnalyzer.QueryContext.create(
        null, "002", Arrays.asList("001", "002"), Collections.emptyList(), Collections.emptyList(), timeline, null);
    HoodieInstant latestActive = mock(HoodieInstant.class);
    when(latestActive.getCompletionTime()).thenReturn("20240103000000");
    when(timeline.getInstantsAsStream()).thenReturn(Stream.of(latestActive));
    InstantRange boundedRange = boundedEarliest.getInstantRange().get();
    assertTrue(boundedEarliest.isConsumingFromEarliest());
    assertFalse(boundedEarliest.isConsumingToLatest());
    assertTrue(boundedRange.isInRange("001"));
    assertTrue(boundedRange.isInRange("002"));
    assertEquals("20240103000000", boundedEarliest.getMaxCompletionTime());
    assertNull(boundedEarliest.getArchivedTimeline());

    IncrementalQueryAnalyzer.QueryContext exact = IncrementalQueryAnalyzer.QueryContext.create(
        "001", "002", Arrays.asList("001", "002"), Collections.emptyList(), Collections.emptyList(), timeline, null);
    assertFalse(exact.isConsumingFromEarliest());
    assertTrue(exact.getInstantRange().get().isInRange("001"));
    assertFalse(exact.getInstantRange().get().isInRange("003"));
    assertThrows(IllegalStateException.class, IncrementalQueryAnalyzer.QueryContext.EMPTY::getLastInstant);
  }

  @ParameterizedTest
  @CsvSource({"clean,false", "clean,true", "rollback,false", "rollback,true"})
  void testActiveNonWriteDoesNotTriggerArchivedTimelineLookup(String action, boolean savepoint) {
    String savepointTime = "20261001000000000";
    String nonWriteTime = "20261002000000000";
    String writeTime = "20261003000000000";
    String completionTime = "20261005000000000";
    List<HoodieInstant> instants = new ArrayList<>();
    if (savepoint) {
      instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, savepointTime, savepointTime));
      instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, savepointTime, savepointTime));
    }
    // Both actions finish in the query window, but only the write belongs in the result.
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, action, nonWriteTime, completionTime));
    instants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, writeTime, completionTime));
    ArchivedTimelineLoader loader = mock(ArchivedTimelineLoader.class);
    MockHoodieTimeline active = new MockHoodieTimeline(instants);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(metaClient.getActiveTimeline()).thenReturn(active);
    when(metaClient.getCommitsTimeline()).thenReturn(active.getCommitsTimeline());
    when(metaClient.getCommitsAndCompactionTimeline()).thenReturn(active.getCommitsAndCompactionTimeline());
    when(metaClient.getTableFormat().getTimelineFactory().createArchivedTimelineLoader()).thenReturn(loader);
    try (CompletionTimeQueryViewV2 view = new CompletionTimeQueryViewV2(metaClient)) {
      assertEquals(writeTime, view.getCursorInstant());
      assertFalse(view.isArchived(nonWriteTime));
      assertFalse(view.isArchived(writeTime));
      if (savepoint) {
        assertFalse(view.isArchived(savepointTime));
      }
      when(metaClient.getTableFormat().getTimelineFactory().createCompletionTimeQueryView(metaClient)).thenReturn(view);
      IncrementalQueryAnalyzer.QueryContext result = IncrementalQueryAnalyzer.builder()
          .metaClient(metaClient)
          .startCompletionTime(completionTime)
          .endCompletionTime(completionTime)
          .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
          .build().analyze();
      assertEquals(Collections.singletonList(writeTime), result.getInstantTimeList());
      assertTrue(result.getArchivedInstants().isEmpty());
      // The one-day lookback is after writeTime, isolating classification from archive completion-time loading.
      verifyNoInteractions(loader);
      verify(metaClient, never()).getArchivedTimeline(anyString(), eq(false));
      verify(metaClient, never()).getArchivePath();
    }
  }

  @Test
  void testBuilderRequiresMetaClientAndRangeType() {
    assertThrows(NullPointerException.class, () -> IncrementalQueryAnalyzer.builder()
        .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
        .build());
    assertThrows(NullPointerException.class, () -> IncrementalQueryAnalyzer.builder()
        .metaClient(mock(HoodieTableMetaClient.class))
        .build());
  }
}
