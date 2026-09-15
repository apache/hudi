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

import org.apache.hudi.common.HoodieTableFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestIncrementalQueryAnalyzer {

  private static final String T1 = "20240101010001000";
  private static final String T2 = "20240101010002000";
  private static final String T3 = "20240101010003000";
  private static final String T4 = "20240101010004000";
  private static final String T5 = "20240101010005000";

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

  @Test
  void testBuilderRequiresMetaClientAndRangeType() {
    assertThrows(NullPointerException.class, () -> IncrementalQueryAnalyzer.builder()
        .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
        .build());
    assertThrows(NullPointerException.class, () -> IncrementalQueryAnalyzer.builder()
        .metaClient(mock(HoodieTableMetaClient.class))
        .build());
  }

  @Test
  void testV1LoadsArchivedTimelineOnceAndReusesIt() {
    AnalyzerFixture fixture = analyzerFixture(
        TimelineLayoutVersion.LAYOUT_VERSION_1,
        Arrays.asList(instant(T3), instant(T4)),
        Arrays.asList(instant(T1), instant(T2)));
    when(fixture.completionTimeQueryView.isArchived(T2)).thenReturn(true);

    IncrementalQueryAnalyzer.QueryContext queryContext = fixture.analyzer(T2, T4).analyze();

    assertEquals(Arrays.asList(T2, T3, T4), queryContext.getInstantTimeList());
    assertEquals(Collections.singletonList(T2), requestedTimes(queryContext.getArchivedInstants()));
    assertEquals(Arrays.asList(T3, T4), requestedTimes(queryContext.getActiveInstants()));
    assertSame(fixture.archivedCommitsTimeline, queryContext.getArchivedTimeline());
    verify(fixture.metaClient, times(1)).getArchivedTimeline(T2, false);
    verify(fixture.completionTimeQueryView, never()).getInstantTimes(
        any(HoodieTimeline.class), any(), any(), any(InstantRange.RangeType.class));
  }

  @Test
  void testV1EndOnlyLoadsArchiveOnceAndSelectsLastEligibleInstant() {
    AnalyzerFixture fixture = analyzerFixture(
        TimelineLayoutVersion.LAYOUT_VERSION_1,
        Arrays.asList(instant(T3), instant(T4)),
        Arrays.asList(instant(T1), instant(T2)));

    IncrementalQueryAnalyzer.QueryContext queryContext = fixture.analyzer(null, T2).analyze();

    assertEquals(Collections.singletonList(T2), queryContext.getInstantTimeList());
    assertEquals(Collections.singletonList(T2), requestedTimes(queryContext.getArchivedInstants()));
    assertTrue(queryContext.getActiveInstants().isEmpty());
    assertSame(fixture.archivedCommitsTimeline, queryContext.getArchivedTimeline());
    verify(fixture.metaClient, times(1)).getArchivedTimeline("", false);
  }

  @Test
  void testV1EndOnlyComparesCandidatesAcrossSavepointHole() {
    AnalyzerFixture fixture = analyzerFixture(
        TimelineLayoutVersion.LAYOUT_VERSION_1,
        Arrays.asList(instant(T1), instant(T5)),
        Arrays.asList(instant(T2), instant(T3), instant(T4)));
    when(fixture.completionTimeQueryView.isArchived(T1)).thenReturn(true);

    IncrementalQueryAnalyzer.QueryContext queryContext = fixture.analyzer(null, T4).analyze();

    assertEquals(Collections.singletonList(T4), queryContext.getInstantTimeList());
    assertEquals(Collections.singletonList(T4), requestedTimes(queryContext.getArchivedInstants()));
    assertTrue(queryContext.getActiveInstants().isEmpty());
    assertSame(fixture.archivedCommitsTimeline, queryContext.getArchivedTimeline());
    verify(fixture.metaClient, times(1)).getArchivedTimeline("", false);
  }

  @Test
  void testV1EndOnlyRetainsNewerActiveCandidateAcrossSavepointHole() {
    AnalyzerFixture fixture = analyzerFixture(
        TimelineLayoutVersion.LAYOUT_VERSION_1,
        Arrays.asList(instant(T4), instant(T5)),
        Arrays.asList(instant(T1), instant(T2), instant(T3)));
    when(fixture.completionTimeQueryView.isArchived(T4)).thenReturn(true);

    IncrementalQueryAnalyzer.QueryContext queryContext = fixture.analyzer(null, T4).analyze();

    assertEquals(Collections.singletonList(T4), queryContext.getInstantTimeList());
    assertEquals(Collections.singletonList(T4), requestedTimes(queryContext.getActiveInstants()));
    assertTrue(queryContext.getArchivedInstants().isEmpty());
    assertSame(fixture.archivedCommitsTimeline, queryContext.getArchivedTimeline());
    verify(fixture.metaClient, times(1)).getArchivedTimeline("", false);
  }

  @Test
  void testV1EndOnlyActiveCandidateAfterArchiveBoundaryDoesNotLoadArchive() {
    AnalyzerFixture fixture = analyzerFixture(
        TimelineLayoutVersion.LAYOUT_VERSION_1,
        Arrays.asList(instant(T3), instant(T4)),
        Arrays.asList(instant(T1), instant(T2)));
    when(fixture.completionTimeQueryView.isArchived(T3)).thenReturn(false);

    IncrementalQueryAnalyzer.QueryContext queryContext = fixture.analyzer(null, T3).analyze();

    assertEquals(Collections.singletonList(T3), queryContext.getInstantTimeList());
    assertEquals(Collections.singletonList(T3), requestedTimes(queryContext.getActiveInstants()));
    assertTrue(queryContext.getArchivedInstants().isEmpty());
    assertNull(queryContext.getArchivedTimeline());
    verify(fixture.metaClient, never()).getArchivedTimeline(anyString(), eq(false));
  }

  @Test
  void testV1ActiveOnlyRangeDoesNotLoadArchive() {
    AnalyzerFixture fixture = analyzerFixture(
        TimelineLayoutVersion.LAYOUT_VERSION_1,
        Arrays.asList(instant(T3), instant(T4)),
        Arrays.asList(instant(T1), instant(T2)));
    when(fixture.completionTimeQueryView.isArchived(T3)).thenReturn(false);

    IncrementalQueryAnalyzer.QueryContext queryContext = fixture.analyzer(T3, T4).analyze();

    assertEquals(Arrays.asList(T3, T4), queryContext.getInstantTimeList());
    assertNull(queryContext.getArchivedTimeline());
    verify(fixture.metaClient, never()).getArchivedTimeline(anyString(), eq(false));
  }

  @Test
  void testV1EarliestSnapshotDoesNotLoadArchive() {
    AnalyzerFixture fixture = analyzerFixture(
        TimelineLayoutVersion.LAYOUT_VERSION_1,
        Arrays.asList(instant(T3), instant(T4)),
        Arrays.asList(instant(T1), instant(T2)));

    IncrementalQueryAnalyzer.QueryContext queryContext = fixture.analyzer("earliest", null).analyze();

    assertEquals(Collections.singletonList(T4), queryContext.getInstantTimeList());
    assertTrue(queryContext.getInstantRange().isEmpty());
    verify(fixture.metaClient, never()).getArchivedTimeline(anyString(), eq(false));
  }

  @Test
  void testV1GloballySortsInstantsAcrossSavepointHole() {
    AnalyzerFixture fixture = analyzerFixture(
        TimelineLayoutVersion.LAYOUT_VERSION_1,
        Arrays.asList(instant(T1), instant(T5)),
        Arrays.asList(instant(T2), instant(T3), instant(T4)));

    IncrementalQueryAnalyzer.QueryContext queryContext = fixture.analyzer("earliest", T4).analyze();

    assertEquals(Arrays.asList(T1, T2, T3, T4), queryContext.getInstantTimeList());
    assertEquals(T4, queryContext.getLastInstant());
    assertTrue(queryContext.getInstantRange().get().isInRange(T4));
    verify(fixture.metaClient, times(1)).getArchivedTimeline("", false);
  }

  @Test
  void testV1DeduplicatesInstantsAcrossConcurrentArchival() {
    AnalyzerFixture fixture = analyzerFixture(
        TimelineLayoutVersion.LAYOUT_VERSION_1,
        Arrays.asList(instant(T1), instant(T4), instant(T5)),
        Arrays.asList(instant(T2), instant(T3), instant(T4)));

    IncrementalQueryAnalyzer.QueryContext queryContext = fixture.analyzer("earliest", T4).analyze();

    assertEquals(Arrays.asList(T1, T2, T3, T4), queryContext.getInstantTimeList());
    assertEquals(T4, queryContext.getLastInstant());
    verify(fixture.metaClient, times(1)).getArchivedTimeline("", false);
  }

  @Test
  void testV1PreservesActiveSideLimitForMixedRange() {
    AnalyzerFixture fixture = analyzerFixture(
        TimelineLayoutVersion.LAYOUT_VERSION_1,
        Arrays.asList(instant(T3), instant(T4)),
        Arrays.asList(instant(T1), instant(T2)));
    when(fixture.completionTimeQueryView.isArchived(T2)).thenReturn(true);

    IncrementalQueryAnalyzer.QueryContext queryContext = fixture.analyzerBuilder(T2, T4)
        .limit(1)
        .build()
        .analyze();

    assertEquals(Arrays.asList(T2, T3), queryContext.getInstantTimeList());
    assertEquals(Collections.singletonList(T2), requestedTimes(queryContext.getArchivedInstants()));
    assertEquals(Collections.singletonList(T3), requestedTimes(queryContext.getActiveInstants()));
    verify(fixture.metaClient, times(1)).getArchivedTimeline(T2, false);
  }

  @Test
  void testV1AppliesOpenClosedRangeDirectlyToRequestedTime() {
    AnalyzerFixture fixture = analyzerFixture(
        TimelineLayoutVersion.LAYOUT_VERSION_1,
        Arrays.asList(instant(T3), instant(T4)),
        Arrays.asList(instant(T1), instant(T2)));
    when(fixture.completionTimeQueryView.isArchived(T1)).thenReturn(true);

    IncrementalQueryAnalyzer.QueryContext queryContext = fixture.analyzerBuilder(T1, T4)
        .rangeType(InstantRange.RangeType.OPEN_CLOSED)
        .build()
        .analyze();

    assertEquals(Arrays.asList(T2, T3, T4), queryContext.getInstantTimeList());
    assertEquals(Collections.singletonList(T2), requestedTimes(queryContext.getArchivedInstants()));
    verify(fixture.metaClient, times(1)).getArchivedTimeline(T1, false);
  }

  @Test
  void testV1EarliestBoundedRangeLoadsArchiveOnce() {
    AnalyzerFixture fixture = analyzerFixture(
        TimelineLayoutVersion.LAYOUT_VERSION_1,
        Arrays.asList(instant(T3), instant(T4)),
        Arrays.asList(instant(T1), instant(T2)));

    IncrementalQueryAnalyzer.QueryContext queryContext = fixture.analyzer("earliest", T3).analyze();

    assertEquals(Arrays.asList(T1, T2, T3), queryContext.getInstantTimeList());
    assertTrue(queryContext.isConsumingFromEarliest());
    assertEquals(Arrays.asList(T1, T2), requestedTimes(queryContext.getArchivedInstants()));
    verify(fixture.metaClient, times(1)).getArchivedTimeline("", false);
  }

  @Test
  void testV2ContinuesToUseCompletionTimeQueryView() {
    AnalyzerFixture fixture = analyzerFixture(
        TimelineLayoutVersion.LAYOUT_VERSION_2,
        Arrays.asList(instant(T3), instant(T4)),
        Collections.emptyList());
    when(fixture.completionTimeQueryView.getInstantTimes(
        any(HoodieTimeline.class), any(), any(), any(InstantRange.RangeType.class)))
        .thenReturn(Arrays.asList(T3, T4));

    IncrementalQueryAnalyzer.QueryContext queryContext = fixture.analyzer(T3, T4).analyze();

    assertEquals(Arrays.asList(T3, T4), queryContext.getInstantTimeList());
    verify(fixture.completionTimeQueryView, times(1)).getInstantTimes(
        any(HoodieTimeline.class), any(), any(), eq(InstantRange.RangeType.CLOSED_CLOSED));
    verify(fixture.metaClient, never()).getArchivedTimeline(anyString(), eq(false));
  }

  private static HoodieInstant instant(String requestedTime) {
    HoodieInstant instant = mock(HoodieInstant.class);
    when(instant.requestedTime()).thenReturn(requestedTime);
    return instant;
  }

  private static HoodieTimeline timeline(List<HoodieInstant> instants) {
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(timeline.filterCompletedAndCompactionInstants()).thenReturn(timeline);
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    when(timeline.getInstantsAsStream()).thenAnswer(invocation -> instants.stream());
    when(timeline.lastInstant()).thenReturn(instants.isEmpty()
        ? Option.empty()
        : Option.of(instants.get(instants.size() - 1)));
    return timeline;
  }

  private static List<String> requestedTimes(List<HoodieInstant> instants) {
    return instants.stream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
  }

  private static AnalyzerFixture analyzerFixture(
      TimelineLayoutVersion layoutVersion,
      List<HoodieInstant> activeInstants,
      List<HoodieInstant> archivedInstants) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableFormat tableFormat = mock(HoodieTableFormat.class);
    TimelineFactory timelineFactory = mock(TimelineFactory.class);
    CompletionTimeQueryView completionTimeQueryView = mock(CompletionTimeQueryView.class);
    HoodieTimeline activeTimeline = timeline(activeInstants);
    HoodieTimeline archivedCommitsTimeline = timeline(archivedInstants);
    HoodieArchivedTimeline archivedTimeline = mock(HoodieArchivedTimeline.class);

    when(metaClient.getTimelineLayoutVersion()).thenReturn(layoutVersion);
    when(metaClient.getTableFormat()).thenReturn(tableFormat);
    when(metaClient.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
    when(metaClient.getCommitsAndCompactionTimeline()).thenReturn(activeTimeline);
    when(metaClient.getArchivedTimeline(anyString(), eq(false))).thenReturn(archivedTimeline);
    when(tableFormat.getTimelineFactory()).thenReturn(timelineFactory);
    when(timelineFactory.createCompletionTimeQueryView(metaClient)).thenReturn(completionTimeQueryView);
    when(completionTimeQueryView.isEmptyTable()).thenReturn(false);
    when(archivedTimeline.getCommitsTimeline()).thenReturn(archivedCommitsTimeline);

    return new AnalyzerFixture(
        metaClient, completionTimeQueryView, archivedCommitsTimeline);
  }

  private static class AnalyzerFixture {
    private final HoodieTableMetaClient metaClient;
    private final CompletionTimeQueryView completionTimeQueryView;
    private final HoodieTimeline archivedCommitsTimeline;

    private AnalyzerFixture(
        HoodieTableMetaClient metaClient,
        CompletionTimeQueryView completionTimeQueryView,
        HoodieTimeline archivedCommitsTimeline) {
      this.metaClient = metaClient;
      this.completionTimeQueryView = completionTimeQueryView;
      this.archivedCommitsTimeline = archivedCommitsTimeline;
    }

    private IncrementalQueryAnalyzer analyzer(String start, String end) {
      return analyzerBuilder(start, end).build();
    }

    private IncrementalQueryAnalyzer.Builder analyzerBuilder(String start, String end) {
      return IncrementalQueryAnalyzer.builder()
          .metaClient(metaClient)
          .startCompletionTime(start)
          .endCompletionTime(end)
          .rangeType(InstantRange.RangeType.CLOSED_CLOSED);
    }
  }
}
