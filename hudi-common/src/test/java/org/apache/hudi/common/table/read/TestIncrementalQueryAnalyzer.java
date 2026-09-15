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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
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
