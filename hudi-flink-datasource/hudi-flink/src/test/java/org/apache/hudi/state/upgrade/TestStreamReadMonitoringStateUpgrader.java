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

package org.apache.hudi.state.upgrade;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.timeline.versioning.DefaultTimelineFactory;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.state.upgrade.source.StreamReadMonitoringStateUpgrader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStreamReadMonitoringStateUpgrader {

  private static final TimelineFactory TIMELINE_FACTORY = new DefaultTimelineFactory();

  @Mock
  private HoodieTableMetaClient metaClient;

  @Mock
  private HoodieActiveTimeline activeTimeline;

  @Mock
  private HoodieArchivedTimeline archivedTimeline;

  private StreamReadMonitoringStateUpgrader upgrader;

  // issuedInstant
  private final String requestTimeTs = "20250203000000000";
  // issuedOffset
  private final String completionTimeTs = "20250203000000900";

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    upgrader = new StreamReadMonitoringStateUpgrader(metaClient, requestTimeTs);
  }

  @Test
  void testDetectVersion_V0State() {
    List<String> v0State = Collections.singletonList(requestTimeTs);
    assertEquals(StateVersion.V0, upgrader.detectVersion(v0State));
  }

  @Test
  void testDetectVersion_V1State() {
    List<String> v1State = Arrays.asList(requestTimeTs, completionTimeTs);
    assertEquals(StateVersion.V1, upgrader.detectVersion(v1State));
  }

  @Test
  void testDetectVersion_InvalidState() {
    List<String> invalidState = Arrays.asList(requestTimeTs, completionTimeTs, "3");
    assertThrows(IllegalStateException.class, () -> upgrader.detectVersion(invalidState));
  }

  @Test
  void testCanUpgrade() {
    assertFalse(upgrader.canUpgrade(StateVersion.V0, StateVersion.V0));
    assertTrue(upgrader.canUpgrade(StateVersion.V0, StateVersion.V1));
    assertFalse(upgrader.canUpgrade(StateVersion.V1, StateVersion.V0));
    assertFalse(upgrader.canUpgrade(StateVersion.V1, StateVersion.V1));
  }

  @Test
  void testUpgrade_V0ToV1_ActiveTimeline() {
    List<String> oldState = Collections.singletonList(requestTimeTs);
    HoodieInstant activeInstant = new HoodieInstant(COMPLETED, COMMIT_ACTION, completionTimeTs, completionTimeTs, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);

    when(activeTimeline.isBeforeTimelineStarts(requestTimeTs)).thenReturn(false);
    when(activeTimeline.findInstantsAfterOrEquals(requestTimeTs, 3))
        .thenReturn(TIMELINE_FACTORY.createDefaultTimeline(Stream.of(activeInstant), null));

    List<String> result = upgrader.upgrade(oldState, StateVersion.V0, StateVersion.V1);

    assertEquals(2, result.size());
    assertEquals(requestTimeTs, result.get(0));
    assertEquals(completionTimeTs, result.get(1));
    // verify that #getArchivedTimelien was NEVER invoked during test
    verify(metaClient, never()).getArchivedTimeline(any(), anyBoolean());
  }

  @Test
  void testUpgrade_V0ToV1_ArchivedTimeline() {
    List<String> oldState = Collections.singletonList(requestTimeTs);
    HoodieInstant archivedInstant = new HoodieInstant(COMPLETED, COMMIT_ACTION, completionTimeTs, completionTimeTs, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);

    when(activeTimeline.isBeforeTimelineStarts(requestTimeTs)).thenReturn(true);
    when(metaClient.getArchivedTimeline(StringUtils.EMPTY_STRING, false)).thenReturn(archivedTimeline);
    when(archivedTimeline.findInstantsAfterOrEquals(requestTimeTs, 3))
        .thenReturn(TIMELINE_FACTORY.createDefaultTimeline(Stream.of(archivedInstant), null));

    List<String> result = upgrader.upgrade(oldState, StateVersion.V0, StateVersion.V1);

    assertEquals(2, result.size());
    assertEquals(requestTimeTs, result.get(0));
    assertEquals(completionTimeTs, result.get(1));
    // verify that #getArchivedTimelien was invoked during test
    verify(metaClient).getArchivedTimeline(StringUtils.EMPTY_STRING, false);
  }

  @Test
  void testUpgrade_V0ToV1_NoInstantFound() {
    List<String> oldState = Collections.singletonList(requestTimeTs);

    // Mock timeline to not return any instants
    when(activeTimeline.isBeforeTimelineStarts(requestTimeTs)).thenReturn(false);
    when(activeTimeline.findInstantsAfterOrEquals(requestTimeTs, 3))
        .thenReturn(TIMELINE_FACTORY.createDefaultTimeline(Stream.of(), null));

    assertThrows(HoodieException.class, () ->
        upgrader.upgrade(oldState, StateVersion.V0, StateVersion.V1));
  }

  @Test
  void testUpgrade_V1ToV1_NoChange() {
    List<String> v1State = Arrays.asList(requestTimeTs, completionTimeTs);
    List<String> result = upgrader.upgrade(v1State, StateVersion.V1, StateVersion.V1);
    assertEquals(v1State, result);
  }

  @Test
  void testUpgrade_UnsupportedPath() {
    List<String> state = Collections.singletonList(requestTimeTs);
    assertThrows(HoodieUpgradeDowngradeException.class, () ->
        upgrader.upgrade(state, StateVersion.V0, StateVersion.UNKNOWN));
  }

}
