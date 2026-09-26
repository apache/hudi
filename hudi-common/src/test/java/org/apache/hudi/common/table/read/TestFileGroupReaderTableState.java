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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.Types;
import org.apache.hudi.common.schema.internal.utils.SerDeHelper;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.DefaultInstantGenerator;
import org.apache.hudi.common.table.timeline.versioning.v1.BaseTimelineV1;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantGeneratorV1;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link CommittedInstants} and {@link FileGroupReaderTableState}.
 */
class TestFileGroupReaderTableState {

  // Second-granularity instants predate millisecond instant times; "999" is the suffix added when they are extended.
  private static final String SECONDS_INSTANT = "20240101000000";
  private static final DefaultInstantGenerator INSTANT_GENERATOR = new DefaultInstantGenerator();
  private static final List<String> CANDIDATES = Arrays.asList(
      "20230101000000000", // before the first completed instant: treated as committed (archived)
      SECONDS_INSTANT + "999", // extended form of a completed second-granularity instant
      "20240201000000000", // completed
      "20240215000000000", // neither completed nor inflight, after the timeline start
      "20240301000000000", // inflight
      "20240401000000000", // completed
      "20240501000000000"); // after every instant

  @Test
  void committedInstantsMatchTimelineSemantics() throws Exception {
    HoodieTimeline commitsTimeline = new MockHoodieTimeline(Arrays.asList(
        completed(SECONDS_INSTANT), completed("20240201000000000"), inflight("20240301000000000"), completed("20240401000000000")));
    CommittedInstants fromTimeline = CommittedInstants.fromCommitsTimeline(commitsTimeline);
    CommittedInstants fromStrings = CommittedInstants.of(new ArrayList<>(fromTimeline.getCompletedInstants()),
        new ArrayList<>(fromTimeline.getInflightInstants()), fromTimeline.getTimelineStartInstant());
    CommittedInstants deserialized = roundTrip(fromTimeline);

    for (String instant : CANDIDATES) {
      boolean expected = !commitsTimeline.filterInflights().containsInstant(instant)
          && commitsTimeline.filterCompletedInstants().containsOrBeforeTimelineStarts(instant);
      assertEquals(expected, fromTimeline.isCommitted(instant), instant);
      assertEquals(expected, fromStrings.isCommitted(instant), instant);
      assertEquals(expected, deserialized.isCommitted(instant), instant);
    }
    assertTrue(fromTimeline.isCommitted("20230101000000000"));
    assertTrue(fromTimeline.isCommitted(SECONDS_INSTANT + "999"));
    assertFalse(fromTimeline.isCommitted("20240215000000000"));
    assertFalse(fromTimeline.isCommitted("20240301000000000"));
  }

  /**
   * Tables before version 8 use the layout version 1 timeline: completed instants carry no completion time, and
   * instants written before millisecond instant times have second granularity.
   */
  @Test
  void committedInstantsMatchLayoutVersionOneTimelineSemantics() throws Exception {
    InstantGeneratorV1 generator = new InstantGeneratorV1();
    String secondsInflight = "20240110000000";
    HoodieTimeline commitsTimeline = new BaseTimelineV1(Stream.of(
        generator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, SECONDS_INSTANT),
        generator.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, secondsInflight),
        generator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "20240201000000000"),
        generator.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, "20240215000000000"),
        generator.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "20240301000000000"),
        generator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "20240401000000000")), null);
    CommittedInstants fromTimeline = CommittedInstants.fromCommitsTimeline(commitsTimeline);
    CommittedInstants deserialized = roundTrip(fromTimeline);

    List<String> candidates = new ArrayList<>(CANDIDATES);
    candidates.addAll(Arrays.asList(
        SECONDS_INSTANT, // completed second-granularity instant in its original form
        secondsInflight,
        secondsInflight + "999", // extended form of an inflight second-granularity instant
        "20231231000000")); // second-granularity instant before the timeline start
    for (String instant : candidates) {
      boolean expected = !commitsTimeline.filterInflights().containsInstant(instant)
          && commitsTimeline.filterCompletedInstants().containsOrBeforeTimelineStarts(instant);
      assertEquals(expected, fromTimeline.isCommitted(instant), instant);
      assertEquals(expected, deserialized.isCommitted(instant), instant);
    }
    assertEquals(Option.of(SECONDS_INSTANT), fromTimeline.getTimelineStartInstant());
    assertTrue(fromTimeline.isCommitted(SECONDS_INSTANT));
    assertTrue(fromTimeline.isCommitted(SECONDS_INSTANT + "999"));
    assertTrue(fromTimeline.isCommitted("20231231000000"));
    assertFalse(fromTimeline.isCommitted(secondsInflight + "999"));
    assertFalse(fromTimeline.isCommitted("20240215000000000"));
    assertFalse(fromTimeline.isCommitted("20240301000000000"));
    assertTrue(fromTimeline.isCommitted("20240401000000000"));
  }

  @Test
  void committedInstantsOfEmptyTimeline() {
    CommittedInstants committed = CommittedInstants.fromCommitsTimeline(new MockHoodieTimeline(Collections.emptyList()));
    assertFalse(committed.getTimelineStartInstant().isPresent());
    assertFalse(committed.isCommitted("20240101000000000"));
  }

  @Test
  void capturedStateAnswersWithoutMetaClient() throws Exception {
    CommittedInstants committed = CommittedInstants.of(Arrays.asList("20240201000000000"), Arrays.asList("20240301000000000"),
        Option.of("20240201000000000"));
    String history = SerDeHelper.toJson(Arrays.asList(schemaWithVersion(100L, "a"), schemaWithVersion(200L, "b")));
    FileGroupReaderTableState state = roundTrip(
        FileGroupReaderTableState.of(new StoragePath("/tmp/table"), new HoodieTableConfig(), Option.of(committed), Option.of(history)));

    assertEquals("/tmp/table", state.getBasePath().toString());
    assertTrue(state.isCommitted("20240201000000000"));
    assertFalse(state.isCommitted("20240301000000000"));
    // A version resolves to the newest schema at or before it.
    assertEquals("a", state.getInternalSchema(150L).getRecord().fields().get(0).name());
    assertEquals("b", state.getInternalSchema(250L).getRecord().fields().get(0).name());
    assertTrue(state.getInternalSchema(50L).isEmptySchema());
  }

  @Test
  void uncapturedValuesFailClearly() {
    FileGroupReaderTableState state = FileGroupReaderTableState.of(new StoragePath("/tmp/table"), new HoodieTableConfig(), Option.empty(), Option.empty());
    assertThrows(IllegalStateException.class, () -> state.isCommitted("20240201000000000"));
    assertThrows(IllegalStateException.class, () -> state.getInternalSchema(100L));
  }

  private static HoodieInstant completed(String requestedTime) {
    return INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, requestedTime, requestedTime);
  }

  private static HoodieInstant inflight(String requestedTime) {
    return INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, requestedTime);
  }

  private static InternalSchema schemaWithVersion(long versionId, String fieldName) {
    return new InternalSchema(versionId, Types.RecordType.get(Arrays.asList(Types.Field.get(1, true, fieldName, Types.LongType.get()))));
  }

  @SuppressWarnings("unchecked")
  private static <T> T roundTrip(T value) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(value);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }
}
