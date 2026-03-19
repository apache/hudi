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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.fs.NoOpConsistencyGuard;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.testutils.Assertions.assertStreamEquals;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_PARSER;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.apache.hudi.common.util.CleanerUtils.getCleanerPlan;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieActiveTimeline}.
 */
public class TestHoodieActiveTimeline extends HoodieCommonTestHarness {

  private HoodieActiveTimeline timeline;

  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Test
  public void testLoadingInstantsFromFiles() throws IOException {
    InstantGenerator instantGenerator = INSTANT_GENERATOR;
    TimelineFactory timelineFactory = TIMELINE_FACTORY;
    HoodieInstant instant1 = instantGenerator.createNewInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieInstant instant2 = instantGenerator.createNewInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "3");
    HoodieInstant instant3 = instantGenerator.createNewInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "5");
    HoodieInstant instant4 = instantGenerator.createNewInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "8");
    HoodieInstant instant1Complete = instantGenerator.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieInstant instant2Complete = instantGenerator.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "3");
    HoodieInstant instant3Complete = instantGenerator.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "5");
    HoodieInstant instant4Complete = instantGenerator.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "8");

    HoodieInstant instant5 = instantGenerator.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "9");

    timeline = timelineFactory.createActiveTimeline(metaClient);
    timeline.createNewInstant(instant1);
    timeline.transitionRequestedToInflight(instant1, Option.empty());
    // Won't lock here since InProcessLockProvider is not in hudi-common
    timeline.saveAsComplete(instantGenerator.createNewInstant(State.INFLIGHT, instant1.getAction(), instant1.requestedTime()),
        Option.empty(), HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
    timeline.createNewInstant(instant2);
    timeline.transitionRequestedToInflight(instant2, Option.empty());
    timeline.saveAsComplete(instantGenerator.createNewInstant(State.INFLIGHT, instant2.getAction(), instant2.requestedTime()),
        Option.empty(), HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
    timeline.createNewInstant(instant3);
    timeline.transitionRequestedToInflight(instant3, Option.empty());
    timeline.saveAsComplete(instantGenerator.createNewInstant(State.INFLIGHT, instant3.getAction(), instant3.requestedTime()),
        Option.empty(), HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
    timeline.createNewInstant(instant4);
    timeline.transitionRequestedToInflight(instant4, Option.empty());
    timeline.saveAsComplete(instantGenerator.createNewInstant(State.INFLIGHT, instant4.getAction(), instant4.requestedTime()),
        Option.empty(), HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
    timeline.createNewInstant(instant5);
    timeline = timeline.reload();

    assertEquals(5, timeline.countInstants(), "Total instants should be 5");
    assertStreamEquals(
        Stream.of(instant1Complete, instant2Complete, instant3Complete, instant4Complete, instant5),
        timeline.getInstantsAsStream(), "Check the instants stream");
    assertStreamEquals(
        Stream.of(instant1Complete, instant2Complete, instant3Complete, instant4Complete, instant5),
        timeline.getCommitTimeline().getInstantsAsStream(), "Check the instants stream");

    assertStreamEquals(
        Stream.of(instant1Complete, instant2Complete, instant3Complete, instant4Complete, instant5),
        timeline.getCommitAndReplaceTimeline().getInstantsAsStream(), "Check the instants stream");
    assertStreamEquals(
        Stream.of(instant1Complete, instant2Complete, instant3Complete, instant4Complete),
        timeline.getCommitAndReplaceTimeline().filterCompletedInstants().getInstantsAsStream(),
        "Check the instants stream");
    assertStreamEquals(Stream.of(instant5),
        timeline.getCommitAndReplaceTimeline().filterPendingExcludingCompactionAndLogCompaction().getInstantsAsStream(),
        "Check the instants stream");
  }

  @Test
  public void testTimelineOperationsBasic() {
    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    assertTrue(timeline.empty());
    assertEquals(0, timeline.countInstants());
    assertEquals(Option.empty(), timeline.firstInstant());
    assertEquals(Option.empty(), timeline.nthInstant(5));
    assertEquals(Option.empty(), timeline.nthInstant(-1));
    assertEquals(Option.empty(), timeline.lastInstant());
    assertFalse(timeline.containsInstant(INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "01")));
  }

  @Test
  public void testTimelineOperations() {
    timeline = new MockHoodieTimeline(Stream.of("01", "03", "05", "07", "09", "11", "13", "15", "17", "19"),
        Stream.of("21", "23"));
    assertStreamEquals(Stream.of("05", "07", "09", "11"),
        timeline.getCommitAndReplaceTimeline().filterCompletedInstants().findInstantsInRange("04", "11")
            .getInstantsAsStream().map(HoodieInstant::requestedTime),
        "findInstantsInRange should return 4 instants");
    assertStreamEquals(Stream.of("03", "05", "07", "09", "11"),
        timeline.getCommitAndReplaceTimeline().filterCompletedInstants().findInstantsInClosedRange("03", "11")
            .getInstantsAsStream().map(HoodieInstant::requestedTime),
        "findInstantsInClosedRange should return 5 instants");
    assertStreamEquals(Stream.of("09", "11"),
        timeline.getCommitAndReplaceTimeline().filterCompletedInstants().findInstantsAfter("07", 2)
            .getInstantsAsStream().map(HoodieInstant::requestedTime),
        "findInstantsAfter 07 should return 2 instants");
    assertStreamEquals(Stream.of("01", "03", "05"),
        timeline.getCommitAndReplaceTimeline().filterCompletedInstants().findInstantsBefore("07")
            .getInstantsAsStream().map(HoodieInstant::requestedTime),
        "findInstantsBefore 07 should return 3 instants");
    assertFalse(timeline.empty());
    assertFalse(timeline.getCommitAndReplaceTimeline().filterPendingExcludingCompactionAndLogCompaction().empty());
    assertEquals(12, timeline.countInstants());
    assertEquals("01", timeline.firstInstant(
        HoodieTimeline.COMMIT_ACTION, State.COMPLETED).get().requestedTime());
    assertEquals("21", timeline.firstInstant(
        HoodieTimeline.COMMIT_ACTION, State.INFLIGHT).get().requestedTime());
    assertFalse(timeline.firstInstant(
        HoodieTimeline.COMMIT_ACTION, State.REQUESTED).isPresent());
    assertFalse(timeline.firstInstant(
        HoodieTimeline.REPLACE_COMMIT_ACTION, State.COMPLETED).isPresent());

    HoodieTimeline activeCommitTimeline = timeline.getCommitAndReplaceTimeline().filterCompletedInstants();
    assertEquals(10, activeCommitTimeline.countInstants());

    assertEquals("01", activeCommitTimeline.firstInstant().get().requestedTime());
    assertEquals("11", activeCommitTimeline.nthInstant(5).get().requestedTime());
    assertEquals("19", activeCommitTimeline.lastInstant().get().requestedTime());
    assertEquals("09", activeCommitTimeline.nthFromLastInstant(5).get().requestedTime());
    assertTrue(activeCommitTimeline.containsInstant(INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "09")));
    assertFalse(activeCommitTimeline.isBeforeTimelineStarts("02"));
    assertTrue(activeCommitTimeline.isBeforeTimelineStarts("00"));
  }

  @Test
  public void testAllowTempCommit() {
    shouldAllowTempCommit(true, hoodieMetaClient -> {
      timeline = TIMELINE_FACTORY.createActiveTimeline(hoodieMetaClient);

      HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "1");
      timeline.createNewInstant(instant1);

      timeline.saveAsComplete(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, instant1.getAction(),
          instant1.requestedTime()), Option.of(new HoodieCommitMetadata()), HoodieInstantTimeGenerator.getCurrentInstantTimeStr());

      timeline = timeline.reload();

      assertTrue(timeline.getContiguousCompletedWriteTimeline().lastInstant().isPresent());
      assertEquals(instant1.requestedTime(), timeline.getContiguousCompletedWriteTimeline().lastInstant().get().requestedTime());
    });
  }

  @Test
  public void testGetContiguousCompletedWriteTimeline() {
    // a mock timeline with holes
    timeline = new MockHoodieTimeline(Stream.of("01", "03", "05", "07", "13", "15", "17"),
        Stream.of("09", "11", "19"));
    assertTrue(timeline.getContiguousCompletedWriteTimeline().lastInstant().isPresent());
    assertEquals("07", timeline.getContiguousCompletedWriteTimeline().lastInstant().get().requestedTime());

    // add some instants where two are inflight and one of them (instant8 below) is not part of write timeline
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieInstant instant2 = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "2");
    HoodieInstant instant3 = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "3");
    HoodieInstant instant4 = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "4");
    HoodieInstant instant5 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "5");
    HoodieInstant instant6 = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "6");
    HoodieInstant instant7 = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "7");
    HoodieInstant instant8 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.RESTORE_ACTION, "8");

    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    timeline.createCompleteInstant(instant1);
    timeline.createCompleteInstant(instant2);
    timeline.createCompleteInstant(instant3);
    timeline.createCompleteInstant(instant4);
    timeline.createNewInstant(instant5);
    timeline.createCompleteInstant(instant6);
    timeline.createCompleteInstant(instant7);
    timeline.createNewInstant(instant8);
    timeline.setInstants(Stream.of(instant1, instant2, instant3, instant4, instant5, instant6, instant7, instant8).collect(Collectors.toList()));

    assertTrue(timeline.getContiguousCompletedWriteTimeline().lastInstant().isPresent());
    assertEquals(instant4.requestedTime(), timeline.getContiguousCompletedWriteTimeline().lastInstant().get().requestedTime());
    // transition both inflight instants to complete
    timeline.saveAsComplete(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, instant5.getAction(), instant5.requestedTime()), Option.empty(), HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
    timeline.saveAsComplete(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, instant8.getAction(), instant8.requestedTime()), Option.empty(), HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
    timeline = timeline.reload();
    // instant8 in not considered in write timeline, so last completed instant in timeline should be instant7
    assertTrue(timeline.getContiguousCompletedWriteTimeline().lastInstant().isPresent());
    assertEquals(instant7.requestedTime(), timeline.getContiguousCompletedWriteTimeline().lastInstant().get().requestedTime());
  }

  @Test
  public void testTimelineWithSavepointAndHoles() {
    timeline = new MockHoodieTimeline(Stream.of(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "01"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, "01"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "03"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, "03"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "05") // this can be DELTA_COMMIT/REPLACE_COMMIT as well
    ).collect(Collectors.toList()));
    assertTrue(timeline.isBeforeTimelineStarts("00"));
    assertTrue(timeline.isBeforeTimelineStarts("01"));
    assertTrue(timeline.isBeforeTimelineStarts("02"));
    assertTrue(timeline.isBeforeTimelineStarts("03"));
    assertTrue(timeline.isBeforeTimelineStarts("04"));
    assertFalse(timeline.isBeforeTimelineStarts("05"));
    assertFalse(timeline.isBeforeTimelineStarts("06"));

    // with an inflight savepoint in between
    timeline = new MockHoodieTimeline(Stream.of(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "01"),
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.SAVEPOINT_ACTION, "01"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "03"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, "03"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "05")
    ).collect(Collectors.toList()));
    assertTrue(timeline.isBeforeTimelineStarts("00"));
    assertTrue(timeline.isBeforeTimelineStarts("01"));
    assertTrue(timeline.isBeforeTimelineStarts("02"));
    assertTrue(timeline.isBeforeTimelineStarts("03"));
    assertTrue(timeline.isBeforeTimelineStarts("04"));
    assertFalse(timeline.isBeforeTimelineStarts("05"));
    assertFalse(timeline.isBeforeTimelineStarts("06"));

    // with a pending replacecommit after savepoints
    timeline = new MockHoodieTimeline(Stream.of(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "01"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, "01"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "03"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, "03"),
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "05"),
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, "06"),
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, "07")
    ).collect(Collectors.toList()));
    assertTrue(timeline.isBeforeTimelineStarts("00"));
    assertTrue(timeline.isBeforeTimelineStarts("01"));
    assertTrue(timeline.isBeforeTimelineStarts("02"));
    assertTrue(timeline.isBeforeTimelineStarts("03"));
    assertTrue(timeline.isBeforeTimelineStarts("04"));
    assertFalse(timeline.isBeforeTimelineStarts("05"));
    assertFalse(timeline.isBeforeTimelineStarts("06"));
  }

  @Test
  public void testTimelineGetOperations() {
    List<HoodieInstant> allInstants = getAllInstants();
    Supplier<Stream<HoodieInstant>> allInstantsSup = allInstants::stream;
    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient, true);
    timeline.setInstants(allInstants);

    /*
     * Helper function to check HoodieTimeline only contains some type of Instant actions.
     * @param timeline The HoodieTimeline to check
     * @param actions The actions that should be present in the timeline being checked
     */
    BiConsumer<HoodieTimeline, Set<String>> checkTimeline = (HoodieTimeline timeline, Set<String> actions) -> {
      List<HoodieInstant> expectedInstants = allInstantsSup.get().filter(i -> actions.contains(i.getAction())).collect(Collectors.toList());
      List<HoodieInstant> unexpectedInstants = allInstantsSup.get().filter(i -> !actions.contains(i.getAction())).collect(Collectors.toList());

      // The test set is the instant of all actions and states, so the instants returned by the timeline get operation cannot be empty.
      // At the same time, it helps to detect errors such as incomplete test sets
      assertFalse(expectedInstants.isEmpty());
      expectedInstants.forEach(i -> assertTrue(timeline.containsInstant(i)));
      unexpectedInstants.forEach(i -> assertFalse(timeline.containsInstant(i)));
    };

    // Test that various types of getXXX operations from HoodieActiveTimeline
    // return the correct set of Instant
    checkTimeline.accept(timeline.getCommitsTimeline(), CollectionUtils.createSet(
        HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieTimeline.CLUSTERING_ACTION));
    checkTimeline.accept(timeline.getWriteTimeline(), CollectionUtils.createSet(
        HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.LOG_COMPACTION_ACTION,
        HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieTimeline.CLUSTERING_ACTION));
    checkTimeline.accept(timeline.getCommitAndReplaceTimeline(), CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieTimeline.CLUSTERING_ACTION));
    checkTimeline.accept(timeline.getDeltaCommitTimeline(), Collections.singleton(HoodieTimeline.DELTA_COMMIT_ACTION));
    checkTimeline.accept(timeline.getCleanerTimeline(), Collections.singleton(HoodieTimeline.CLEAN_ACTION));
    checkTimeline.accept(timeline.getRollbackTimeline(), Collections.singleton(HoodieTimeline.ROLLBACK_ACTION));
    checkTimeline.accept(timeline.getRollbackAndRestoreTimeline(), CollectionUtils.createSet(HoodieTimeline.RESTORE_ACTION, HoodieTimeline.ROLLBACK_ACTION));
    checkTimeline.accept(timeline.getRestoreTimeline(), Collections.singleton(HoodieTimeline.RESTORE_ACTION));
    checkTimeline.accept(timeline.getSavePointTimeline(), Collections.singleton(HoodieTimeline.SAVEPOINT_ACTION));
    checkTimeline.accept(timeline.getAllCommitsTimeline(), CollectionUtils.createSet(
        HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.CLEAN_ACTION, HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.LOG_COMPACTION_ACTION,
        HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieTimeline.CLUSTERING_ACTION, HoodieTimeline.SAVEPOINT_ACTION, HoodieTimeline.ROLLBACK_ACTION, HoodieTimeline.INDEXING_ACTION));

    // Get some random Instants
    Random rand = new Random();
    Set<String> randomActions = allInstantsSup.get().filter(i -> rand.nextBoolean())
        .map(HoodieInstant::getAction).collect(Collectors.toSet());
    checkTimeline.accept(timeline.getTimelineOfActions(randomActions), randomActions);
  }

  @Test
  public void testTimelineInstantOperations() {
    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient, true);
    assertEquals(0, timeline.countInstants(), "No instant present");
    // revertToInflight
    HoodieInstant commit = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "1");
    timeline.createCompleteInstant(commit);
    timeline = timeline.reload();
    assertEquals(1, timeline.countInstants());
    commit = timeline.getInstantsAsStream().findFirst().get();
    assertTrue(timeline.containsInstant(commit));
    HoodieInstant inflight = timeline.revertToInflight(commit);
    // revert creates the .requested file
    timeline = timeline.reload();
    assertEquals(1, timeline.countInstants());
    assertTrue(timeline.containsInstant(inflight));
    assertFalse(timeline.containsInstant(commit));

    // deleteInflight
    timeline.deleteInflight(inflight);
    timeline = timeline.reload();
    assertEquals(1, timeline.countInstants());
    assertFalse(timeline.containsInstant(inflight));
    assertFalse(timeline.containsInstant(commit));

    // deletePending
    timeline.createCompleteInstant(commit);
    timeline.createNewInstant(inflight);
    timeline = timeline.reload();
    assertEquals(1, timeline.countInstants());
    timeline.deletePending(inflight);
    timeline = timeline.reload();
    assertEquals(1, timeline.countInstants());
    assertFalse(timeline.containsInstant(inflight));
    assertTrue(timeline.containsInstant(commit));

    // deleteCompactionRequested
    HoodieInstant compaction = INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "2");
    timeline.createNewInstant(compaction);
    timeline = timeline.reload();
    assertEquals(2, timeline.countInstants());
    timeline.deleteCompactionRequested(compaction);
    timeline = timeline.reload();
    assertEquals(1, timeline.countInstants());
    assertFalse(timeline.containsInstant(inflight));
    assertFalse(timeline.containsInstant(compaction));
    assertTrue(timeline.containsInstant(commit));

    // transitionCompactionXXXtoYYY and revertCompactionXXXtoYYY
    compaction = INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "3");
    timeline.createNewInstant(compaction);
    timeline = timeline.reload();
    assertTrue(timeline.containsInstant(compaction));
    inflight = timeline.transitionCompactionRequestedToInflight(compaction);
    timeline = timeline.reload();
    assertFalse(timeline.containsInstant(compaction));
    assertTrue(timeline.containsInstant(inflight));
    compaction = timeline.revertInstantFromInflightToRequested(inflight);
    timeline = timeline.reload();
    assertTrue(timeline.containsInstant(compaction));
    assertFalse(timeline.containsInstant(inflight));
    inflight = timeline.transitionCompactionRequestedToInflight(compaction);
    timeline.reload();
    assertFalse(timeline.filterPendingExcludingCompaction().containsInstant(compaction));
    assertTrue(timeline.filterPendingCompactionTimeline().containsInstant(compaction));
    assertTrue(timeline.filterPendingMajorOrMinorCompactionTimeline().containsInstant(compaction));
    compaction = timeline.transitionCompactionInflightToComplete(inflight, new HoodieCommitMetadata(), HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
    timeline = timeline.reload();
    assertTrue(timeline.containsInstant(compaction));
    assertFalse(timeline.containsInstant(inflight));
    assertFalse(timeline.filterPendingCompactionTimeline().containsInstant(compaction));
    assertFalse(timeline.filterPendingMajorOrMinorCompactionTimeline().containsInstant(compaction));

    // transitionCleanXXXtoYYY
    HoodieInstant clean = INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "4");
    timeline.saveToCleanRequested(clean, Option.empty());
    timeline = timeline.reload();
    assertTrue(timeline.containsInstant(clean));
    inflight = timeline.transitionCleanRequestedToInflight(clean);
    timeline = timeline.reload();
    assertFalse(timeline.containsInstant(clean));
    assertTrue(timeline.containsInstant(inflight));
    clean = timeline.transitionCleanInflightToComplete(inflight, Option.empty(), HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
    timeline = timeline.reload();
    assertTrue(timeline.containsInstant(clean));
    assertFalse(timeline.containsInstant(inflight));

    // Various states of Instants
    HoodieInstant srcInstant = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.RESTORE_ACTION, "5");
    HoodieInstant otherInstant = INSTANT_GENERATOR.getRequestedInstant(srcInstant);
    assertEquals(otherInstant, INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.RESTORE_ACTION, "5"));
    otherInstant = INSTANT_GENERATOR.getCleanRequestedInstant("5");
    assertEquals(otherInstant, INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "5"));
    otherInstant = INSTANT_GENERATOR.getCleanInflightInstant("5");
    assertEquals(otherInstant, INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "5"));
    otherInstant = INSTANT_GENERATOR.getCompactionRequestedInstant("5");
    assertEquals(otherInstant, INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "5"));
    otherInstant = INSTANT_GENERATOR.getCompactionInflightInstant("5");
    assertEquals(otherInstant, INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "5"));

    // containsOrBeforeTimelineStarts
    List<HoodieInstant> allInstants = getAllInstants();
    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient, true);
    timeline.setInstants(allInstants);

    timeline.setInstants(allInstants);
    timeline.createNewInstant(INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "2"));
    allInstants.stream().map(HoodieInstant::requestedTime).forEach(s -> assertTrue(timeline.containsOrBeforeTimelineStarts(s)));
    assertTrue(timeline.containsOrBeforeTimelineStarts("0"));
    assertFalse(timeline.containsOrBeforeTimelineStarts(String.valueOf(System.currentTimeMillis() + 1000)));
    assertFalse(timeline.getTimelineHash().isEmpty());
  }

  @Test
  public void testCreateInstants() {
    List<HoodieInstant> allInstants = getAllInstants();
    for (HoodieInstant instant : allInstants) {
      if (instant.isCompleted()) {
        timeline.createCompleteInstant(instant);
      } else {
        timeline.createNewInstant(instant);
      }
    }

    timeline = timeline.reload();
    for (HoodieInstant instant : allInstants) {
      assertTrue(timeline.containsInstant(instant));
    }
  }

  @Test
  public void testInstantFilenameOperations() {
    HoodieInstant instantRequested = INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.RESTORE_ACTION, "5");
    HoodieInstant instantInflight = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.RESTORE_ACTION, "5");
    HoodieInstant instantComplete = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.RESTORE_ACTION, "5", "6");
    assertEquals(INSTANT_FILE_NAME_GENERATOR.getCommitFromCommitFile(INSTANT_FILE_NAME_GENERATOR.getFileName(instantRequested)), "5");
    assertEquals(INSTANT_FILE_NAME_GENERATOR.getCommitFromCommitFile(INSTANT_FILE_NAME_GENERATOR.getFileName(instantInflight)), "5");
    assertEquals(INSTANT_FILE_NAME_GENERATOR.getCommitFromCommitFile(INSTANT_FILE_NAME_GENERATOR.getFileName(instantComplete)), "5_6");

    assertEquals(INSTANT_FILE_NAME_GENERATOR.makeInflightRestoreFileName(
            INSTANT_FILE_NAME_PARSER.extractTimestamp(INSTANT_FILE_NAME_GENERATOR.getFileName(instantComplete))),
        INSTANT_FILE_NAME_GENERATOR.getFileName(instantInflight));
  }

  @Test
  public void testFiltering() {
    List<HoodieInstant> allInstants = getAllInstants();
    Supplier<Stream<HoodieInstant>> sup = allInstants::stream;

    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    timeline.setInstants(allInstants);

    // getReverseOrderedInstants
    Stream<HoodieInstant> instants = timeline.getReverseOrderedInstants();
    List<HoodieInstant> v1 = instants.collect(Collectors.toList());
    List<HoodieInstant> v2 = sup.get().collect(Collectors.toList());
    Collections.reverse(v2);
    assertEquals(v1, v2);

    /*
     * Helper function to check HoodieTimeline only contains some type of Instant states.
     * @param timeline The HoodieTimeline to check
     * @param states The states that should be present in the timeline being checked
     */
    BiConsumer<HoodieTimeline, Set<State>> checkFilter = (HoodieTimeline timeline, Set<State> states) -> {
      sup.get().filter(i -> states.contains(i.getState())).forEach(i -> assertTrue(timeline.containsInstant(i)));
      sup.get().filter(i -> !states.contains(i.getState())).forEach(i -> assertFalse(timeline.containsInstant(i)));
    };

    checkFilter.accept(timeline.filter(i -> false), new HashSet<>());
    checkFilter.accept(timeline.filterInflights(), Collections.singleton(State.INFLIGHT));
    checkFilter.accept(timeline.filterInflightsAndRequested(),
        CollectionUtils.createSet(State.INFLIGHT, State.REQUESTED));

    // filterCompletedAndCompactionInstants
    // This cannot be done using checkFilter as it involves both states and actions
    final HoodieTimeline t1 = timeline.filterCompletedAndCompactionInstants();
    assertTrue(t1.countInstants() > 0);
    final Set<State> states = CollectionUtils.createSet(State.COMPLETED);
    final Set<String> actions = Collections.singleton(HoodieTimeline.COMPACTION_ACTION);
    sup.get().filter(i -> states.contains(i.getState()) || actions.contains(i.getAction()))
        .forEach(i -> assertTrue(t1.containsInstant(i)));
    sup.get().filter(i -> !(states.contains(i.getState()) || actions.contains(i.getAction())))
        .forEach(i -> assertFalse(t1.containsInstant(i)));

    // filterPendingCompactionTimeline
    final HoodieTimeline t2 = timeline.filterPendingCompactionTimeline();
    assertTrue(t2.countInstants() > 0);
    sup.get().filter(i -> i.getAction().equals(HoodieTimeline.COMPACTION_ACTION))
        .forEach(i -> assertTrue(t2.containsInstant(i)));
    sup.get().filter(i -> !i.getAction().equals(HoodieTimeline.COMPACTION_ACTION))
        .forEach(i -> assertFalse(t2.containsInstant(i)));

    // filterPendingIndexTimeline
    final HoodieTimeline t3 = timeline.filterPendingIndexTimeline();
    assertEquals(2, t3.countInstants());
    sup.get().filter(i -> i.getAction().equals(HoodieTimeline.INDEXING_ACTION) && !i.isCompleted())
        .forEach(i -> assertTrue(t3.containsInstant(i)));
    sup.get().filter(i -> !i.getAction().equals(HoodieTimeline.INDEXING_ACTION) || i.getState() == State.COMPLETED)
        .forEach(i -> assertFalse(t3.containsInstant(i)));

    // filterCompletedIndexTimeline
    final HoodieTimeline t4 = timeline.filterCompletedIndexTimeline();
    assertEquals(1, t4.countInstants());
    sup.get().filter(i -> i.getAction().equals(HoodieTimeline.INDEXING_ACTION) && i.isCompleted())
        .forEach(i -> assertTrue(t4.containsInstant(i)));
    sup.get().filter(i -> !i.getAction().equals(HoodieTimeline.INDEXING_ACTION) || !i.isCompleted())
        .forEach(i -> assertFalse(t4.containsInstant(i)));

    // filterCompletedInstantsOrRewriteTimeline
    final HoodieTimeline t5 = timeline.filterCompletedInstantsOrRewriteTimeline();
    assertTrue(t5.countInstants() > 0);
    sup.get().filter(i -> CollectionUtils.createSet(HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.LOG_COMPACTION_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION).contains(i.getAction())
        || i.isCompleted()).forEach(i -> assertTrue(t5.containsInstant(i)));
    sup.get().filter(i -> !(CollectionUtils.createSet(HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.LOG_COMPACTION_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION).contains(i.getAction())
        || i.isCompleted())).forEach(i -> assertFalse(t5.containsInstant(i)));

    // filterPendingMajorOrMinorCompactionTimeline
    final HoodieTimeline t6 = timeline.filterPendingMajorOrMinorCompactionTimeline();
    assertTrue(t6.countInstants() > 0);
    sup.get().filter(i -> CollectionUtils.createSet(HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.LOG_COMPACTION_ACTION).contains(i.getAction())
        && !i.isCompleted()).forEach(i -> assertTrue(t6.containsInstant(i)));
    sup.get().filter(i -> !(CollectionUtils.createSet(HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.LOG_COMPACTION_ACTION).contains(i.getAction())
        && !i.isCompleted())).forEach(i -> assertFalse(t6.containsInstant(i)));

    // filterPendingExcludingCompaction
    final HoodieTimeline t7 = timeline.filterPendingExcludingCompaction();
    assertTrue(t7.countInstants() > 0);
    sup.get().filter(i -> !HoodieTimeline.COMPACTION_ACTION.equals(i.getAction()) && !i.isCompleted())
        .forEach(i -> assertTrue(t7.containsInstant(i)));
    sup.get().filter(i -> !(!HoodieTimeline.COMPACTION_ACTION.equals(i.getAction()) && !i.isCompleted()))
        .forEach(i -> assertFalse(t7.containsInstant(i)));
  }

  @Test
  public void testRollbackActionsTimeline() {
    int instantTime = 1;
    List<HoodieInstant> allInstants = new ArrayList<>();
    allInstants.add(metaClient.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, String.format("%03d", instantTime++)));
    HoodieInstant rollbackInstant = metaClient.createNewInstant(State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, String.format("%03d", instantTime++));
    allInstants.add(rollbackInstant);

    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    timeline.setInstants(allInstants);
    assertEquals(1, timeline.filterRequestedRollbackTimeline().countInstants());
    assertEquals(1, timeline.filterPendingRollbackTimeline().countInstants());

    rollbackInstant = metaClient.createNewInstant(State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, rollbackInstant.requestedTime());
    allInstants.set(1, rollbackInstant);
    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    timeline.setInstants(allInstants);
    assertEquals(0, timeline.filterRequestedRollbackTimeline().countInstants());
    assertEquals(1, timeline.filterPendingRollbackTimeline().countInstants());

    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    rollbackInstant = metaClient.createNewInstant(State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, rollbackInstant.requestedTime());
    allInstants.set(1, rollbackInstant);
    timeline.setInstants(allInstants);
    assertEquals(0, timeline.filterPendingRollbackTimeline().countInstants());
    assertEquals(0, timeline.filterRequestedRollbackTimeline().countInstants());
  }

  @Test
  void testParsingCommitDetails() throws IOException {
    HoodieInstant commitInstant = metaClient.createNewInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieInstant cleanInstant = metaClient.createNewInstant(State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "2");

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    HoodieWriteStat hoodieWriteStat = new HoodieWriteStat();
    hoodieWriteStat.setFileId("file_id1");
    hoodieWriteStat.setPath("path1");
    hoodieWriteStat.setPrevCommit("1");
    commitMetadata.addWriteStat("partition1", hoodieWriteStat);
    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    timeline.createNewInstant(commitInstant);
    timeline.transitionRequestedToInflight(commitInstant, Option.empty());
    HoodieInstant completeCommitInstant = metaClient.createNewInstant(State.INFLIGHT, commitInstant.getAction(), commitInstant.requestedTime());
    timeline.saveAsComplete(completeCommitInstant, Option.of(commitMetadata), HoodieInstantTimeGenerator.getCurrentInstantTimeStr());
    HoodieActiveTimeline timelineAfterFirstInstant = timeline.reload();

    HoodieInstant completedCommitInstant = timelineAfterFirstInstant.lastInstant().get();
    assertEquals(commitMetadata,
        timelineAfterFirstInstant.readCommitMetadata(completedCommitInstant));

    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan();
    cleanerPlan.setLastCompletedCommitTimestamp("1");
    cleanerPlan.setPolicy("policy");
    cleanerPlan.setVersion(TimelineLayoutVersion.CURR_VERSION);
    cleanerPlan.setPartitionsToBeDeleted(Collections.singletonList("partition1"));
    timeline.saveToCleanRequested(cleanInstant, Option.of(cleanerPlan));

    assertEquals(cleanerPlan, getCleanerPlan(metaClient, cleanInstant));

    HoodieTimeline mergedTimeline = timelineAfterFirstInstant.mergeTimeline(timeline.reload());
    assertEquals(commitMetadata, mergedTimeline.readCommitMetadata(completedCommitInstant));
    assertEquals(cleanerPlan, getCleanerPlan(metaClient, cleanInstant));
    assertEquals(commitMetadata, mergedTimeline.readCommitMetadata(completedCommitInstant));
  }

  @Test
  void missingInstantCausesError() {
    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    HoodieInstant commitInstant = metaClient.createNewInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "1");
    assertThrows(HoodieIOException.class, () -> timeline.getInstantContentStream(commitInstant));
  }

  @Test
  void getInstantReaderReferencesSelf() {
    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    assertSame(timeline, timeline.getInstantReader());
  }

  @Test
  public void testReplaceActionsTimeline() {
    int instantTime = 1;
    List<HoodieInstant> allInstants = new ArrayList<>();
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, String.format("%03d", instantTime++));
    allInstants.add(instant1);
    HoodieInstant instant2 = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, String.format("%03d", instantTime++));
    allInstants.add(instant2);
    HoodieInstant instant3 = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, String.format("%03d", instantTime++));
    allInstants.add(instant3);

    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    timeline.setInstants(allInstants);
    List<HoodieInstant> validReplaceInstants =
        timeline.getCompletedReplaceTimeline().getInstants();

    assertEquals(1, validReplaceInstants.size());
    assertEquals(instant3.requestedTime(), validReplaceInstants.get(0).requestedTime());
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, validReplaceInstants.get(0).getAction());

    assertStreamEquals(
        Stream.of(instant1, instant2, instant3),
        timeline.getCommitAndReplaceTimeline().getInstantsAsStream(), "Check the instants stream");

    assertStreamEquals(
        Stream.of(instant1, instant2),
        timeline.getCommitTimeline().getInstantsAsStream(), "Check the instants stream");
  }

  @Test
  public void testMinTimestamp() {
    String timestamp1 = "20240601040632402";
    String timestamp2 = "20250601040632402";
    assertEquals(timestamp1, InstantComparison.minTimestamp(null, timestamp1));
    assertEquals(timestamp1, InstantComparison.minTimestamp("", timestamp1));
    assertEquals(timestamp1, InstantComparison.minTimestamp(timestamp1, null));
    assertEquals(timestamp1, InstantComparison.minTimestamp(timestamp1, ""));
    assertEquals(timestamp1, InstantComparison.minTimestamp(timestamp1, timestamp2));
  }

  @Test
  public void testParseDateFromInstantTime() throws ParseException {
    String secondGranularityInstant = "20210101120101123";
    Date defaultSecsGranularityDate = TimelineUtils.parseDateFromInstantTime(secondGranularityInstant);

    // Parse expected date in the same way as the method under test
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    long expectedTime = format.parse(secondGranularityInstant).getTime();
    assertEquals(expectedTime, defaultSecsGranularityDate.getTime());
  }

  @Test
  public void testMetadataCompactionInstantDateParsing() throws ParseException {
    // default second granularity instant ID
    String secondGranularityInstant = "20210101120101123";
    Date defaultSecsGranularityDate = TimelineUtils.parseDateFromInstantTime(secondGranularityInstant);
    // metadata table compaction/cleaning : ms granularity instant ID
    String compactionInstant = secondGranularityInstant + "001";
    Date defaultMsGranularityDate = TimelineUtils.parseDateFromInstantTime(compactionInstant);
    assertEquals(0, defaultMsGranularityDate.getTime() - defaultSecsGranularityDate.getTime(), "Expected the ms part to be 0");
    assertTrue(InstantComparison.compareTimestamps(secondGranularityInstant, LESSER_THAN, compactionInstant));
    assertTrue(InstantComparison.compareTimestamps(compactionInstant, GREATER_THAN, secondGranularityInstant));
  }

  @Test
  public void testMillisGranularityInstantDateParsing() throws ParseException {
    // Old second granularity instant ID
    String secondGranularityInstant = "20210101120101";
    Date defaultMsGranularityDate = TimelineUtils.parseDateFromInstantTime(secondGranularityInstant);
    // New ms granularity instant ID
    String specificMsGranularityInstant = secondGranularityInstant + "009";
    Date msGranularityDate = TimelineUtils.parseDateFromInstantTime(specificMsGranularityInstant);
    assertEquals(999, defaultMsGranularityDate.getTime() % 1000, "Expected the ms part to be 999");
    assertEquals(9, msGranularityDate.getTime() % 1000, "Expected the ms part to be 9");

    // Ensure that any date math which expects second granularity still works
    String laterDateInstant = "20210101120111"; // + 10 seconds from original instant
    assertEquals(
        10,
        TimelineUtils.parseDateFromInstantTime(laterDateInstant).getTime() / 1000
            - TimelineUtils.parseDateFromInstantTime(secondGranularityInstant).getTime() / 1000,
        "Expected the difference between later instant and previous instant to be 10 seconds"
    );
  }

  @Test
  public void testInvalidInstantDateParsing() throws ParseException {
    // Test all invalid timestamp in HoodieTimeline, shouldn't throw any error and should return a correct value
    assertEquals(Long.parseLong(HoodieTimeline.INIT_INSTANT_TS),
        TimelineUtils.parseDateFromInstantTimeSafely(HoodieTimeline.INIT_INSTANT_TS).get().getTime());
    assertEquals(Long.parseLong(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS),
        TimelineUtils.parseDateFromInstantTimeSafely(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS).get().getTime());
    assertEquals(Long.parseLong(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS),
        TimelineUtils.parseDateFromInstantTimeSafely(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS).get().getTime());

    // Test metadata table compaction instant date parsing with INIT_INSTANT_TS, should return Option.empty
    assertEquals(Option.empty(),
        TimelineUtils.parseDateFromInstantTimeSafely(HoodieTimeline.INIT_INSTANT_TS + "001"));

    // Test a valid instant timestamp, should equal the same result as ActiveTimelineUtils.parseDateFromInstantTime
    String testInstant = "20210101120101";
    assertEquals(TimelineUtils.parseDateFromInstantTime(testInstant).getTime(),
        TimelineUtils.parseDateFromInstantTimeSafely(testInstant).get().getTime());
  }

  @Test
  public void testInstantCompletionTimeBackwardCompatibility() {
    HoodieInstant requestedInstant = INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieInstant inflightInstant = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "2");
    HoodieInstant completeInstant = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "3");

    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    timeline.createNewInstant(requestedInstant);
    timeline.createNewInstant(inflightInstant);

    // Note:
    // 0.x meta file name pattern: ${instant_time}.action[.state]
    // 1.x meta file name pattern: ${instant_time}_${completion_time}.action[.state].
    String legacyCompletedFileName = INSTANT_FILE_NAME_GENERATOR.makeCommitFileName(completeInstant.requestedTime());
    metaClient.getStorage().createImmutableFileInPath(new StoragePath(metaClient.getTimelinePath().toString(), legacyCompletedFileName), Option.empty());

    timeline = timeline.reload();
    assertThat("Some instants might be missing", timeline.countInstants(), is(3));
    List<HoodieInstant> instants = timeline.getInstants();
    assertNull(instants.get(0).getCompletionTime(), "Requested instant does not have completion time");
    assertNull(instants.get(1).getCompletionTime(), "Inflight instant does not have completion time");
    assertNotNull(instants.get(2).getCompletionTime(), "Completed instant has modification time as completion time for 0.x release");
    assertEquals(instants.get(2).requestedTime() + HoodieTimeline.COMMIT_EXTENSION, INSTANT_FILE_NAME_GENERATOR.getFileName(instants.get(2)), "Instant file name should not have completion time");
  }

  /**
   * Returns an exhaustive list of all possible HoodieInstant.
   *
   * @return list of HoodieInstant
   */
  private List<HoodieInstant> getAllInstants() {
    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    List<HoodieInstant> allInstants = new ArrayList<>();
    long instantTime = 1;
    for (State state : State.values()) {
      if (state == State.NIL) {
        continue;
      }
      for (String action : HoodieTimeline.VALID_ACTIONS_IN_TIMELINE) {
        // Following are not valid combinations of actions and state so we should
        // not be generating them.
        if (state == State.REQUESTED) {
          if (action.equals(HoodieTimeline.SAVEPOINT_ACTION) || action.equals(HoodieTimeline.RESTORE_ACTION)) {
            continue;
          }
        }
        // Compaction complete is called commit complete
        if (state == State.COMPLETED && action.equals(HoodieTimeline.COMPACTION_ACTION)) {
          action = HoodieTimeline.COMMIT_ACTION;
        }
        // LogCompaction complete is called deltacommit complete
        if (state == State.COMPLETED && action.equals(HoodieTimeline.LOG_COMPACTION_ACTION)) {
          action = HoodieTimeline.DELTA_COMMIT_ACTION;
        }
        // Cluster complete is called replacecommit complete
        if (state == State.COMPLETED && action.equals(HoodieTimeline.CLUSTERING_ACTION)) {
          action = HoodieTimeline.REPLACE_COMMIT_ACTION;
        }

        allInstants.add(INSTANT_GENERATOR.createNewInstant(state, action, String.format("%03d", instantTime++)));
      }
    }
    return allInstants;
  }

  private void shouldAllowTempCommit(boolean allowTempCommit, Consumer<HoodieTableMetaClient> fun) {
    if (allowTempCommit) {
      HoodieStorage storage = metaClient.getStorage();
      FileSystem fs = (FileSystem) storage.getFileSystem();
      HoodieWrapperFileSystem newFs = new HoodieWrapperFileSystem(fs, new NoOpConsistencyGuard());
      metaClient.setStorage(new HoodieHadoopStorage(newFs));
      try {
        fun.accept(metaClient);
      } finally {
        metaClient.setStorage(storage);
      }
      return;
    }
    fun.accept(metaClient);
  }
}
