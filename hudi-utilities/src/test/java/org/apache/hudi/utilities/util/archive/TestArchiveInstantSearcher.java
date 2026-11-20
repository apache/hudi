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

package org.apache.hudi.utilities.util.archive;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.apache.hudi.utilities.util.archive.FakeArchive.Fixture;
import static org.apache.hudi.utilities.util.archive.FakeArchive.spec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How a search resolves each target: what it found, what it can call absent, and what it refuses to
 * call absent.
 */
class TestArchiveInstantSearcher {

  private final List<String> delivered = Collections.synchronizedList(new ArrayList<>());

  private final BiConsumer<String, GenericRecord> collector = (instantTime, record) ->
      delivered.add(instantTime + "="
          + record.get(org.apache.hudi.common.table.HoodieTableMetaClient.COMMIT_TIME_KEY));

  private static ArchiveInstantSearcher searcher(Fixture fixture, int threads) {
    return new ArchiveInstantSearcher(fixture, fixture, threads, 8L * FakeArchive.GB);
  }

  @Test
  void reportsWhereEachInstantLivesAndWhatIsGenuinelyAbsent() {
    Fixture fixture = new Fixture(
        spec("f2", FakeArchive.GB).holding("300", "310"),
        spec("f1", FakeArchive.GB).holding("200", "210"),
        spec("f0", FakeArchive.GB).holding("100", "110"));

    Map<String, InstantLocation> resolved =
        searcher(fixture, 1).search(Arrays.asList("310", "200", "999"), collector);

    Map<String, InstantLocation> expected = new HashMap<>();
    expected.put("310", InstantLocation.found(fixture.pathOf("f2")));
    expected.put("200", InstantLocation.found(fixture.pathOf("f1")));
    expected.put("999", InstantLocation.notFound());
    assertEquals(expected, resolved);
    assertEquals(Arrays.asList("310=310", "200=200"), new ArrayList<>(delivered));
  }

  @Test
  void skipsAFileWhoseRangeHoldsNoLiveTarget() {
    Fixture fixture = new Fixture(
        spec("newest", FakeArchive.GB).holding("300", "310"),
        spec("middle", FakeArchive.GB).holding("200", "210"),
        spec("oldest", FakeArchive.GB).holding("100", "110"));

    searcher(fixture, 1).search(Arrays.asList("310", "110"), collector);

    assertEquals(1, fixture.scanCount("newest"));
    assertEquals(1, fixture.scanCount("oldest"));
    assertEquals(0, fixture.scanCount("middle"), "a probed range holding nothing wanted was read");
    assertEquals(1, fixture.probeCount("middle"), "the file should still have been probed");
  }

  @Test
  void skipsAFileFallingInAGapBetweenLiveTargets() {
    // The outer [100, 900] bound covers this file; only a per-target check rules it out.
    Fixture fixture = new Fixture(
        spec("high", FakeArchive.GB).holding("900"),
        spec("gap", FakeArchive.GB).holding("400", "500"),
        spec("low", FakeArchive.GB).holding("100"));

    searcher(fixture, 1).search(Arrays.asList("100", "900"), collector);

    assertEquals(0, fixture.scanCount("gap"));
  }

  @Test
  void scansAFileWhoseProbeFailed() {
    Fixture fixture = new Fixture(
        spec("unprobeable", FakeArchive.GB).holding("200").withFailingProbe());

    Map<String, InstantLocation> resolved =
        searcher(fixture, 1).search(Collections.singletonList("200"), collector);

    assertEquals(1, fixture.scanCount("unprobeable"), "a failed probe must not be read as 'covers nothing'");
    assertEquals(InstantLocation.found(fixture.pathOf("unprobeable")), resolved.get("200"));
  }

  @Test
  void corruptFileMakesItsRangeIndeterminateRatherThanAbsent() {
    // The file covers [10, 30] but a corrupt block swallowed 20. Absence here would be a lie.
    Fixture fixture = new Fixture(
        spec("damaged", FakeArchive.GB).holding("10", "30").damaged("a corrupt block"));

    Map<String, InstantLocation> resolved =
        searcher(fixture, 1).search(Arrays.asList("20", "40"), collector);

    assertEquals(InstantSearchState.INDETERMINATE, resolved.get("20").state());
    assertEquals(InstantSearchState.NOT_FOUND, resolved.get("40").state(),
        "a target outside the damaged range is still cleanly absent");
  }

  @Test
  void anUnreadableFileOfUnknownRangePoisonsEveryRemainingTarget() {
    Fixture fixture = new Fixture(
        spec("unreadable", FakeArchive.GB).holding("10").withFailingProbe().damaged("unreadable"));

    Map<String, InstantLocation> resolved =
        searcher(fixture, 1).search(Arrays.asList("10", "20", "999"), collector);

    assertEquals(InstantLocation.found(fixture.pathOf("unreadable")), resolved.get("10"),
        "an instant actually delivered stays found");
    assertEquals(InstantSearchState.INDETERMINATE, resolved.get("20").state());
    assertEquals(InstantSearchState.INDETERMINATE, resolved.get("999").state());
  }

  @Test
  void cleanScanThatFoundNothingIsAbsenceNotDamage() {
    // The merge-archive carve-out relies on this: the real scanner reports a clean outcome for a
    // half-written merged file, because its content still lives in the sources it was built from.
    Fixture fixture = new Fixture(spec("merged", FakeArchive.GB).holding("10", "30"));

    Map<String, InstantLocation> resolved =
        searcher(fixture, 1).search(Collections.singletonList("20"), collector);

    assertEquals(1, fixture.scanCount("merged"), "the file's range covers the target, so it is read");
    assertEquals(InstantSearchState.NOT_FOUND, resolved.get("20").state());
  }

  @Test
  void stopsWalkingOnceEveryTargetIsFound() {
    Fixture fixture = new Fixture(
        spec("newest", FakeArchive.GB).holding("300"),
        spec("middle", FakeArchive.GB).holding("200"),
        spec("oldest", FakeArchive.GB).holding("100"));

    searcher(fixture, 1).search(Collections.singletonList("300"), collector);

    assertEquals(0, fixture.probeCount("middle"), "files past the last target should not be touched");
    assertEquals(0, fixture.probeCount("oldest"));
  }

  @Test
  void deliversEveryRecordForAnInstantWithinTheFileThatHoldsIt() {
    // The v6 archive writes a requested, an inflight and a completed entry per instant, and only one
    // of them carries write stats. Stopping at the first would make the result depend on read order.
    Fixture fixture = new Fixture(
        spec("f0", FakeArchive.GB).holdingRepeated("200", 3));

    searcher(fixture, 1).search(Collections.singletonList("200"), collector);

    assertEquals(Arrays.asList("200=200", "200=200", "200=200"), new ArrayList<>(delivered));
  }

  @Test
  void theNewestFileHoldingAnInstantIsTheOneRecorded() {
    Fixture fixture = new Fixture(
        spec("newer", FakeArchive.GB).holding("200"),
        spec("older", FakeArchive.GB).holding("200"));

    Map<String, InstantLocation> resolved =
        searcher(fixture, 1).search(Collections.singletonList("200"), collector);

    assertEquals(InstantLocation.found(fixture.pathOf("newer")), resolved.get("200"));
    assertEquals(0, fixture.scanCount("older"), "the older duplicate is no longer worth reading");
  }

  @Test
  void collapsesDuplicateTargetsIntoOneEntry() {
    Fixture fixture = new Fixture(spec("f0", FakeArchive.GB).holding("200"));

    Map<String, InstantLocation> resolved =
        searcher(fixture, 1).search(Arrays.asList("200", "200", "200"), collector);

    assertEquals(1, resolved.size());
    assertEquals(InstantLocation.found(fixture.pathOf("f0")), resolved.get("200"));
  }

  @Test
  void noTargetsYieldsNoWork() {
    Fixture fixture = new Fixture(spec("f0", FakeArchive.GB).holding("200"));

    assertTrue(searcher(fixture, 1).search(Collections.emptyList(), collector).isEmpty());
    assertEquals(0, fixture.probeCount("f0"));
  }

  @Test
  void anEmptyArchiveMakesEveryTargetCleanlyAbsent() {
    Fixture fixture = new Fixture();

    Map<String, InstantLocation> resolved =
        searcher(fixture, 1).search(Arrays.asList("100", "200"), collector);

    assertEquals(InstantSearchState.NOT_FOUND, resolved.get("100").state());
    assertEquals(InstantSearchState.NOT_FOUND, resolved.get("200").state());
  }

  @Test
  void failedListingIsFatalRatherThanAbsence() {
    Fixture fixture = new Fixture(spec("f0", FakeArchive.GB).holding("200"))
        .failingList(new IOException("archive directory is gone"));

    assertThrows(HoodieIOException.class,
        () -> searcher(fixture, 1).search(Collections.singletonList("200"), collector));
  }

  @Test
  void failingConsumerSurfacesAsCallerBugNotDamage() {
    Fixture fixture = new Fixture(spec("f0", FakeArchive.GB).holding("200"));
    IllegalStateException boom = new IllegalStateException("consumer blew up");

    HoodieException thrown = assertThrows(HoodieException.class,
        () -> searcher(fixture, 1).search(Collections.singletonList("200"),
            (instantTime, record) -> {
              throw boom;
            }));
    assertSame(boom, thrown.getCause());
  }

  @Test
  void rejectsAnInvalidConfiguration() {
    Fixture fixture = new Fixture();
    assertThrows(IllegalArgumentException.class,
        () -> new ArchiveInstantSearcher(fixture, fixture, 0, FakeArchive.GB));
    assertThrows(IllegalArgumentException.class,
        () -> new ArchiveInstantSearcher(fixture, fixture, 1, 0L));
  }

}
