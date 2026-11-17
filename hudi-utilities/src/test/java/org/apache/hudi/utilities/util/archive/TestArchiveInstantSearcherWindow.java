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

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.hudi.utilities.util.archive.FakeArchive.Fixture;
import static org.apache.hudi.utilities.util.archive.FakeArchive.spec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The elastic consumption window: how many bytes of archive file may be in flight, and the guarantee
 * that a file too large to afford is waited for rather than stepped over.
 */
class TestArchiveInstantSearcherWindow {

  private static final long GRACE_MILLIS = 250L;
  private static final long JOIN_TIMEOUT_SECONDS = 30L;

  private final ExecutorService driver = Executors.newSingleThreadExecutor();
  private final BiConsumer<String, GenericRecord> ignore = (instantTime, record) -> {
  };

  @AfterEach
  void tearDown() {
    driver.shutdownNow();
  }

  /**
   * Four files of 1G, 1G, 3G, 1G under a 2G budget with two workers. The 3G file cannot share the
   * budget with anything, so it must wait for the window to drain and then run alone -- and, above
   * all, the 1G file behind it must not be pulled forward past it.
   */
  @Test
  void waitsForAnUnaffordableFileRatherThanSteppingOverIt() throws Exception {
    Fixture fixture = new Fixture(
        spec("f0", FakeArchive.GB).holding("100").gated(),
        spec("f1", FakeArchive.GB).holding("200").gated(),
        spec("f2", 3L * FakeArchive.GB).holding("300").gated(),
        spec("f3", FakeArchive.GB).holding("400").gated());

    ArchiveInstantSearcher searcher =
        new ArchiveInstantSearcher(fixture, fixture, 2, 2L * FakeArchive.GB);
    Future<Map<String, InstantLocation>> search = driver.submit(
        () -> searcher.search(Arrays.asList("100", "200", "300", "400"), ignore));

    assertTrue(fixture.awaitEntered("f0", 5000L));
    assertTrue(fixture.awaitEntered("f1", 5000L));
    assertFalse(fixture.awaitEntered("f2", GRACE_MILLIS),
        "the 3G file was admitted while 2G of budget was already in flight");

    fixture.release("f0");
    assertFalse(fixture.awaitEntered("f2", GRACE_MILLIS),
        "the 3G file was admitted with 1G still in flight, exceeding the budget");
    assertFalse(fixture.awaitEntered("f3", 0L),
        "the 3G file was stepped over and a later file started ahead of it");

    fixture.release("f1");
    assertTrue(fixture.awaitEntered("f2", 5000L),
        "the 3G file was never admitted even once the window had drained");
    assertFalse(fixture.awaitEntered("f3", GRACE_MILLIS),
        "another file joined the 3G file, which alone already exceeds the budget");

    fixture.release("f2");
    assertTrue(fixture.awaitEntered("f3", 5000L));
    fixture.release("f3");
    search.get(JOIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    List<String> events = fixture.events();
    assertTrue(indexOf(events, "enter:f2") > indexOf(events, "exit:f1"),
        "the 3G file started before the window drained: " + events);
    assertTrue(indexOf(events, "enter:f3") > indexOf(events, "exit:f2"),
        "a file joined the sole-occupant 3G file: " + events);
    assertExclusive(events, "f2");
    assertTrue(fixture.invariantViolations().isEmpty(), fixture.invariantViolations().toString());
  }

  /** A file no budget can afford still runs; the alternative is never reading it at all. */
  @Test
  void admitsAFileLargerThanTheWholeBudgetAsSoleOccupant() throws Exception {
    Fixture fixture = new Fixture(
        spec("huge", 5L * FakeArchive.GB).holding("100"),
        spec("small", FakeArchive.GB).holding("200"));

    ArchiveInstantSearcher searcher =
        new ArchiveInstantSearcher(fixture, fixture, 8, 2L * FakeArchive.GB);
    Map<String, InstantLocation> resolved =
        searcher.search(Arrays.asList("100", "200"), ignore);

    assertEquals(InstantLocation.found(fixture.pathOf("huge")), resolved.get("100"));
    assertEquals(InstantLocation.found(fixture.pathOf("small")), resolved.get("200"));
    assertExclusive(fixture.events(), "huge");
  }

  /**
   * Many files, many workers, a budget tight enough to force blocking. Every instant that exists
   * must be found, in the right file, and no file may ever be skipped.
   */
  @RepeatedTest(5)
  void neverSkipsAFileUnderContention() {
    int fileCount = 200;
    int instantsPerFile = 10;
    Random random = new Random(20260817L);

    FakeArchive.FileSpec[] specs = new FakeArchive.FileSpec[fileCount];
    for (int i = 0; i < fileCount; i++) {
      // Index 0 is the newest file, so it carries the highest instants.
      int base = (fileCount - 1 - i) * instantsPerFile;
      FakeArchive.FileSpec spec =
          spec("f" + i, 1_000_000L + random.nextInt(19_000_000));
      for (int k = 0; k < instantsPerFile; k++) {
        spec.holding(instant(base + k));
      }
      specs[i] = spec;
    }
    Fixture fixture = new Fixture(specs);

    List<String> targets = new ArrayList<>();
    for (int i = 0; i < fileCount * instantsPerFile; i += 47) {
      targets.add(instant(i));
    }
    // One instant that does not exist, so the walk runs to the oldest file rather than stopping.
    targets.add(instant(fileCount * instantsPerFile + 1));

    ArchiveInstantSearcher searcher =
        new ArchiveInstantSearcher(fixture, fixture, 16, 30_000_000L);
    Map<String, InstantLocation> resolved = searcher.search(targets, ignore);

    assertTrue(fixture.invariantViolations().isEmpty(), fixture.invariantViolations().toString());
    assertEquals(targets.size(), resolved.size());
    for (int i = 0; i < targets.size() - 1; i++) {
      String target = targets.get(i);
      int fileIndex = fileCount - 1 - (Integer.parseInt(target) / instantsPerFile);
      assertEquals(InstantLocation.found(fixture.pathOf("f" + fileIndex)), resolved.get(target),
          "wrong location for " + target);
    }
    assertEquals(InstantSearchState.NOT_FOUND,
        resolved.get(targets.get(targets.size() - 1)).state());
  }

  /**
   * The prefix check is what the two cases above rely on to prove nothing was skipped, and a check
   * that never fires would pass them silently. So it is pinned directly: a window that really skipped
   * a file is reported, and a legal window is not -- including one whose files finished out of order,
   * so that a CONSUMED file sits between two CONSUMING ones.
   */
  @Test
  void thePrefixCheckReportsARealSkipAndNothingElse() {
    Fixture legal = new Fixture(spec("f0", 1L), spec("f1", 1L), spec("f2", 1L), spec("f3", 1L));
    List<ArchiveFile> legalFiles = legal.archiveFiles();
    legalFiles.get(0).setState(ConsumeState.CONSUMING);
    legalFiles.get(1).setState(ConsumeState.CONSUMED);
    legalFiles.get(2).setState(ConsumeState.CONSUMING);
    legal.checkPrefixInvariant("legal");
    assertTrue(legal.invariantViolations().isEmpty(), legal.invariantViolations().toString());

    Fixture skipped = new Fixture(spec("f0", 1L), spec("f1", 1L), spec("f2", 1L), spec("f3", 1L));
    skipped.archiveFiles().get(0).setState(ConsumeState.CONSUMING);
    skipped.archiveFiles().get(2).setState(ConsumeState.CONSUMED);
    skipped.checkPrefixInvariant("skipped");
    assertEquals(
        Collections.singletonList(
            "skipped: f1 is NOT_CONSUMED while an older file is already in or past the window"),
        skipped.invariantViolations());
  }

  private static String instant(int value) {
    return String.format("%05d", value);
  }

  private static int indexOf(List<String> events, String event) {
    int index = events.indexOf(event);
    assertTrue(index >= 0, event + " never happened: " + events);
    return index;
  }

  /** Asserts nothing else entered or left the window while this file was being read. */
  private static void assertExclusive(List<String> events, String name) {
    int from = indexOf(events, "enter:" + name);
    int to = indexOf(events, "exit:" + name);
    for (int i = from + 1; i < to; i++) {
      String event = events.get(i);
      assertTrue(event.endsWith(":" + name),
          name + " shared the window with " + event + ": " + events);
    }
  }
}
