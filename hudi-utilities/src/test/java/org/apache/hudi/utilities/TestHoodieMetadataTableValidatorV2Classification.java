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

package org.apache.hudi.utilities;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantReader;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.BaseTimelineV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.utilities.util.DiffPattern;
import org.apache.hudi.utilities.util.DiffTally;
import org.apache.hudi.utilities.util.DuplicateBaseFileDimension;
import org.apache.hudi.utilities.util.FileDiffRecord;
import org.apache.hudi.utilities.util.FsMdtDiffDimension;
import org.apache.hudi.utilities.util.Inconclusive;
import org.apache.hudi.utilities.util.InconclusiveCause;
import org.apache.hudi.utilities.util.MismatchRecord;
import org.apache.hudi.utilities.util.PendingServiceState;
import org.apache.hudi.utilities.util.ServiceActivityDelta;
import org.apache.hudi.utilities.util.ValidationMode;
import org.apache.hudi.utilities.util.Verdict;

import org.apache.spark.SparkException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * How {@link HoodieMetadataTableValidatorV2} classifies a failure decides which alert fires.
 *
 * <p>The validation job maps a {@link HoodieValidationException} to a critical data-correctness
 * page and everything else to an infrastructure alert. Misclassifying a torn-down node as a
 * validation failure sends someone to rebuild a metadata table that was never broken; misclassifying
 * real divergence as infrastructure hides corruption. Both directions are asserted here.
 *
 * <p>A real teardown cannot be reproduced in a test, so these drive the classifier with the
 * exception chains Spark actually produces.
 */
public class TestHoodieMetadataTableValidatorV2Classification {

  private static final String PARTITION = "2026/08/16";

  @ParameterizedTest
  @ValueSource(strings = {
      "Job 3 cancelled because SparkContext was shut down",
      "Cannot call methods on a stopped SparkContext",
      "Job aborted due to stage failure: Task 0 in stage 4.0 failed 4 times, most recent failure: "
          + "Lost task 0.3 in stage 4.0: ExecutorLostFailure (executor 7 exited caused by one of "
          + "the running tasks) Reason: Executor decommission",
      "Container killed by YARN for exceeding memory limits",
      "Container from a bad node: container_1234 on host ip-10-0-0-1"})
  void infrastructureFailuresAreNotValidationFailures(String sparkMessage) {
    Exception fromSpark = new SparkException(sparkMessage);

    assertTrue(HoodieMetadataTableValidatorV2.matchInfraFailure(fromSpark).isPresent(),
        "should be recognised as infrastructure: " + sparkMessage);

    RuntimeException classified = HoodieMetadataTableValidatorV2.classifyFailure(fromSpark);
    assertFalse(classified instanceof HoodieValidationException,
        "an infrastructure failure classified as a validation failure raises a data-correctness "
            + "page for a node that was simply reclaimed: " + classified);
    assertTrue(classified.getMessage().contains("MDT_VALIDATOR_INFRA_FAILURE"),
        "the marker alerting keys on must be present: " + classified.getMessage());
  }

  /** A driver shutdown interrupts the fan-out, and an interrupt carries no message to match on. */
  @Test
  void interruptionIsInfrastructure() {
    Exception interrupted = new HoodieException("fan-out interrupted", new InterruptedException());

    assertTrue(HoodieMetadataTableValidatorV2.matchInfraFailure(interrupted).isPresent());
    assertFalse(HoodieMetadataTableValidatorV2.classifyFailure(interrupted)
        instanceof HoodieValidationException);
  }

  /**
   * Divergence that was actually observed outranks a teardown that happened afterwards: the finding
   * is real, and the next run may not reproduce it.
   */
  @Test
  void validationFailureWinsOverAConcurrentTeardown() {
    HoodieValidationException divergence = new HoodieValidationException(
        "Validation of latest file slices (v2) for partition 2016/03/15 failed");
    Exception fromSpark = new SparkException(
        "Job aborted due to stage failure: ExecutorLostFailure (executor 2 exited)", divergence);

    assertTrue(HoodieMetadataTableValidatorV2.matchInfraFailure(fromSpark).isPresent(),
        "fixture must carry an infrastructure signature for this test to mean anything");
    assertSame(divergence, HoodieMetadataTableValidatorV2.classifyFailure(fromSpark),
        "a confirmed divergence must not be masked by a teardown that followed it");
  }

  /**
   * A file that vanished on its own is the divergence this validator exists to catch. The
   * infrastructure classification must not swallow it.
   */
  @Test
  void missingFileOnItsOwnIsNotInfrastructure() {
    Exception missingFile = new HoodieIOException(
        "Unable to get seekable input stream for x.log", new FileNotFoundException("x.log"));

    assertFalse(HoodieMetadataTableValidatorV2.matchInfraFailure(missingFile).isPresent());
  }

  /** Anything unrecognised is passed through as itself, neither validation nor infrastructure. */
  @Test
  void unrecognisedFailuresArePassedThrough() {
    Exception other = new IllegalStateException("something else entirely");

    assertFalse(HoodieMetadataTableValidatorV2.matchInfraFailure(other).isPresent());
    assertSame(other, HoodieMetadataTableValidatorV2.classifyFailure(other));
  }

  /**
   * The default has to pass its own guard, or a run that never mentioned
   * {@code --validation-output-dir} would be rejected outright — which is the one way this guard
   * could break every run at once.
   *
   * <p>That the default sits inside {@code TOOLING_DIR_PREFIX} is <b>not</b> asserted here,
   * deliberately: {@code DEFAULT_VALIDATION_OUTPUT_DIR} is
   * {@code TOOLING_DIR_PREFIX + "/table_validation_job"}, a compile-time concatenation, so the
   * coupling holds by construction and an assertion restating it would only be able to pass. Nor
   * does the prefix buy any behaviour to cover: it names a namespace, and what keeps a report
   * directory from being read as table data is the partition-metadata rule and the base/log file
   * filter, neither of which looks at a name.
   */
  @Test
  void theDefaultOutputDirectoryPassesItsOwnGuard() {
    HoodieMetadataTableValidatorV2.validateOutputDir(
        HoodieMetadataTableValidator.Config.DEFAULT_VALIDATION_OUTPUT_DIR);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      // Visible: every tool that lists a table by convention would show it as data.
      "validation_output",
      "table_validation_job",
      // The timeline's, read by every Hudi reader there is.
      ".hoodie",
      ".hoodie/validation"})
  void outputDirectoryThatIsVisibleOrInsideTheTimelineIsRejected(String outputDir) {
    assertThrows(IllegalArgumentException.class,
        () -> HoodieMetadataTableValidatorV2.validateOutputDir(outputDir),
        "must be rejected: " + outputDir);
  }

  @Test
  void emptyOutputDirectoryIsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> HoodieMetadataTableValidatorV2.validateOutputDir(""));
    assertThrows(IllegalArgumentException.class,
        () -> HoodieMetadataTableValidatorV2.validateOutputDir(null));
  }

  /**
   * A run that asks for neither dimension is refused before anything is read.
   *
   * <p>It would list nothing, compare nothing and return {@code true}, which no caller can tell
   * from a run that examined the table and found it healthy — the worst answer this validator can
   * give, and the one thing worse than failing loudly. Rejected at the top of {@code run()}, so it
   * surfaces as a configuration error rather than as an empty report directory an operator finds
   * later.
   *
   * <p>The message must name <b>both</b> flags: an operator who deliberately turned one dimension
   * off has to be told which other one they also have off, and naming only the combination leaves
   * them to guess which two flags it means.
   */
  @Test
  void bothDimensionsSwitchedOffIsRejected() {
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:/tmp/validates-nothing";
    config.validateFsMdtDiff = false;
    config.validateDuplicateBaseFiles = false;
    HoodieMetadataTableValidatorV2 validator = new HoodieMetadataTableValidatorV2(
        null, config, new Properties(), Option.of(mock(HoodieTableMetaClient.class)));

    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, validator::run,
        "a run that validates nothing must never be allowed to report success");

    assertTrue(thrown.getMessage().contains("--validate-fs-mdt-diff"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("--validate-duplicate-base-files"),
        thrown.getMessage());
  }

  /** A hidden sibling of the timeline is fine; only living <em>under</em> it is not. */
  @Test
  void hiddenDirectoryBesideTheTimelineIsAccepted() {
    HoodieMetadataTableValidatorV2.validateOutputDir(".hoodie_validation");
    HoodieMetadataTableValidatorV2.validateOutputDir(".my_reports/run");
  }

  /**
   * The bypasses a {@code startsWith(".")} test cannot see.
   *
   * <p>{@code StoragePath} resolves through {@code URI.normalize()}, so a relative reference is
   * gone by the time the path is built and the guard has to catch it in the string it was given.
   * Each of these would otherwise land somewhere the guard is written to keep reports out of:
   * {@code ./.hoodie} is the timeline with two characters in front of it, {@code ..} and {@code .}
   * both name a directory that is not below the output root at all, and a colon can be read as a
   * URI scheme and take the whole path elsewhere. The same rules {@link
   * HoodieMetadataTableValidatorV2#validateRunId} applies, applied here per segment because this
   * one is allowed to be more than one segment deep.
   */
  @ParameterizedTest
  @ValueSource(strings = {
      // The timeline, reached by a relative reference the guard must not normalise away.
      "./.hoodie",
      ".x/../.hoodie",
      // Not below the output root at all.
      "..",
      ".",
      "./",
      ".reports/../..",
      // A hidden first segment does not make a nested .hoodie acceptable.
      ".reports/.hoodie",
      // Absolute: new StoragePath(root, "/elsewhere") discards the root entirely.
      "/.reports",
      // A colon can be read as a URI scheme.
      ".reports:evil/run"})
  void outputDirectoryThatEscapesItsRootIsRejected(String outputDir) {
    assertThrows(IllegalArgumentException.class,
        () -> HoodieMetadataTableValidatorV2.validateOutputDir(outputDir),
        "must be rejected: " + outputDir);
  }

  /**
   * The output <em>root</em> was never checked at all, so every guard on the directory could be
   * sidestepped by moving the offending segment one level up: a root of {@code &lt;table&gt;/.hoodie}
   * with the default output directory writes a run's reports into the timeline.
   *
   * <p>The root is a location of the caller's choosing — another bucket, another filesystem — so it
   * is not held to the directory's rules. The one thing it may not be is somebody's timeline.
   */
  @Test
  void outputRootInsideATimelineIsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> HoodieMetadataTableValidatorV2.validateOutputRoot("/tmp/table/.hoodie"));
    assertThrows(IllegalArgumentException.class,
        () -> HoodieMetadataTableValidatorV2.validateOutputRoot("s3://bucket/tbl/.hoodie/reports"));
    assertThrows(IllegalArgumentException.class,
        () -> HoodieMetadataTableValidatorV2.validateOutputRoot(".hoodie"));
  }

  /** Empty means "the table's base path", and a root elsewhere is exactly what the flag is for. */
  @Test
  void outputRootElsewhereIsAccepted() {
    HoodieMetadataTableValidatorV2.validateOutputRoot("");
    HoodieMetadataTableValidatorV2.validateOutputRoot(null);
    HoodieMetadataTableValidatorV2.validateOutputRoot("s3://bucket/validation-reports");
    HoodieMetadataTableValidatorV2.validateOutputRoot("file:///tmp/reports");
    HoodieMetadataTableValidatorV2.validateOutputRoot("/tmp/table");
    HoodieMetadataTableValidatorV2.validateOutputRoot("hdfs://ns/tables/.hoodie_validation");
  }

  /**
   * {@code new StoragePath(parent, child)} normalises {@code ..}, so an unchecked run id would put a
   * run's whole record somewhere else entirely under {@code --validation-output-dir} -- and the
   * caller that supplied it would then read back a different run's reports, or none.
   */
  @Test
  void runIdThatWouldEscapeTheOutputDirectoryIsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> HoodieMetadataTableValidatorV2.validateRunId("../../etc"));
    assertThrows(IllegalArgumentException.class,
        () -> HoodieMetadataTableValidatorV2.validateRunId("a/b"));
    assertThrows(IllegalArgumentException.class,
        () -> HoodieMetadataTableValidatorV2.validateRunId("."));
    HoodieMetadataTableValidatorV2.validateRunId("run-42");
    HoodieMetadataTableValidatorV2.validateRunId("");
  }

  @Test
  void failFastIsParsedFromEitherSpellingAndIsTheDefault() {
    assertEquals(ValidationMode.FAIL_FAST,
        HoodieMetadataTableValidatorV2.parseValidationMode("fail-fast"));
    assertEquals(ValidationMode.FAIL_FAST,
        HoodieMetadataTableValidatorV2.parseValidationMode("FAIL_FAST"));
    assertEquals(ValidationMode.FAIL_FAST,
        HoodieMetadataTableValidatorV2.parseValidationMode(""));
    assertEquals(ValidationMode.FAIL_FAST,
        HoodieMetadataTableValidatorV2.parseValidationMode(null));
  }

  /**
   * The inverse of a test this branch deliberately removes.
   *
   * <p>Its predecessor asserted that {@code exhaustive} was <em>refused</em> at startup, which was
   * true while only fail-fast existed. This branch is the follow-up that implements exhaustive, so
   * the refusal is gone and the mode must now parse. Left as an assertion rather than simply deleted:
   * a silent removal would let the refusal creep back in unnoticed, and the mode failing to parse is
   * the one way exhaustive can be unreachable without any other test noticing.
   */
  @Test
  void exhaustiveIsAcceptedNowThatItIsImplemented() {
    assertEquals(ValidationMode.EXHAUSTIVE,
        HoodieMetadataTableValidatorV2.parseValidationMode("exhaustive"));
    assertEquals(ValidationMode.EXHAUSTIVE,
        HoodieMetadataTableValidatorV2.parseValidationMode("EXHAUSTIVE"));
  }

  @Test
  void anUnknownValidationModeIsRejectedRatherThanDefaulted() {
    assertThrows(IllegalArgumentException.class,
        () -> HoodieMetadataTableValidatorV2.parseValidationMode("fail_fest"));
  }

  /**
   * A partition that cannot be listed fails the run rather than reporting itself clean.
   *
   * <p>The throw must stay a {@link HoodieIOException} the whole way down: an empty report is
   * indistinguishable from a partition that was checked and found nothing, so absorbing the fault
   * would let a run in which every listing errored out report success for this dimension — while
   * classifying it as a {@link HoodieValidationException} would page someone for corruption that was
   * never established.
   */
  @Test
  void storageFaultInTheDanglingCheckFailsTheRunAsInfrastructure() throws Exception {
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.listDirectEntries(any(StoragePath.class), any(StoragePathFilter.class)))
        .thenThrow(new IOException("simulated storage fault"));
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getStorage()).thenReturn(storage);

    DuplicateBaseFileDimension.Context ctx = danglingCheckContext(metaClient);

    HoodieIOException thrown = assertThrows(HoodieIOException.class,
        () -> DuplicateBaseFileDimension.detectDuplicates(ctx, "2024/01/01", new AtomicLong()),
        "a partition that cannot be listed must not be reported as validated");

    for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
      assertFalse(cause instanceof HoodieValidationException,
          "a storage fault must not be classified as metadata divergence");
    }
    assertFalse(
        HoodieMetadataTableValidatorV2.classifyFailure(thrown) instanceof HoodieValidationException,
        "hasValidationFailure() must stay false so the job raises an infrastructure alert");
  }

  /**
   * The same rule holds in exhaustive mode, which is the half this branch changes.
   *
   * <p>The stack this branch rebases absorbed a failed listing into a warning here, on the reasoning
   * that exhaustive mode should not stop early. That conflates a missing <em>finding</em> with a
   * broken <em>precondition</em>: the mode's licence to keep going covers findings it has, not checks
   * it could not run.
   */
  @Test
  void failedListingIsStillInfrastructureInExhaustiveMode() throws Exception {
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.listDirectEntries(any(StoragePath.class), any(StoragePathFilter.class)))
        .thenThrow(new IOException("simulated storage fault"));
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getStorage()).thenReturn(storage);

    assertThrows(HoodieIOException.class,
        () -> DuplicateBaseFileDimension.detectDuplicates(
            danglingCheckContext(metaClient, ValidationMode.EXHAUSTIVE), "2024/01/01",
            new AtomicLong()));
  }

  /** With the dimension switched off the listing never happens, so a broken storage cannot bite. */
  @Test
  void theOffSwitchSkipsTheListingEntirely() {
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getStorage()).thenReturn(storage);

    DuplicateBaseFileDimension.Context ctx = new DuplicateBaseFileDimension.Context(
        ValidationMode.FAIL_FAST, metaClient, null,
        "file:/tmp/does_not_need_to_exist", 1, false, false, null, null);

    assertTrue(
        DuplicateBaseFileDimension.detectDuplicates(ctx, "2024/01/01", new AtomicLong()).isEmpty());
  }

  /**
   * Steps 1-3 pin the metadata snapshot to a timeline that did not move while the snapshot was
   * being built, and give up rather than validate a snapshot nothing can explain.
   *
   * <p>Three attempts <em>in total</em>, not one plus three retries, and the snapshot is
   * discarded only <em>between</em> attempts -- the last one's timeline server is the caller's to
   * stop, so a fourth teardown here would close it out from under the {@code finally} that owns it.
   */
  @Test
  void pinningGivesUpAfterThreeAttemptsWhenTheTimelineKeepsMoving() throws IOException {
    MovingTimeline attempts = new MovingTimeline(Integer.MAX_VALUE);

    HoodieMetadataTableValidatorV2.PinResult result =
        HoodieMetadataTableValidatorV2.pinSnapshot(attempts);

    assertFalse(result.isPinned(),
        "a snapshot the timeline moved under must not be presented as pinned");
    assertEquals(3, result.getAttempts());
    assertEquals(3, attempts.builds, "three attempts in total, not one plus three retries");
    assertEquals(2, attempts.discards,
        "the snapshot is discarded only between attempts; the last one belongs to the caller");
  }

  /** The ordinary case: nothing moved, so one attempt is spent and no view is thrown away. */
  @Test
  void pinningSpendsOneAttemptOnAStillTimeline() throws IOException {
    MovingTimeline attempts = new MovingTimeline(1);

    HoodieMetadataTableValidatorV2.PinResult result =
        HoodieMetadataTableValidatorV2.pinSnapshot(attempts);

    assertTrue(result.isPinned());
    assertEquals(1, result.getAttempts());
    assertEquals(0, attempts.discards, "nothing moved, so nothing is rebuilt");
    assertSame(attempts.lastBuilt, result.getTimeline(),
        "the pinned timeline is the one the snapshot was built against");
  }

  /** A table that settles is retried into, and the retry rebuilds the metadata view. */
  @Test
  void pinningRetriesUntilTheTimelineSettles() throws IOException {
    MovingTimeline attempts = new MovingTimeline(3);

    HoodieMetadataTableValidatorV2.PinResult result =
        HoodieMetadataTableValidatorV2.pinSnapshot(attempts);

    assertTrue(result.isPinned());
    assertEquals(3, result.getAttempts());
    assertEquals(2, attempts.discards);
  }

  /**
   * Strict equality is over {@code (instantTime, action, state)} triples, not over instant times: a
   * write that only advanced from requested to inflight adds no instant time and still means the
   * metadata snapshot was built across a moving timeline.
   */
  @Test
  void stateChangeAloneCountsAsMovement() throws IOException {
    HoodieTimeline before = timelineOf(
        instant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "20240101000000000"));
    HoodieTimeline after = timelineOf(
        instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "20240101000000000"));

    HoodieMetadataTableValidatorV2.PinResult result = HoodieMetadataTableValidatorV2.pinSnapshot(
        new HoodieMetadataTableValidatorV2.SnapshotAttempt() {
          @Override
          public HoodieTimeline buildSnapshot() {
            return before;
          }

          @Override
          public HoodieTimeline reReadTimeline() {
            return after;
          }

          @Override
          public void discardSnapshot() {
          }
        });

    assertFalse(result.isPinned(),
        "requested -> inflight moves the timeline without adding an instant time");
  }

  /**
   * A run that could not pin its snapshot validated nothing, and the two modes part company there:
   * fail-fast reports success and leaves one marked log line for alerting to count, exhaustive
   * refuses outright.
   *
   * <p>This is the one place the design deliberately overrides the rule that a prerequisite failure
   * fails the run in both modes, with the residual signalling defects accepted. The marker is the
   * whole of what a fail-fast caller gets, since it cannot tell the skip from a clean run by return
   * value.
   */
  @Test
  void failFastReturnsSuccessWithTheMarkerWhenPinningFails() {
    HoodieMetadataTableValidatorV2 validator = validatorFor("file:/tmp/unpinnable");

    assertTrue(validator.concludeUnpinnedRun(ValidationMode.FAIL_FAST,
            DuplicateBaseFileDimension.Result.none(), 0),
        "fail-fast reports success rather than failing a healthy table it could not survey");
    assertTrue(HoodieMetadataTableValidatorV2.pinningSkippedMessage("file:/tmp/unpinnable")
            .contains("MDT_VALIDATOR_PINNING_SKIPPED"),
        "the marker alerting counts consecutive skips on must be in the line");
  }

  @Test
  void exhaustiveThrowsWhenPinningFails() {
    HoodieMetadataTableValidatorV2 validator = validatorFor("file:/tmp/unpinnable");

    HoodieValidationException thrown = assertThrows(HoodieValidationException.class,
        () -> validator.concludeUnpinnedRun(ValidationMode.EXHAUSTIVE,
            DuplicateBaseFileDimension.Result.none(), 0));
    assertTrue(thrown.getMessage().contains("file:/tmp/unpinnable"), thrown.getMessage());
  }

  /**
   * The archival counterpart, and it parts the two modes exactly where the pinning skip does.
   *
   * <p>The archiver removes completed instants from the active timeline and leaves nothing on it to
   * say so, so a run it overlapped holds a delta that is missing a whole class of change --
   * {@code ServiceActivityDelta} models the timeline as append-only, and archival is the one actor that
   * refutes that. Everything phase 1 observed is still true; what is gone is the account of the
   * window needed to say what any of it means.
   *
   * <p>Fail-fast reports success because archival is routine and scheduled: failing every run that
   * overlaps one would report a healthy table as broken on the strength of its own housekeeping.
   * The marker is the whole of what such a caller gets.
   */
  @Test
  void failFastReturnsSuccessWithTheMarkerWhenArchivalRan() {
    HoodieMetadataTableValidatorV2 validator = validatorFor("file:/tmp/archiving");

    assertTrue(validator.concludeInconclusiveRun(ValidationMode.FAIL_FAST,
            new Inconclusive(InconclusiveCause.ARCHIVAL_RAN, "the archive changed"),
            DuplicateBaseFileDimension.Result.none(), 0),
        "archival is routine housekeeping and must not fail a healthy table");
    assertTrue(HoodieMetadataTableValidatorV2
            .skippedMessage(InconclusiveCause.ARCHIVAL_RAN, "file:/tmp/archiving",
                "the archive changed")
            .contains("MDT_VALIDATOR_ARCHIVAL_SKIPPED"),
        "the marker alerting counts consecutive skips on must be in the line");
  }

  /**
   * Pins the archival exception's text byte for byte, not just its type, because the {@code
   * Inconclusive} refactor changed how this message is assembled -- moving it from a
   * directly-thrown literal into {@code concludeInconclusiveRun} building it from {@link
   * InconclusiveCause#ARCHIVAL_RAN} and a reason string -- but is not permitted to change what it
   * says. The spec requires the archival path stay byte-for-byte identical; an {@code assertEquals}
   * on the full message is what actually holds that line, where asserting only the exception type
   * or a substring would let the assembly drift unnoticed.
   */
  @Test
  void exhaustiveThrowsWhenArchivalRan() {
    HoodieMetadataTableValidatorV2 validator = validatorFor("file:/tmp/archiving");

    HoodieValidationException thrown = assertThrows(HoodieValidationException.class,
        () -> validator.concludeInconclusiveRun(ValidationMode.EXHAUSTIVE,
            new Inconclusive(InconclusiveCause.ARCHIVAL_RAN, "the archive changed"),
            DuplicateBaseFileDimension.Result.none(), 0));

    assertEquals("Archival ran while validating file:/tmp/archiving: the archive changed. The "
            + "timeline delta the FS-MDT comparison would adjudicate against is therefore missing "
            + "instants that left the active timeline, so that dimension reached no verdict and no "
            + "findings were written for it",
        thrown.getMessage(),
        "the archival exception is unchanged byte for byte; only how it is assembled moved");
  }

  /**
   * The recompute cause reaches the same two arms and must be distinguishable in every channel an
   * operator reads: a run sanctioned for a recompute counted against the archival threshold would
   * page the wrong person about the wrong table property.
   */
  @Test
  void failFastReturnsSuccessWithItsOwnMarkerWhenPhaseOneRecomputed() {
    HoodieMetadataTableValidatorV2 validator = validatorFor("file:/tmp/recompute");

    assertTrue(validator.concludeInconclusiveRun(ValidationMode.FAIL_FAST,
            new Inconclusive(InconclusiveCause.PHASE_ONE_RECOMPUTED, "2 task(s) re-ran"),
            DuplicateBaseFileDimension.Result.none(), 0),
        "a lost cache block is not a finding about the table");

    String line = HoodieMetadataTableValidatorV2.skippedMessage(
        InconclusiveCause.PHASE_ONE_RECOMPUTED, "file:/tmp/recompute", "2 task(s) re-ran");
    assertTrue(line.contains("MDT_VALIDATOR_RECOMPUTE_SKIPPED"), line);
    assertFalse(line.contains("MDT_VALIDATOR_ARCHIVAL_SKIPPED"), line);
    assertTrue(line.contains("2 task(s) re-ran"), line);
  }

  @Test
  void exhaustiveThrowsWhenPhaseOneRecomputed() {
    HoodieMetadataTableValidatorV2 validator = validatorFor("file:/tmp/recompute");

    HoodieValidationException thrown = assertThrows(HoodieValidationException.class,
        () -> validator.concludeInconclusiveRun(ValidationMode.EXHAUSTIVE,
            new Inconclusive(InconclusiveCause.PHASE_ONE_RECOMPUTED, "2 task(s) re-ran"),
            DuplicateBaseFileDimension.Result.none(), 0));

    assertTrue(thrown.getMessage().startsWith("Phase 1 was recomputed after the barrier while "
        + "validating "), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("2 task(s) re-ran"), thrown.getMessage());
  }

  /**
   * A skip whose dimension A found nothing stays SKIPPED rather than becoming a usable clean run.
   *
   * <p>The subtlety the dimension-A rescue has to preserve. {@code writeFindings} answers "did this
   * run find anything", and on a clean dimension A it publishes a handle whose {@code isUsable()} is
   * true -- correct when both dimensions looked, wrong here, because dimension B never ran. A
   * consumer reading "usable, nothing found" would take it for a clean table when half the question
   * was never asked. Findings are worth publishing; an absence of findings from half a run is not.
   */
  @Test
  void skippedRunWhoseDuplicateDimensionFoundNothingIsNotReportedAsUsable() {
    HoodieMetadataTableValidatorV2 validator = validatorFor("file:/tmp/unpinnable");

    assertTrue(validator.concludeUnpinnedRun(ValidationMode.FAIL_FAST,
            DuplicateBaseFileDimension.Result.none(), 4),
        "fail-fast still reports success");

    assertTrue(validator.getRunReports().get().wasSkipped(),
        "dimension A found nothing and dimension B never ran, so nobody looked at anything: "
            + validator.getRunReports().get());
    assertFalse(validator.getRunReports().get().isUsable(),
        "a half-run that found nothing must not read downstream as a clean table");
  }

  /**
   * Either way the run publishes a skipped handle, so a caller can tell this from a clean run.
   * Fail-fast returns success and writes nothing, exactly as a clean fail-fast run does, so the
   * return value alone cannot separate them.
   */
  @Test
  void anArchivalSkipIsReportedAsSkippedRatherThanAsAnEmptyCleanRun() {
    HoodieMetadataTableValidatorV2 validator = validatorFor("file:/tmp/archiving");

    validator.concludeInconclusiveRun(ValidationMode.FAIL_FAST,
        new Inconclusive(InconclusiveCause.ARCHIVAL_RAN, "the archive changed"),
        DuplicateBaseFileDimension.Result.none(), 0);

    assertTrue(validator.getRunReports().isPresent());
    assertTrue(validator.getRunReports().get().wasSkipped(),
        "a skip that reports itself usable would read downstream as a clean table");
  }

  private static HoodieMetadataTableValidatorV2 validatorFor(String basePath) {
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = basePath;
    // The meta client is handed in so the constructor does not go looking for a table; nothing on
    // this path reads it, and neither the Spark context nor the table needs to exist.
    return new HoodieMetadataTableValidatorV2(null, config, new Properties(),
        Option.of(mock(HoodieTableMetaClient.class)));
  }

  /**
   * Phase 1 counts the storage-side population it examined. {@code filesSeen} is load-bearing: it
   * is the denominator of a mitigation job's blast-radius ceiling — a zero silently disables it.
   *
   * <p>Also pins which list is which. The two sides are symmetric in type and not in meaning, so a
   * swapped argument pair compiles, runs, and reports every FS-exclusive file as MDT-exclusive —
   * turning "the metadata table never learned about this file" into "the metadata table names a file
   * storage does not have", which is a different finding with a different verdict.
   */
  @Test
  void phaseOneCountsWhatItExamined() {
    FileSlice onlyOnStorage = sliceWithBaseFile("fg-fs", "20240101000000000");
    FileSlice onBothSides = sliceWithBaseFile("fg-both", "20240102000000000");

    DiffTally tally = diffOf(
        Collections.singletonList(onBothSides),
        Arrays.asList(onBothSides, onlyOnStorage));

    assertEquals(1, tally.getRecords().size(),
        "the file group only storage holds is the one finding: " + tally.getRecords());
    FileDiffRecord record = tally.getRecords().get(0);
    assertEquals(DiffPattern.FS_EXCLUSIVE, record.getDiffPattern(),
        "a file group storage holds and the metadata table does not is FS-exclusive; MDT-exclusive "
            + "here would mean the two lists were passed the wrong way round");
    assertEquals("fg-fs", record.getFileGroupId());
    assertEquals(2, tally.getFilesSeen(),
        "both storage-side base files were examined, not only the one that differed");
  }

  /**
   * A file group the metadata table does not know is reported whole in one run, and its older
   * slices join the population: a mitigation job's blast-radius ceiling must not see a run that
   * deletes more files than it examined. A known file group's older slices stay out of both.
   */
  @Test
  void phaseOneReportsEverySliceOfAnFsExclusiveFileGroup() {
    FileSlice orphanLatest = sliceWithBaseFile("fg-fs", "20240103000000000");
    FileSlice orphanOlder = sliceWithBaseFile("fg-fs", "20240101000000000");
    FileSlice knownLatest = sliceWithBaseFile("fg-both", "20240102000000000");
    FileSlice knownOlder = sliceWithBaseFile("fg-both", "20240100000000000");

    DiffTally tally = diffOf(Collections.singletonList(knownLatest),
        Arrays.asList(knownLatest, orphanLatest),
        Arrays.asList(knownLatest, knownOlder, orphanLatest, orphanOlder));

    assertEquals(Arrays.asList(
            "fg-fs_1-0-1_20240103000000000.parquet", "fg-fs_1-0-1_20240101000000000.parquet"),
        tally.getRecords().stream().map(FileDiffRecord::getFileName).collect(Collectors.toList()));
    assertEquals(3, tally.getFilesSeen(),
        "the two latest base files plus the orphan's older one; the known group's older slice "
            + "is neither compared nor counted");
  }

  /**
   * A file an in-flight clean is deleting is a diff record, not a hole in the population.
   *
   * <p>v2 does not filter both sides of the comparison with {@code --skip-data-files-for-cleaning}.
   * That filter covers only base files ({@code getBaseFilesForCleaning} filters to base-file
   * extensions), and it filters the population rather than explaining it, so {@code filesSeen}
   * shrinks and the file appears in no report at all. Phase 2 answers the same question properly
   * and for log files too: the pending-clean
   * reconciliation reads what the clean planned to delete and sanctions exactly those files.
   *
   * <p>So the assertion is that the record EXISTS and reaches phase 2 carrying what phase 2 needs.
   * What it is worth is phase 2's to say.
   */
  @Test
  void fileAnInflightCleanIsDeletingIsStillPartOfThePopulation() {
    FileSlice beingCleaned = sliceWithBaseFile("fg-cleaning", "20240101000000000");
    FileSlice onBothSides = sliceWithBaseFile("fg-both", "20240102000000000");

    DiffTally tally = diffOf(Collections.singletonList(onBothSides),
        Arrays.asList(onBothSides, beingCleaned));

    assertEquals(1, tally.getRecords().size(),
        "the file the clean is deleting differs between the two sides and must be reported");
    assertEquals("fg-cleaning", tally.getRecords().get(0).getFileGroupId());
    assertEquals(2, tally.getFilesSeen(),
        "filesSeen counts the whole storage-side population; nothing is filtered out of it");
  }

  /**
   * Phase 1 emits the differing files themselves, not one report per partition: a record is the unit
   * of the phase-2 shuffle, and the per-partition header counters travel on the accumulator instead.
   */
  @Test
  void phaseOneEmitsRecordsCarryingEverythingPhaseTwoNeeds() {
    DiffTally tally = diffOf(Collections.emptyList(),
        Arrays.asList(sliceWithBaseFile("fg-a", "20240101000000000"),
            sliceWithBaseFile("fg-b", "20240102000000000")));

    assertEquals(2, tally.getRecords().size());
    for (FileDiffRecord record : tally.getRecords()) {
      assertEquals(PARTITION, record.getPartition(),
          "a record has to carry its own partition once the RDD stops being per-partition");
      assertNotNull(record.getWritingInstant(), "the shuffle keys on the writing instant");
    }
  }

  /**
   * The ordering property this whole change exists to establish: {@code T_aft} is read only after
   * phase 1 has been forced to completion.
   *
   * <p>Adjacent statements do not guarantee it. Reversing the two, or replacing phase 1's
   * {@code count()} with a lazy transformation, both leave a run that compiles and passes while
   * producing records that describe storage as it was <em>after</em> the timeline delta that is
   * supposed to explain them. The guard is asked of the executors — how many phase-1 tasks actually
   * ran — precisely so that a flag the driver sets for itself cannot answer it.
   */
  @Test
  void theTimelineIsNotReadAgainUntilPhaseOneHasRun() {
    AtomicBoolean read = new AtomicBoolean();

    IllegalStateException noPhaseOne = assertThrows(IllegalStateException.class,
        () -> FsMdtDiffDimension.readAfterBarrier(null, () -> {
          read.set(true);
          return "T_aft";
        }));
    assertTrue(noPhaseOne.getMessage().contains("has not run at all"), noPhaseOne.getMessage());

    IllegalStateException lazyPhaseOne = assertThrows(IllegalStateException.class,
        () -> FsMdtDiffDimension.readAfterBarrier(phaseOneThatRan(0, 4), () -> {
          read.set(true);
          return "T_aft";
        }));
    assertTrue(lazyPhaseOne.getMessage().contains("0 of 4"), lazyPhaseOne.getMessage());

    assertFalse(read.get(), "the timeline must not have been read on either refusal");
    assertEquals("T_aft",
        FsMdtDiffDimension.readAfterBarrier(phaseOneThatRan(4, 4), () -> "T_aft"));
  }

  /** A retried or speculative task counts twice; over-counting still proves the listings happened. */
  @Test
  void retriedPhaseOneTaskDoesNotBreakTheBarrier() {
    assertEquals("T_aft",
        FsMdtDiffDimension.readAfterBarrier(phaseOneThatRan(5, 4), () -> "T_aft"));
  }

  // ---- phase 2: which records are adjudicated, and which are settled without a timeline --------

  /**
   * The filter into phase 2: only a record whose writing instant is known, and whose side the
   * adjudication table has a row for, is worth shuffling. Everything else is settled on the driver,
   * and is still written to {@code diff/} with a verdict.
   */
  @Test
  void onlyAttributableExclusiveRecordsTakeTheAdjudicationShuffle() {
    assertTrue(FsMdtDiffDimension.takesTheAdjudicationShuffle(
        diffRecord(DiffPattern.FS_EXCLUSIVE, MismatchRecord.FileType.BASE, "20240101000000000")),
        "an FS-exclusive file with a resolved instant is the shuffle's whole point");
    assertTrue(FsMdtDiffDimension.takesTheAdjudicationShuffle(
        diffRecord(DiffPattern.MDT_EXCLUSIVE, MismatchRecord.FileType.BASE, "20240101000000000")),
        "an MDT-exclusive base file carries the instant its name parses to");

    assertFalse(FsMdtDiffDimension.takesTheAdjudicationShuffle(
        diffRecord(DiffPattern.FS_EXCLUSIVE, MismatchRecord.FileType.LOG, null)),
        "no instant, no row: the question was never askable");
    assertFalse(FsMdtDiffDimension.takesTheAdjudicationShuffle(
        diffRecord(DiffPattern.MDT_EXCLUSIVE, MismatchRecord.FileType.LOG, null)),
        "an MDT-exclusive log file is unattributable on every table version");
    assertFalse(FsMdtDiffDimension.takesTheAdjudicationShuffle(
        diffRecord(DiffPattern.BOTH_DISAGREE, MismatchRecord.FileType.BASE, "20240101000000000")),
        "both sides holding the file needs no timeline at all");
  }

  /** Both sides hold the file and disagree about it: no timeline can explain that away. */
  @Test
  void bothSidesDisagreeingIsMdtCorruptionWithoutAskingTheTimeline() {
    FileDiffRecord settled = FsMdtDiffDimension.settleWithoutTheTimeline(
        diffRecord(DiffPattern.BOTH_DISAGREE, MismatchRecord.FileType.BASE, "20240101000000000"),
        nothingHappened(), PendingServiceState.empty());

    assertEquals(Verdict.MDT_CORRUPTION, settled.getVerdict());
  }

  /**
   * An MDT-exclusive log file is unattributable by construction, so the only question left is
   * whether a clean or a rollback took it. All three answers are distinct, and the middle one is the
   * point: a plan that could not be read is not a plan that said nothing.
   */
  @Test
  void unattributableMdtExclusiveLogFileFollowsTheReconciler() {
    FileDiffRecord record =
        diffRecord(DiffPattern.MDT_EXCLUSIVE, MismatchRecord.FileType.LOG, null);

    assertEquals(Verdict.SANCTIONED, FsMdtDiffDimension.settleWithoutTheTimeline(
        record, cleanFreed(record.getFileName()), PendingServiceState.empty())
        .getVerdict());
    assertEquals(Verdict.NOT_SETTLED, FsMdtDiffDimension.settleWithoutTheTimeline(
        record, unreadablePlan(), PendingServiceState.empty()).getVerdict());
    assertEquals(Verdict.MDT_CORRUPTION, FsMdtDiffDimension.settleWithoutTheTimeline(
        record, nothingHappened(), PendingServiceState.empty()).getVerdict());
  }

  /**
   * The two groups whose instant never resolved. Neither is a finding about the table: one question
   * was never askable and the other was deliberately excluded from adjudication, and both are
   * reported rather than dropped so that "not adjudicated" cannot read as "adjudicated and clean".
   */
  @Test
  void recordWithNoWritingInstantIsNotSettled() {
    assertEquals(Verdict.NOT_SETTLED, FsMdtDiffDimension.settleWithoutTheTimeline(
        diffRecord(DiffPattern.MDT_EXCLUSIVE, MismatchRecord.FileType.BASE, null),
        nothingHappened(), PendingServiceState.empty()).getVerdict());
    assertEquals(Verdict.NOT_SETTLED, FsMdtDiffDimension.settleWithoutTheTimeline(
        diffRecord(DiffPattern.FS_EXCLUSIVE, MismatchRecord.FileType.LOG, null),
        nothingHappened(), PendingServiceState.empty()).getVerdict());
  }

  /**
   * The last group {@code settleWithoutTheTimeline} settles: a record carrying a resolved writing
   * instant that nonetheless reached the settled side.
   *
   * <p>Such a record is one whose diff-pattern and file-type combination has no adjudication row,
   * an {@code MDT_EXCLUSIVE} record with no file type, which the builder permits. The detail must
   * say that, and must not send an operator to {@code dropCounts}, where there is nothing for this
   * file, nor claim the instant was unattributable when it resolved perfectly well.
   */
  @Test
  void recordWithAnInstantButNoAdjudicationRowSaysSoRatherThanBlamingDropCounts() {
    FileDiffRecord settled = FsMdtDiffDimension.settleWithoutTheTimeline(
        resolvedButUnadjudicable(), nothingHappened(), PendingServiceState.empty());

    assertEquals(Verdict.NOT_SETTLED, settled.getVerdict());
    assertTrue(settled.getVerdictDetail().contains("20240109000000000"),
        "the detail has to name the instant it did resolve: " + settled.getVerdictDetail());
    assertTrue(settled.getVerdictDetail().contains("match no adjudication row"),
        "the detail has to name the real cause -- this (diff pattern, file type) pair has no "
            + "adjudication row -- not merely rule out the old one: " + settled.getVerdictDetail());
    assertFalse(settled.getVerdictDetail().contains("key space"),
        "the key space subset filter is gone; naming it sends an operator after a cause that no "
            + "longer exists: " + settled.getVerdictDetail());
    assertFalse(settled.getVerdictDetail().contains("dropCounts"),
        "there is no dropCounts entry for this file, and sending an operator to look for one wastes "
            + "the one lead they have: " + settled.getVerdictDetail());
    assertFalse(settled.getVerdictDetail().contains("no writing instant could be attributed"),
        "this file's writing instant resolved perfectly well: " + settled.getVerdictDetail());
  }

  /**
   * The two phase-2 populations are complements by construction: every phase-1 record either takes
   * the shuffle, or is settled without a timeline lookup. A record in neither would silently vanish
   * from the report; one in both would be decided twice.
   */
  @Test
  void everyRecordEitherTakesTheShuffleOrIsSettledWithoutIt() {
    List<FileDiffRecord> sampled = Arrays.asList(
        diffRecord(DiffPattern.FS_EXCLUSIVE, MismatchRecord.FileType.BASE, "20240101000000000"),
        diffRecord(DiffPattern.MDT_EXCLUSIVE, MismatchRecord.FileType.BASE, "20240102000000000"),
        diffRecord(DiffPattern.MDT_EXCLUSIVE, MismatchRecord.FileType.LOG, null),
        diffRecord(DiffPattern.FS_EXCLUSIVE, MismatchRecord.FileType.LOG, null),
        diffRecord(DiffPattern.BOTH_DISAGREE, MismatchRecord.FileType.BASE, "20240103000000000"),
        unwatermarked());
    // What countByInstant will see: the writing instants of the records that take the shuffle.
    // unwatermarked() carries a resolved instant and an FS_EXCLUSIVE pattern, so it belongs here
    // too.
    Set<String> keySpace = new TreeSet<>();
    for (FileDiffRecord record : sampled) {
      if (FsMdtDiffDimension.takesTheAdjudicationShuffle(record)) {
        keySpace.add(record.getWritingInstant());
      }
    }
    assertEquals(new TreeSet<>(Arrays.asList(
        "20240101000000000", "20240102000000000", "20240108000000000")), keySpace);

    List<FileDiffRecord> population = new ArrayList<>(sampled);
    population.add(resolvedButUnadjudicable());

    List<FileDiffRecord> shuffled = new ArrayList<>();
    List<FileDiffRecord> settled = new ArrayList<>();
    for (FileDiffRecord record : population) {
      if (FsMdtDiffDimension.takesTheAdjudicationShuffle(record)) {
        shuffled.add(record);
      } else {
        settled.add(FsMdtDiffDimension.settleWithoutTheTimeline(
            record, nothingHappened(), PendingServiceState.empty()));
      }
    }

    assertEquals(3, shuffled.size(),
        "the three attributable exclusive records take the shuffle: " + shuffled);
    assertEquals(population.size(), shuffled.size() + settled.size(),
        "a record in neither population vanishes from the report; one in both is decided twice");
    for (FileDiffRecord record : settled) {
      assertNotNull(record.getVerdict(),
          "every settled record still reaches diff/ with a verdict: " + record.getFileName());
    }
    FileDiffRecord unadjudicable = null;
    for (FileDiffRecord record : settled) {
      if (resolvedButUnadjudicable().getFileName().equals(record.getFileName())) {
        unadjudicable = record;
      }
    }
    assertNotNull(unadjudicable, "one predicate decides both populations, so a record it rejects "
        + "must be settled rather than shuffled");
  }

  // ---- phase 2: the driver-side fold --------------------------------------------------------

  /**
   * The collect is sorted before it is written, so two runs over one unchanged table produce the
   * same document however the shuffle happened to return the records.
   */
  @Test
  void sortedOrdersTheRecordsTheDocumentIsWrittenFrom() {
    List<FileDiffRecord> records = Arrays.asList(
        decidedFile("c.parquet"), decidedFile("a.parquet"), decidedFile("b.parquet"));

    List<FileDiffRecord> sorted = FsMdtDiffDimension.sorted(records);

    assertEquals(3, sorted.size(), "sorting must not drop or add a record");
    assertEquals("a.parquet", sorted.get(0).getFileName(),
        "the written order must not depend on the order the shuffle returned them in, or two runs "
            + "over one unchanged table write different documents");
    assertEquals("b.parquet", sorted.get(1).getFileName());
    assertEquals("c.parquet", sorted.get(2).getFileName());
  }

  /**
   * The drop counts are summed across the tasks that reported them, by class.
   *
   * <p>A task's entry reaches the driver at most once — a failed attempt reports no external
   * accumulator, and a {@code ResultTask}'s updates merge only for the attempt whose output the job
   * keeps — so summing cannot double-count a retry. What would double-count is a phase-1 block
   * recomputed by a later action, and that same event sends the run down the inconclusive path
   * before any of these figures is reported.
   */
  @Test
  void mergeDropCountsSumsEveryTasksEntryByClass() {
    List<Map<String, Long>> perTask = Arrays.asList(
        Collections.singletonMap("COMMAND_BLOCK", 3L),
        Collections.singletonMap("COMMAND_BLOCK", 2L),
        Collections.singletonMap("UNREADABLE", 1L));

    Map<String, Long> merged = FsMdtDiffDimension.mergeDropCounts(perTask);

    assertEquals(Long.valueOf(5L), merged.get("COMMAND_BLOCK"),
        "two tasks each dropped command blocks, and the run dropped both lots");
    assertEquals(Long.valueOf(1L), merged.get("UNREADABLE"));
  }

  /** No task dropped anything, which is a fact worth reporting as an empty map rather than a null. */
  @Test
  void mergeDropCountsOfNothingIsEmpty() {
    assertTrue(FsMdtDiffDimension.mergeDropCounts(Collections.emptyList()).isEmpty());
  }

  // ---- phase 2: what the verdicts do to the run ------------------------------------------------

  /**
   * One failure for the dimension, however many records failed and whatever partitions they sit in.
   * The records are all in the document with their partitions on them, so a failure per partition
   * told a consumer nothing the findings do not.
   */
  @Test
  void fatalVerdictsRaiseExactlyOneFailureForTheWholeDimension() {
    List<FileDiffRecord> records = Arrays.asList(
        decided(Verdict.DANGLING), decided(Verdict.MDT_CORRUPTION), decided(Verdict.SANCTIONED),
        decided(Verdict.SANCTIONED), decided(Verdict.NOT_SETTLED));

    Option<HoodieValidationException> failure =
        FsMdtDiffDimension.diffFailure(contextIn(ValidationMode.EXHAUSTIVE), records);

    assertTrue(failure.isPresent(), "two records reached a fatal verdict");
    String message = failure.get().getMessage();
    assertTrue(message.contains("2 of 5"), message);
    assertTrue(message.contains("DANGLING=1"), message);
    assertTrue(message.contains("MDT_CORRUPTION=1"), message);
    assertFalse(message.contains("SANCTIONED"),
        "a sanctioned record is explained, not a finding, and must not appear in the counts");
  }

  /**
   * The failure still says where to go and look. One exception for the dimension is not a reason to
   * stop naming the partitions, and fail-fast writes no document to look them up in.
   */
  @Test
  void theFailureNamesEveryDamagedPartition() {
    List<FileDiffRecord> records = Arrays.asList(
        decidedIn("2026/08/15", Verdict.DANGLING), decidedIn("2026/08/16", Verdict.DANGLING),
        decidedIn("2026/08/15", Verdict.MDT_CORRUPTION),
        decidedIn("2026/08/17", Verdict.SANCTIONED));

    String message = FsMdtDiffDimension.diffFailure(contextIn(ValidationMode.FAIL_FAST), records)
        .get().getMessage();

    assertTrue(message.contains("2 partition(s)"), message);
    assertTrue(message.contains("2026/08/15"), message);
    assertTrue(message.contains("2026/08/16"), message);
    assertFalse(message.contains("2026/08/17"),
        "the sanctioned record's partition is not damaged and must not be named: " + message);
  }

  /** One message must not grow with the table, so the naming is capped and says what it dropped. */
  @Test
  void theFailureCapsHowManyPartitionsItNames() {
    List<FileDiffRecord> records = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      records.add(decidedIn(String.format("2026/08/%02d", i), Verdict.DANGLING));
    }

    String message = FsMdtDiffDimension.diffFailure(contextIn(ValidationMode.EXHAUSTIVE), records)
        .get().getMessage();

    assertTrue(message.contains("12 partition(s)"), "the count is never capped: " + message);
    assertTrue(message.contains("and 2 more"), "the names are: " + message);
    assertFalse(message.contains("2026/08/11"),
        "the eleventh name is past the cap: " + message);
  }

  /** {@code NOT_SETTLED} is the answer "nobody could tell", and it never fails a run. */
  @Test
  void nothingButNotSettledLeavesTheRunPassing() {
    assertTrue(FsMdtDiffDimension.diffFailure(contextIn(ValidationMode.EXHAUSTIVE),
        Collections.singletonList(decided(Verdict.NOT_SETTLED))).isEmpty());
  }

  /** A run that found nothing at all raises nothing at all. */
  @Test
  void noRecordsRaisesNoFailure() {
    assertTrue(FsMdtDiffDimension.diffFailure(contextIn(ValidationMode.EXHAUSTIVE),
        Collections.emptyList()).isEmpty());
  }

  /**
   * Fail-fast raises the same failure exhaustive does. What differs is the logging: fail-fast writes
   * no document, so it names a bounded sample of the failing records on the way past, which is the
   * only evidence an operator gets from that mode.
   */
  @Test
  void failFastRaisesTheSameOneFailure() {
    List<FileDiffRecord> records = Arrays.asList(
        decided(Verdict.DANGLING), decided(Verdict.SANCTIONED));

    Option<HoodieValidationException> failure =
        FsMdtDiffDimension.diffFailure(contextIn(ValidationMode.FAIL_FAST), records);

    assertTrue(failure.isPresent());
    assertTrue(failure.get().getMessage().contains("1 of 2"), failure.get().getMessage());
  }

  // ---- phase 2 fixtures -------------------------------------------------------------------------

  /** One diff record of the shape phase 1 emits. */
  private static FileDiffRecord diffRecord(DiffPattern pattern, MismatchRecord.FileType fileType,
                                           String writingInstant) {
    FileDiffRecord.Builder builder = FileDiffRecord.newBuilder()
        .withPartition(PARTITION)
        .withFileName(fileType == MismatchRecord.FileType.BASE
            ? "fg1_0-1-2_20240101000000000.parquet" : ".fg1_20240101000000000.log.1_0-1-2")
        .withFileGroupId("fg1")
        .withFileType(fileType)
        .withDiffPattern(pattern);
    if (writingInstant == null) {
      builder.withUnresolvedInstant(MismatchRecord.InstantUnresolvedReason.UNREADABLE);
    } else {
      builder.withWritingInstant(writingInstant, MismatchRecord.InstantSource.FILE_NAME);
    }
    return builder.build();
  }

  /**
   * A record phase 1 could not watermark. It takes the shuffle exactly like any other resolved,
   * exclusive record, on the strength of its writing instant and diff pattern alone.
   */
  private static FileDiffRecord unwatermarked() {
    return FileDiffRecord.newBuilder()
        .withPartition(PARTITION).withFileName("fg8_0-1-2_20240108000000000.parquet")
        .withFileGroupId("fg8").withFileType(MismatchRecord.FileType.BASE)
        .withDiffPattern(DiffPattern.FS_EXCLUSIVE)
        .withWritingInstant("20240108000000000", MismatchRecord.InstantSource.FILE_NAME)
        .build();
  }

  /**
   * A record with a resolved instant that still cannot be adjudicated: {@code MDT_EXCLUSIVE} with no
   * file type, so it matches neither the log-file branch nor the base-file arm of
   * {@code takesTheAdjudicationShuffle}. The builder does not require a file type, which is what
   * makes this constructible.
   */
  private static FileDiffRecord resolvedButUnadjudicable() {
    return FileDiffRecord.newBuilder()
        .withPartition(PARTITION).withFileName("fg9_0-1-2_20240109000000000.parquet")
        .withFileGroupId("fg9")
        .withDiffPattern(DiffPattern.MDT_EXCLUSIVE)
        .withWritingInstant("20240109000000000", MismatchRecord.InstantSource.FILE_NAME)
        .build();
  }

  /** A decided record of a given file name, for the driver-side rebuild. */
  private static FileDiffRecord decidedFile(String fileName) {
    return FileDiffRecord.newBuilder()
        .withPartition(PARTITION).withFileName(fileName).withFileGroupId("fg1")
        .withFileType(MismatchRecord.FileType.BASE)
        .withDiffPattern(DiffPattern.FS_EXCLUSIVE)
        .withWritingInstant("20240101000000000", MismatchRecord.InstantSource.FILE_NAME)
        .build()
        .withVerdict(Verdict.DANGLING, "for the test");
  }

  /** A record that has already been through phase 2, in a partition of its own. */
  private static FileDiffRecord decidedIn(String partition, Verdict verdict) {
    return FileDiffRecord.newBuilder()
        .withPartition(partition).withFileName("fg1_0-1-2_20240101000000000.parquet")
        .withFileGroupId("fg1")
        .withFileType(MismatchRecord.FileType.BASE)
        .withDiffPattern(DiffPattern.FS_EXCLUSIVE)
        .withWritingInstant("20240101000000000", MismatchRecord.InstantSource.FILE_NAME)
        .build()
        .withVerdict(verdict, "for the test");
  }

  /** A record that has already been through phase 2. */
  private static FileDiffRecord decided(Verdict verdict) {
    return diffRecord(DiffPattern.FS_EXCLUSIVE, MismatchRecord.FileType.BASE, "20240101000000000")
        .withVerdict(verdict, "for the test");
  }

  /** A timeline delta in which nothing a reconciler cares about happened. */
  private static ServiceActivityDelta nothingHappened() {
    return deltaWith(Collections.emptySet(), Collections.emptySet(), false);
  }

  /** A delta in which a clean observed during the run freed {@code fileName}. */
  private static ServiceActivityDelta cleanFreed(String fileName) {
    return deltaWith(Collections.singleton(fileName), Collections.emptySet(), false);
  }

  /** A delta whose plan for this partition could not be read: not proof of anything. */
  private static ServiceActivityDelta unreadablePlan() {
    return deltaWith(Collections.emptySet(), Collections.emptySet(), true);
  }

  private static ServiceActivityDelta deltaWith(Set<String> cleanFreed, Set<String> rollbackFreed,
                                         boolean readFailed) {
    ServiceActivityDelta delta = mock(ServiceActivityDelta.class);
    when(delta.fileNamesFreedByClean(any())).thenReturn(cleanFreed);
    when(delta.fileNamesFreedByRollback(any())).thenReturn(rollbackFreed);
    when(delta.rolledBackInstants()).thenReturn(Collections.emptySet());
    when(delta.readFailedFor(any())).thenReturn(readFailed);
    return delta;
  }

  private static FsMdtDiffDimension.PhaseOne phaseOneThatRan(long observed, long expected) {
    // The RDD is phase 2's to consume and the barrier never touches it.
    return new FsMdtDiffDimension.PhaseOne(null, 0L, expected, observed);
  }

  /**
   * One partition's diff, driven through the real {@code diffOnePartition} with a stubbed
   * file-system-view template standing in for the storage listing.
   */
  private static DiffTally diffOf(List<FileSlice> fromMdt, List<FileSlice> fromFs) {
    return diffOf(fromMdt, fromFs, fromFs);
  }

  /** As {@link #diffOf(List, List)}, with storage also holding slices older than the latest. */
  private static DiffTally diffOf(List<FileSlice> fromMdt, List<FileSlice> latestFromFs,
                                  List<FileSlice> allFromFs) {
    HoodieTableFileSystemView fsView = mock(HoodieTableFileSystemView.class);
    when(fsView.getLatestFileSlices(PARTITION)).thenReturn(latestFromFs.stream());
    when(fsView.getAllFileSlices(PARTITION)).thenReturn(allFromFs.stream());
    return FsMdtDiffDimension.diffOnePartition(
        Pair.of(PARTITION, fromMdt), contextIn(ValidationMode.EXHAUSTIVE, fsView),
        (partition, fileName) -> null);
  }

  /** A context carrying only what the code under test reads: the mode and the base path. */
  private static FsMdtDiffDimension.Context contextIn(ValidationMode mode) {
    return contextIn(mode, mock(HoodieTableFileSystemView.class));
  }

  private static FsMdtDiffDimension.Context contextIn(ValidationMode mode,
                                                      HoodieTableFileSystemView fsView) {
    return new FsMdtDiffDimension.Context(mode, mock(HoodieTableMetaClient.class), null, fsView,
        "file:/tmp/table", 1, 1L, null, new Properties());
  }

  private static FileSlice sliceWithBaseFile(String fileId, String instant) {
    FileSlice slice = new FileSlice(new HoodieFileGroupId(PARTITION, fileId), instant);
    StoragePath path = new StoragePath(
        "file:/tmp/table/" + PARTITION + "/" + fileId + "_1-0-1_" + instant + ".parquet");
    slice.setBaseFile(new HoodieBaseFile(new StoragePathInfo(path, 42L, false, (short) 0, 0L, 0L)));
    return slice;
  }

  private static HoodieTimeline timelineOf(HoodieInstant... instants) {
    return new BaseTimelineV2(Arrays.stream(instants), new HoodieInstantReader() {
    });
  }

  private static HoodieInstant instant(HoodieInstant.State state, String action, String time) {
    return new HoodieInstant(state, action, time, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  /**
   * A table being written to while its metadata snapshot is built: every re-read shows a timeline
   * that moved, until the attempt this one is told to settle on.
   *
   * <p>The settled re-read returns a <em>different object with the same content</em>, so a
   * comparison that got as far as identity rather than state would fail this.
   */
  private static final class MovingTimeline
      implements HoodieMetadataTableValidatorV2.SnapshotAttempt {

    private final int settlesOnAttempt;
    private int builds;
    private int discards;
    private HoodieTimeline lastBuilt;

    MovingTimeline(int settlesOnAttempt) {
      this.settlesOnAttempt = settlesOnAttempt;
    }

    @Override
    public HoodieTimeline buildSnapshot() {
      builds++;
      lastBuilt = timelineOf(
          instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "2024010100000000" + builds));
      return lastBuilt;
    }

    @Override
    public HoodieTimeline reReadTimeline() {
      if (builds >= settlesOnAttempt) {
        return timelineOf(instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION,
            "2024010100000000" + builds));
      }
      return timelineOf(
          instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "2024010100000000" + builds),
          instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION,
              "2024010200000000" + builds));
    }

    @Override
    public void discardSnapshot() {
      discards++;
    }
  }

  private static DuplicateBaseFileDimension.Context danglingCheckContext(
      HoodieTableMetaClient metaClient) {
    return danglingCheckContext(metaClient, ValidationMode.FAIL_FAST);
  }

  private static DuplicateBaseFileDimension.Context danglingCheckContext(
      HoodieTableMetaClient metaClient, ValidationMode mode) {
    return new DuplicateBaseFileDimension.Context(
        mode, metaClient, null, "file:/tmp/does_not_need_to_exist", 1, false, true,
        null, null);
  }
}
