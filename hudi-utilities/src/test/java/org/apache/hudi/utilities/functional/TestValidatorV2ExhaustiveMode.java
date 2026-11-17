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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.HoodieMetadataTableValidator;
import org.apache.hudi.utilities.HoodieMetadataTableValidatorV2;
import org.apache.hudi.utilities.util.DanglingReportWriter;
import org.apache.hudi.utilities.util.FileDiffReportWriter;
import org.apache.hudi.utilities.util.MismatchReportWriter;
import org.apache.hudi.utilities.util.ValidationMode;
import org.apache.hudi.utilities.util.ValidationRunReports;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exhaustive mode end to end: damage a real table one way at a time, run the validator, and read
 * the findings back off storage.
 *
 * <p>Deliberately driven through {@link HoodieMetadataTableValidator} rather than the analyzer, so
 * each test covers the whole path — flag parsing, the executor-side comparison, the report
 * travelling back with a failing partition, and the driver's write.
 *
 * <p>Table setup, damage and report reading all come from
 * {@link ValidatorV2FunctionalTestBase}; only the cases live here. Every table written here is at
 * the current table version. The version matrix is
 * {@link TestValidatorV2DanglingFiles}'s job.
 */
@Tag("functional")
public class TestValidatorV2ExhaustiveMode extends ValidatorV2FunctionalTestBase {

  @Test
  void healthyTableWritesNothingAndPasses() throws Exception {
    writeTable("COPY_ON_WRITE");

    assertTrue(runValidator(ValidationMode.EXHAUSTIVE), "a healthy table must pass");

    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "a clean run must not leave a directory behind — a fleet on a schedule would accumulate "
            + "one per table per run");
  }

  /** A base file deleted behind the metadata table's back takes its whole file group with it. */
  @Test
  void baseFileDeletedBehindTheMetadataTableIsReported() throws Exception {
    writeTable("COPY_ON_WRITE");
    StoragePath victim = anyFile(".parquet", false);
    assertTrue(storage.deleteFile(victim), "precondition: the base file must actually be removed");

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE), "the table diverges, so validation fails");

    JsonNode document = readDiff();
    assertEquals(1, document.get("recordCount").asLong(), "one file was removed: " + document);
    JsonNode record = document.get("records").get(0);
    assertEquals("OUTBOUND_DELETION", record.get("verdict").asText(),
        "a completed instant's file is gone from storage and no clean or rollback explains it: "
            + document);
    assertEquals("MDT_EXCLUSIVE", record.get("diffPattern").asText());
    assertEquals("MDT_EXCLUSIVE_FILE_GROUP", record.get("sourcePattern").asText(),
        "the finer grain the diff pattern collapsed from is kept for a human reader");
    assertEquals(victim.getName(), record.get("fileName").asText());
    assertEquals("BASE", record.get("fileType").asText());
    assertEquals("FILE_NAME", record.get("instantSource").asText(),
        "a base file's own name carries the instant that wrote it");
    // A fatal verdict is not SANCTIONED, so it survives into the actionable document as well.
    assertEquals("OUTBOUND_DELETION",
        recordFor(readDiffAdjudicated(), victim.getName()).get("verdict").asText());
    assertTrue(storage.exists(new StoragePath(diffDir(FileDiffReportWriter.DIFF_DIR),
        MismatchReportWriter.COMPLETE_MARKER)));
  }

  /**
   * The retried-write shape for log files: a second copy under a new log version, which the delta
   * commit that wrote the original does not name.
   *
   * <p>Such a file is not tolerated as an unaccounted storage file on a passing run, and that is
   * deliberate: tolerating it would be a <em>window</em> rather than a verdict. Once the writing
   * instant archives, {@code HoodieFileGroup.isFileSliceCommitted:182-188} on version 6 and {@code
   * filterUncommittedFiles:594-611} on version 8+ both reclassify the orphan as legitimate data by a
   * pure timestamp comparison, so a file nothing will ever reference is silently promoted to data.
   * The commit completed before {@code T_prev}, was read, and does not name the copy — positive
   * proof — so the verdict is {@link org.apache.hudi.utilities.util.Verdict#DANGLING} and the run
   * fails.
   *
   * <p>The verdict is asserted on the record, not merely on the run: "the run failed" would keep
   * passing if some other file in this table started failing it instead.
   */
  @Test
  void uncommittedLogFileOnStorageIsDanglingAndFailsTheRun() throws Exception {
    writeTable("MERGE_ON_READ");
    StoragePath original = anyFile(".log.", true);
    // A new log VERSION, not just a new write token. Two log files differing only in write token
    // collapse inside the file slice the same way duplicate base files do, so a copy under a new
    // token is invisible to both sides and produces no finding at all -- the latest-slice blind
    // spot this validator does not attempt to close.
    StoragePath copy = withLogVersion(original, 9);
    copyFile(original, copy);

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE),
        "a log file under a completed commit that does not name it is a retry orphan, and must "
            + "not be tolerated");

    JsonNode record = recordFor(readDiff(), copy.getName());
    assertEquals("DANGLING", record.get("verdict").asText(),
        "the commit completed before T_prev, was read, and does not name this file");
    assertEquals("FS_EXCLUSIVE", record.get("diffPattern").asText());
    assertEquals("FS_EXCLUSIVE_LOG_FILE", record.get("sourcePattern").asText());
    assertEquals("LOG", record.get("fileType").asText());
    // The instant must come from the block header: a log file's NAME carries its slice's base
    // commit time, not the delta commit that wrote it.
    assertEquals("LOG_BLOCK_HEADER", record.get("instantSource").asText());
    assertFalse(record.get("writingInstant").isNull(),
        "the verdict rests on an instant, so the record has to carry it: " + record);
    assertFalse(record.get("writingInstant").asText().isEmpty(), record.toString());
    // The original beside it is named by the same commit and must not be swept up with the copy.
    assertNull(recordOrNull(readDiff(), original.getName()),
        "the log file the delta commit names is not a difference at all: " + readDiff());
    assertEquals("DANGLING",
        recordFor(readDiffAdjudicated(), copy.getName()).get("verdict").asText(),
        "a delete authorisation belongs in the actionable document");
  }

  /** The caller's run id, not a generated timestamp, names the directory the reports land in. */
  @Test
  void runIdGivenByTheCallerNamesTheReportDirectory() throws Exception {
    writeTable("COPY_ON_WRITE");
    assertTrue(storage.deleteFile(anyFile(".parquet", false)),
        "precondition: the base file must actually be removed");

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE, true, "", "run-42"),
        "the table diverges, so validation fails");

    assertEquals("run-42", runDir().getName(),
        "expected the caller's run id to name the report directory, found: " + runDir());
    assertTrue(storage.exists(new StoragePath(diffDir(FileDiffReportWriter.DIFF_DIR),
        MismatchReportWriter.COMPLETE_MARKER)));
  }

  /** An empty run id -- the default -- still falls back to a generated timestamp. */
  @Test
  void emptyRunIdStillFallsBackToTheGeneratedTimestamp() throws Exception {
    writeTable("COPY_ON_WRITE");
    assertTrue(storage.deleteFile(anyFile(".parquet", false)),
        "precondition: the base file must actually be removed");

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE, true, "", ""));

    String runId = runDir().getName();
    assertTrue(runId.matches("\\d{14}"), "expected a generated timestamp, got " + runId);
  }

  /**
   * {@code run()} answers only the verdict; a caller that wants to know what a run wrote and
   * whether it was safe to act on needs {@link HoodieMetadataTableValidatorV2#getRunReports()}.
   * Driven straight through the v2 validator, rather than the {@link HoodieMetadataTableValidator}
   * wrapper {@link #runValidator} uses, because the wrapper does not hand its v2 instance back.
   */
  @Test
  void runReportsNameEveryDirectoryTheRunWrote() throws Exception {
    writeTable("COPY_ON_WRITE");
    assertTrue(storage.deleteFile(anyFile(".parquet", false)),
        "precondition: the base file must actually be removed");

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.useValidatorV2 = true;
    config.ignoreFailed = true;
    config.validationMode = ValidationMode.EXHAUSTIVE.getFlagValue();
    config.validationOutputRoot = "";
    config.validationRunId = "run-7";
    config.fsViewMemoryFraction = 0.05;

    HoodieMetadataTableValidatorV2 validator =
        new HoodieMetadataTableValidatorV2(jsc, config, new Properties());
    validator.run();

    ValidationRunReports reports = validator.getRunReports().get();
    assertEquals("run-7", reports.getRunId());
    assertTrue(reports.getMismatchDir().isPresent(),
        "the FS-MDT dimension's raw document takes the mismatch slot: " + reports);
    // The two dimensions conclude independently, and this table's only damage is one the FS-MDT
    // dimension finds. There is no duplicate base file, so the dangling dimension has nothing to
    // adjudicate and its slot is legitimately empty -- which is precisely why the run's conclusion
    // needs a slot of its own. Reading getAdjudicatedDir()'s absence as "clean" is the mistake
    // getDiffAdjudicatedDir() exists to make impossible.
    assertTrue(reports.getDiffAdjudicatedDir().isPresent(),
        "a run whose only findings are FS-MDT ones still reaches a conclusion: " + reports);
    assertFalse(reports.getAdjudicatedDir().isPresent(),
        "no duplicate base file was planted, so this dimension had nothing to adjudicate: "
            + reports);
    assertFalse(reports.getSuspectsDir().isPresent(), reports.toString());
    assertTrue(reports.isUsable());
    assertTrue(reports.isFsMdtDiffAssessed(), "the FS-MDT comparison ran: " + reports);
  }

  /** A refused reconciliation must never be read as "nothing to adjudicate". */
  @Test
  void refusedReconciliationIsReportedAsRefusedNotAsNothingToDo() {
    ValidationRunReports refused = ValidationRunReports.refused("run-8");
    assertFalse(refused.isUsable());
    assertFalse(refused.getAdjudicatedDir().isPresent());
  }

  /** The opposite case: a clean run is usable even though it names no directory. */
  @Test
  void runThatFoundNothingIsUsableAndNamesNoDirectory() {
    ValidationRunReports clean =
        ValidationRunReports.of("run-9", Option.empty(), Option.empty(), Option.empty());
    assertTrue(clean.isUsable());
    assertFalse(clean.getAdjudicatedDir().isPresent());
    assertTrue(clean.isFsMdtDiffAssessed());
  }

  /** Only a handle whose FS-MDT comparison ran may be read as a statement about the metadata table. */
  @Test
  void onlyAHandleWhoseFsMdtComparisonRanReportsItAssessed() {
    ValidationRunReports duplicateOnly = ValidationRunReports.withoutFsMdtDiff(
        "run-10", Option.of("dangling_files"), Option.of("adjudicated"));
    assertTrue(duplicateOnly.isUsable(), "dimension A's findings stand: " + duplicateOnly);
    assertFalse(duplicateOnly.wasSkipped(), duplicateOnly.toString());
    assertFalse(duplicateOnly.isFsMdtDiffAssessed(), duplicateOnly.toString());
    assertFalse(duplicateOnly.getMismatchDir().isPresent());
    assertFalse(duplicateOnly.getDiffAdjudicatedDir().isPresent());
    assertFalse(ValidationRunReports.skipped("run-11").isFsMdtDiffAssessed());
    assertFalse(ValidationRunReports.refused("run-12").isFsMdtDiffAssessed());
  }

  /**
   * A run that could not pin its snapshot still publishes what dimension A proved.
   *
   * <p>This is the regression test for the discard the dimension split made possible. Dimension A
   * runs to completion before dimension B is even constructed -- it pins no snapshot, opens no
   * metadata table and reads its own timeline -- so a failure to pin says nothing about a verdict it
   * had already reached. Both skip paths nonetheless returned before {@code writeFindings}, so a
   * table that can never be pinned (a continuously-written one) lost dimension A's answer on every
   * run, forever.
   *
   * <p>Asserted through a real {@code run()} with a real duplicate planted, because the discard was
   * a property of the return path rather than of either dimension: only driving the whole method
   * shows that the findings reach storage.
   */
  @Test
  void runThatCouldNotPinStillPublishesTheDuplicateDimensionsFindings() throws Exception {
    writeCowTableWithASupersededSlice();
    StoragePath duplicate = duplicateOf(anySupersededBaseFile());

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.useValidatorV2 = true;
    config.validationMode = ValidationMode.EXHAUSTIVE.getFlagValue();
    config.validationRunId = "run-unpinnable-with-duplicates";
    config.fsViewMemoryFraction = 0.05;

    HoodieMetadataTableValidatorV2 validator =
        new HoodieMetadataTableValidatorV2(jsc, config, new Properties()) {
          @Override
          protected PinResult pinMetadataSnapshot(SnapshotAttempt attempt) throws IOException {
            // The table presents exactly as one being written to continuously: a real snapshot
            // build against a re-read that can never match it.
            return super.pinMetadataSnapshot(new SnapshotAttempt() {
              @Override
              public HoodieTimeline buildSnapshot() throws IOException {
                return attempt.buildSnapshot();
              }

              @Override
              public HoodieTimeline reReadTimeline() {
                return attempt.reReadTimeline().filter(instant -> false);
              }

              @Override
              public void discardSnapshot() throws IOException {
                attempt.discardSnapshot();
              }
            });
          }
        };

    // Exhaustive still refuses, because dimension B reached no verdict and that is what the mode
    // exists to produce. The refusal does not take dimension A with it.
    assertThrows(HoodieValidationException.class, validator::run,
        "exhaustive refuses a run whose FS-MDT comparison never happened");

    ValidationRunReports reports = validator.getRunReports().get();
    assertFalse(reports.wasSkipped(),
        "dimension A ran and found something, so this run is not one where nobody looked: "
            + reports);
    assertEquals("run-unpinnable-with-duplicates", reports.getRunId());

    // Dimension A's two documents are on storage; dimension B's two are not, because it never ran.
    assertEquals("DUPLICATE_BASE_FILE",
        recordFor(readSuspects(), duplicate.getName()).get("suspectReason").asText(),
        "the suspect dimension A found before the pin failed must survive the failure");
    assertEquals("DANGLING_FILES",
        recordFor(readAdjudicated(), duplicate.getName()).get("pattern").asText(),
        "dimension A adjudicates on its own timeline reads and owes the pin nothing");
    assertFalse(reports.getMismatchDir().isPresent(),
        "the FS-MDT comparison never ran, so it names no directory: " + reports);
    assertFalse(reports.getDiffAdjudicatedDir().isPresent(), reports.toString());
    // Usable for dimension A, but must not read as an FS-MDT comparison that found nothing.
    assertTrue(reports.isUsable(), reports.toString());
    assertFalse(reports.isFsMdtDiffAssessed(),
        "the metadata table was never compared, so the handle must not vouch for it: " + reports);
  }

  /**
   * A fail-fast run that could not pin its metadata snapshot returns {@code true} and writes
   * nothing -- so does a clean fail-fast run, and until {@link ValidationRunReports#wasSkipped()}
   * the two were the same object through every API this class exposes. The behaviour is the
   * engineer's explicit ruling and is unchanged; only the signal is new, and only the signal is
   * asserted here.
   *
   * <p>Driven through a real {@code run()} on a real table rather than through
   * {@code concludeUnpinnedRun} directly, because what is being pinned is that the skip's handle
   * survives the whole method: a later assignment to the reports field, or a return path that
   * bypasses it, would go unseen by a direct call.
   */
  @Test
  void failFastRunThatCouldNotPinItsSnapshotReportsItselfAsSkipped() throws Exception {
    writeTable("COPY_ON_WRITE");

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.useValidatorV2 = true;
    config.validationMode = ValidationMode.FAIL_FAST.getFlagValue();
    config.validationRunId = "run-unpinnable";
    config.fsViewMemoryFraction = 0.05;

    HoodieMetadataTableValidatorV2 validator =
        new HoodieMetadataTableValidatorV2(jsc, config, new Properties()) {
          @Override
          protected PinResult pinMetadataSnapshot(SnapshotAttempt attempt) throws IOException {
            // The real pinning loop over a real snapshot build, given a re-read that can never
            // match it: the table presents exactly as one being written to continuously.
            return super.pinMetadataSnapshot(new SnapshotAttempt() {
              @Override
              public HoodieTimeline buildSnapshot() throws IOException {
                return attempt.buildSnapshot();
              }

              @Override
              public HoodieTimeline reReadTimeline() {
                return attempt.reReadTimeline().filter(instant -> false);
              }

              @Override
              public void discardSnapshot() throws IOException {
                attempt.discardSnapshot();
              }
            });
          }
        };

    assertTrue(validator.run(),
        "fail-fast reports success rather than failing a healthy table it could not survey");

    ValidationRunReports reports = validator.getRunReports().get();
    assertTrue(reports.wasSkipped(), "a run that validated nothing must say so: " + reports);
    assertFalse(reports.isUsable(), "nobody looked, so nothing may be acted on: " + reports);
    assertEquals("run-unpinnable", reports.getRunId());
    assertFalse(reports.getMismatchDir().isPresent(), reports.toString());
    assertFalse(reports.getDiffAdjudicatedDir().isPresent(), reports.toString());
  }

  /**
   * <b>The exhaustive twin: a run that could not pin its snapshot RAISES.</b>
   *
   * <p>The one thing the two modes decide differently about a check that never ran, and the reason the
   * pair has to be tested together: fail-fast reports success because a continuously-written table
   * would otherwise fail every scheduled run on the strength of being busy, while exhaustive exists to
   * produce a findings document a mitigation job acts on — and producing none, silently, is the outcome
   * it must not have. Same fixture as the sibling above, one config value changed.
   *
   * <p>The exception is a {@code HoodieValidationException}, so it reports as a prerequisite failure in
   * the data-correctness class rather than as an infrastructure error. That is an accepted residual, not
   * an oversight, and it is asserted rather than assumed.
   */
  @Test
  void exhaustiveRunThatCouldNotPinItsSnapshotRaises() throws Exception {
    writeTable("COPY_ON_WRITE");

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.useValidatorV2 = true;
    config.validationMode = ValidationMode.EXHAUSTIVE.getFlagValue();
    config.validationRunId = "run-unpinnable-exhaustive";
    config.fsViewMemoryFraction = 0.05;

    HoodieMetadataTableValidatorV2 validator =
        new HoodieMetadataTableValidatorV2(jsc, config, new Properties()) {
          @Override
          protected PinResult pinMetadataSnapshot(SnapshotAttempt attempt) throws IOException {
            return super.pinMetadataSnapshot(new SnapshotAttempt() {
              @Override
              public HoodieTimeline buildSnapshot() throws IOException {
                return attempt.buildSnapshot();
              }

              @Override
              public HoodieTimeline reReadTimeline() {
                return attempt.reReadTimeline().filter(instant -> false);
              }

              @Override
              public void discardSnapshot() throws IOException {
                attempt.discardSnapshot();
              }
            });
          }
        };

    HoodieValidationException raised =
        assertThrows(HoodieValidationException.class, validator::run);
    assertTrue(raised.getMessage().contains("the FS-MDT comparison was not run and no findings "
            + "were written for it"),
        "the message has to say which dimension went unvalidated, which is the fact an operator "
            + "acts on: " + raised.getMessage());
    assertTrue(validator.getRunReports().isPresent(),
        "and the skip's handle survives the throw, so a caller that catches it still learns the run "
            + "was skipped rather than clean");
    assertFalse(validator.getRunReports().get().isUsable(),
        "a run in which nobody looked is not usable");
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "and no findings document was written");
  }

  /**
   * A duplicate-base-file finding survives a skipped FS-MDT comparison in fail-fast with
   * {@code --ignore-failed}: dimension A hands its failure to the driver, so the run must fail,
   * collect it, and report a validation failure, whether dimension B was skipped for pinning or
   * for archival.
   */
  @Test
  void failFastIgnoreFailedKeepsTheDuplicateFindingWhenPinningFails() throws Exception {
    writeTable("COPY_ON_WRITE");
    duplicateOf(anyFile(".parquet", false));

    HoodieMetadataTableValidatorV2 validator = unpinnableValidator(ValidationMode.FAIL_FAST, true,
        "run-unpinnable-duplicate");

    assertFalse(validator.run(), "dimension A proved a duplicate, so the run cannot pass");
    assertDuplicateFindingCollected(validator);
  }

  @Test
  void failFastIgnoreFailedKeepsTheDuplicateFindingWhenArchivalRan() throws Exception {
    writeTable("COPY_ON_WRITE", V6);
    duplicateOf(anyFile(".parquet", false));

    HoodieMetadataTableValidator.Config config = validatorConfig(ValidationMode.FAIL_FAST, true, "",
        "run-archived-duplicate", true, true);
    HoodieMetadataTableValidatorV2 validator =
        new HoodieMetadataTableValidatorV2(jsc, config, new Properties());
    validator.setPhaseObserver(new HoodieMetadataTableValidatorV2.PhaseObserver() {
      @Override
      public void beforePostPhaseOneTimelineRead() {
        try {
          archiveTimeline(writeOptions("COPY_ON_WRITE", V6));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    });

    assertFalse(validator.run(), "dimension A proved a duplicate, so the run cannot pass");
    assertDuplicateFindingCollected(validator);
  }

  /**
   * Exhaustive without {@code --ignore-failed} raises dimension A's failure rather than the pinning
   * message, so the operator reads "duplicates found", not "could not pin".
   */
  @Test
  void exhaustiveRaisesTheDuplicateFindingRatherThanThePinningFailure() throws Exception {
    writeTable("COPY_ON_WRITE");
    duplicateOf(anyFile(".parquet", false));

    HoodieMetadataTableValidatorV2 validator = unpinnableValidator(ValidationMode.EXHAUSTIVE, false,
        "run-unpinnable-duplicate-exhaustive");

    HoodieValidationException raised =
        assertThrows(HoodieValidationException.class, validator::run);
    assertTrue(raised.getMessage().startsWith("Validation of duplicate base files (v2) failed"),
        raised.getMessage());
    assertDuplicateFindingCollected(validator);
  }

  private static void assertDuplicateFindingCollected(HoodieMetadataTableValidatorV2 validator) {
    assertTrue(validator.hasValidationFailure(), "the duplicate finding is a validation failure");
    List<Throwable> throwables = validator.getThrowables();
    assertEquals(1, throwables.size(),
        "one failure per failing dimension, and only dimension A failed: " + throwables);
    assertTrue(throwables.get(0).getMessage().startsWith(
        "Validation of duplicate base files (v2) failed"), throwables.get(0).getMessage());
  }

  /** A v2 validator whose metadata snapshot never pins, as on a continuously written table. */
  private HoodieMetadataTableValidatorV2 unpinnableValidator(ValidationMode mode,
                                                             boolean ignoreFailed, String runId) {
    HoodieMetadataTableValidator.Config config =
        validatorConfig(mode, ignoreFailed, "", runId, true, true);
    return new HoodieMetadataTableValidatorV2(jsc, config, new Properties()) {
      @Override
      protected PinResult pinMetadataSnapshot(SnapshotAttempt attempt) throws IOException {
        return super.pinMetadataSnapshot(new SnapshotAttempt() {
          @Override
          public HoodieTimeline buildSnapshot() throws IOException {
            return attempt.buildSnapshot();
          }

          @Override
          public HoodieTimeline reReadTimeline() {
            return attempt.reReadTimeline().filter(instant -> false);
          }

          @Override
          public void discardSnapshot() throws IOException {
            attempt.discardSnapshot();
          }
        });
      }
    };
  }

  /** A log file the metadata table lists and storage no longer has. */
  @Test
  void logFileDeletedFromStorageIsReportedAgainstTheMetadataTable() throws Exception {
    writeTable("MERGE_ON_READ");
    StoragePath victim = anyFile(".log.", true);
    assertTrue(storage.deleteFile(victim), "precondition: the log file must actually be removed");

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE));

    JsonNode record = recordFor(readDiff(), victim.getName());
    assertEquals("MDT_CORRUPTION", record.get("verdict").asText(),
        "the metadata table names a log file storage does not hold and no clean or rollback "
            + "explains it");
    assertEquals("MDT_EXCLUSIVE", record.get("diffPattern").asText());
    assertEquals("MDT_EXCLUSIVE_LOG_FILE", record.get("sourcePattern").asText());
    assertEquals("NOT_APPLICABLE", record.get("instantSource").asText(),
        "a file that is not on storage cannot be opened to read an instant out of");
    assertTrue(record.get("writingInstant").isNull(),
        "an MDT-exclusive log file is unattributable on every table version: " + record);
    assertEquals("MDT_CORRUPTION",
        recordFor(readDiffAdjudicated(), victim.getName()).get("verdict").asText());
  }

  /** Same file name on both sides, different length: nothing is missing, the record is wrong. */
  @Test
  void baseFileWhoseLengthDivergedIsAnAttributeMismatch() throws Exception {
    writeTable("COPY_ON_WRITE");
    StoragePath victim = anyFile(".parquet", false);
    long originalLength = storage.getPathInfo(victim).getLength();
    appendBytes(victim, 1024);

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE));

    JsonNode record = recordFor(readDiff(), victim.getName());
    assertEquals("MDT_CORRUPTION", record.get("verdict").asText(),
        "both sides hold the file and disagree about it; no timeline state can explain that away");
    assertEquals("BOTH_DISAGREE", record.get("diffPattern").asText());
    assertEquals("BASE_FILE_ATTRIBUTE_MISMATCH", record.get("sourcePattern").asText());
    assertEquals(originalLength, record.get("mdtSizeBytes").asLong());
    assertEquals(originalLength + 1024, record.get("fsSizeBytes").asLong());
  }

  /**
   * A file group on storage the metadata table has never heard of, under a completed commit.
   *
   * <p>The commit completed before {@code T_prev}, records this partition, and records nothing for
   * this file group — a commit speaking about the partition and declining to claim a file whose own
   * name says that very instant wrote it. That is positive proof, so the verdict is
   * {@code DANGLING} rather than a refusal.
   */
  @Test
  void fileGroupOnlyOnStorageIsReportedWithItsInstant() throws Exception {
    writeTable("COPY_ON_WRITE");
    StoragePath original = anyFile(".parquet", false);
    String[] parts = original.getName().split("_");
    StoragePath stranger = new StoragePath(original.getParent(),
        "00000000-1111-2222-3333-444444444444_" + parts[1] + "_" + parts[2]);
    copyFile(original, stranger);

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE));

    JsonNode record = recordFor(readDiff(), stranger.getName());
    assertEquals("DANGLING", record.get("verdict").asText(),
        "the commit completed before T_prev, was read, and does not name this file");
    assertEquals("FS_EXCLUSIVE", record.get("diffPattern").asText());
    assertEquals("FS_EXCLUSIVE_FILE_GROUP", record.get("sourcePattern").asText());
    assertEquals("FILE_NAME", record.get("instantSource").asText());
    assertEquals(parts[2].split("\\.")[0], record.get("writingInstant").asText());
    assertEquals("00000000-1111-2222-3333-444444444444", record.get("fileGroupId").asText());
  }

  /**
   * <b>Fail-fast at the product default, failing on an FS-MDT verdict rather than on a duplicate
   * base file.</b> The distinction matters because the two leave by different doors: a duplicate set
   * is decided on the executor and throws from there, while every FS-MDT verdict is decided on the
   * driver in phase 2 and is raised by {@code doLatestFileSliceValidation} itself. That driver-side
   * throw is reachable in fail-fast, because fail-fast has no executor-side comparison to throw on
   * its behalf, and nothing else exercises it: every other fail-fast case here either leaves
   * {@code --ignore-failed} at the harness default of {@code true} and takes the return-false path,
   * or damages the table in a way dimension A catches first.
   *
   * <p>And it still persists nothing. The reports are exhaustive's alone, and the throw happens
   * <em>after</em> phase 2 has decided rather than before anything was computed — so "fail-fast
   * writes nothing" has to hold on a path where the driver did all of the deciding.
   */
  @Test
  void failFastThrowsOnTheFirstFatalVerdictAndPersistsNothing() throws Exception {
    writeTable("COPY_ON_WRITE");
    assertTrue(storage.deleteFile(anyFile(".parquet", false)),
        "precondition: the base file must actually be removed");

    assertThrows(HoodieValidationException.class,
        () -> runValidator(ValidationMode.FAIL_FAST, false),
        "a file the metadata table names and storage no longer has is OUTBOUND_DELETION, and "
            + "--ignore-failed=false asks for it to be raised");

    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "fail-fast persists nothing, including on the path where the driver decided the verdict");
  }

  /**
   * The mode is what decides whether anything is written. Fail-fast on the same damaged table
   * fails identically and leaves storage untouched.
   */
  @Test
  void failFastModeFailsTheSameWayAndWritesNothing() throws Exception {
    writeTable("COPY_ON_WRITE");
    assertTrue(storage.deleteFile(anyFile(".parquet", false)));

    assertFalse(runValidator(ValidationMode.FAIL_FAST));

    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "fail-fast must stay exactly as it was");
  }

  /**
   * The whole dangling dimension, end to end: a slice with two base files is invisible to the slice
   * comparison — the file system view collapses it to one — so it produces no
   * {@code files_mismatch} finding at all, and the only reason the run fails is the cardinality
   * invariant the detector reads straight off the listing.
   *
   * <p>Then the driver adjudicates: the commit that wrote the slice names the original and not the
   * copy, so the copy is proven dangling and the original is dropped. That asymmetry is the point
   * of the adjudicated report — two suspects in, one deletable file out — and it is a fact about
   * the timeline, which the executors never consulted.
   *
   * <p><b>Also the sole "both dimensions in one shot" case</b>, and where the whole report layout is
   * asserted: four directories, each with its own completion marker, and the header fields a
   * consumer reads before it acts. Folded in from the now-deleted
   * {@code exhaustiveWritesFourDirectoriesWithAVerdictOnEveryDiffRecord}, which asserted the
   * identical layout on a two-file-group fixture (a duplicate in one group, an independent deletion
   * in another) — a complication this test does not need: the SAME file being visible to both
   * dimensions at once is a stronger proof that each one reports its own finding, not a weaker one,
   * and this dimension's own suspect/adjudicated pair already establishes the attribution above.
   * {@code diff/}'s total record count is deliberately not asserted for the reason given below.
   */
  @Test
  void duplicateBaseFileFailsTheRunAndOnlyTheCopyIsAdjudicatedDangling() throws Exception {
    writeTable("COPY_ON_WRITE");
    StoragePath original = anyFile(".parquet", false);
    StoragePath duplicate = duplicateOf(original);

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE),
        "one base file per slice is an invariant; two fails the run on cardinality alone");

    // readDiff/readDiffAdjudicated/readSuspects/readAdjudicated each assert their own directory's
    // _COMPLETE marker, so reading all four is also the assertion that all four directories were
    // written and finished.
    JsonNode diff = readDiff();
    JsonNode diffAdjudicated = readDiffAdjudicated();
    for (JsonNode record : diff.get("records")) {
      assertNotNull(record.get("verdict"), "every diff/ record carries a verdict: " + record);
      assertFalse(record.get("verdict").asText().isEmpty(), record.toString());
    }
    assertTrue(diff.get("filesSeen").asLong() > 0,
        "filesSeen is the blast-radius denominator; a zero silently disables it: " + diff);
    assertEquals(0, diff.get("unmatchedStateCount").asLong(),
        "a non-zero count is a gap in the adjudication table, not a fact about this table: " + diff);
    assertNotNull(diffAdjudicated.get("recordCount"),
        "the fs_mdt_diff dimension's own conclusion must be a well-formed document too: "
            + diffAdjudicated);

    JsonNode suspects = readSuspects();
    assertEquals(2, suspects.get("recordCount").asLong(),
        "every member of the set is a suspect; no timeline was consulted, so no keeper is known: "
            + suspects);
    assertEquals(2, suspects.get("failingRecordCount").asLong());
    assertEquals(2, suspects.get("counts").get("DANGLING_FILE_SUSPECTS").asLong());
    JsonNode suspect = recordFor(suspects, duplicate.getName());
    assertEquals("DUPLICATE_BASE_FILE", suspect.get("suspectReason").asText());
    assertEquals(2, suspect.get("duplicateSetSize").asInt());
    assertEquals("FS", suspect.get("side").asText());

    // Exactly one of the two is deletable, and WHICH one is a fact about the commit rather than
    // about the listing: the commit recorded the original's path and not the copy's.
    //
    // The total record count is deliberately not asserted. Which member of a duplicate set the file
    // system view calls "the" base file of the slice depends on listing order, so the slice
    // comparison sometimes also reports the other member as MDT-exclusive and sometimes finds
    // nothing at all -- the ambiguity that makes a duplicate set worth failing a run over. That
    // record is MDT-side, is never adjudicated, and is carried across untouched when it appears.
    JsonNode adjudicated = readAdjudicated();
    assertEquals(1, adjudicated.get("counts").get("DANGLING_FILES").asLong(),
        "one member of the set is deletable, not both and not neither: " + adjudicated);
    JsonNode proven = recordFor(adjudicated, duplicate.getName());
    assertEquals("DANGLING_FILES", proven.get("pattern").asText(),
        "the file the commit does NOT name is the dangling one");
    assertEquals("DUPLICATE_BASE_FILE", proven.get("suspectReason").asText());
    assertEquals("FS", proven.get("side").asText());
    for (JsonNode record : adjudicated.get("records")) {
      assertFalse(original.getName().equals(record.get("fileName").asText())
              && "DANGLING_FILES".equals(record.get("pattern").asText()),
          "the keeper of a duplicate set must never be condemned: " + record);
    }
    // A suspicion must never reach the report a mitigation job acts on -- neither as a record nor
    // as a count. Asserted on the fields a consumer dispatches on rather than on the document's
    // text: the promoted record's detail still NAMES the category it came out of, which is exactly
    // what someone auditing a deletion wants to read.
    assertNull(adjudicated.get("counts").get("DANGLING_FILE_SUSPECTS"),
        "no suspect count survives adjudication: " + adjudicated.get("counts"));
    for (JsonNode record : adjudicated.get("records")) {
      assertFalse("DANGLING_FILE_SUSPECTS".equals(record.get("pattern").asText()),
          "an unproven category reaches the adjudicated report by no route: " + record);
    }
  }

  /**
   * <b>Rewritten, not carried green.</b> The fail-fast twin of
   * {@link #uncommittedLogFileOnStorageIsDanglingAndFailsTheRun}: the same retry-orphan log file
   * reaches the same {@code DANGLING} verdict in the mode that persists nothing, and the exception
   * {@code --ignore-failed=false} raises is the only record such a run leaves — so it is what has to
   * name the verdict.
   *
   * <p>The verdict can only be reached at all if the driver <em>opened</em> the file: a log file's
   * name carries its slice's base commit below table version 8, so the instant the verdict rests on
   * comes from the first log block's header and from nowhere else. Hand
   * {@code LogFileReader.noLogFilesToRead()} to the phase-1 attribution instead and the record
   * is dropped with no instant, the verdict becomes {@code NOT_SETTLED}, and this run passes.
   *
   * <p>Fail-fast also reads no archive, so this pins that the active timeline alone answers the
   * membership question for an instant it holds.
   */
  @Test
  void logFileNoCommitNamesIsDanglingInFailFastAndPersistsNothing() throws Exception {
    writeTable("MERGE_ON_READ");
    StoragePath original = anyFile(".log.", true);
    StoragePath copy = withLogVersion(original, 9);
    copyFile(original, copy);

    HoodieValidationException raised = assertThrows(HoodieValidationException.class,
        () -> runValidator(ValidationMode.FAIL_FAST, false),
        "the retry-orphan tolerance is gone in both modes, not only in the one that reports");

    assertTrue(raised.getMessage().contains("{DANGLING=1}"),
        "the exception is the whole record a fail-fast run leaves, so it has to carry the verdict: "
            + raised.getMessage());
    assertTrue(raised.getMessage().contains(copy.getParent().getName()),
        "and the partition the operator has to go and look at: " + raised.getMessage());
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "fail-fast persists nothing, on this path as on every other");
  }

  /**
   * A healthy MOR table's log files are all suspects — nothing has been checked against a timeline
   * — and none of them fails the run. The alternative fails every MOR table in a fleet the moment
   * exhaustive mode is switched on.
   */
  @Disabled("Log files are no longer part of the duplicate-base-file dimension at all. The "
      + "detector that dimension uses is DuplicateBaseFileDetector, which reports duplicate base "
      + "files and nothing else; the log-file branch that emitted one UNREFERENCED_LOG_FILE suspect "
      + "per log file in the listing has been removed, not switched off. This case asserts a "
      + "healthy MOR table produces recordCount > 0 from that population, which is now empty by "
      + "design. Whether a log file is referenced is the FS-MDT comparison's question and is "
      + "covered end to end by TestValidatorV2Reconciliation at both table versions.")
  @Test
  void healthyMorTableRecordsItsLogFilesAsSuspectsWithoutFailing() throws Exception {
    writeTable("MERGE_ON_READ");

    assertTrue(runValidator(ValidationMode.EXHAUSTIVE), "a healthy MOR table must still pass");

    JsonNode suspects = readSuspects();
    assertTrue(suspects.get("recordCount").asLong() > 0, "every log file is a suspect");
    assertEquals(0, suspects.get("failingRecordCount").asLong(),
        "a log-file suspect is recorded and tolerated; only a duplicate set fails the run");
    for (JsonNode record : suspects.get("records")) {
      assertEquals("UNREFERENCED_LOG_FILE", record.get("suspectReason").asText());
    }
  }

  /**
   * A duplicate base file fails the run <b>on its own</b>, with nothing else wrong with the table.
   *
   * <p>The set is planted in a <em>superseded</em> slice, which is what makes this deterministic
   * and what makes it worth a test of its own. The comparison this validator performs looks only at
   * latest file slices, so it provably finds nothing here — asserted, by the FS-MDT diff carrying
   * no record at all — and the only thing that can have failed the run is the cardinality
   * invariant the detector read off the listing.
   *
   * <p>Planting it in a latest slice would not prove that. A file slice holding two base files has
   * no defined "the" base file, so the file system view picks one by listing order; when it picks
   * the copy, the slice comparison fails the partition by itself and a run that failed for that
   * reason is indistinguishable from one that failed for this one.
   */
  @Test
  void duplicateInASupersededSliceFailsTheRunOnItsOwn() throws Exception {
    writeCowTableWithASupersededSlice();
    duplicateOf(anySupersededBaseFile());

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE),
        "one base file per slice is an invariant, and it is broken whatever else the table is like");

    assertEquals(0, readDiff().get("recordCount").asLong(),
        "precondition: the slice comparison must have found nothing, so the duplicate set is the "
            + "only thing that can have failed this run: " + readDiff());
    assertEquals(2, readSuspects().get("failingRecordCount").asLong(),
        "both members of the set are recorded, and both fail the run");
  }

  /**
   * Fail-fast finds the same duplicate set and fails on it, and leaves nothing behind. The
   * logged suspect set is the whole record such a run produces.
   *
   * <p>Superseded slice again, for the same reason: the failure has to be attributable.
   */
  @Test
  void failFastFailsOnADuplicateBaseFileAndStillWritesNothing() throws Exception {
    writeCowTableWithASupersededSlice();
    duplicateOf(anySupersededBaseFile());

    assertFalse(runValidator(ValidationMode.FAIL_FAST),
        "cardinality is decided from the listing alone, so fail-fast fails on it too");

    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "fail-fast persists nothing at all, suspects included");
  }

  /**
   * <b>At the product default</b>, {@code --ignore-failed} is {@code false}: {@code classifyBatch}
   * throws on the executor and the driver never receives a record. The suspects therefore have to be
   * written down on the executor, before that throw, or a fail-fast run leaves nothing behind but
   * the exception text — which carries one file name and no others, and since fail-fast persists
   * nothing, this log is the only record such a run produces.
   *
   * <p>Every other fail-fast test here sets {@code ignoreFailed = true} to reach the driver, so
   * this is the only one that exercises the configuration operators actually run.
   */
  @Test
  void failFastAtTheProductDefaultLogsEveryMemberOfTheSuspectSet() throws Exception {
    writeCowTableWithASupersededSlice();
    StoragePath keeper = anySupersededBaseFile();
    StoragePath duplicate = duplicateOf(keeper);

    List<String> logged = runCapturingValidatorLogs(
        () -> assertThrows(HoodieValidationException.class,
            () -> runValidator(ValidationMode.FAIL_FAST, false)));

    List<String> suspectLines = suspectLinesIn(logged);
    // EXACTLY one line per member -- not "at least", and not distinct lines. The count is the
    // assertion: a task collects its sample of survivors and logs each one ONCE before throwing, and
    // an implementation that logged per survivor as it found one AND again over the collected sample
    // would report the whole set twice. Two findings and four lines is what an operator would then
    // have had to reconcile by eye, on the one path where these lines are all a fail-fast run
    // leaves behind.
    assertEquals(2, suspectLines.size(),
        "one line per member of the set, reported once: " + logged);
    assertEquals(1, logged.stream()
            .filter(line -> line.contains("--validation-mode exhaustive")).count(),
        "the closing advice is doubled by the same fault, and is the cheapest place to see it: "
            + logged);
    for (StoragePath member : Arrays.asList(keeper, duplicate)) {
      assertTrue(namesAFile(suspectLines, member.getName()),
          "an operator has to be able to go and look at " + member.getName() + "; got: "
              + suspectLines);
    }
    String first = suspectLines.get(0);
    // The logged partition is the table-relative path, so it is checked against the directory the
    // files actually live in rather than restated.
    String loggedPartition = first.replaceAll(".*partition=(\\S+).*", "$1");
    assertTrue(keeper.getParent().toString().endsWith(loggedPartition),
        "partition=" + loggedPartition + " does not name " + keeper.getParent());
    assertTrue(first.contains("fileGroup=" + keeper.getName().split("_")[0]), first);
    assertTrue(first.contains("reason=DUPLICATE_BASE_FILE"), first);
    assertTrue(logged.stream().anyMatch(line -> line.contains("--validation-mode exhaustive")),
        "the closing line must tell the operator where a machine-readable list comes from");

    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "fail-fast still persists nothing, on this path as on the other");
  }

  /**
   * <b>Exhaustive mode collects unconditionally</b>, so the reports survive a failing
   * partition even at the product default.
   *
   * <p>Throwing on the executor would kill the task and the whole {@code collect()}, and with it
   * every result the task had already finished — so the run that has something to mitigate would be
   * exactly the run whose adjudicated report never got written, and that report is the mitigation
   * job's only input. The verdict is unchanged: the same exception is raised, after the reports are
   * on storage.
   *
   * <p>A single FS-MDT finding is sufficient to prove this point — the reports-survive-the-throw
   * behaviour is a property of exhaustive mode's own collection, not of how many dimensions found
   * something — so this damages only the FS-MDT side and runs with the duplicate-base-file
   * dimension switched off explicitly, rather than reaching for a second, unrelated file group.
   */
  @Test
  void exhaustiveAtTheProductDefaultStillWritesItsReportsBeforeFailing() throws Exception {
    writeTable("COPY_ON_WRITE");
    StoragePath victim = anyFile(".parquet", false);
    assertTrue(storage.deleteFile(victim), "precondition: the base file must actually be removed");

    assertThrows(HoodieValidationException.class,
        () -> runValidator(ValidationMode.EXHAUSTIVE, false, "", "", false, true),
        "the verdict is the one --ignore-failed=false asks for; only its timing moved");

    assertEquals("OUTBOUND_DELETION",
        recordFor(readDiff(), victim.getName()).get("verdict").asText(),
        "the FS-MDT findings must survive the failure that --ignore-failed raises");
  }

  /**
   * A raw report that was expected and could not be written stops the reconciliation.
   *
   * <p>An absent report and a failed write are indistinguishable from the returned directory alone,
   * and treating a failed write as "nothing to write" is not a smaller answer but a less safe one: an
   * adjudicated document written from an incomplete set of raw reports is a document nobody can
   * re-derive, because there is no way to tell afterward which raw report was missing or what it
   * would have said. Lose any one of the raw reports to a storage fault and reconciliation must
   * decline rather than adjudicate from whatever survived. The verdict stays untouched either way.
   */
  @Test
  void rawReportThatCouldNotBeWrittenStopsTheReconciliation() throws Exception {
    writeCowTableWithASupersededSlice();
    duplicateOf(anySupersededBaseFile());
    // An ordinary file where the output root has to be a directory: every write under it throws,
    // which is a permissions error's shape without needing one.
    StoragePath blockedRoot = new StoragePath(basePath + "-reports");
    try (OutputStream out = storage.create(blockedRoot, true)) {
      out.write("not a directory".getBytes(StandardCharsets.UTF_8));
    }

    List<String> logged = runCapturingValidatorLogs(() -> assertFalse(
        runValidator(ValidationMode.EXHAUSTIVE, true, blockedRoot.toString()),
        "the run's verdict is decided by the table, never by whether its reports could be saved"));

    assertTrue(logged.stream().anyMatch(line -> line.startsWith(
        "Not reconciling the exhaustive findings")),
        "reconciliation must refuse rather than adjudicate from whatever survived: " + logged);
    assertFalse(storage.exists(new StoragePath(blockedRoot, OUTPUT_DIR)));
  }

  /**
   * The regression this task exists to prevent: a run whose reconciliation was refused must never
   * read as a clean run through {@link HoodieMetadataTableValidatorV2#getRunReports()}. Both leave
   * {@code adjudicated/} absent, and a downstream mitigation job that deletes files acts on the
   * difference between them -- act on nothing, or act on whatever survived a storage fault.
   *
   * <p>Same unwritable-output-root mechanism as
   * {@link #rawReportThatCouldNotBeWrittenStopsTheReconciliation} -- a file occupies the directory
   * position, so every write under it throws and {@code persist(...)} records the failure -- driven
   * through the v2 validator directly (as {@link #runReportsNameEveryDirectoryTheRunWrote} is)
   * because the {@link HoodieMetadataTableValidator} wrapper {@link #runValidator} uses never hands
   * its v2 instance back. The "Not reconciling" log assertion is the precondition that pins the run
   * to the refused branch specifically -- not to some other reason {@code getRunReports()} might
   * come back empty or clean -- before the handle itself is asserted on.
   */
  @Test
  void reportWriteFailureIsReportedAsRefusedNotAsClean() throws Exception {
    writeCowTableWithASupersededSlice();
    duplicateOf(anySupersededBaseFile());
    StoragePath blockedRoot = new StoragePath(basePath + "-refused-reports");
    try (OutputStream out = storage.create(blockedRoot, true)) {
      out.write("not a directory".getBytes(StandardCharsets.UTF_8));
    }

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.useValidatorV2 = true;
    config.ignoreFailed = true;
    config.validationMode = ValidationMode.EXHAUSTIVE.getFlagValue();
    config.validationOutputRoot = blockedRoot.toString();
    config.validationRunId = "run-refused";
    config.fsViewMemoryFraction = 0.05;
    HoodieMetadataTableValidatorV2 validator =
        new HoodieMetadataTableValidatorV2(jsc, config, new Properties());

    List<String> logged = runCapturingValidatorLogs(() -> assertFalse(validator.run(),
        "the run's verdict is decided by the table, never by whether its reports could be saved"));

    // Precondition: the run must actually have reached writeFindings and refused there -- not
    // merely found nothing to adjudicate, which would make the assertions below vacuous.
    assertTrue(logged.stream().anyMatch(line -> line.startsWith(
        "Not reconciling the exhaustive findings")),
        "precondition: reconciliation must have actually refused, not merely found nothing to do: "
            + logged);

    ValidationRunReports reports = validator.getRunReports().get();
    assertEquals("run-refused", reports.getRunId());
    assertFalse(reports.isUsable(),
        "a report write failed, so reconciliation declined and a mitigation job must never act on "
            + "a run whose reconciliation was refused");
    assertFalse(reports.getAdjudicatedDir().isPresent());
  }

  /**
   * The third way {@code adjudicated/} can be absent, and the one the run handle must not get
   * wrong: both raw reports were written, and then the reconciliation pass <em>threw</em>.
   *
   * <p>Distinct from {@link #reportWriteFailureIsReportedAsRefusedNotAsClean}, where a raw write
   * fails and the pass declines before it starts. Here the pass runs, has real findings to
   * reconcile, and dies writing its answer — which leaves precisely the storage shape of a run that
   * had nothing to adjudicate. A consumer reading only {@code getAdjudicatedDir()} is then told the
   * table is clean while the findings sit on storage unread, so the handle itself has to carry the
   * difference.
   *
   * <p>The failure is aimed at the adjudicated write specifically by occupying its directory
   * position with a <em>file</em>: {@code fs_mdt_diff/} and {@code dangling_files/} are written
   * first and are unaffected, and only {@code MismatchReportWriter.writeDocument}'s
   * {@code createDirectory} for {@code adjudicated/} lands on it. Same technique as the refused
   * test, one directory deeper.
   *
   * <p><b>The damage is a duplicate base file, and it has to be.</b> The reconciliation pass
   * adjudicates the duplicate-base-file dimension alone since the dimensions were split, so a table
   * whose only damage the FS-MDT diff finds produces no suspects, the pass short-circuits before it
   * starts, and the guard this test exists for never runs — the run would reach an empty
   * {@code adjudicated/} by the innocent route and every assertion below would be vacuous. The
   * superseded slice is what keeps the two dimensions apart: the diff provably never looks at it.
   */
  @Test
  void reconciliationThatThrewIsNotReportedAsACleanRun() throws Exception {
    writeCowTableWithASupersededSlice();
    duplicateOf(anySupersededBaseFile());

    String runId = "run-adjudication-failed";
    StoragePath runDir = new StoragePath(new StoragePath(basePath, OUTPUT_DIR), runId);
    try (OutputStream out =
             storage.create(new StoragePath(runDir, DanglingReportWriter.ADJUDICATED_DIR), true)) {
      out.write("not a directory".getBytes(StandardCharsets.UTF_8));
    }

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.useValidatorV2 = true;
    config.ignoreFailed = true;
    config.validationMode = ValidationMode.EXHAUSTIVE.getFlagValue();
    config.validationOutputRoot = "";
    config.validationRunId = runId;
    config.fsViewMemoryFraction = 0.05;
    HoodieMetadataTableValidatorV2 validator =
        new HoodieMetadataTableValidatorV2(jsc, config, new Properties());

    List<String> logged = runCapturingValidatorLogs(() -> assertFalse(validator.run(),
        "the run's verdict is decided by the table, never by whether its reports could be saved"));

    // Preconditions. The raw report must have been written -- otherwise this is the refused case
    // wearing a different hat -- and the reconciliation must actually have thrown rather than merely
    // found nothing to do, which would make the assertions below vacuous.
    assertTrue(storage.exists(new StoragePath(runDir, DanglingReportWriter.SUSPECTS_DIR)),
        "precondition: the raw suspects -- the pass's only input -- must have been written before "
            + "it threw, or this is the refused case wearing a different hat");
    assertTrue(logged.stream().anyMatch(line -> line.startsWith(
        "Could not reconcile the exhaustive findings")),
        "precondition: the reconciliation must have thrown, not merely found nothing: " + logged);
    assertFalse(logged.stream().anyMatch(line -> line.startsWith(
        "Not reconciling the exhaustive findings")),
        "precondition: no raw write may have failed, or this is the refused case: " + logged);

    ValidationRunReports reports = validator.getRunReports().get();
    assertEquals(runId, reports.getRunId());
    assertFalse(reports.getAdjudicatedDir().isPresent());
    assertFalse(reports.isUsable(),
        "a reconciliation that threw reached no conclusion, so its run must never read as clean");
  }

  /**
   * <b>Each dimension is a complete run on its own.</b> With the FS-MDT comparison switched off, a
   * table whose only damage is a duplicate base file still fails, still writes {@code
   * dangling_files/} and {@code adjudicated/}, and writes no {@code fs_mdt_diff/} at all -- because
   * nothing looked, which is a different fact from looking and finding nothing.
   */
  @Test
  void duplicateOnlyRunWritesDimensionAsDocumentsAndNoDiff() throws Exception {
    writeTable("COPY_ON_WRITE");
    StoragePath duplicate = duplicateOf(anyFile(".parquet", false));

    // The trailing false is the one thing under test; the base class's overload sets every other
    // field, so there is a single place a run is configured. validateLatestFileSlices stays true in
    // there, which is what pins down that this dimension is switched off by v2's own flag and not
    // by v1's.
    assertFalse(runValidator(ValidationMode.EXHAUSTIVE, true, "", "", true, false),
        "a duplicate base file fails the run whether or not the slice comparison also ran");

    // The dimension that did run produced its whole conclusion: the raw suspects and, from them,
    // the adjudicated document that names the copy the commit does not reference.
    assertEquals("DUPLICATE_BASE_FILE",
        recordFor(readSuspects(), duplicate.getName()).get("suspectReason").asText());
    assertEquals("DANGLING_FILES",
        recordFor(readAdjudicated(), duplicate.getName()).get("pattern").asText(),
        "one dimension alone still reaches the mitigation job's only input");

    // And the dimension that did not run left nothing at all. An empty fs_mdt_diff/ would say the
    // comparison ran and found nothing, which is a claim about the table this run never made.
    assertFalse(storage.exists(new StoragePath(runDir(), FileDiffReportWriter.REPORT_DIR)),
        "nothing looked at file slices, so no fs_mdt_diff/ may exist to be read as having looked");
  }

  /**
   * With the FS-MDT comparison off, even a run that found nothing must not publish a handle that
   * vouches for the metadata table.
   */
  @Test
  void duplicateOnlyRunThatFoundNothingDoesNotReportTheMetadataTableAssessed() throws Exception {
    writeTable("COPY_ON_WRITE");

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.useValidatorV2 = true;
    config.ignoreFailed = true;
    config.validateFsMdtDiff = false;
    config.validationMode = ValidationMode.EXHAUSTIVE.getFlagValue();
    config.validationOutputRoot = "";
    config.validationRunId = "run-duplicate-only-clean";
    config.fsViewMemoryFraction = 0.05;

    HoodieMetadataTableValidatorV2 validator =
        new HoodieMetadataTableValidatorV2(jsc, config, new Properties());
    assertTrue(validator.run());

    ValidationRunReports reports = validator.getRunReports().get();
    assertTrue(reports.isUsable(), reports.toString());
    assertFalse(reports.getAdjudicatedDir().isPresent(), reports.toString());
    assertFalse(reports.isFsMdtDiffAssessed(), reports.toString());
  }
}
