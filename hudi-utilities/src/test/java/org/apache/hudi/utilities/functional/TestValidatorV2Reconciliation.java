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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormatWriter;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.utilities.util.FileDiffReportWriter;
import org.apache.hudi.utilities.util.InstantState;
import org.apache.hudi.utilities.util.InstantStateIndex;
import org.apache.hudi.utilities.util.LogFileInstantReader;
import org.apache.hudi.utilities.util.ServiceActivityDelta;
import org.apache.hudi.utilities.util.ValidationMode;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two-phase reconciliation end to end: what the FS-MDT diff <b>concludes</b> about a file, on
 * real Spark-written tables at the two table versions a fleet actually holds.
 *
 * <p>{@link TestValidatorV2ExhaustiveMode} damages a table one way at a time and reads the report
 * back; {@link TestValidatorV2DanglingFiles} does the same for the duplicate-base-file dimension.
 * This suite is about the <em>verdict</em> — a function of the state of a file's writing instant
 * before the run and after it, which is the only thing separating a metadata table that lost a
 * committed file from one that is legitimately behind a writer, and a fatal
 * {@code OUTBOUND_DELETION} from a routine clean.
 *
 * <p><b>Real timeline surgery, not hand-built instants.</b> Every pending service here is written
 * through {@link HoodieActiveTimeline}'s own transitions with a real plan behind it, so the naming,
 * the layout and the plan encoding are the writer's on both table versions rather than this test's
 * guess at them — which is the difference that made two defects visible only
 * against a real table.
 */
@Tag("functional")
public class TestValidatorV2Reconciliation extends ValidatorV2FunctionalTestBase {

  /**
   * <b>An in-flight write never fails a run, in either mode — and the two table versions reach that
   * by different routes.</b>
   *
   * <p>A file whose writing instant is inflight at both reads is a concurrent writer's half-finished
   * output. Failing a run on it would fail every table with a writer attached.
   *
   * <p>The instant is a <b>compaction</b>, and it has to be: both sides of the comparison are built
   * over {@code TimelineUtils.getVisibleTimelineForFsView}, which is completed instants <em>plus
   * compaction instants</em>, so a compaction is the one pending action whose output files a
   * version-6 view will show at all. An ordinary inflight commit's files are invisible to both
   * sides — the gap
   * {@link TestValidatorV2DanglingFiles#anOrphanInsideTheActiveWindowIsInvisibleToTheValidator}
   * pins from the other direction.
   *
   * <ul>
   *   <li><b>version 6</b>: {@code HoodieFileGroup.isFileSliceCommitted} gates on
   *       {@code containsOrBeforeTimelineStarts}, which the pending compaction instant satisfies, so
   *       the file is on the FS side, absent from the metadata table, and reaches the adjudicator —
   *       where {@code (prev, aft) = (INFLIGHT, INFLIGHT)} is the named {@code SANCTIONED} row;
   *   <li><b>versions 8 and 9</b>: {@code AbstractTableFileSystemView.filterUncommittedFiles:594-611}
   *       drops a base file whose instant the completion-time view does not call completed, so the
   *       file never reaches the diff and there is <b>no record to adjudicate</b>. <b>The
   *       {@code SANCTIONED} in-flight row is unreachable end to end on those versions</b> — pinned
   *       here rather than assumed, because a change that made such files visible would make the row
   *       live and this parameterisation is where that would first show.
   * </ul>
   *
   * <p>Fail-fast is asserted as well as exhaustive, and not merely for symmetry: fail-fast reads no
   * archive, so a rule that leaned on an archived read would come apart there and nowhere else.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 9})
  void inflightWriteNeverFailsARunInEitherMode(int tableVersion) throws Exception {
    writeTable("COPY_ON_WRITE", tableVersion);
    String inflight = plantInflightCompaction();
    StoragePath beingWritten = strangerUnderInstant(inflight);

    assertTrue(runValidator(ValidationMode.FAIL_FAST),
        "a concurrent writer's half-written output must not fail a run at table version "
            + tableVersion);
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "and fail-fast persists nothing, whatever it concluded");

    assertTrue(runValidator(ValidationMode.EXHAUSTIVE),
        "nor in the mode that reports everything: " + beingWritten);

    if (tableVersion < 8) {
      JsonNode record = recordFor(readDiff(), beingWritten.getName());
      assertEquals("SANCTIONED", record.get("verdict").asText(),
          "an in-flight write, explained by the writing instant's own state: " + record);
      assertEquals("FS_EXCLUSIVE", record.get("diffPattern").asText());
      assertEquals(inflight, record.get("writingInstant").asText());
      assertNull(recordOrNull(readDiffAdjudicated(), beingWritten.getName()),
          "SANCTIONED is the one verdict the actionable document excludes: "
              + readDiffAdjudicated());
    } else {
      assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
          "GAP, pinned rather than assumed: on table version 8 and above filterUncommittedFiles "
              + "drops an uncommitted base file before the comparison sees it, so an in-flight "
              + "write produces no record of any kind and the SANCTIONED row is unreachable. If "
              + "you are reading this because a report now exists, that row has become live -- "
              + "assert SANCTIONED on it rather than restoring the invisibility");
    }
  }

  /**
   * <b>The genuine retry-orphan log file — same file id, same delta commit, same log version, the
   * write token's attempt id incremented — is {@code DANGLING} and fails the run, in both modes and
   * at both table versions.</b>
   *
   * <p>This is the shape a retried Spark task actually leaves: {@code HoodieAppendHandle} names its
   * output {@code .<fileId>_<deltaCommit>.log.<version>_<taskPartitionId>-<stageId>-<taskAttemptId>}
   * ({@code FSUtils.makeLogFileName:427-433}, {@code FSUtils.makeWriteToken:116-118}), a retry
   * re-runs the same task with the next attempt id, and the commit records only the attempt that
   * won. The copy here is built from the original by {@link #nextAttemptOf} rather than by
   * substituting a literal, so a change to the naming scheme breaks this fixture instead of
   * quietly making it test something else.
   *
   * <p><b>Both members of the pair are visible; they do not collapse.</b>
   * {@code HoodieFileGroup.addBaseFile:110-133} deliberately folds two base files of one slice down
   * to one, and {@code addLogFile:140-145} has no such fold — it forwards straight to
   * {@code FileSlice.addLogFile}, whose {@code TreeSet} is ordered by
   * {@code HoodieLogFile.LogFileComparator:222-247}, which tie-breaks on the <em>write token</em>
   * after the delta commit and the log version. So two log files differing only in write token are
   * two distinct members of one slice, the metadata table's slice names only one of them, and the
   * other is {@code FS_EXCLUSIVE_LOG_FILE}. For an orphan at a new log <em>version</em>, see
   * {@link #orphanLogFileAtALogVersionNoCommitWroteIsDangling}.
   *
   * <p>Such files are not tolerated, because tolerating them would be a <em>window</em> rather
   * than a verdict — once the writing instant archives,
   * {@code HoodieFileGroup.isFileSliceCommitted:182-188} on version 6 and
   * {@code filterUncommittedFiles:594-611} on version 8+ both reclassify the orphan as legitimate
   * data by a pure timestamp comparison, so a file no commit will ever reference is silently
   * promoted into the table's data.
   *
   * <p><b>Version 6 is not a repeat of version 9 here.</b> Below table version 8 a log file's name
   * carries its slice's <em>base commit</em>; at and above it carries the delta commit that wrote
   * it. The verdict rests on the instant read out of the first log block's header, so on version 6
   * the instant the run condemns the file on is provably not the one in its name — asserted,
   * because a reader that trusted the name would ask about the wrong instant on exactly one of these
   * two tables and would be right on the other. The retry copy is a byte copy, so its first block's
   * header carries the same instant the original's does: the header is the only thing that can
   * distinguish them from their names, and it does not.
   *
   * <p>The negative twin is
   * {@link #retryShapedLogFileUnderAnInflightDeltaCommitNeverFailsARun}: the identical shape whose
   * writing delta commit has not completed is a live write, and must not fail a run.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 9})
  void retryOrphanLogFileUnderACompletedCommitIsDangling(int tableVersion) throws Exception {
    writeTable("MERGE_ON_READ", tableVersion);
    StoragePath original = anyFile(".log.", true);
    StoragePath retry = retryOf(original);

    assertThrows(HoodieValidationException.class,
        () -> runValidator(ValidationMode.FAIL_FAST, false),
        "the tolerance is gone in fail-fast too, at table version " + tableVersion);
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "fail-fast persists nothing, including on the path where the driver decided the verdict");

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE),
        "and it is the same verdict in the mode that reports it");

    JsonNode diff = readDiff();
    JsonNode record = recordFor(diff, retry.getName());
    assertEquals("DANGLING", record.get("verdict").asText(),
        "the commit completed before T_prev, was read, and does not name this file: " + record);
    assertEquals("FS_EXCLUSIVE_LOG_FILE", record.get("sourcePattern").asText(),
        "the FS-exclusive log-file pattern, not a whole-slice or whole-group one: the retry sits "
            + "inside a slice the metadata table does know about");
    assertEquals("FS_EXCLUSIVE", record.get("diffPattern").asText());
    assertEquals("LOG", record.get("fileType").asText());
    assertEquals("LOG_BLOCK_HEADER", record.get("instantSource").asText(),
        "the instant comes from the first block's header, never from the file name");

    String instantInTheName = new HoodieLogFile(retry).getDeltaCommitTime();
    String condemnedOn = record.get("writingInstant").asText();
    if (tableVersion < 8) {
      assertNotEquals(instantInTheName, condemnedOn,
          "below table version 8 a log file's name carries its slice's BASE commit, so a reader "
              + "that trusted the name would have asked about " + instantInTheName
              + " rather than about the delta commit that wrote the file");
    } else {
      assertEquals(instantInTheName, condemnedOn,
          "at table version 8 and above the name carries the writing instant, and the block header "
              + "has to agree with it rather than diverge");
    }
    assertEquals("DANGLING",
        recordFor(readDiffAdjudicated(), retry.getName()).get("verdict").asText());
    assertNull(recordOrNull(diff, original.getName()),
        "the log file the delta commit does name is not a difference at all");
    assertEquals(1, countOfVerdict(diff, "DANGLING"),
        () -> "exactly one file is condemned -- the losing attempt, never the winning one: " + diff);
  }

  /**
   * <b>The other FS-exclusive log-file shape: a log file at a log <em>version</em> no commit ever
   * wrote.</b> Same verdict, same pattern, different damage — what an interrupted rollover leaves
   * behind, where {@link #retryOrphanLogFileUnderACompletedCommitIsDangling} is what a retried task
   * leaves behind.
   *
   * <p><b>Kept as its own case rather than folded into the retry one because it is different
   * damage, not merely a different file name.</b> A retry orphan is a duplicate: two attempts at one
   * unit of work, one of which won, and the loser is redundant with data that is already in the
   * table. An extra log version is a <em>fragment</em>: an append that rolled over and then died,
   * whose contents no other file holds. They arrive by different accidents, they would be produced
   * by different bugs, and — the reason it matters here rather than only philosophically — a
   * mitigation that deletes one is doing something different from a mitigation that deletes the
   * other. Both must reach {@code DANGLING}, and only a case each says so.
   *
   * <p>The comparator arms the two exercise do differ ({@code LogFileComparator}'s log-version
   * branch at line 242 here, its write-token branch at line 230 there), but that is not on its own a
   * reason to keep this: any ordinary slice with more than one log version exercises the version
   * arm, so it is not this case that protects it.
   *
   * <p>A new log version is not <em>necessary</em> for this case: a write-token pair does not
   * collapse inside the file slice — see the retry case's doc for the code that shows it.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 9})
  void orphanLogFileAtALogVersionNoCommitWroteIsDangling(int tableVersion) throws Exception {
    writeTable("MERGE_ON_READ", tableVersion);
    StoragePath original = anyFile(".log.", true);
    StoragePath copy = new StoragePath(original.getParent(),
        original.getName().replaceFirst("\\.log\\.\\d+_", ".log.9_"));
    copyFile(original, copy);

    assertThrows(HoodieValidationException.class,
        () -> runValidator(ValidationMode.FAIL_FAST, false),
        "the tolerance is gone in fail-fast too, at table version " + tableVersion);
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "fail-fast persists nothing, including on the path where the driver decided the verdict");

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE),
        "and it is the same verdict in the mode that reports it");

    JsonNode record = recordFor(readDiff(), copy.getName());
    assertEquals("DANGLING", record.get("verdict").asText(),
        "the commit completed before T_prev, was read, and does not name this file: " + record);
    assertEquals("FS_EXCLUSIVE_LOG_FILE", record.get("sourcePattern").asText());
    assertEquals("LOG_BLOCK_HEADER", record.get("instantSource").asText());

    String instantInTheName = original.getName().split("_")[1].split("\\.log")[0];
    String condemnedOn = record.get("writingInstant").asText();
    if (tableVersion < 8) {
      assertNotEquals(instantInTheName, condemnedOn,
          "below table version 8 a log file's name carries its slice's BASE commit, so a reader "
              + "that trusted the name would have asked about " + instantInTheName
              + " rather than about the delta commit that wrote the file");
    } else {
      assertEquals(instantInTheName, condemnedOn,
          "at table version 8 and above the name carries the writing instant, and the block header "
              + "has to agree with it rather than diverge");
    }
    assertEquals("DANGLING",
        recordFor(readDiffAdjudicated(), copy.getName()).get("verdict").asText());
    assertNull(recordOrNull(readDiff(), original.getName()),
        "the log file the delta commit does name is not a difference at all");
  }

  /**
   * <b>The boundary the retry case needs: the same shape under a delta commit that has <em>not</em>
   * completed is a live write, and never fails a run.</b>
   *
   * <p>Proving that a retry orphan fails a run says nothing about whether it is the <em>only</em>
   * thing that does. Every in-flight MOR writer has, at any moment, exactly this on storage — a log
   * file in a known file slice that no completed commit names — and the difference between it and
   * the orphan is one bit: whether the instant in its first block's header has completed. Get that
   * bit wrong and this validator fails every MOR table with a writer attached, which is a strictly
   * worse outcome than the tolerance the retry case removes.
   *
   * <p><b>The fixture is a real writer's output, not a byte copy.</b> A copy carries the original's
   * block header, so its writing instant would still be the completed one; the only way to put the
   * retry shape under an in-flight instant is to write a real log block whose {@code INSTANT_TIME}
   * header names one. The name is what {@code HoodieAppendHandle} would produce for that instant:
   * the slice's base commit below table version 8, the delta commit itself at 8 and above — which is
   * what makes the two arms below diverge.
   *
   * <p><b>The two table versions reach "passes" by different routes, and both are asserted.</b>
   * <ul>
   *   <li><b>version 6</b>: {@code filterBaseFileAfterPendingCompaction:570-586} copies every log
   *       file of a slice through untouched — there is <em>no</em> commit filtering of log files
   *       below table version 8 — so the file is on the FS side, absent from the metadata table, and
   *       reaches the adjudicator, where {@code (prev, aft) = (INFLIGHT, INFLIGHT)} is the named
   *       {@code SANCTIONED} row. This is the one place in this suite where that row is reached end
   *       to end: {@link #inflightWriteNeverFailsARunInEitherMode} shows it is unreachable through
   *       base files at version 8 and above, and this shows the log-file route keeps it live.
   *   <li><b>versions 8 and 9</b>: {@code filterUncommittedFiles:594-611} drops every log file whose
   *       {@code getDeltaCommitTime()} — the instant in the <em>name</em> — is not completed, so the
   *       file never reaches the diff and there is no record to adjudicate.
   * </ul>
   *
   * <p>The version-8 arm asserts an absence, so the fixture is pinned independently: the file is on
   * storage and {@link LogFileInstantReader} resolves its header to the in-flight instant. Without
   * that, a fixture that silently wrote nothing would pass this test.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 9})
  void retryShapedLogFileUnderAnInflightDeltaCommitNeverFailsARun(int tableVersion)
      throws Exception {
    writeTable("MERGE_ON_READ", tableVersion);
    StoragePath committed = anyFile(".log.", true);
    String inflight = plantInflightDeltaCommit();
    StoragePath liveWrite = plantRetryShapedLogFile(committed, inflight, tableVersion);

    assertTrue(storage.exists(liveWrite), "precondition: the fixture must have written a file");
    LogFileInstantReader.Result header = LogFileInstantReader.read(metaClient(), liveWrite);
    assertEquals(inflight, header.getInstantTime(),
        "precondition: the block header must name the in-flight delta commit, or the arms below "
            + "assert nothing about in-flight writes: " + liveWrite);
    // The version-8 arm asserts an absence, and filterUncommittedFiles would drop this file just as
    // readily if the instant in its name did not exist at all -- so a fixture that planted no
    // instant would satisfy that arm without ever exercising "in flight". Asserted at both versions
    // rather than only where it is load-bearing, so the two arms share one proven precondition.
    assertTrue(metaClient().getActiveTimeline().filterInflights().containsInstant(inflight),
        "precondition: " + inflight + " must genuinely be an in-flight instant on the timeline, "
            + "not merely absent from it");

    assertTrue(runValidator(ValidationMode.FAIL_FAST),
        "a MOR writer's half-written log file must not fail a run at table version " + tableVersion);
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "and fail-fast persists nothing, whatever it concluded");

    assertTrue(runValidator(ValidationMode.EXHAUSTIVE),
        "nor in the mode that reports everything: " + liveWrite);

    if (tableVersion < 8) {
      JsonNode diff = readDiff();
      JsonNode record = recordFor(diff, liveWrite.getName());
      assertEquals("SANCTIONED", record.get("verdict").asText(),
          "one bit apart from the DANGLING retry orphan -- the writing instant has not completed: "
              + record);
      assertEquals("FS_EXCLUSIVE_LOG_FILE", record.get("sourcePattern").asText());
      assertEquals("LOG_BLOCK_HEADER", record.get("instantSource").asText());
      assertEquals(inflight, record.get("writingInstant").asText(),
          "condemned-on instant comes from the header; the name carries the slice's base commit");
      assertNotEquals(inflight, new HoodieLogFile(liveWrite).getDeltaCommitTime(),
          "and below table version 8 those two are provably different, which is what makes the "
              + "assertion above about the header rather than about the name");
      assertEquals(0, countOfVerdict(diff, "DANGLING"),
          () -> "DANGLING is a delete authorisation; a live write may never receive one: " + diff);
      JsonNode adjudicated = readDiffAdjudicated();
      assertNull(recordOrNull(adjudicated, liveWrite.getName()),
          () -> "SANCTIONED is the one verdict the actionable document excludes: " + adjudicated);
    } else {
      assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
          "GAP, pinned rather than assumed: on table version 8 and above filterUncommittedFiles "
              + "drops a log file whose delta commit has not completed before the comparison sees "
              + "it, so an in-flight MOR write produces no record of any kind. If you are reading "
              + "this because a report now exists, assert SANCTIONED on it rather than restoring "
              + "the invisibility");
    }
  }

  /**
   * <b>An FS-exclusive file that a completed commit <em>does</em> name is {@code MDT_CORRUPTION}</b>
   * — the primary condition this validator exists to detect, and the one row in the table that says
   * the metadata table lost a committed file.
   *
   * <p>It is the twin of {@link #fsExclusiveOrphanIsDangling}'s row and of the
   * {@code SANCTIONED} row below it, and the three differ only in what the commit says and when it
   * completed. Same pattern, same file, same storage: the commit completed <b>before</b>
   * {@code T_prev} and names the file, so the metadata snapshot was taken after the commit finished
   * and its silence is a fault. Had the same commit completed <em>during</em> the run, the metadata
   * table would be legitimately behind and the verdict {@code SANCTIONED}. <b>Those two rows must
   * never be merged</b>; only the prev state separates them.
   *
   * <p><b>The {@code SANCTIONED} twin is not reachable from a table fixture</b> and is deliberately
   * not faked here: it requires an instant to complete between the two timeline reads of a single
   * run, which no amount of setup before the run can arrange. It is asserted against constructed
   * state pairs in {@code TestFileDiffAdjudicator}, which is the level at which "completed during
   * the run" is expressible at all.
   *
   * <p>The damage is a write stat added to a completed commit naming a file the metadata table was
   * built without — which is what a metadata table that dropped a record looks like from the
   * outside, reached without having to corrupt the metadata table's own log files and risk the
   * reader failing instead of under-reporting.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 9})
  void fsExclusiveFileACompletedCommitNamesIsMdtCorruption(int tableVersion) throws Exception {
    writeTable("COPY_ON_WRITE", tableVersion);
    StoragePath original = anyFile(".parquet", false);
    String[] parts = original.getName().split("_");
    String instant = parts[2].split("\\.")[0];
    StoragePath lost = new StoragePath(original.getParent(),
        "00000000-1111-2222-3333-444444444444_" + parts[1] + "_" + parts[2]);
    copyFile(original, lost);
    nameFileInCommit(instant, relativePartitionOf(lost), lost.getName());

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE),
        "a committed file the metadata table does not list is a fault in the metadata table");

    JsonNode record = recordFor(readDiff(), lost.getName());
    assertEquals("MDT_CORRUPTION", record.get("verdict").asText(),
        "the commit completed before T_prev and names this file, so the metadata table lost a "
            + "committed file at table version " + tableVersion + ": " + record);
    assertEquals("FS_EXCLUSIVE", record.get("diffPattern").asText());
    assertEquals(instant, record.get("writingInstant").asText());
    assertEquals("MDT_CORRUPTION",
        recordFor(readDiffAdjudicated(), lost.getName()).get("verdict").asText(),
        "a rebuild is an action, so this belongs in the actionable document");
    assertEquals(0, countOfVerdict(readDiff(), "DANGLING"),
        "the same file with the commit silent about it would be DANGLING -- a delete "
            + "authorisation. Nothing here may be one: " + readDiff());
  }

  /**
   * The control for the row above, on the same fixture with the one thing changed that decides it:
   * the commit is <b>not</b> edited, so it says nothing about the file and the verdict is
   * {@code DANGLING} rather than {@code MDT_CORRUPTION}.
   *
   * <p>Worth its own case because the two verdicts license opposite actions — delete the file, or
   * rebuild the metadata table — off the same pattern, the same states and the same storage. A
   * membership lookup that quietly stopped reading the commit would turn every one of these into
   * the other, and only a pair of tests differing in exactly that input can see it.
   */
  @Test
  void fsExclusiveOrphanIsDangling() throws Exception {
    writeTable("COPY_ON_WRITE");
    StoragePath original = anyFile(".parquet", false);
    String[] parts = original.getName().split("_");
    StoragePath orphan = new StoragePath(original.getParent(),
        "00000000-1111-2222-3333-444444444444_" + parts[1] + "_" + parts[2]);
    copyFile(original, orphan);

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE));

    assertEquals("DANGLING", recordFor(readDiff(), orphan.getName()).get("verdict").asText(),
        "the commit was read and does not name this file; the only difference from "
            + "fsExclusiveFileACompletedCommitNamesIsMdtCorruption is that write stat");
  }

  /**
   * <b>A rollback pending at {@code T_prev} explains a file the metadata table still names.</b>
   *
   * <p>The same damage as
   * {@link TestValidatorV2ExhaustiveMode#baseFileDeletedBehindTheMetadataTableIsReported} — a base
   * file gone from storage that a completed commit's metadata table still lists — and the opposite
   * verdict, because a rollback plan requested before the run had already undertaken to delete it.
   * {@code OUTBOUND_DELETION} is fatal, so this is the difference between a healthy table with a
   * table service in progress and a failed run.
   *
   * <p>A rollback <em>pending at both reads</em> is deliberately the shape used, because it is the
   * one {@code ServiceActivityDelta} cannot see: its state never changes during the run, so it contributes
   * nothing to a state diff while having already deleted files the metadata table still names. That
   * is exactly the gap {@code PendingServiceState} exists to close, and this is the end-to-end proof
   * that it is wired in.
   */
  @ParameterizedTest
  @ValueSource(ints = {6, 9})
  void rollbackPendingAtTPrevSanctionsTheFileItPlannedToDelete(int tableVersion) throws Exception {
    writeTable("COPY_ON_WRITE", tableVersion);
    StoragePath victim = anyFile(".parquet", false);
    String writtenBy = victim.getName().split("_")[2].split("\\.")[0];
    assertTrue(storage.deleteFile(victim), "precondition: the base file must actually be removed");
    plantPendingRollback(writtenBy, relativePartitionOf(victim), victim.getName());

    assertTrue(runValidator(ValidationMode.EXHAUSTIVE),
        "a rollback that had already planned to delete this file explains it, so the run passes at "
            + "table version " + tableVersion);

    JsonNode record = recordFor(readDiff(), victim.getName());
    assertEquals("SANCTIONED", record.get("verdict").asText(),
        "a clean or rollback explains this file, and the alternative verdict is fatal: " + record);
    assertEquals("MDT_EXCLUSIVE", record.get("diffPattern").asText());
    assertNull(recordOrNull(readDiffAdjudicated(), victim.getName()),
        "an explained difference is not an actionable one: " + readDiffAdjudicated());
  }

  /**
   * <b>The same for a clean, and both halves in one test.</b> The identical damage is a fatal
   * {@code OUTBOUND_DELETION} with nothing to explain it, and {@code SANCTIONED} once a clean
   * pending at {@code T_prev} has undertaken to delete the file.
   *
   * <p>Run twice over one table, with the plan written between the runs and nothing else changed, so
   * the verdict flipping can only be the clean plan being read. This is also the brief's
   * "an ordinary clean must not produce {@code OUTBOUND_DELETION}" in the form a table fixture can
   * actually reach: a clean that runs wholly <em>after</em> {@code T_aft} needs an interleaving no
   * fixture can produce, and no rule or test covers that condition.
   */
  @Test
  void cleanPendingAtTPrevSanctionsTheFileItPlannedToDelete() throws Exception {
    writeTable("COPY_ON_WRITE");
    StoragePath victim = anyFile(".parquet", false);
    assertTrue(storage.deleteFile(victim), "precondition: the base file must actually be removed");

    assertFalse(runValidator(ValidationMode.EXHAUSTIVE, true, "", "before-the-clean"),
        "precondition: with nothing to explain it, this damage is fatal");
    assertEquals("OUTBOUND_DELETION", recordFor(readDiffIn("before-the-clean"), victim.getName())
        .get("verdict").asText());

    plantPendingClean(relativePartitionOf(victim), victim.getName());

    assertTrue(runValidator(ValidationMode.EXHAUSTIVE, true, "", "after-the-clean"),
        "a clean that had already planned to delete this file explains it");
    assertEquals("SANCTIONED", recordFor(readDiffIn("after-the-clean"), victim.getName())
            .get("verdict").asText(),
        "the only thing that changed between the two runs is the clean plan");
  }

  /**
   * <b>A CDC-enabled table: no CDC file reaches {@code DANGLING}.</b>
   *
   * <p>CDC files are recorded in {@code HoodieWriteStat.getCdcStats()} and <em>nowhere else</em>
   * ({@code HoodieTableMetadataUtil:501-503}), so a membership rule reading only each stat's
   * {@code path} would find no commit naming any of them and condemn every CDC file on the table at
   * once — a whole-table false positive, on a feature whose whole point is that the files are
   * retained. Remove {@code getCdcStats()} from {@code InstantMembershipResolver}'s membership set
   * and this run fails.
   */
  @Test
  void cdcEnabledTableCondemnsNoCdcFile() throws Exception {
    Map<String, String> writeOptions = writeOptions("COPY_ON_WRITE");
    writeOptions.put("hoodie.table.cdc.enabled", "true");
    writeOptions.put("hoodie.table.cdc.supplemental.logging.mode", "data_before_after");
    makeInsertDf("000", 20).write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Overwrite).save(basePath);
    upsert(writeOptions);

    assertTrue(cdcFilesOnStorage() > 0,
        "precondition: the fixture has to actually produce CDC files, or this asserts nothing");

    assertTrue(runValidator(ValidationMode.EXHAUSTIVE),
        "a healthy CDC-enabled table must pass");
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "and must leave no finding of any kind behind");
  }

  /**
   * <b>A commit that lands after the metadata snapshot appears in {@code dlt} and in none of the
   * diff's records.</b>
   *
   * <p>The whole design rests on {@code dlt} describing the timeline movement the diff cannot: the
   * listing is materialised before {@code T_aft} is read, so a commit that lands after the listing
   * is in the delta between the two timelines and in no record the diff produced. Both halves are
   * asserted against the real objects — {@link InstantStateIndex} off the timeline before the write,
   * {@link ServiceActivityDelta#between} off the timeline after it — and then the run over the resulting
   * table reports nothing, because a legitimate commit is timeline movement and never a difference.
   *
   * <p>The interleaving itself is not reproduced, and deliberately not: a test that raced a writer
   * against phase 1 would assert this only on the runs where it happened to win the race. What is
   * reproduced is the pair of timeline reads either side of a real commit, which is precisely what
   * {@code dlt} is built from.
   */
  @Test
  void commitLandingAfterTheSnapshotIsInTheDeltaAndNotInTheDiff() throws Exception {
    writeTable("COPY_ON_WRITE");
    HoodieTableMetaClient before = metaClient();
    InstantStateIndex prevIndex = InstantStateIndex.from(before.getActiveTimeline());

    upsert(writeOptions("COPY_ON_WRITE"));

    HoodieTableMetaClient after = metaClient();
    ServiceActivityDelta dlt = ServiceActivityDelta.between(prevIndex, after.getActiveTimeline(), after);
    String landed = after.getActiveTimeline().filterCompletedInstants().lastInstant().get()
        .requestedTime();

    assertFalse(prevIndex.resolveAtPrev(landed).isResolved(),
        "precondition: the instant must genuinely be absent from T_prev, or this proves nothing");
    // The delta tracks clean and rollback activity and NOTHING else, so an ordinary commit is
    // invisible to it -- by design, not by omission. Asking it about a commit is asking the wrong
    // object; the aft timeline is what carries one.
    assertTrue(dlt.noServiceActivity(),
        "an ordinary commit is not clean or rollback activity and must not register as any");
    assertEquals(InstantState.COMPLETED,
        InstantStateIndex.from(after.getActiveTimeline()).resolveAtPrev(landed).getState(),
        "the aft index is what carries a commit that landed between the two reads");

    assertTrue(runValidator(ValidationMode.EXHAUSTIVE),
        "an ordinary commit is timeline movement, never a difference");
    assertFalse(storage.exists(new StoragePath(basePath, OUTPUT_DIR)),
        "so it produces no record at all");
  }

  // ---------------------------------------------------------------- fixtures

  /** {@code fs_mdt_diff/diff/} of a named run, for a test that performs more than one. */
  private JsonNode readDiffIn(String runId) throws IOException {
    StoragePath dir = new StoragePath(new StoragePath(new StoragePath(
            new StoragePath(basePath, OUTPUT_DIR), runId), FileDiffReportWriter.REPORT_DIR),
        FileDiffReportWriter.DIFF_DIR);
    return readDocument(dir, FileDiffReportWriter.DIFF_FILE);
  }

  /**
   * A compaction instant left <b>inflight</b>, with a real plan behind it.
   *
   * <p>Written through the active timeline's own transitions rather than by placing files, so the
   * naming, the layout and the plan encoding are the writer's on both table versions. The plan has
   * no operations: the file this fixture is about is planted beside it, and a plan with operations
   * would additionally make the file system view expect their outputs.
   *
   * @return the instant time
   */
  private String plantInflightCompaction() throws IOException {
    String instantTime = instantAfterEverything();
    HoodieTableMetaClient client = metaClient();
    HoodieActiveTimeline timeline = client.getActiveTimeline();
    HoodieInstant requested = client.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
    timeline.saveToCompactionRequested(requested, HoodieCompactionPlan.newBuilder()
        .setOperations(new ArrayList<>()).setVersion(2).build());
    timeline.transitionCompactionRequestedToInflight(requested);
    return instantTime;
  }

  /**
   * Adds a write stat naming {@code fileName} to the completed commit {@code instantTime}, in
   * place.
   *
   * <p>Read and written back through the meta client's own {@link CommitMetadataSerDe}, so the
   * encoding is the table version's rather than this test's guess at it — version 6 writes commit
   * metadata as JSON and version 8 and above as Avro, and a fixture that picked one would silently
   * produce an unreadable commit on the other table and a refusal instead of the verdict it meant
   * to assert.
   */
  private void nameFileInCommit(String instantTime, String partition, String fileName)
      throws IOException {
    HoodieTableMetaClient client = metaClient();
    HoodieInstant commit = client.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
        .getInstantsAsStream().filter(i -> instantTime.equals(i.requestedTime())).findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "no completed commit at " + instantTime + " on " + basePath));
    HoodieCommitMetadata metadata = client.getActiveTimeline().readCommitMetadata(commit);
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setFileId(fileName.split("_")[0]);
    stat.setPath(partition + StoragePath.SEPARATOR + fileName);
    stat.setPartitionPath(partition);
    metadata.addWriteStat(partition, stat);
    StoragePath instantFile = new StoragePath(client.getTimelinePath(),
        client.getInstantFileNameGenerator().getFileName(commit));
    try (OutputStream out = storage.create(instantFile, true)) {
      client.getCommitMetadataSerDe().getInstantWriter(metadata).get().writeToStream(out);
    }
  }

  /**
   * The write token of a <b>retry</b> of the task that produced {@code writeToken}: a write token is
   * {@code <taskPartitionId>-<stageId>-<taskAttemptId>} ({@code FSUtils.makeWriteToken:116-118}) and
   * a retry re-runs the same task in the same stage with the next attempt id.
   *
   * <p>Parsed and re-made through {@link FSUtils#makeWriteToken} rather than string-patched, so a
   * change to the token's shape fails here instead of silently minting a token that still parses.
   */
  private static String nextAttemptOf(String writeToken) {
    String[] parts = writeToken.split("-");
    if (parts.length != 3) {
      throw new IllegalStateException("not a <taskPartitionId>-<stageId>-<taskAttemptId> write "
          + "token: " + writeToken);
    }
    return FSUtils.makeWriteToken(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]),
        Long.parseLong(parts[2]) + 1);
  }

  /**
   * The retry orphan a second attempt of the task that wrote {@code original} would leave: the same
   * file id, delta commit and log version, with the write token's attempt id incremented, holding
   * the same bytes — and therefore the same first-block header instant.
   *
   * <p>Assembled with {@link FSUtils#makeLogFileName} out of the original's own parsed components
   * rather than by substituting into its name, so the fixture is expressed in the writer's terms.
   * The equality checks are the fixture's own proof that it changed the write token and nothing
   * else: a naming change that moved the attempt id would otherwise leave this test injecting some
   * other shape and still passing.
   */
  private StoragePath retryOf(StoragePath original) throws IOException {
    HoodieLogFile logFile = new HoodieLogFile(original);
    StoragePath retry = new StoragePath(original.getParent(),
        FSUtils.makeInlineLogFileName(logFile.getFileId(), "." + logFile.getFileExtension(),
            logFile.getDeltaCommitTime(), logFile.getLogVersion(),
            nextAttemptOf(logFile.getLogWriteToken())));
    HoodieLogFile parsed = new HoodieLogFile(retry);
    assertEquals(logFile.getFileId(), parsed.getFileId(), "the retry is the same file group");
    assertEquals(logFile.getDeltaCommitTime(), parsed.getDeltaCommitTime(),
        "and the same delta commit");
    assertEquals(logFile.getLogVersion(), parsed.getLogVersion(), "and the same log version");
    // The write token is the only thing that differs, and WITHIN it only the attempt id. Asserting
    // the first two components unchanged is what makes this a retry rather than merely a different
    // token: were makeWriteToken's parameter order to change, incrementing the stage id would still
    // satisfy an assertion that the token as a whole differs, and this fixture would go on passing
    // while injecting the output of a different task.
    String[] attempted = logFile.getLogWriteToken().split("-");
    String[] retried = parsed.getLogWriteToken().split("-");
    assertEquals(attempted[0], retried[0], "the same task partition id");
    assertEquals(attempted[1], retried[1], "and the same stage id");
    assertNotEquals(attempted[2], retried[2],
        "so the attempt id is the only thing that differs: " + retry.getName());
    copyFile(original, retry);
    return retry;
  }

  /**
   * A delta commit left <b>inflight</b>, written through the active timeline's own transitions —
   * requested first, then inflight, which is the order a writer produces them in.
   *
   * @return the instant time
   */
  private String plantInflightDeltaCommit() throws IOException {
    String instantTime = instantAfterEverything();
    HoodieTableMetaClient client = metaClient();
    HoodieActiveTimeline timeline = client.getActiveTimeline();
    HoodieInstant requested = client.getInstantGenerator().createNewInstant(
        HoodieInstant.State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime);
    timeline.createNewInstant(requested);
    timeline.transitionRequestedToInflight(requested, Option.empty());
    return instantTime;
  }

  /**
   * A real log file written into {@code committed}'s file slice by a task attempt of the in-flight
   * delta commit {@code inflightInstant} — the retry orphan's shape with its writing instant not yet
   * completed.
   *
   * <p>Written through {@link HoodieLogFormat}'s own writer rather than copied, because the instant
   * this file is judged on is the one in its first block's {@code INSTANT_TIME} header and a copy
   * would carry the completed instant of the file it was copied from. The block is an empty Avro
   * data block: the reader that matters here opens the file with a null reader schema and stops at
   * the first block's header ({@code LogFileInstantReader:186-201}), so the records are irrelevant
   * and the header is the whole fixture.
   *
   * <p><b>The name is the writer's, which is version-dependent.</b> Below table version 8 a log
   * file's name carries its slice's base commit — so the same instant {@code committed} carries —
   * and at 8 and above it carries the delta commit that wrote it. Reproducing that is the point:
   * {@code filterUncommittedFiles} keys on the name's instant, so a fixture that used the wrong one
   * would exercise the wrong side of the version-8 boundary.
   */
  private StoragePath plantRetryShapedLogFile(StoragePath committed, String inflightInstant,
                                              int tableVersion)
      throws IOException, InterruptedException {
    HoodieLogFile sibling = new HoodieLogFile(committed);
    String instantInTheName =
        tableVersion < 8 ? sibling.getDeltaCommitTime() : inflightInstant;
    try (HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder()
        .withParentPath(committed.getParent())
        .withFileExtension("." + sibling.getFileExtension())
        .withLogFileId(sibling.getFileId())
        .withInstantTime(instantInTheName)
        .withLogVersion(sibling.getLogVersion())
        .withLogWriteToken(nextAttemptOf(sibling.getLogWriteToken()))
        .withTableVersion(HoodieTableVersion.fromVersionCode(tableVersion))
        .withStorage(storage)
        .withSizeThreshold(Long.MAX_VALUE)
        .build()) {
      Map<HoodieLogBlock.HeaderMetadataType, String> header =
          new EnumMap<>(HoodieLogBlock.HeaderMetadataType.class);
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, inflightInstant);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
      writer.appendBlock(new HoodieAvroDataBlock(
          Collections.emptyList(), header, HoodieRecord.RECORD_KEY_METADATA_FIELD));
      return writer.getLogFile().getPath();
    }
  }

  /**
   * A base file under a file id nothing has ever heard of, carrying {@code instantTime}, so the
   * timeline can be asked about that instant and has a definite answer.
   */
  private StoragePath strangerUnderInstant(String instantTime) throws IOException {
    StoragePath original = anyFile(".parquet", false);
    StoragePath stranger = new StoragePath(original.getParent(),
        "00000000-1111-2222-3333-444444444444_9-9-9_" + instantTime + ".parquet");
    copyFile(original, stranger);
    return stranger;
  }

  private long cdcFilesOnStorage() throws IOException {
    long found = 0;
    for (StoragePathInfo info : storage.listFiles(new StoragePath(basePath))) {
      String name = info.getPath().getName();
      if (info.getPath().toString().contains("/.hoodie/")) {
        continue;
      }
      if (name.contains("-cdc") || name.contains(".cdc")) {
        found++;
      }
    }
    return found;
  }
}
