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

package org.apache.hudi.utilities.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The five groups {@link FsMdtDiffDimension#settleWithoutTheTimeline} settles, with the beacon
 * asserted alongside the verdict. The MDT-exclusive log-file group has three answers of its own, so
 * six tests cover the five groups: {@code SWT_BOTH_DISAGREE}, the log-file group's
 * {@code SWT_MDT_LOG_SANCTIONED} / {@code SWT_MDT_LOG_UNRESOLVED} / {@code SWT_MDT_LOG_CORRUPTION},
 * {@code SWT_SHAPE_TAKES_NO_ROW}, and {@code SWT_NO_WRITING_INSTANT}.
 *
 * <p>These are here rather than in a functional suite because <b>each of them is about the shape of a
 * record rather than about the state of a table</b>, and one of the five cannot be produced from a
 * real table at all:
 *
 * <ul>
 *   <li>{@code SWT_SHAPE_TAKES_NO_ROW} needs an MDT-exclusive record whose file type was never set.
 *       The builder permits it and no phase-1 emitter produces one.
 * </ul>
 *
 * <p>The MDT-exclusive log-file rows are reachable end to end and are covered there as well; they are
 * repeated here because this is where the three answers of the reconciler sit side by side, which is
 * the property that matters: only {@code NA} may reach the fatal verdict, and both
 * {@code SANCTIONED} and {@code UNRESOLVED} must withdraw it.
 *
 * <p>The function is a pure function of its three arguments and holds no state, which is what makes
 * this level of test the honest one for a record shape.
 */
class TestSettleWithoutTheTimelineBranches {

  private static final String PARTITION = "2026/08/17";
  private static final String FILE_GROUP_ID = "fg1";
  private static final String INSTANT = "100";

  /**
   * <b>An MDT-exclusive log file a clean or rollback claims is {@code SANCTIONED}.</b>
   *
   * <p>An MDT-exclusive log file is unattributable on every table version — it is not on storage to
   * open, and below version 8 its name carries the slice's base commit rather than the writing one —
   * so it takes the clean-only rule and never the timeline.
   */
  @Test
  void mdtExclusiveLogFileAServiceClaimsIsSanctioned() {
    FileDiffRecord record = mdtLog().build();
    ServiceActivityDelta dlt = ServiceActivityDelta.forTesting()
        .withCleanFreedFile(PARTITION, record.getFileName()).build();
    assertEquals(Verdict.SANCTIONED, settle(record, dlt).getVerdict());
    assertTrue(BranchBeacons.takenBy(() -> settle(record, dlt))
        .contains(Branch.SWT_MDT_LOG_SANCTIONED));
  }

  /**
   * <b>An MDT-exclusive log file whose explaining plan could not be read is {@code NOT_SETTLED}</b>,
   * never the fatal verdict.
   *
   * <p>This is the whole reason the reconciler has a third answer. Reaching {@code MDT_CORRUPTION} by
   * exclusion would report a healthy table as corrupt whenever one clean plan happened to be
   * unreadable, and the group's fatal arm is only allowed to fire on a positive {@code NA}.
   */
  @Test
  void mdtExclusiveLogFileWhosePlanCouldNotBeReadIsNotSettled() {
    FileDiffRecord record = mdtLog().build();
    ServiceActivityDelta dlt = ServiceActivityDelta.forTesting().withReadFailure(PARTITION).build();
    assertEquals(Verdict.NOT_SETTLED, settle(record, dlt).getVerdict());
    assertTrue(BranchBeacons.takenBy(() -> settle(record, dlt))
        .contains(Branch.SWT_MDT_LOG_UNRESOLVED));
  }

  /**
   * <b>And with every plan readable and none accounting for it: {@code MDT_CORRUPTION}.</b>
   *
   * <p>The third of the same group, present so the trio reads as one decision: the fatal verdict is
   * reached from a positive answer about the table, not from the absence of one.
   */
  @Test
  void mdtExclusiveLogFileNothingAccountsForIsCorruption() {
    FileDiffRecord record = mdtLog().build();
    assertEquals(Verdict.MDT_CORRUPTION, settle(record).getVerdict());
    assertTrue(BranchBeacons.takenBy(() -> settle(record))
        .contains(Branch.SWT_MDT_LOG_CORRUPTION));
  }

  /**
   * <b>A record whose (diff pattern, file type) pair takes no shuffle arm is {@code NOT_SETTLED}</b>,
   * with a detail that names the instant, the pattern and the type.
   *
   * <p>The only group that is a fact about the record's shape rather than about the file's contents.
   * It exists so such a record does not fall through to the no-writing-instant group below, whose
   * message would send an operator to a drop count that holds nothing for this file.
   */
  @Test
  void recordWhoseShapeMatchesNoRowIsNotSettledAndSaysSo() {
    FileDiffRecord record = FileDiffRecord.newBuilder()
        .withPartition(PARTITION)
        .withFileName("fg1_0-1-0_100.parquet")
        .withFileGroupId(FILE_GROUP_ID)
        .withDiffPattern(DiffPattern.MDT_EXCLUSIVE)
        .withWritingInstant(INSTANT, MismatchRecord.InstantSource.FILE_NAME)
        .build();

    FileDiffRecord settled = settle(record);
    assertEquals(Verdict.NOT_SETTLED, settled.getVerdict());
    assertTrue(settled.getVerdictDetail().contains(INSTANT),
        "the detail has to name the instant, because it resolved: " + settled.getVerdictDetail());
    assertTrue(settled.getVerdictDetail().contains("Nothing is wrong with the file"),
        "and say so, rather than reading as a finding: " + settled.getVerdictDetail());
    assertTrue(BranchBeacons.takenBy(() -> settle(record)).contains(Branch.SWT_SHAPE_TAKES_NO_ROW));
  }

  /**
   * <b>A record with no attributable writing instant was excluded from adjudication rather than
   * adjudicated</b>, and its detail says where to look instead.
   *
   * <p>The distinction from the group above is the one an operator acts on: this file <em>is</em> in
   * the partition's drop counts, and that one is not.
   */
  @Test
  void recordWithNoWritingInstantIsReportedAsExcludedNotAsAdjudicated() {
    FileDiffRecord record = FileDiffRecord.newBuilder()
        .withPartition(PARTITION)
        .withFileName("fg1_0-1-0_100.parquet")
        .withFileGroupId(FILE_GROUP_ID)
        .withFileType(MismatchRecord.FileType.BASE)
        .withDiffPattern(DiffPattern.FS_EXCLUSIVE)
        .withUnresolvedInstant(MismatchRecord.InstantUnresolvedReason.NAME_NOT_PARSEABLE)
        .build();

    FileDiffRecord settled = settle(record);
    assertEquals(Verdict.NOT_SETTLED, settled.getVerdict());
    assertTrue(settled.getVerdictDetail().contains("dropCounts"),
        "the detail has to point at the drop counts, which is where this file is accounted for: "
            + settled.getVerdictDetail());
    assertTrue(BranchBeacons.takenBy(() -> settle(record))
        .contains(Branch.SWT_NO_WRITING_INSTANT));
  }

  /**
   * <b>Both sides holding the file and disagreeing is {@code MDT_CORRUPTION} unconditionally</b>: no
   * timeline state can explain that away, so none is consulted.
   */
  @Test
  void bothSidesDisagreeingIsCorruptionWithoutAskingAnything() {
    FileDiffRecord record = FileDiffRecord.newBuilder()
        .withPartition(PARTITION)
        .withFileName("fg1_0-1-0_100.parquet")
        .withFileGroupId(FILE_GROUP_ID)
        .withFileType(MismatchRecord.FileType.BASE)
        .withDiffPattern(DiffPattern.BOTH_DISAGREE)
        .withWritingInstant(INSTANT, MismatchRecord.InstantSource.FILE_NAME)
        .build();

    assertEquals(Verdict.MDT_CORRUPTION, settle(record).getVerdict());
    assertTrue(BranchBeacons.takenBy(() -> settle(record)).contains(Branch.SWT_BOTH_DISAGREE));
  }

  // ---------------------------------------------------------------- fixtures

  private static FileDiffRecord.Builder mdtLog() {
    return FileDiffRecord.newBuilder()
        .withPartition(PARTITION)
        .withFileName(".fg1_100.log.1_0-1-0")
        .withFileGroupId(FILE_GROUP_ID)
        .withFileType(MismatchRecord.FileType.LOG)
        .withDiffPattern(DiffPattern.MDT_EXCLUSIVE);
  }

  private static FileDiffRecord settle(FileDiffRecord record) {
    return settle(record, ServiceActivityDelta.forTesting().build());
  }

  private static FileDiffRecord settle(FileDiffRecord record, ServiceActivityDelta dlt) {
    return FsMdtDiffDimension.settleWithoutTheTimeline(record, dlt, PendingServiceState.empty());
  }

}
