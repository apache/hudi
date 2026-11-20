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

import org.apache.hudi.common.util.JsonUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestFileDiffRecord {

  private static FileDiffRecord sample() {
    return FileDiffRecord.newBuilder()
        .withPartition("2026/08/15")
        .withFileName("fg1_0-1-2_20260815120000.parquet")
        .withFileGroupId("fg1")
        .withBaseInstantTime("20260815120000")
        .withFileType(MismatchRecord.FileType.BASE)
        .withDiffPattern(DiffPattern.FS_EXCLUSIVE)
        .withSourcePattern(MismatchPattern.FS_EXCLUSIVE_BASE_FILE)
        .withWritingInstant("20260815120000", MismatchRecord.InstantSource.FILE_NAME)
        .withDetail("only storage has it")
        .build();
  }

  @Test
  void roundTripsThroughJsonWithEveryFieldPreserved() throws Exception {
    FileDiffRecord original = sample().withVerdict(Verdict.DANGLING, "no commit names it");
    String json = JsonUtils.getObjectMapper().writeValueAsString(original);
    FileDiffRecord back = JsonUtils.getObjectMapper().readValue(json, FileDiffRecord.class);

    assertEquals("2026/08/15", back.getPartition());
    assertEquals("fg1_0-1-2_20260815120000.parquet", back.getFileName());
    assertEquals(DiffPattern.FS_EXCLUSIVE, back.getDiffPattern());
    assertEquals(MismatchPattern.FS_EXCLUSIVE_BASE_FILE, back.getSourcePattern());
    assertEquals("20260815120000", back.getWritingInstant());
    assertEquals(Verdict.DANGLING, back.getVerdict());
    assertEquals("no commit names it", back.getVerdictDetail());
  }

  @Test
  void withVerdictLeavesTheSourceRecordUntouched() {
    FileDiffRecord original = sample();
    FileDiffRecord decided = original.withVerdict(Verdict.SANCTIONED, "in commit metadata");

    assertEquals(null, original.getVerdict());
    assertEquals(Verdict.SANCTIONED, decided.getVerdict());
    assertEquals(original.getFileName(), decided.getFileName());
  }

  @Test
  void onlySanctionedAndNotSettledPass() {
    assertFalse(Verdict.SANCTIONED.isValidationFailure());
    assertFalse(Verdict.NOT_SETTLED.isValidationFailure());
    assertTrue(Verdict.DANGLING.isValidationFailure());
    assertTrue(Verdict.MDT_CORRUPTION.isValidationFailure());
    assertTrue(Verdict.OUTBOUND_DELETION.isValidationFailure());
    assertTrue(Verdict.CODE_BUG.isValidationFailure());
  }

  @Test
  void diffPatternCollapsesTheNineDiffPatternsBySide() {
    assertEquals(DiffPattern.FS_EXCLUSIVE, DiffPattern.of(MismatchSide.FS));
    assertEquals(DiffPattern.MDT_EXCLUSIVE, DiffPattern.of(MismatchSide.MDT));
    assertEquals(DiffPattern.BOTH_DISAGREE, DiffPattern.of(MismatchSide.BOTH));
  }

  @Test
  void tallyKeepsAndCountsEveryRecord() {
    DiffTally tally = new DiffTally();
    tally.add(sample());
    tally.add(sample());
    tally.add(sample());

    assertEquals(3, tally.getRecords().size());
    assertEquals(3, tally.getTotalCount());
  }

  @Test
  void tallyCountsDropsByClass() {
    DiffTally tally = new DiffTally();
    tally.countDrop("COMMAND_BLOCK");
    tally.countDrop("COMMAND_BLOCK");
    tally.countDrop("UNREADABLE");

    assertEquals(Long.valueOf(2L), tally.getDropCounts().get("COMMAND_BLOCK"));
    assertEquals(Long.valueOf(1L), tally.getDropCounts().get("UNREADABLE"));
  }

  /**
   * The fold a phase-1 task performs as each partition's future is drained. A dropped file is never
   * a record, so the two halves have to survive the merge independently.
   */
  @Test
  void tallyMergeFoldsRecordsDropsAndFilesSeen() {
    DiffTally task = new DiffTally();
    DiffTally first = new DiffTally();
    first.add(sample());
    first.countDrop("COMMAND_BLOCK");
    first.addFilesSeen(10L);
    DiffTally second = new DiffTally();
    second.countDrop("COMMAND_BLOCK");
    second.countDrop("UNREADABLE");
    second.addFilesSeen(5L);

    task.merge(first);
    task.merge(second);

    assertEquals(1, task.getRecords().size(), "the second partition found nothing to record");
    assertEquals(1, task.getTotalCount());
    assertEquals(15L, task.getFilesSeen(), "a partition with no findings still counts its files");
    assertEquals(Long.valueOf(2L), task.getDropCounts().get("COMMAND_BLOCK"));
    assertEquals(Long.valueOf(1L), task.getDropCounts().get("UNREADABLE"));
  }

  /**
   * The shuffle sorts values, so the ordering has to be total and deterministic: two runs over an
   * unchanged table must repartition identically.
   */
  @Test
  void compareToOrdersByPartitionThenFileNameThenDiffPattern() {
    FileDiffRecord first = diffRecord("2024/01/01", "a.parquet", DiffPattern.FS_EXCLUSIVE);
    FileDiffRecord laterPartition = diffRecord("2024/01/02", "a.parquet", DiffPattern.FS_EXCLUSIVE);
    FileDiffRecord laterName = diffRecord("2024/01/01", "b.parquet", DiffPattern.FS_EXCLUSIVE);
    FileDiffRecord otherPattern = diffRecord("2024/01/01", "a.parquet", DiffPattern.MDT_EXCLUSIVE);

    assertTrue(first.compareTo(laterPartition) < 0);
    assertTrue(laterPartition.compareTo(first) > 0);
    assertTrue(first.compareTo(laterName) < 0);
    assertEquals(0, first.compareTo(first));
    assertTrue(first.compareTo(otherPattern) != 0,
        "(partition, fileName) identifies a FILE; two findings about one file must not compare equal");
  }

  /**
   * A crossed fixture: sorting by {@code fileName} first would put these two in the opposite order
   * from sorting by {@code partition} first, so this pins the priority between the two fields
   * rather than merely exercising each in isolation.
   */
  @Test
  void compareToPrioritizesPartitionOverFileNameOnACrossedFixture() {
    FileDiffRecord earlierPartitionLaterName = diffRecord("2024/01/01", "b.parquet", DiffPattern.FS_EXCLUSIVE);
    FileDiffRecord laterPartitionEarlierName = diffRecord("2024/01/02", "a.parquet", DiffPattern.FS_EXCLUSIVE);

    assertTrue(earlierPartitionLaterName.compareTo(laterPartitionEarlierName) < 0,
        "partition is the first key: 2024/01/01 sorts before 2024/01/02 regardless of file name");
    assertTrue(laterPartitionEarlierName.compareTo(earlierPartitionLaterName) > 0);
  }

  /**
   * Null-safe on every field, and deliberately so rather than defensively: this comparator runs
   * inside a Spark shuffle, where a NullPointerException fails the whole stage.
   */
  @Test
  void compareToIsNullSafeOnEveryField() {
    FileDiffRecord complete = diffRecord("2024/01/01", "a.parquet", DiffPattern.FS_EXCLUSIVE);
    FileDiffRecord noPartition = diffRecord(null, "a.parquet", DiffPattern.FS_EXCLUSIVE);
    FileDiffRecord noName = diffRecord("2024/01/01", null, DiffPattern.FS_EXCLUSIVE);
    FileDiffRecord noPattern = diffRecord("2024/01/01", "a.parquet", null);

    assertEquals(0, noPartition.compareTo(noPartition));
    assertEquals(0, noName.compareTo(noName));
    assertEquals(0, noPattern.compareTo(noPattern));
    assertEquals(-Integer.signum(complete.compareTo(noPartition)),
        Integer.signum(noPartition.compareTo(complete)));
    assertEquals(-Integer.signum(complete.compareTo(noName)),
        Integer.signum(noName.compareTo(complete)));
    assertEquals(-Integer.signum(complete.compareTo(noPattern)),
        Integer.signum(noPattern.compareTo(complete)));

    // Antisymmetry alone holds under either null convention; pin the direction explicitly.
    // Nulls sort first, per the class javadoc.
    assertTrue(noPartition.compareTo(complete) < 0, "a null partition must sort before a present one");
    assertTrue(noName.compareTo(complete) < 0, "a null fileName must sort before a present one");
    assertTrue(noPattern.compareTo(complete) < 0, "a null diffPattern must sort before a present one");
  }

  /**
   * Sorting is repeatable, which is what the shuffle relies on. Checked against a fixed expected
   * order, not just two shuffles agreeing with each other -- self-consistency alone cannot catch a
   * wrong-but-deterministic order (e.g. a fileName-before-partition priority swap), only a
   * non-deterministic one.
   */
  @Test
  void sortingIsRepeatable() {
    FileDiffRecord aParquet = diffRecord("2024/01/01", "a.parquet", DiffPattern.FS_EXCLUSIVE);
    FileDiffRecord bParquetSamePartition = diffRecord("2024/01/01", "b.parquet", DiffPattern.MDT_EXCLUSIVE);
    FileDiffRecord bParquetLaterPartition = diffRecord("2024/01/02", "b.parquet", DiffPattern.FS_EXCLUSIVE);
    List<FileDiffRecord> records = new ArrayList<>(Arrays.asList(
        bParquetLaterPartition, bParquetSamePartition, aParquet));
    List<FileDiffRecord> other = new ArrayList<>(records);
    Collections.shuffle(other, new java.util.Random(42));

    Collections.sort(records);
    Collections.sort(other);

    List<FileDiffRecord> expected = Arrays.asList(aParquet, bParquetSamePartition, bParquetLaterPartition);
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i).getPartition(), records.get(i).getPartition());
      assertEquals(expected.get(i).getFileName(), records.get(i).getFileName());
      assertEquals(expected.get(i).getDiffPattern(), records.get(i).getDiffPattern());
    }
    for (int i = 0; i < records.size(); i++) {
      assertEquals(records.get(i).getPartition(), other.get(i).getPartition());
      assertEquals(records.get(i).getFileName(), other.get(i).getFileName());
      assertEquals(records.get(i).getDiffPattern(), other.get(i).getDiffPattern());
    }
  }

  @Test
  void equalsAgreesWithCompareToOnTheThreeOrderingKeys() {
    FileDiffRecord one = diffRecord("p", "f.parquet", DiffPattern.FS_EXCLUSIVE);
    FileDiffRecord same = diffRecord("p", "f.parquet", DiffPattern.FS_EXCLUSIVE);

    assertEquals(0, one.compareTo(same));
    assertEquals(one, same, "compareTo says these are the same finding, so equals must agree");
    assertEquals(one.hashCode(), same.hashCode());
  }

  @Test
  void equalsSeparatesRecordsEachOrderingKeySeparates() {
    FileDiffRecord base = diffRecord("p", "f.parquet", DiffPattern.FS_EXCLUSIVE);

    for (FileDiffRecord other : Arrays.asList(
        diffRecord("other", "f.parquet", DiffPattern.FS_EXCLUSIVE),
        diffRecord("p", "other.parquet", DiffPattern.FS_EXCLUSIVE),
        diffRecord("p", "f.parquet", DiffPattern.MDT_EXCLUSIVE))) {
      assertNotEquals(0, base.compareTo(other));
      assertNotEquals(base, other, "differs on an ordering key, so it is a different finding");
    }
  }

  @Test
  void equalsIgnoresEverythingCompareToIgnores() {
    // The verdict is decided by the run, so two runs over an unchanged table must not disagree
    // about which findings they hold. compareTo excludes it deliberately; equals follows.
    FileDiffRecord undecided = diffRecord("p", "f.parquet", DiffPattern.FS_EXCLUSIVE);
    FileDiffRecord decided = undecided.withVerdict(Verdict.DANGLING, "no commit names it");

    assertEquals(0, undecided.compareTo(decided));
    assertEquals(undecided, decided);
    assertEquals(undecided.hashCode(), decided.hashCode());
  }

  @Test
  void equalsIsNullSafeAndTypeSafeTheWayCompareToIs() {
    FileDiffRecord nullKeys = FileDiffRecord.newBuilder().build();

    assertEquals(0, nullKeys.compareTo(FileDiffRecord.newBuilder().build()));
    assertEquals(nullKeys, FileDiffRecord.newBuilder().build());
    assertEquals(nullKeys.hashCode(), FileDiffRecord.newBuilder().build().hashCode());
    assertNotEquals(nullKeys, diffRecord("p", "f.parquet", DiffPattern.FS_EXCLUSIVE));
    assertNotEquals(nullKeys, "not a record");
    assertNotEquals(nullKeys, null);
  }

  private static FileDiffRecord diffRecord(String partition, String fileName, DiffPattern pattern) {
    return FileDiffRecord.newBuilder()
        .withPartition(partition)
        .withFileName(fileName)
        .withDiffPattern(pattern)
        .withFileGroupId("fg1")
        .withBaseInstantTime("1005")
        .withFileType(MismatchRecord.FileType.BASE)
        .withWritingInstant("1005", MismatchRecord.InstantSource.FILE_NAME)
        .build();
  }
}
