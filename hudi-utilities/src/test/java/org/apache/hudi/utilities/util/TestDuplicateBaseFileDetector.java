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

import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The bucketing rule a raw partition listing is put through to find duplicate base files:
 * cardinality alone, with no timeline read at grouping time. Whether a cardinality finding is
 * tolerated as a write racing the validation run is {@link DuplicateBaseFileDimension}'s classify
 * stage's question, covered by {@link TestDuplicateInstantWindow} and the functional suites, not by
 * this class.
 */
class TestDuplicateBaseFileDetector {

  private static final String PARTITION = "2026/08/12";

  private static StoragePathInfo file(String name, long length) {
    return new StoragePathInfo(new StoragePath("/tbl/" + PARTITION + "/" + name),
        length, false, (short) 0, 0L, 0L);
  }

  private static List<MismatchRecord> detect(List<StoragePathInfo> files) {
    return DuplicateBaseFileDetector.detectSuspects(files, PARTITION);
  }

  private static List<String> reportedNames(List<MismatchRecord> suspects) {
    return suspects.stream().map(MismatchRecord::getFileName).collect(Collectors.toList());
  }

  private static long failingCount(List<MismatchRecord> suspects) {
    return suspects.stream().filter(MismatchRecord::isFailsValidation).count();
  }

  private static long countOf(List<MismatchRecord> suspects, MismatchPattern pattern) {
    return suspects.stream().filter(record -> record.getPattern() == pattern).count();
  }

  private static final List<StoragePathInfo> ONE_DUPLICATE_PAIR = Arrays.asList(
      file("fg1_0-1-0_1005.parquet", 10L), file("fg1_0-2-0_1005.parquet", 11L));

  @Test
  void singleBaseFilePerSliceReportsNothing() {
    assertTrue(detect(Arrays.asList(file("fg1_0-1-0_1005.parquet", 10L),
        file("fg2_0-1-0_1005.parquet", 10L))).isEmpty());
  }

  @Test
  void everyMemberOfADuplicateSetIsASuspect() {
    List<MismatchRecord> suspects = detect(ONE_DUPLICATE_PAIR);

    assertEquals(2, suspects.size(),
        "both members are reported: cardinality alone decides the finding");
    assertEquals(2, failingCount(suspects), "the invariant is broken; the run fails");
    for (MismatchRecord record : suspects) {
      assertEquals(MismatchPattern.DANGLING_FILE_SUSPECTS, record.getPattern());
      assertEquals(MismatchSide.FS, record.getSide());
      assertEquals(MismatchRecord.SuspectReason.DUPLICATE_BASE_FILE, record.getSuspectReason());
      assertEquals(2, record.getDuplicateSetSize());
      assertNull(record.getExpectedFileName());
      assertTrue(record.isFailsValidation());
    }
  }

  @Test
  void suspectCarriesEnoughToGoAndLookAtTheFile() {
    MismatchRecord record = detect(ONE_DUPLICATE_PAIR).get(0);

    assertEquals("fg1", record.getFileGroupId());
    assertEquals("1005", record.getBaseInstantTime());
    assertEquals("1005", record.getInstantTime());
    assertEquals(MismatchRecord.InstantSource.FILE_NAME, record.getInstantSource());
    assertEquals(MismatchRecord.FileType.BASE, record.getFileType());
    assertEquals(PARTITION, record.getPartition());
  }

  @Test
  void setOfThreeReportsAllThree() {
    List<MismatchRecord> suspects = detect(Arrays.asList(
        file("fg1_0-1-0_1005.parquet", 10L), file("fg1_0-2-0_1005.parquet", 11L),
        file("fg1_0-3-0_1005.parquet", 12L)));

    assertEquals(3, suspects.size());
    assertEquals(3, suspects.get(0).getDuplicateSetSize());
  }

  /**
   * Two sets in one partition are sized independently. A detector that counted the partition's
   * duplicates rather than each slice's would report 5 for both, and every consumer deciding how
   * much of a file group is at stake would be reading the wrong number.
   */
  @Test
  void twoDuplicateSetsInOnePartitionAreSizedIndependently() {
    List<MismatchRecord> suspects = detect(Arrays.asList(
        file("fg1_0-1-0_1005.parquet", 10L), file("fg1_0-2-0_1005.parquet", 11L),
        file("fg2_0-1-0_1005.parquet", 12L), file("fg2_0-2-0_1005.parquet", 13L),
        file("fg2_0-3-0_1005.parquet", 14L)));

    assertEquals(5, suspects.size());
    assertEquals(5, failingCount(suspects), "every member of both sets fails the run");
    assertEquals(5L, countOf(suspects, MismatchPattern.DANGLING_FILE_SUSPECTS),
        "the per-pattern count covers both sets");
    for (MismatchRecord record : suspects) {
      assertEquals("fg1".equals(record.getFileGroupId()) ? 2 : 3, record.getDuplicateSetSize(),
          "each slice is sized on its own members: " + record.getFileName());
    }
  }

  @Test
  void differentInstantsForOneFileGroupAreDifferentSlicesNotDuplicates() {
    assertTrue(detect(Arrays.asList(file("fg1_0-1-0_1005.parquet", 10L),
        file("fg1_0-1-0_1006.parquet", 10L))).isEmpty());
  }

  @Test
  void everyMemberOfALargeDuplicateSetIsReported() {
    List<StoragePathInfo> files = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      files.add(file("fg1_0-" + i + "-0_1005.parquet", 10L));
    }
    List<MismatchRecord> suspects = DuplicateBaseFileDetector.detectSuspects(files, PARTITION);

    assertEquals(5, suspects.size());
  }

  @Test
  void anUnpartitionedTableIsHandled() {
    List<MismatchRecord> suspects =
        DuplicateBaseFileDetector.detectSuspects(ONE_DUPLICATE_PAIR, "");

    assertEquals(2, suspects.size());
    assertEquals("", suspects.get(0).getPartition());
  }

  /**
   * Two runs over an unchanged table must produce byte-identical reports: the duplicate set is
   * emitted in file-name order, not listing order.
   */
  @Test
  void duplicateSetMembersAreEmittedInFileNameOrder() {
    List<StoragePathInfo> reversed = Arrays.asList(
        file("fg1_0-2-0_1005.parquet", 11L), file("fg1_0-1-0_1005.parquet", 10L));

    List<MismatchRecord> suspects = detect(reversed);

    assertEquals(Arrays.asList("fg1_0-1-0_1005.parquet", "fg1_0-2-0_1005.parquet"),
        reportedNames(suspects));
  }

  /**
   * A MOR table's log files must be invisible to this detector. A log file's name carries the base
   * commit time of its slice rather than the delta commit that wrote it, so whether it is accounted
   * for cannot be decided from a listing at all — and a record that fails validation on an
   * undecidable question would fail every MOR table in the fleet.
   */
  @Test
  void logFilesAreNotReportedAtAll() {
    List<StoragePathInfo> files = new ArrayList<>(ONE_DUPLICATE_PAIR);
    files.add(file(".fg9_1005.log.1_0-1-0", 10L));
    files.add(file(".fg9_1005.log.2_0-1-0", 10L));
    files.add(file(".hoodie_partition_metadata", 1L));

    List<MismatchRecord> suspects = detect(files);

    assertEquals(2, suspects.size(), "only the duplicate base files: " + suspects);
    assertEquals(2, failingCount(suspects),
        "every record this detector emits fails the run; there is no tolerated finding left");
    for (MismatchRecord record : suspects) {
      assertEquals(MismatchRecord.FileType.BASE, record.getFileType());
    }
  }

  /**
   * Log files and partition metadata alone, with no base file in the listing at all, hold no
   * cardinality invariant to break. Distinct from {@link #logFilesAreNotReportedAtAll}, which
   * proves log files are invisible *beside* a real duplicate; this proves they are invisible on
   * their own, so a MOR partition with no duplicate at all reports nothing rather than the smallest
   * possible non-empty report.
   */
  @Test
  void logFilesAndPartitionMetadataAreIgnored() {
    List<StoragePathInfo> files = Arrays.asList(file(".fg1_1005.log.1_0-1-0", 10L),
        file(".hoodie_partition_metadata", 1L));

    assertTrue(detect(files).isEmpty(),
        "neither a log file nor partition metadata can break a base-file cardinality invariant");
  }

  /**
   * The same claim as {@link #logFilesAndPartitionMetadataAreIgnored}, stated as the classify stage
   * reads it: a surviving suspect is what fails a run, and a MOR table's ordinary delta-commit log
   * files must never produce one.
   */
  @Test
  void partitionOfNothingButLogFilesReportsNothing() {
    List<MismatchRecord> suspects = detect(Arrays.asList(
        file(".fg1_1005.log.1_0-1-0", 10L), file(".fg2_1005.log.1_0-1-0", 10L)));

    assertTrue(suspects.isEmpty(), "a log file breaks no cardinality invariant a listing can see");
    assertEquals(0, failingCount(suspects),
        "a surviving suspect is what fails the run; a MOR table must produce none");
  }

  /**
   * A listing entry whose name is neither a base file nor a log file is skipped, not parsed. The
   * name below carries a base file's extension and nothing else a base file's name has, so it
   * reaches {@code FSUtils.isBaseFile} and is turned away by the pattern rather than by the
   * extension — the branch a detector that tested the extension alone would get wrong, and getting
   * it wrong throws out of the whole partition's detection rather than skipping one file.
   *
   * <p>This is a property of the detector, not of the validator. {@code
   * HoodieMetadataTableValidatorV2.detectDuplicateBaseFiles} does not shield callers from a
   * listing failure here — an unlistable partition fails the run as an infrastructure error
   * rather than being reported as validated, and that behaviour is covered directly by {@code
   * TestHoodieMetadataTableValidatorV2Classification#storageFaultInTheDanglingCheckFailsTheRunAsInfrastructure}.
   */
  @Test
  void nameThatIsNeitherABaseNorALogFileIsSkippedRatherThanParsed() {
    List<StoragePathInfo> files = new ArrayList<>(ONE_DUPLICATE_PAIR);
    files.add(file("recovered-data.parquet", 10L));

    List<MismatchRecord> suspects = detect(files);

    assertEquals(Arrays.asList("fg1_0-1-0_1005.parquet", "fg1_0-2-0_1005.parquet"),
        reportedNames(suspects),
        "the unparseable name contributes nothing and does not derail the rest");
    assertEquals(2, failingCount(suspects));
  }

  @Test
  void singleUnderscoreNameDoesNotJoinGeneratedBaseFileSlice() {
    List<StoragePathInfo> files = new ArrayList<>(ONE_DUPLICATE_PAIR);
    files.add(file("fg1_1005.parquet", 12L));
    AtomicLong dropped = new AtomicLong();

    assertEquals(Arrays.asList("fg1_0-1-0_1005.parquet", "fg1_0-2-0_1005.parquet"),
        reportedNames(DuplicateBaseFileDetector.detectSuspects(files, PARTITION, dropped)));
    assertEquals(1L, dropped.get());
  }
}
