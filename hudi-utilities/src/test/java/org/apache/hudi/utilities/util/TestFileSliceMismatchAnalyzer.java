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

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The bucketing rules, over hand-built slice lists.
 *
 * <p>No table and no Spark: every pattern below is decided from the two lists alone, so the test
 * says what the rule is rather than what one fixture happens to produce. The log-file patterns —
 * the block-header instant, the command-block drop, and the drop for an instant that could not be
 * read at all — need a real file to be read only in production; here they are exercised against an
 * injected {@link LogFileReader}, so no table and no Spark are needed for them
 * either.
 */
class TestFileSliceMismatchAnalyzer {

  private static final String PARTITION = "2026/08/12";

  @Test
  void agreeingListsProduceNoFindings() {
    List<FileSlice> mdt = Collections.singletonList(sliceWithBaseFile("fg1", "1000", 10L));
    List<FileSlice> fs = Collections.singletonList(sliceWithBaseFile("fg1", "1000", 10L));

    DiffTally tally = analyze(mdt, fs);

    assertTrue(tally.getRecords().isEmpty(),
        "identical listings must produce nothing: " + tally.getRecords());
    assertEquals(0, tally.getTotalCount());
  }

  @Test
  void fileGroupOnlyOnStorageIsFsExclusive() {
    List<FileSlice> mdt = Collections.emptyList();
    List<FileSlice> fs = Collections.singletonList(sliceWithBaseFile("fg1", "1000", 10L));

    DiffTally tally = analyze(mdt, fs);

    FileDiffRecord record = only(tally);
    assertEquals(DiffPattern.FS_EXCLUSIVE, record.getDiffPattern());
    assertEquals(MismatchPattern.FS_EXCLUSIVE_FILE_GROUP, record.getSourcePattern());
    assertEquals("fg1", record.getFileGroupId());
    assertEquals(MismatchRecord.FileType.BASE, record.getFileType());
    assertEquals(10L, record.getSizeBytes());
    // The instant a dangling-file check would interrogate, taken from the name.
    assertEquals("1000", record.getWritingInstant());
    assertEquals(MismatchRecord.InstantSource.FILE_NAME, record.getInstantSource());
  }

  @Test
  void fileGroupOnlyInTheMetadataTableIsMdtExclusive() {
    List<FileSlice> mdt = Collections.singletonList(sliceWithBaseFile("fg1", "1000", 10L));
    List<FileSlice> fs = Collections.emptyList();

    FileDiffRecord record = only(analyze(mdt, fs));

    assertEquals(DiffPattern.MDT_EXCLUSIVE, record.getDiffPattern());
    assertEquals(MismatchPattern.MDT_EXCLUSIVE_FILE_GROUP, record.getSourcePattern());
  }

  /**
   * The group is on both sides and they disagree about which slice is current. Both slices are
   * reported: which side is wrong is not knowable from the listings, and a consumer that only saw
   * one of them could not tell a metadata table that is behind from one that has lost a file.
   */
  @Test
  void differingBaseInstantsReportBothSlices() {
    List<FileSlice> mdt = Collections.singletonList(sliceWithBaseFile("fg1", "1000", 10L));
    List<FileSlice> fs = Collections.singletonList(sliceWithBaseFile("fg1", "1005", 20L));

    DiffTally tally = FileSliceMismatchAnalyzer.analyze(mdt, fs, () -> {
      throw new AssertionError("a group the metadata table knows must not have every slice read");
    }, PARTITION, LogFileReader.noLogFilesToRead());

    assertEquals(2, tally.getTotalCount(), "both sides' slices must be reported");
    assertEquals(1L, countBy(tally, MismatchPattern.FS_EXCLUSIVE_FILE_SLICE));
    assertEquals(1L, countBy(tally, MismatchPattern.MDT_EXCLUSIVE_FILE_SLICE));
    FileDiffRecord fsSide = recordFor(tally, MismatchPattern.FS_EXCLUSIVE_FILE_SLICE);
    assertEquals("1005", fsSide.getWritingInstant());
    assertTrue(fsSide.getDetail().contains("1000"),
        "the finding must name the other side's slice: " + fsSide.getDetail());
  }

  @Test
  void baseFileOnlyOnStorageWithinAMatchingSliceIsFsExclusive() {
    List<FileSlice> mdt = Collections.singletonList(sliceWithoutBaseFile("fg1", "1000"));
    List<FileSlice> fs = Collections.singletonList(sliceWithBaseFile("fg1", "1000", 10L));

    FileDiffRecord record = only(analyze(mdt, fs));

    assertEquals(MismatchPattern.FS_EXCLUSIVE_BASE_FILE, record.getSourcePattern());
    assertEquals(DiffPattern.FS_EXCLUSIVE, record.getDiffPattern());
  }

  @Test
  void baseFileOnlyInTheMetadataTableWithinAMatchingSliceIsMdtExclusive() {
    List<FileSlice> mdt = Collections.singletonList(sliceWithBaseFile("fg1", "1000", 10L));
    List<FileSlice> fs = Collections.singletonList(sliceWithoutBaseFile("fg1", "1000"));

    FileDiffRecord record = only(analyze(mdt, fs));

    assertEquals(MismatchPattern.MDT_EXCLUSIVE_BASE_FILE, record.getSourcePattern());
  }

  /**
   * Both sides hold a base file for one slice under different names: <b>ONE</b> record, the
   * storage-side one.
   *
   * <p>The storage half is unconditional — the listing holds a base file the metadata table does not
   * name for this slice, whatever else is true — and phase 2 settles it from the writing instant's
   * commit metadata. No {@code MDT_EXCLUSIVE_*} record is emitted, because such a record asserts
   * that storage does not hold the file and a listing comparison cannot establish that here: both
   * inputs project ONE base file per file group, so on a retry-orphan slice each side surfaces
   * whichever duplicate its own view kept and the metadata table's file is routinely still on
   * storage. Emitting it made a mitigation job rebootstrap a metadata table that was never wrong.
   *
   * <p>Nothing is lost by not emitting it: the metadata table's file name is carried in the one
   * record's DETAIL, which is what the last assertion here pins.
   */
  @Test
  void differingBaseFileNamesEmitOnlyTheStorageSideRecord() {
    List<FileSlice> mdt = Collections.singletonList(
        sliceWithBaseFile("fg1", "1000", baseFileName("fg1", "1-0-1", "1000"), 10L));
    List<FileSlice> fs = Collections.singletonList(
        sliceWithBaseFile("fg1", "1000", baseFileName("fg1", "2-0-2", "1000"), 10L));

    DiffTally tally = analyze(mdt, fs);

    assertEquals(1, tally.getTotalCount(),
        "the metadata half asserts something a listing comparison cannot establish, so it is not "
            + "emitted at all");
    FileDiffRecord record = only(tally);
    assertEquals(MismatchPattern.FS_EXCLUSIVE_BASE_FILE, record.getSourcePattern());
    assertEquals(baseFileName("fg1", "2-0-2", "1000"), record.getFileName());
    assertEquals(0, countBy(tally, MismatchPattern.MDT_EXCLUSIVE_BASE_FILE),
        "and specifically not an MDT-exclusive one, which is what a mitigation job would read as "
            + "'the metadata table lost a file' and answer with a full rebootstrap");
    assertTrue(record.getDetail().contains(baseFileName("fg1", "1-0-1", "1000")),
        "the metadata table's file must still be NAMED, or dropping its record would lose "
            + "information an operator needs: " + record.getDetail());
  }

  /**
   * The narrowness guard. Every OTHER MDT-exclusive branch's premise IS established by the listing,
   * so those records are still emitted — suppressing them too would turn a scoped fix into a
   * general blindness to metadata-table loss.
   */
  @Test
  void mdtExclusiveBaseFileWithNoStorageSideBaseFileIsStillEmitted() {
    List<FileSlice> mdt = Collections.singletonList(sliceWithBaseFile("fg1", "1000", 10L));
    List<FileSlice> fs = Collections.singletonList(sliceWithoutBaseFile("fg1", "1000"));

    FileDiffRecord record = only(analyze(mdt, fs));

    assertEquals(MismatchPattern.MDT_EXCLUSIVE_BASE_FILE, record.getSourcePattern(),
        "the storage-side slice has no base file at all, which is a genuine loss");
    assertEquals(MismatchPattern.MDT_EXCLUSIVE_FILE_GROUP,
        only(analyze(mdt, Collections.emptyList())).getSourcePattern(),
        "nor is a whole file group the storage listing does not have");
  }

  @Test
  void sameBaseFileNameWithADifferentLengthIsAnAttributeMismatch() {
    String name = baseFileName("fg1", "1-0-1", "1000");
    List<FileSlice> mdt = Collections.singletonList(sliceWithBaseFile("fg1", "1000", name, 10L));
    List<FileSlice> fs = Collections.singletonList(sliceWithBaseFile("fg1", "1000", name, 4096L));

    FileDiffRecord record = only(analyze(mdt, fs));

    assertEquals(MismatchPattern.BASE_FILE_ATTRIBUTE_MISMATCH, record.getSourcePattern());
    assertEquals(DiffPattern.BOTH_DISAGREE, record.getDiffPattern(),
        "nothing is missing here, so neither side owns the finding");
    assertEquals(4096L, record.getFsSizeBytes());
    assertEquals(10L, record.getMdtSizeBytes());
    assertNull(record.getSizeBytes(),
        "a single size would hide which side reported which length");
  }

  /**
   * A base-file name the instant cannot be parsed out of, on the {@code FS} side, is dropped from
   * the report and counted instead — not reported with an unresolved reason as it once was.
   *
   * <p>An {@code FS_EXCLUSIVE} record with no instant is exactly the population phase 2's
   * adjudicator cannot use (there is no row to ask), so keeping it would only hand that adjudicator
   * a record it has to special-case; dropping it here and counting it is the fail-visible
   * alternative to inventing a placeholder instant.
   */
  @Test
  void anUnparseableFsExclusiveBaseFileNameIsDroppedAndCounted() {
    List<FileSlice> fs = Collections.singletonList(
        sliceWithBaseFile("fg1", "1000", "not-a-hudi-file-name", 10L));

    DiffTally tally = analyze(Collections.emptyList(), fs);

    assertTrue(tally.getRecords().isEmpty(), "unattributable finding: " + tally.getRecords());
    assertEquals(Long.valueOf(1L), tally.getDropCounts().get("NAME_NOT_PARSEABLE"));
  }

  /** Order in, order out: the comparison keys by file group, so shuffled inputs agree. */
  @Test
  void theResultDoesNotDependOnInputOrder() {
    List<FileSlice> mdt = Arrays.asList(sliceWithBaseFile("fg1", "1000", 10L),
        sliceWithBaseFile("fg2", "1000", 10L), sliceWithBaseFile("fg3", "1000", 10L));
    List<FileSlice> fs = Arrays.asList(sliceWithBaseFile("fg3", "1000", 10L),
        sliceWithBaseFile("fg1", "1000", 99L));

    DiffTally tally = analyze(mdt, fs);

    assertEquals(1L, countBy(tally, MismatchPattern.MDT_EXCLUSIVE_FILE_GROUP),
        "fg2 is only in the metadata table");
    assertEquals(1L, countBy(tally, MismatchPattern.BASE_FILE_ATTRIBUTE_MISMATCH),
        "fg1 differs in length");
    assertEquals(2, tally.getTotalCount());
  }

  /**
   * A log file whose first block is a command block is never adjudicable — deleting a rollback
   * marker resurrects the data it rolled back — so it is dropped from the report and counted,
   * whatever instant its header happens to carry.
   */
  @Test
  void commandBlockLogFileIsDroppedAndCounted() {
    DiffTally tally = analyzeWith(
        logFileReaderReturning(LogFileInstantReader.Result.of(
            "100", HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK)));

    assertTrue(tally.getRecords().isEmpty());
    assertEquals(Long.valueOf(1L), tally.getDropCounts().get("COMMAND_BLOCK"));
  }

  /**
   * A log file whose instant could not be read at all — no block, an unreadable file, or a block
   * with no {@code INSTANT_TIME} header — is dropped and counted the same way a command block is,
   * but by the generic {@code !result.isResolved()} path rather than the command-block-specific
   * one; {@code UNREADABLE} stands in for all three reasons here since the three share the drop
   * logic and differ only in {@link LogFileInstantReader.Result#getReason()}.
   */
  @Test
  void anUnreadableFsExclusiveLogFileIsDroppedAndCountedByItsReason() {
    DiffTally tally = analyzeWith(
        logFileReaderReturning(LogFileInstantReader.Result.unresolved(
            MismatchRecord.InstantUnresolvedReason.UNREADABLE)));

    assertTrue(tally.getRecords().isEmpty());
    assertEquals(Long.valueOf(1L), tally.getDropCounts().get("UNREADABLE"));
  }

  @Test
  void anFsExclusiveLogFileCarriesTheBlockHeaderInstantNotTheFileNameInstant() {
    DiffTally tally = analyzeWith(
        logFileReaderReturning(LogFileInstantReader.Result.of(
            "999", HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK)));

    FileDiffRecord record = tally.getRecords().get(0);
    assertEquals("999", record.getWritingInstant());
    assertEquals(MismatchRecord.InstantSource.LOG_BLOCK_HEADER, record.getInstantSource());
  }

  /**
   * A file group the metadata table does not know is reported whole: every slice on storage, each
   * file under its own writing instant, so no slice waits for the one above it to be deleted.
   */
  @Test
  void everySliceOfAnFsExclusiveFileGroupIsReported() {
    FileSlice latest = sliceWithBaseFile("fg1", "1003", 30L);
    FileSlice middle = sliceWithBaseFile("fg1", "1002", 20L);
    FileSlice oldest = sliceWithLogFile("fg1", "1001", logFileName("fg1", "1001", "1-0-1"));

    DiffTally tally = FileSliceMismatchAnalyzer.analyze(Collections.emptyList(),
        Collections.singletonList(latest), () -> Arrays.asList(latest, middle, oldest), PARTITION,
        logFileReaderReturning(LogFileInstantReader.Result.of(
            "1001", HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK)));

    assertEquals(Arrays.asList(
            baseFileName("fg1", "1-0-1", "1003"), baseFileName("fg1", "1-0-1", "1002"),
            logFileName("fg1", "1001", "1-0-1")),
        tally.getRecords().stream().map(FileDiffRecord::getFileName).collect(Collectors.toList()));
    assertEquals(Arrays.asList("1003", "1002", "1001"),
        tally.getRecords().stream().map(FileDiffRecord::getWritingInstant)
            .collect(Collectors.toList()));
    assertTrue(tally.getRecords().stream().allMatch(record ->
            record.getSourcePattern() == MismatchPattern.FS_EXCLUSIVE_FILE_GROUP
                && record.getDiffPattern() == DiffPattern.FS_EXCLUSIVE),
        "every slice of the group is FS-exclusive: " + tally.getRecords());
    assertTrue(recordNamed(tally, baseFileName("fg1", "1-0-1", "1002")).getDetail().contains("1003"),
        "an older slice's finding names the latest slice it sits under");
    assertEquals(2, tally.getFilesSeen(),
        "the older slices' files are counted here; the latest slices are the caller's to count");
  }

  /**
   * Older slices of a group both sides know are not compared: the cleaner's retained versions are
   * on both sides, and only the latest is checked file by file.
   */
  @Test
  void olderSlicesOfAKnownFileGroupAreNotReported() {
    FileSlice latest = sliceWithBaseFile("fg1", "1003", 30L);
    FileSlice older = sliceWithBaseFile("fg1", "1002", 20L);

    DiffTally tally = FileSliceMismatchAnalyzer.analyze(Collections.singletonList(latest),
        Collections.singletonList(latest), () -> Arrays.asList(latest, older), PARTITION,
        LogFileReader.noLogFilesToRead());

    assertTrue(tally.getRecords().isEmpty(), "nothing differs: " + tally.getRecords());
    assertEquals(0, tally.getFilesSeen());
  }

  /** Every slice of the partition is fetched only when a file group is missing from the metadata table. */
  @Test
  void allSlicesAreNotFetchedWithoutAnFsExclusiveFileGroup() {
    List<FileSlice> slices = Collections.singletonList(sliceWithBaseFile("fg1", "1000", 10L));

    DiffTally tally = FileSliceMismatchAnalyzer.analyze(slices, slices, () -> {
      throw new AssertionError("every slice of the partition was fetched with no FS-exclusive group");
    }, PARTITION, LogFileReader.noLogFilesToRead());

    assertTrue(tally.getRecords().isEmpty());
  }

  /**
   * Several file groups the metadata table does not know, of different depths, are each reported
   * whole from one read of the partition, and a known group beside them is not.
   */
  @Test
  void everyFsExclusiveFileGroupIsReportedWholeFromOneRead() {
    FileSlice known = sliceWithBaseFile("fgKnown", "1003", 30L);
    FileSlice knownOlder = sliceWithBaseFile("fgKnown", "1001", 10L);
    FileSlice deepLatest = sliceWithBaseFile("fgDeep", "1003", 30L);
    FileSlice deepMiddle = sliceWithBaseFile("fgDeep", "1002", 20L);
    FileSlice deepOldest = sliceWithBaseFile("fgDeep", "1001", 10L);
    FileSlice shallow = sliceWithBaseFile("fgShallow", "1002", 20L);
    AtomicInteger reads = new AtomicInteger();

    DiffTally tally = FileSliceMismatchAnalyzer.analyze(Collections.singletonList(known),
        Arrays.asList(known, deepLatest, shallow), () -> {
          reads.incrementAndGet();
          return Arrays.asList(known, knownOlder, deepLatest, deepMiddle, deepOldest, shallow);
        }, PARTITION, LogFileReader.noLogFilesToRead());

    assertEquals(1, reads.get(), "every slice of the partition is read once, not once per group");
    assertEquals(4, tally.getRecords().size(), "fgDeep's three and fgShallow's one: "
        + tally.getRecords());
    assertTrue(tally.getRecords().stream().noneMatch(record ->
            record.getFileName().startsWith("fgKnown")),
        "the known group's older slice is not compared: " + tally.getRecords());
    assertEquals(3L, tally.getRecords().stream()
        .filter(record -> record.getFileName().startsWith("fgDeep")).count());
  }

  /**
   * A latest slice with no files, as a pending write leaves it once its uncommitted files are
   * filtered, adds no record and does not stop the older slices beneath it being reported.
   */
  @Test
  void anEmptyLatestSliceOfAnFsExclusiveFileGroupDoesNotHideTheOlderSlices() {
    FileSlice emptyLatest = sliceWithoutBaseFile("fg1", "1003");
    FileSlice older = sliceWithBaseFile("fg1", "1002", 20L);

    DiffTally tally = FileSliceMismatchAnalyzer.analyze(Collections.emptyList(),
        Collections.singletonList(emptyLatest), () -> Arrays.asList(emptyLatest, older), PARTITION,
        LogFileReader.noLogFilesToRead());

    assertEquals(Collections.singletonList(baseFileName("fg1", "1-0-1", "1002")),
        tally.getRecords().stream().map(FileDiffRecord::getFileName).collect(Collectors.toList()));
    assertEquals(1, tally.getFilesSeen());
  }

  private static DiffTally analyze(List<FileSlice> mdt, List<FileSlice> fs) {
    return FileSliceMismatchAnalyzer.analyze(mdt, fs, () -> fs, PARTITION,
        LogFileReader.noLogFilesToRead());
  }

  /** A single FS-exclusive log file, read through whatever {@code logFileReader} is handed. */
  private static DiffTally analyzeWith(LogFileReader logFileReader) {
    List<FileSlice> fs = Collections.singletonList(
        sliceWithLogFile("fg1", "1000", logFileName("fg1", "1000", "1-0-1")));
    return FileSliceMismatchAnalyzer.analyze(Collections.emptyList(), fs, () -> fs, PARTITION,
        logFileReader);
  }

  private static LogFileReader logFileReaderReturning(
      LogFileInstantReader.Result result) {
    return (partitionPath, fileName) -> result;
  }

  private static FileDiffRecord only(DiffTally tally) {
    assertEquals(1, tally.getRecords().size(),
        "expected exactly one finding, got: " + tally.getRecords());
    return tally.getRecords().get(0);
  }

  private static long countBy(DiffTally tally, MismatchPattern pattern) {
    return tally.getRecords().stream()
        .filter(record -> record.getSourcePattern() == pattern).count();
  }

  private static FileDiffRecord recordFor(DiffTally tally, MismatchPattern pattern) {
    List<FileDiffRecord> matching = tally.getRecords().stream()
        .filter(record -> record.getSourcePattern() == pattern).collect(Collectors.toList());
    assertEquals(1, matching.size(), "expected one " + pattern + ", got: " + tally.getRecords());
    return matching.get(0);
  }

  private static FileDiffRecord recordNamed(DiffTally tally, String fileName) {
    List<FileDiffRecord> matching = tally.getRecords().stream()
        .filter(record -> record.getFileName().equals(fileName)).collect(Collectors.toList());
    assertEquals(1, matching.size(), "expected one " + fileName + ", got: " + tally.getRecords());
    return matching.get(0);
  }

  private static String baseFileName(String fileId, String writeToken, String instant) {
    return fileId + "_" + writeToken + "_" + instant + ".parquet";
  }

  private static String logFileName(String fileId, String baseInstant, String writeToken) {
    return "." + fileId + "_" + baseInstant + ".log.1_" + writeToken;
  }

  private static FileSlice sliceWithBaseFile(String fileId, String instant, long size) {
    return sliceWithBaseFile(fileId, instant, baseFileName(fileId, "1-0-1", instant), size);
  }

  private static FileSlice sliceWithBaseFile(String fileId, String instant, String fileName,
                                             long size) {
    FileSlice slice = sliceWithoutBaseFile(fileId, instant);
    StoragePath path = new StoragePath("file:///tmp/table/" + PARTITION + "/" + fileName);
    // The explicit-id constructor: the parsing one refuses a name it cannot parse, and the
    // unparseable-name case is exactly what one of these fixtures needs to build.
    slice.setBaseFile(new HoodieBaseFile(
        new StoragePathInfo(path, size, false, (short) 0, 0L, 0L), fileId, instant, null));
    return slice;
  }

  private static FileSlice sliceWithLogFile(String fileId, String instant, String logFileName) {
    FileSlice slice = sliceWithoutBaseFile(fileId, instant);
    StoragePath path = new StoragePath("file:///tmp/table/" + PARTITION + "/" + logFileName);
    slice.addLogFile(new HoodieLogFile(path));
    return slice;
  }

  private static FileSlice sliceWithoutBaseFile(String fileId, String instant) {
    return new FileSlice(new HoodieFileGroupId(PARTITION, fileId), instant);
  }
}
