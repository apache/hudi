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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.utilities.HoodieMetadataTableValidator;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two dangling-dimension reports, read back off storage rather than asserted from the objects
 * that wrote them — which is also how the driver consumes them.
 */
class TestDanglingReportWriter {

  @TempDir
  Path tempDir;

  private static final String TIMESTAMP = "20260813091500";
  private static final String OUTPUT_DIR =
      HoodieMetadataTableValidator.Config.DEFAULT_VALIDATION_OUTPUT_DIR;
  private static final String TABLE = "s3://bucket/tbl";

  @Test
  void suspectsAreWrittenUnderTheirOwnLeafDirectoryAndReadBackWhole() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    List<MismatchRecord> suspects = Arrays.asList(
        suspect("fg1_0-1-0_1005.parquet", MismatchRecord.SuspectReason.DUPLICATE_BASE_FILE),
        suspect("fg1_0-2-0_1005.parquet", MismatchRecord.SuspectReason.DUPLICATE_BASE_FILE));

    Option<String> written = DanglingReportWriter.writeSuspects(storage, root, OUTPUT_DIR,
        TIMESTAMP, TABLE, "exhaustive", 4, suspects, 42L);

    assertTrue(written.isPresent());
    assertTrue(written.get().endsWith(DanglingReportWriter.SUSPECTS_DIR),
        "the suspects have a leaf directory of their own, beside fs_mdt_diff/: " + written.get());
    assertTrue(written.get().contains(TIMESTAMP),
        "one run's reports share a timestamp so they are found together: " + written.get());
    assertTrue(storage.exists(
        new StoragePath(new StoragePath(written.get()), MismatchReportWriter.COMPLETE_MARKER)));
    assertTrue(storage.exists(new StoragePath(new StoragePath(written.get()),
        DanglingReportWriter.SUSPECTS_FILE)), "dangling.json, not mismatches.json");

    MismatchReportWriter.Document read = DanglingReportWriter.readSuspects(storage, written.get());
    assertEquals(2, read.getRecordCount());
    assertEquals(2, read.getFailingRecordCount());
    assertEquals(4, read.getPartitionsValidated());
    assertEquals(TABLE, read.getTablePath());
    assertEquals(TIMESTAMP, read.getTimestamp());
    assertEquals("exhaustive", read.getMode());
    assertEquals(42L, read.getFilesSeen(),
        "the denominator a delete list is sized against travels with the document");
    assertEquals(2L, read.getCounts().get(MismatchPattern.DANGLING_FILE_SUSPECTS.name()));
    assertEquals(2, read.getRecords().size());
    MismatchRecord first = read.getRecords().get(0);
    assertEquals("fg1_0-1-0_1005.parquet", first.getFileName());
    assertEquals(MismatchPattern.DANGLING_FILE_SUSPECTS, first.getPattern());
    assertEquals(MismatchSide.FS, first.getSide());
    assertEquals(MismatchRecord.SuspectReason.DUPLICATE_BASE_FILE, first.getSuspectReason());
    assertEquals(MismatchRecord.InstantSource.FILE_NAME, first.getInstantSource());
    assertEquals("1005", first.getInstantTime());
    assertTrue(first.isFailsValidation());
  }

  /**
   * The read is not decoration. The driver reconciles what was <em>persisted</em>, not the objects
   * it happens to be holding, so a field that does not survive the round trip is a field the
   * adjudication silently loses.
   */
  @Test
  void everyFieldTheAdjudicationNeedsSurvivesTheRoundTrip() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    MismatchRecord suspect = MismatchRecord.newBuilder()
        .withPartition("2026/08/12")
        .withPattern(MismatchPattern.DANGLING_FILE_SUSPECTS)
        .withFileGroupId("fg7")
        .withBaseInstantTime("1000")
        .withFileName(".fg7_1000.log.1_0-9-0")
        .withFileType(MismatchRecord.FileType.LOG)
        .withSizeBytes(4096L)
        .withInstant(null, MismatchRecord.InstantSource.NOT_APPLICABLE)
        .withSuspectReason(MismatchRecord.SuspectReason.UNREFERENCED_LOG_FILE)
        .withDuplicateSet(null, null)
        .withFailsValidation(false)
        .withDetail("read on the driver")
        .build();

    Option<String> written = DanglingReportWriter.writeSuspects(storage, root, OUTPUT_DIR,
        TIMESTAMP, TABLE, "exhaustive", 1, Collections.singletonList(suspect), 1L);

    MismatchRecord read = DanglingReportWriter.readSuspects(storage, written.get())
        .getRecords().get(0);
    // The four the reconciliation asks its questions on: where, which group, which file, and
    // whether an instant was ever established.
    assertEquals("2026/08/12", read.getPartition());
    assertEquals("fg7", read.getFileGroupId());
    assertEquals(".fg7_1000.log.1_0-9-0", read.getFileName());
    assertEquals(MismatchRecord.InstantSource.NOT_APPLICABLE, read.getInstantSource());
    assertEquals(MismatchRecord.FileType.LOG, read.getFileType());
    assertEquals(MismatchRecord.SuspectReason.UNREFERENCED_LOG_FILE, read.getSuspectReason());
    assertEquals("1000", read.getBaseInstantTime());
    assertEquals(4096L, read.getSizeBytes());
    assertFalse(read.isFailsValidation());
    assertEquals("read on the driver", read.getDetail());
  }

  /**
   * A run that found no suspects leaves nothing behind, not even the directory — a fleet on a
   * schedule would otherwise accumulate one per table per run.
   */
  @Test
  void noSuspectsWritesNothing() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();

    Option<String> written = DanglingReportWriter.writeSuspects(storage, root, OUTPUT_DIR,
        TIMESTAMP, TABLE, "exhaustive", 4, Collections.emptyList(), 120L);

    assertFalse(written.isPresent());
    assertFalse(storage.exists(new StoragePath(root, OUTPUT_DIR)), "not even the directory");
  }

  /** Nothing survived reconciliation, so there is no answer to publish. */
  @Test
  void anEmptyAdjudicatedDocumentWritesNothing() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();

    assertFalse(DanglingReportWriter.writeAdjudicated(storage, root, OUTPUT_DIR, TIMESTAMP,
        MismatchReportWriter.documentOf(TABLE, TIMESTAMP, "exhaustive", 4, 0L,
            Collections.emptyList())).isPresent());
    assertFalse(DanglingReportWriter.writeAdjudicated(storage, root, OUTPUT_DIR, TIMESTAMP, null)
        .isPresent(), "a reconciliation that produced nothing is not a reason to throw");
    assertFalse(storage.exists(new StoragePath(root, OUTPUT_DIR)));
  }

  @Test
  void adjudicatedIsWrittenUnderItsOwnLeafDirectoryAndReadBack() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    MismatchRecord dangling = MismatchRecord.newBuilder()
        .withPartition("2026/08/12")
        .withPattern(MismatchPattern.DANGLING_FILES)
        .withFileGroupId("fg1")
        .withFileName("fg1_0-2-0_1005.parquet")
        .withFileType(MismatchRecord.FileType.BASE)
        .withInstant("1005", MismatchRecord.InstantSource.FILE_NAME)
        .withSuspectReason(MismatchRecord.SuspectReason.DUPLICATE_BASE_FILE)
        .withFailsValidation(true)
        .withDetail("proven unreferenced")
        .build();

    Option<String> written = DanglingReportWriter.writeAdjudicated(storage, root, OUTPUT_DIR,
        TIMESTAMP, MismatchReportWriter.documentOf(TABLE, TIMESTAMP, "exhaustive", 4, 0L,
            Arrays.asList(dangling)));

    assertTrue(written.get().endsWith(DanglingReportWriter.ADJUDICATED_DIR));
    MismatchReportWriter.Document read =
        DanglingReportWriter.readAdjudicated(storage, written.get());
    assertEquals(1, read.getRecordCount());
    assertEquals(1L, read.getCounts().get(MismatchPattern.DANGLING_FILES.name()));
    assertEquals(MismatchPattern.DANGLING_FILES, read.getRecords().get(0).getPattern());
    assertEquals("fg1_0-2-0_1005.parquet", read.getRecords().get(0).getFileName());
  }

  /**
   * Both of this dimension's reports sit under one {@code <root>/<dir>/<timestamp>/}, in two
   * different leaf directories. A consumer finds the adjudicated answer beside the evidence it was
   * drawn from, and reads both with one reader.
   */
  @Test
  void bothReportsShareOneRunDirectoryAndDifferOnlyInTheLeaf() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    List<MismatchRecord> suspects = Collections.singletonList(
        suspect("fg1_0-1-0_1005.parquet", MismatchRecord.SuspectReason.DUPLICATE_BASE_FILE));

    String suspectDir = DanglingReportWriter.writeSuspects(storage, root, OUTPUT_DIR, TIMESTAMP,
        TABLE, "exhaustive", 4, suspects, 9L).get();
    String adjudicatedDir = DanglingReportWriter.writeAdjudicated(storage, root, OUTPUT_DIR,
        TIMESTAMP, MismatchReportWriter.documentOf(TABLE, TIMESTAMP, "exhaustive", 4, 9L,
            suspects)).get();

    StoragePath runDir = new StoragePath(suspectDir).getParent();
    assertEquals(runDir, new StoragePath(adjudicatedDir).getParent());
    assertEquals(TIMESTAMP, runDir.getName());
    assertEquals(2, storage.listDirectEntries(runDir).size(),
        "two leaf directories, one run: " + storage.listDirectEntries(runDir));
    // Read back through the schema both share, from two different files.
    assertEquals(1, DanglingReportWriter.readSuspects(storage, suspectDir).getRecordCount());
    assertEquals(1, DanglingReportWriter.readAdjudicated(storage, adjudicatedDir).getRecordCount());
  }

  /** Every suspect a partition found reaches the written document. */
  @Test
  void everySuspectIsWritten() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    List<MismatchRecord> suspects = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      suspects.add(suspect("fg1_0-" + i + "-0_1005.parquet",
          MismatchRecord.SuspectReason.DUPLICATE_BASE_FILE));
    }

    Option<String> written = DanglingReportWriter.writeSuspects(storage, root, OUTPUT_DIR,
        TIMESTAMP, TABLE, "exhaustive", 1, suspects, 5L);

    MismatchReportWriter.Document read = DanglingReportWriter.readSuspects(storage, written.get());
    assertEquals(5, read.getRecords().size());
    assertEquals(5, read.getRecordCount());
    assertEquals(5L, read.getCounts().get(MismatchPattern.DANGLING_FILE_SUSPECTS.name()));
  }

  private HoodieStorage storage() {
    return new HoodieHadoopStorage(tempDir.toUri().toString(), new Configuration());
  }

  private static MismatchRecord suspect(String fileName, MismatchRecord.SuspectReason reason) {
    return MismatchRecord.newBuilder()
        .withPartition("2026/08/12")
        .withPattern(reason == null
            ? MismatchPattern.FS_EXCLUSIVE_BASE_FILE : MismatchPattern.DANGLING_FILE_SUSPECTS)
        .withFileGroupId(fileName.split("_")[0])
        .withBaseInstantTime("1005")
        .withFileName(fileName)
        .withFileType(MismatchRecord.FileType.BASE)
        .withSizeBytes(10L)
        .withInstant("1005", MismatchRecord.InstantSource.FILE_NAME)
        .withSuspectReason(reason)
        .withDuplicateSet(reason == null ? null : 2, null)
        .withFailsValidation(true)
        .withDetail("file slice holds 2 base files")
        .build();
  }
}
