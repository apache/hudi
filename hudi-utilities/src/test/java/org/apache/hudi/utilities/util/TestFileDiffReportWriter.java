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
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What one run leaves on storage under {@code fs_mdt_diff/}, read back rather than asserted from
 * the objects that wrote them.
 */
class TestFileDiffReportWriter {

  @TempDir
  Path tempDir;

  private static final String TIMESTAMP = "20260815120000";
  private static final String OUTPUT_DIR =
      HoodieMetadataTableValidator.Config.DEFAULT_VALIDATION_OUTPUT_DIR;
  private static final String TABLE = "s3://bucket/tbl";

  @Test
  void diffHoldsTheFullPopulationAndAdjudicatedExcludesSanctioned() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    List<FileDiffRecord> records = Arrays.asList(
        record("fg1", Verdict.SANCTIONED, "explained by a completed instant"),
        record("fg2", Verdict.NOT_SETTLED, "the archive could not resolve the instant"),
        record("fg3", Verdict.DANGLING, "no commit names this file"));

    Option<String> diffDir = FileDiffReportWriter.writeDiff(storage, root, OUTPUT_DIR, TIMESTAMP,
        TABLE, "exhaustive", 1, records, 0L, noDrops(), 0L);
    Option<String> adjudicatedDir = FileDiffReportWriter.writeAdjudicated(storage, root, OUTPUT_DIR,
        TIMESTAMP, TABLE, "exhaustive", 1, records, 0L, noDrops(), 0L);

    assertTrue(diffDir.isPresent());
    assertTrue(diffDir.get().endsWith(FileDiffReportWriter.DIFF_DIR));
    assertTrue(adjudicatedDir.isPresent());
    assertTrue(adjudicatedDir.get().endsWith(FileDiffReportWriter.ADJUDICATED_DIR));

    FileDiffReportWriter.Document diff = FileDiffReportWriter.read(storage, diffDir.get());
    assertEquals(3, diff.getRecordCount(), "diff/ holds the full population, sanctioned included");
    assertTrue(hasVerdict(diff, Verdict.SANCTIONED), "diff/ must carry the sanctioned record");
    assertTrue(hasVerdict(diff, Verdict.NOT_SETTLED));
    assertTrue(hasVerdict(diff, Verdict.DANGLING));

    FileDiffReportWriter.Document adjudicated = FileDiffReportWriter.read(storage, adjudicatedDir.get());
    assertEquals(2, adjudicated.getRecordCount(), "adjudicated/ excludes the sanctioned record");
    assertFalse(hasVerdict(adjudicated, Verdict.SANCTIONED),
        "nobody in adjudicated/ may be SANCTIONED: that verdict is not a finding");
    assertTrue(hasVerdict(adjudicated, Verdict.NOT_SETTLED),
        "NOT_SETTLED survives into adjudicated/: 'nobody could tell' is the finding");
    assertTrue(hasVerdict(adjudicated, Verdict.DANGLING));
  }

  /**
   * The header carries the counts that say what could not be determined. A report that silently
   * dropped these would read as a clean table.
   */
  @Test
  void headerCountsSurviveTheRoundTrip() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    List<FileDiffRecord> records =
        Collections.singletonList(record("fg1", Verdict.NOT_SETTLED, "no row named this state pair"));

    Option<String> written = FileDiffReportWriter.writeDiff(storage, root, OUTPUT_DIR, TIMESTAMP,
        TABLE, "exhaustive", 2, records, 500L,
        Collections.singletonMap("UNRESOLVED_INSTANT", 2L), 7L);

    FileDiffReportWriter.Document document = FileDiffReportWriter.read(storage, written.get());
    assertEquals(2, document.getPartitionsValidated());
    assertEquals(500L, document.getFilesSeen(), "filesSeen is the mitigation job's denominator");
    assertEquals(2L, document.getDropCounts().get("UNRESOLVED_INSTANT"));
    assertEquals(1L, document.getNotSettledCount());
    assertEquals(7L, document.getUnmatchedStateCount(),
        "the adjudicator's unmatched-state counter must survive into the report");
  }

  /** Every record reaches diff/, and the header count describes all of them. */
  @Test
  void everyRecordIsCarriedIntoDiff() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    List<FileDiffRecord> records = Arrays.asList(
        record("fg1", Verdict.DANGLING, "d1"),
        record("fg2", Verdict.DANGLING, "d2"),
        record("fg3", Verdict.DANGLING, "d3"));

    Option<String> written = FileDiffReportWriter.writeDiff(storage, root, OUTPUT_DIR, TIMESTAMP,
        TABLE, "exhaustive", 2, records, 0L, noDrops(), 0L);

    FileDiffReportWriter.Document document = FileDiffReportWriter.read(storage, written.get());
    assertEquals(3, document.getRecords().size());
    assertEquals(3, document.getRecordCount());
  }

  /**
   * A run where every file was dropped in phase 1 (Task 10: an unattributable record is excluded
   * from adjudication and counted via {@link DiffTally#countDrop}, never carried forward as a
   * record) has {@code recordCount == 0} with a non-empty {@code dropCounts}. That must still be
   * written: {@code recordCount == 0} alone is not "nothing to report" when drops (or an
   * unmatched-state signal) exist, or a run that dropped every file for a reason still reads as a
   * clean table.
   */
  @Test
  void dropsOnlyStillWritesTheReportSoCountsSurvive() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    List<FileDiffRecord> records = Collections.emptyList();
    Map<String, Long> drops = Collections.singletonMap("UNRESOLVED_INSTANT", 2L);

    Option<String> diffDir = FileDiffReportWriter.writeDiff(storage, root, OUTPUT_DIR, TIMESTAMP,
        TABLE, "exhaustive", 1, records, 42L, drops, 0L);
    assertTrue(diffDir.isPresent(),
        "a run that only dropped files, and found no records, must still report what it dropped");
    FileDiffReportWriter.Document diff = FileDiffReportWriter.read(storage, diffDir.get());
    assertEquals(0, diff.getRecordCount());
    assertEquals(42L, diff.getFilesSeen());
    assertEquals(2L, diff.getDropCounts().get("UNRESOLVED_INSTANT"));

    Option<String> adjudicatedDir = FileDiffReportWriter.writeAdjudicated(storage, root, OUTPUT_DIR,
        TIMESTAMP, TABLE, "exhaustive", 1, records, 42L, drops, 0L);
    assertTrue(adjudicatedDir.isPresent(), "adjudicated/ must also surface drop-only counts");
    FileDiffReportWriter.Document adjudicated =
        FileDiffReportWriter.read(storage, adjudicatedDir.get());
    assertEquals(0, adjudicated.getRecordCount());
    assertEquals(42L, adjudicated.getFilesSeen());
    assertEquals(2L, adjudicated.getDropCounts().get("UNRESOLVED_INSTANT"));
  }

  /**
   * The same rule applies to an unmatched-state signal on its own: a design gap in the adjudication
   * table must not be silently dropped just because it produced no records and no phase-1 drops.
   */
  @Test
  void unmatchedStateSignalAloneStillWritesTheReport() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    Option<String> written = FileDiffReportWriter.writeDiff(storage, root, OUTPUT_DIR, TIMESTAMP,
        TABLE, "exhaustive", 1, Collections.emptyList(), 0L, noDrops(), 3L);

    assertTrue(written.isPresent(), "an unmatched-state signal must not be silently dropped");
    FileDiffReportWriter.Document document = FileDiffReportWriter.read(storage, written.get());
    assertEquals(3L, document.getUnmatchedStateCount());
  }

  /**
   * {@code filesSeen} is independent of both {@code add()} and {@code countDrop()}: a run that
   * listed a large population and found nothing still has a {@code filesSeen} tally the blast-radius
   * ceiling needs. "We examined N files and found nothing" must not read the same on disk as "we do
   * not know whether anything ran".
   */
  @Test
  void filesSeenOnlyStillWritesTheReport() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    Option<String> diffWritten = FileDiffReportWriter.writeDiff(storage, root, OUTPUT_DIR, TIMESTAMP,
        TABLE, "exhaustive", 1, Collections.emptyList(), 1000L, noDrops(), 0L);
    assertTrue(diffWritten.isPresent(),
        "a run that examined files and found nothing must still report how many it examined");
    assertEquals(1000L, FileDiffReportWriter.read(storage, diffWritten.get()).getFilesSeen());

    Option<String> adjudicatedWritten = FileDiffReportWriter.writeAdjudicated(storage, root,
        OUTPUT_DIR, TIMESTAMP, TABLE, "exhaustive", 1, Collections.emptyList(), 1000L, noDrops(),
        0L);
    assertTrue(adjudicatedWritten.isPresent(), "adjudicated/ must also surface a files-seen-only run");
    assertEquals(1000L, FileDiffReportWriter.read(storage, adjudicatedWritten.get()).getFilesSeen());
  }

  /**
   * A run that found nothing leaves nothing behind, in either directory — a fleet validating on a
   * schedule would otherwise accumulate an empty directory per table per run.
   */
  @Test
  void noRecordsWritesNothingInEitherDirectory() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    assertFalse(FileDiffReportWriter.writeDiff(storage, root, OUTPUT_DIR, TIMESTAMP, TABLE,
        "exhaustive", 1, Collections.emptyList(), 0L, noDrops(), 0L).isPresent());
    assertFalse(FileDiffReportWriter.writeAdjudicated(storage, root, OUTPUT_DIR, TIMESTAMP, TABLE,
        "exhaustive", 1, Collections.emptyList(), 0L, noDrops(), 0L).isPresent());
    assertFalse(storage.exists(new StoragePath(root, OUTPUT_DIR)), "not even the directory");
  }

  /**
   * All sanctioned records means adjudicated/ has nothing to publish, even though diff/ does.
   */
  @Test
  void allSanctionedWritesDiffButNotAdjudicated() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    List<FileDiffRecord> records =
        Collections.singletonList(record("fg1", Verdict.SANCTIONED, "explained"));

    assertTrue(FileDiffReportWriter.writeDiff(storage, root, OUTPUT_DIR, TIMESTAMP, TABLE,
        "exhaustive", 1, records, 0L, noDrops(), 0L).isPresent());
    assertFalse(FileDiffReportWriter.writeAdjudicated(storage, root, OUTPUT_DIR, TIMESTAMP, TABLE,
        "exhaustive", 1, records, 0L, noDrops(), 0L).isPresent(),
        "every record was sanctioned, so adjudicated/ has nothing to say");
  }

  /**
   * The property that stands in for "a consumer checks {@code _COMPLETE}". The marker is written
   * last and a failure to write it must fail the whole write, exactly as {@code MismatchReportWriter}
   * behaves — see its javadoc for why.
   */
  @Test
  void markerThatCannotBeWrittenFailsTheWholeWrite() {
    HoodieStorage storage = storageRefusingToCreate(FileDiffReportWriter.COMPLETE_MARKER);
    String root = tempDir.toUri().toString();
    List<FileDiffRecord> records =
        Collections.singletonList(record("fg1", Verdict.DANGLING, "no commit names this file"));

    assertThrows(IOException.class,
        () -> FileDiffReportWriter.writeDiff(storage, root, OUTPUT_DIR, TIMESTAMP, TABLE,
            "exhaustive", 1, records, 0L, noDrops(), 0L),
        "a marker that could not be written must not leave a directory behind that reads as finished");
  }

  private static Map<String, Long> noDrops() {
    return Collections.emptyMap();
  }

  private HoodieStorage storage() {
    return new HoodieHadoopStorage(tempDir.toUri().toString(), new Configuration());
  }

  private HoodieStorage storageRefusingToCreate(String fileName) {
    return new HoodieHadoopStorage(tempDir.toUri().toString(), new Configuration()) {
      @Override
      public OutputStream create(StoragePath path, boolean overwrite) throws IOException {
        if (fileName.equals(path.getName())) {
          throw new IOException("cannot write " + path);
        }
        return super.create(path, overwrite);
      }
    };
  }

  private static boolean hasVerdict(FileDiffReportWriter.Document document, Verdict verdict) {
    return document.getRecords().stream().anyMatch(r -> r.getVerdict() == verdict);
  }

  private static FileDiffRecord record(String fileGroupId, Verdict verdict, String why) {
    FileDiffRecord unadjudicated = FileDiffRecord.newBuilder()
        .withPartition("2026/08/15")
        .withFileName(fileGroupId + "_0-1-0_20260815120000.parquet")
        .withFileGroupId(fileGroupId)
        .withBaseInstantTime("20260815120000")
        .withFileType(MismatchRecord.FileType.BASE)
        .withDiffPattern(DiffPattern.FS_EXCLUSIVE)
        .withSourcePattern(MismatchPattern.FS_EXCLUSIVE_BASE_FILE)
        .withWritingInstant("20260815120000", MismatchRecord.InstantSource.FILE_NAME)
        .withDetail("only storage has it")
        .build();
    assertNull(unadjudicated.getVerdict(), "phase 1 records carry no verdict");
    return unadjudicated.withVerdict(verdict, why);
  }
}
