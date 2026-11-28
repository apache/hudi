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

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormatWriter;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The search running against real v6 archive files on real storage.
 *
 * <p>The unit tests drive the scheduler with synthetic files, which is the only way to produce a 3G
 * file or a corrupt block on demand. What they cannot prove is how the real scanner reads genuine
 * log files, so that is checked here.
 */
class TestHoodieArchiveInstantSearch {

  @TempDir
  Path tempDir;

  private HoodieTableMetaClient metaClient;
  private HoodieArchiveFileCatalog catalog;
  private HoodieArchiveFileScanner scanner;

  @BeforeEach
  void setUp() throws Exception {
    metaClient = HoodieTestUtils.init(
        tempDir.toAbsolutePath().toString(), HoodieTableType.COPY_ON_WRITE, HoodieTableVersion.SIX);
    catalog = new HoodieArchiveFileCatalog(metaClient);
    scanner = new HoodieArchiveFileScanner(metaClient);
  }

  /**
   * A file's ends do not bound what it holds: a batch written before the archiver sorted it holds
   * its cleans ahead of its commits. So the real scanner never offers a range to skip a file on.
   */
  @Test
  void probeNeverOffersARangeToSkipAFileOn() throws Exception {
    writeArchiveFile(1, Arrays.asList(
        Arrays.asList("00100", "00110", "00120"),
        Arrays.asList("00150", "00160", "00170")));

    assertFalse(scanner.probe(onlyFile()).isPresent());
  }

  /**
   * An instant a file holds outside the span of its first and last records is still found, in every
   * file, however the search is scheduled.
   */
  @Test
  void anInstantOutsideTheSpanOfItsFilesEndsIsFound() throws Exception {
    writeArchiveFile(1, Arrays.asList(
        Collections.singletonList("00100"),
        Collections.singletonList("00050"),
        Collections.singletonList("00200")));
    writeArchiveFile(2, Arrays.asList(
        Collections.singletonList("00300"),
        Collections.singletonList("00060"),
        Collections.singletonList("00400")));

    Map<String, InstantLocation> resolved =
        new ArchiveInstantSearcher(catalog, scanner, 1, 64L * 1024L * 1024L)
            .search(Arrays.asList("00050", "00060", "00999"), (instantTime, record) -> {
            });

    Map<String, StoragePath> byVersion = filesByVersion();
    Map<String, InstantLocation> expected = new HashMap<>();
    expected.put("00050", InstantLocation.found(byVersion.get("1")));
    expected.put("00060", InstantLocation.found(byVersion.get("2")));
    expected.put("00999", InstantLocation.notFound());
    assertEquals(expected, resolved);
  }

  @Test
  void catalogListsArchiveFilesNewestFirst() throws Exception {
    writeArchiveFile(1, Collections.singletonList(Arrays.asList("00100", "00110")));
    writeArchiveFile(2, Collections.singletonList(Arrays.asList("00200", "00210")));
    writeArchiveFile(3, Collections.singletonList(Arrays.asList("00300", "00310")));

    List<ArchiveFile> files = catalog.list();

    Map<String, StoragePath> byVersion = filesByVersion();
    assertEquals(Arrays.asList(byVersion.get("3"), byVersion.get("2"), byVersion.get("1")),
        files.stream().map(ArchiveFile::path).collect(Collectors.toList()));
  }

  @Test
  void locatesInstantsAcrossRealArchiveFilesAndHandsOverTheirRecords() throws Exception {
    writeArchiveFile(1, Collections.singletonList(Arrays.asList("00100", "00110")));
    writeArchiveFile(2, Collections.singletonList(Arrays.asList("00200", "00210")));
    writeArchiveFile(3, Collections.singletonList(Arrays.asList("00300", "00310")));

    List<String> delivered = Collections.synchronizedList(new ArrayList<>());
    Map<String, InstantLocation> resolved =
        new ArchiveInstantSearcher(catalog, scanner, 4, 64L * 1024L * 1024L)
            .search(Arrays.asList("00110", "00210", "00300", "00999"),
                (instantTime, record) -> delivered.add(
                    record.get(HoodieTableMetaClient.COMMIT_TIME_KEY).toString()));

    Map<String, StoragePath> byVersion = filesByVersion();
    Map<String, InstantLocation> expected = new HashMap<>();
    expected.put("00110", InstantLocation.found(byVersion.get("1")));
    expected.put("00210", InstantLocation.found(byVersion.get("2")));
    expected.put("00300", InstantLocation.found(byVersion.get("3")));
    expected.put("00999", InstantLocation.notFound());
    assertEquals(expected, resolved);

    List<String> sorted = new ArrayList<>(delivered);
    Collections.sort(sorted);
    assertEquals(Arrays.asList("00110", "00210", "00300"), sorted);
  }

  @Test
  void anEmptyArchiveDirectoryMakesEveryTargetCleanlyAbsent() {
    Map<String, InstantLocation> resolved =
        new ArchiveInstantSearcher(catalog, scanner, 2, 64L * 1024L * 1024L)
            .search(Arrays.asList("00100", "00200"), (instantTime, record) -> {
            });

    assertEquals(InstantSearchState.NOT_FOUND, resolved.get("00100").state());
    assertEquals(InstantSearchState.NOT_FOUND, resolved.get("00200").state());
  }

  /**
   * Corruption ahead of the first data block makes every instant the file could hold
   * {@code INDETERMINATE}, never absent: absence is what licenses a delete downstream.
   */
  @Test
  void corruptionBeforeTheFirstDataBlockMakesTheFilesInstantsIndeterminate() throws Exception {
    writeArchiveFile(1, Arrays.asList(
        Arrays.asList("00100", "00110"),
        Arrays.asList("00150", "00160", "00170")));
    corruptFirstBlockHeader(onlyFile());

    List<String> delivered = Collections.synchronizedList(new ArrayList<>());
    Map<String, InstantLocation> resolved =
        new ArchiveInstantSearcher(catalog, scanner, 2, 64L * 1024L * 1024L)
            .search(Arrays.asList("00100", "00170"),
                (instantTime, record) -> delivered.add(instantTime));

    assertEquals(InstantSearchState.INDETERMINATE, resolved.get("00100").state(),
        "an instant lost to the corrupt block was reported absent");
    // The reader does not resynchronise past the damage, so the later block is lost with it and
    // nothing is delivered. That costs recall, never correctness: undeterminable is not absence.
    assertEquals(Collections.emptyList(), new ArrayList<>(delivered));
    assertEquals(InstantSearchState.INDETERMINATE, resolved.get("00170").state());
  }

  /**
   * Makes the file's first block read back as a {@code HoodieCorruptBlock} while leaving the rest
   * intact, by disagreeing the block size in its header with the one in its footer, which is the
   * mismatch {@code HoodieLogFileReader.isBlockCorrupted} looks for. The reader then scans on to the
   * next magic marker, so later blocks stay readable.
   */
  private void corruptFirstBlockHeader(ArchiveFile file) throws Exception {
    Path onDisk = Paths.get(file.path().toUri().getPath());
    byte[] bytes = Files.readAllBytes(onDisk);
    int blockSizeOffset = HoodieLogFormat.MAGIC.length;
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.putLong(blockSizeOffset, buffer.getLong(blockSizeOffset) + Long.BYTES);
    Files.write(onDisk, bytes);
  }

  private ArchiveFile onlyFile() throws Exception {
    List<ArchiveFile> files = catalog.list();
    assertEquals(1, files.size(), "expected exactly one archive file");
    return files.get(0);
  }

  /** Maps the archive-file version suffix to its path, so expectations can name files by version. */
  private Map<String, StoragePath> filesByVersion() throws Exception {
    Map<String, StoragePath> byVersion = new HashMap<>();
    for (ArchiveFile file : catalog.list()) {
      String name = file.path().getName();
      String rest = name.substring(".commits_.archive.".length());
      int end = 0;
      while (end < rest.length() && Character.isDigit(rest.charAt(end))) {
        end++;
      }
      byVersion.put(rest.substring(0, end), file.path());
    }
    return byVersion;
  }

  /**
   * Writes one {@code .commits_.archive.<version>} file, one avro data block per supplied group --
   * the same shape {@code TimelineArchiverV1.writeToFile} produces, which batches instants into
   * blocks.
   */
  private void writeArchiveFile(int version, List<List<String>> blocks) throws Exception {
    try (HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder()
        .withParentPath(metaClient.getArchivePath())
        .withInstantTime("")
        .withLogFileId("commits")
        .withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION)
        .withLogVersion(version)
        .withStorage(metaClient.getStorage())
        .build()) {
      for (List<String> instantTimes : blocks) {
        Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
        header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA,
            HoodieArchivedMetaEntry.getClassSchema().toString());
        List<HoodieRecord> records = new ArrayList<>();
        for (String instantTime : instantTimes) {
          records.add(new HoodieAvroIndexedRecord(entry(instantTime)));
        }
        writer.appendBlock(new HoodieAvroDataBlock(records, header,
            metaClient.getTableConfig().getRecordKeyFieldProp()));
      }
    }
  }

  private static GenericRecord entry(String instantTime) {
    return HoodieArchivedMetaEntry.newBuilder()
        .setCommitTime(instantTime)
        .setActionType("commit")
        .setActionState("COMPLETED")
        .build();
  }
}
